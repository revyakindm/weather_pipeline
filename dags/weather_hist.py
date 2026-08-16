import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import pandas as pd

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import psycopg2

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator

import logging
log = logging.getLogger("airflow.task")

class DatabaseManage:
    def __init__(self, host, port, user, password, db_name, autocommit=False):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db_name
        )

        if autocommit:
            self.connection.autocommit = True

        self.cursor = self.connection.cursor()

    def fetch(self, query, how_many_lines='all', params=None):
        self.cursor.execute(query, params)
        columns = [desc[0] for desc in self.cursor.description]
        if how_many_lines == 'all':
            row = self.cursor.fetchall()
        else:
            row = self.cursor.fetchone()

        return row, columns

def _db():
    config = BaseHook.get_connection('weather_db')
    return DatabaseManage(
        host=config.host,
        port=config.port,
        user=config.login,
        password=config.password,
        db_name=config.schema,
        autocommit=True
    )

def _minio():
    minio_login = Variable.get('MINIO_ACCESS_KEY')
    minio_pass = Variable.get('MINIO_SECRET_KEY')
    minio_conn = boto3.client(
        's3',
        aws_access_key_id=minio_login,
        aws_secret_access_key=minio_pass,
        endpoint_url='http://minio:9000'
    )

    return minio_conn


def _get_last_loaded(city, minio_conn):
    try:
        resp = minio_conn.get_object(
            Bucket='weather-raw',
            Key=f'meta-open-meteo/{city}/last_loaded'
        )
        body = resp['Body'].read().decode('utf-8')
        return datetime.strptime(body, '%Y-%m-%d').date()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            raise

def _put_last_loaded(city, d, minio_conn):
    minio_conn.put_object(
        Bucket='weather-raw',
        Key=f'meta-open-meteo/{city}/last_loaded',
        Body=d.strftime('%Y-%m-%d').encode('utf-8')
    )

@task
def to_minio():
    minio_conn = _minio()

    cities = {  'Madrid':{'latitude':40.418407, 'longitude':-3.712746},
                'Valencia':{'latitude':39.464109, 'longitude':-0.375720},
                'Barcelona':{'latitude':41.388830, 'longitude':2.186581},
                'Salamanca':{'latitude':40.969370, 'longitude':-5.660824},
                'Gijon':{'latitude':43.540131, 'longitude':-5.667884}
               }

    log.info('Загрузка данных в minio')
    for city in cities:
        log.info(f'Город: {city}')

        last_date = _get_last_loaded(city, minio_conn)

        start = last_date + relativedelta(days=1) if last_date else datetime(2020, 1, 1).date()
        end = datetime.now().date() - relativedelta(days=1)

        max_loaded = last_date
        try:
            while start <= end:
                finish = min(start + relativedelta(years=1) - relativedelta(days=1), end)
                params = {
                    'latitude': cities[city]['latitude'],
                    'longitude': cities[city]['longitude'],
                    'start_date': start,
                    'end_date': finish,
                    'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum'
                }

                response = requests.get('https://archive-api.open-meteo.com/v1/archive', params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if 'daily' not in data:
                    raise ValueError(f'В ответе API нет ключа daily: {data}')

                df = pd.DataFrame(data['daily'])
                for key in data.keys():
                    if key not in ['daily_units', 'daily']:
                        df[key] = data[key]
                for idx, row in df.iterrows():
                    row_upd = row.drop('time').to_dict()
                    body = json.dumps(row_upd)
                    key = f'open-meteo/city={city}/dt={row["time"]}/weather.json'
                    minio_conn.put_object(Bucket='weather-raw', Key=key, Body=body.encode('utf-8'))

                max_loaded = finish
                start = start + relativedelta(years=1)

            if max_loaded and max_loaded != last_date:
                _put_last_loaded(city, max_loaded, minio_conn)
        except Exception:
            log.exception(f'Ошибка при загрузке данных в minio: %s', city)
            raise

@task
def from_minio_to_db():
    db = _db()
    config = BaseHook.get_connection('weather_db')

    try:
        spark = SparkSession.builder \
            .appName('weather') \
            .config('spark.jars.packages',
                    'org.apache.hadoop:hadoop-aws:3.3.4,'
                    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
                    'org.postgresql:postgresql:42.7.1') \
            .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
            .config('spark.hadoop.fs.s3a.access.key', Variable.get('MINIO_ACCESS_KEY')) \
            .config('spark.hadoop.fs.s3a.secret.key', Variable.get('MINIO_SECRET_KEY')) \
            .config('spark.driver.host', '127.0.0.1') \
            .config('spark.driver.bindAddress', '127.0.0.1') \
            .getOrCreate()

        df = spark.read.json('s3a://weather-raw/open-meteo/')

        df_weather_data = df.select(
            F.col('dt').cast('date'),
            'city',
            F.col('precipitation_sum').cast('double'),
            F.col('temperature_2m_max').cast('double'),
            F.col('temperature_2m_min').cast('double'),
        )

        df_city_data = df.select(
            'city',
            F.col('latitude').cast('double'),
            F.col('longitude').cast('double')
        ).dropDuplicates(['city'])

        # проверка даты последней записи
        city_and_max_date, _ = db.fetch('select city, max(dt) from ods.weather group by city')
        df_city_max_date = spark.createDataFrame(city_and_max_date, ['city', 'max_dt'])
        df_weather_data_to_db = df_weather_data.join(df_city_max_date, on='city', how='left') \
            .filter(
            F.col('max_dt').isNull() | (F.col('dt') > F.col('max_dt'))
        ) \
            .drop('max_dt')

        # проверка уже записанные города
        all_cities, _ = db.fetch('select distinct city from ods.cities', how_many_lines='all')
        all_cities_lst = [c[0] for c in all_cities if c] if all_cities else []
        df_city_data_to_db = df_city_data.filter(~F.col('city').isin(all_cities_lst)) if all_cities_lst else df_city_data

        def to_weather_db(df, table):

            return df.write\
                    .format('jdbc')\
                    .option('url', 'jdbc:postgresql://postgres:5432/weather_db')\
                    .option('dbtable', table)\
                    .option('user', config.login)\
                    .option('password', config.password)\
                    .option('driver', 'org.postgresql.Driver')\
                    .mode('append')\
                    .save()

        to_weather_db(df_weather_data_to_db, 'ods.weather')
        to_weather_db(df_city_data_to_db, 'ods.cities')
    finally:
        db.connection.close()

@task
def start():
    pass

@task
def end():
    pass

default_args = {
    "owner": "revyakindm",
    "start_date": datetime(2026, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=45)
}

with DAG(
    dag_id='weather_hist',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:
    update_city_months_stats = BashOperator(
        task_id='update_city_months_stats',
        bash_command="dbt run --project-dir /opt/airflow/weather_dbt --profiles-dir /opt/airflow/weather_dbt"
    )
    (
        start()
        >> to_minio()
        >> from_minio_to_db()
        >> update_city_months_stats
        >> end()

    )
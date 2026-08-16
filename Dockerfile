FROM apache/airflow:3.3.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
  openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0 \
      -i https://mirrors.aliyun.com/pypi/simple \
      --trusted-host mirrors.aliyun.com

COPY requirements.txt /
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
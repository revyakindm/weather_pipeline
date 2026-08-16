{{ config(materialized='table') }}

select to_char(dt, 'FMMonth') mnth, city,
       round(avg(precipitation_sum)::numeric, 2) avg_precipitation_sum,
       round(avg(temperature_2m_max)::numeric, 2) avg_temperature_2m_max,
       round(avg(temperature_2m_min)::numeric, 2) avg_temperature_2m_min,
       round(avg(temperature_2m_max)::numeric, 2) - 5 * round(avg(precipitation_sum)::numeric, 2) comf_index
from ods.weather
group by mnth, city
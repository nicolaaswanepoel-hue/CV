select
  city,
  valid_time,
  temperature_2m::numeric as temp_c,
  precipitation::numeric  as precip_mm
from "airflow"."weather"."observation_hourly"
{{ config(materialized='table') }}
select
  city,
  date_trunc('day', valid_time) as day,
  avg(abs_temp_err)   as mae_temp_c,
  avg(abs_precip_err) as mae_precip_mm
from {{ ref('fct_hourly_errors') }}
group by 1,2

-- depends_on: {{ ref('stg_observations') }}
-- depends_on: {{ ref('stg_forecasts') }}

{{
  config(
    materialized='incremental',
    unique_key=['city','valid_time'],
    on_schema_change='append_new_columns'
  )
}}

with obs as (
  select city, valid_time, temp_c, precip_mm
  from {{ ref('stg_observations') }}
  {% if is_incremental() %}
    where valid_time > (
      select coalesce(max(valid_time), '1900-01-01'::timestamptz) from {{ this }}
    )
  {% endif %}
),
latest_fcst as (
  select *
  from (
    select
      city,
      valid_time,
      model_run,
      temp_c_forecast,
      precip_mm_forecast,
      row_number() over (
        partition by city, valid_time
        order by model_run desc
      ) as rn
    from {{ ref('stg_forecasts') }}
  ) f
  where rn = 1
)

select
  o.city,
  o.valid_time,
  o.temp_c,
  o.precip_mm,
  f.model_run,
  f.temp_c_forecast,
  f.precip_mm_forecast,
  abs(f.temp_c_forecast - o.temp_c)        as abs_temp_err,
  abs(f.precip_mm_forecast - o.precip_mm)  as abs_precip_err
from obs o
left join latest_fcst f
  on  f.city = o.city
  and f.valid_time = o.valid_time

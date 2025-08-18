select
  city,
  valid_time,
  model_run,
  temperature_2m::numeric as temp_c_forecast,
  precipitation::numeric  as precip_mm_forecast
from {{ source('weather', 'forecasts') }}

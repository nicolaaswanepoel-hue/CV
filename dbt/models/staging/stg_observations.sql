select
  city,
  valid_time,
  temperature_2m::numeric as temp_c,
  precipitation::numeric  as precip_mm
from {{ source('weather', 'observations') }}

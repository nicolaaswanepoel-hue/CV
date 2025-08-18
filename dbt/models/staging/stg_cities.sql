select city, upper(city) as city_upper
from {{ ref('cities') }}
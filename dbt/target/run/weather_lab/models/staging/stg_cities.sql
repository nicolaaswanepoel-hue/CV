
  create view "airflow"."analytics_staging"."stg_cities__dbt_tmp"
    
    
  as (
    select city, upper(city) as city_upper
from "airflow"."analytics"."cities"
  );
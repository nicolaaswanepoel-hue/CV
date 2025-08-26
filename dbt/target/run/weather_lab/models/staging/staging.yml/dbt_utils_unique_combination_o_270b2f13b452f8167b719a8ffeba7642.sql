
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        city, valid_time
    from "airflow"."analytics_staging"."stg_observations"
    group by city, valid_time
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_time
from "airflow"."analytics"."stg_forecasts"
where valid_time is null



  
  
      
    ) dbt_internal_test
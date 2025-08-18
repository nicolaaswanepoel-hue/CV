
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city
from "airflow"."analytics_mart"."fct_hourly_errors"
where city is null



  
  
      
    ) dbt_internal_test
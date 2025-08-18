
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor
from "airflow"."analytics"."stg_forecasts"
where vendor is null



  
  
      
    ) dbt_internal_test
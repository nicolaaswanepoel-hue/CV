
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select model_run
from "airflow"."analytics_staging"."stg_forecasts"
where model_run is null



  
  
      
    ) dbt_internal_test
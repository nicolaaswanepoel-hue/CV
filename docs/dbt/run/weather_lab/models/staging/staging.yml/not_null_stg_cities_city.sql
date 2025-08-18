
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city
from "airflow"."analytics_staging"."stg_cities"
where city is null



  
  
      
    ) dbt_internal_test
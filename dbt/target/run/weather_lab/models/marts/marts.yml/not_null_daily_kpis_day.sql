
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select day
from "airflow"."analytics_mart"."daily_kpis"
where day is null



  
  
      
    ) dbt_internal_test
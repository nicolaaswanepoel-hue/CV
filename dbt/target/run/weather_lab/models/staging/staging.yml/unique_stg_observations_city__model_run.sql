
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ['city', 'model_run'] as unique_field,
    count(*) as n_records

from "airflow"."analytics"."stg_observations"
where ['city', 'model_run'] is not null
group by ['city', 'model_run']
having count(*) > 1



  
  
      
    ) dbt_internal_test
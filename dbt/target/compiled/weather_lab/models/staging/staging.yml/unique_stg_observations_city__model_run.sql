
    
    

select
    ['city', 'model_run'] as unique_field,
    count(*) as n_records

from "airflow"."analytics"."stg_observations"
where ['city', 'model_run'] is not null
group by ['city', 'model_run']
having count(*) > 1



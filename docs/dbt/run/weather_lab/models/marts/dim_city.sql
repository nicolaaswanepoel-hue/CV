
  
    

  create  table "airflow"."analytics_mart"."dim_city__dbt_tmp"
  
  
    as
  
  (
    
select distinct city from "airflow"."analytics_staging"."stg_observations"
  );
  
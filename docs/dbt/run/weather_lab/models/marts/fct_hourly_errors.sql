
      
        delete from "airflow"."analytics_mart"."fct_hourly_errors" as DBT_INTERNAL_DEST
        where (city, valid_time) in (
            select distinct city, valid_time
            from "fct_hourly_errors__dbt_tmp194548073917" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airflow"."analytics_mart"."fct_hourly_errors" ("city", "valid_time", "temp_c", "precip_mm", "model_run", "temp_c_forecast", "precip_mm_forecast", "abs_temp_err", "abs_precip_err")
    (
        select "city", "valid_time", "temp_c", "precip_mm", "model_run", "temp_c_forecast", "precip_mm_forecast", "abs_temp_err", "abs_precip_err"
        from "fct_hourly_errors__dbt_tmp194548073917"
    )
  
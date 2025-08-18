{{ config(materialized='table') }}
select distinct city from {{ ref('stg_observations') }}

 {{ config(materialized='table') }}
 
  select 
  year
  ,month
  ,day
  ,fuel_code
  ,service_station_name
  ,price
  from  {{ ref('stg_fuel_price' )}}
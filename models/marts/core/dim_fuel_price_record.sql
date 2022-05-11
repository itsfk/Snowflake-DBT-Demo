 {{ config(materialized='table') }}
 
  select 
   new_date
  ,fuel_code
  ,service_station_name
  ,price
  from  {{ ref('stg_fuel_price' )}}
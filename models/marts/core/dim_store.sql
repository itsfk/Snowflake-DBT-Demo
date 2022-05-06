 {{ config(materialized='table') }}
 
  select 
    distinct(service_station_name) as service_station_name
  ,postcode
  ,suburb
  ,brand
  from  {{ ref('stg_fuel_price' )}}

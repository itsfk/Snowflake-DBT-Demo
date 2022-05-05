{{ config(materialized='view') }}

  select year(to_date(price_updated_date)) as year
  ,month(to_date(price_updated_date)) as month
  ,day(to_date(price_updated_date)) as day
  ,fuel_code
  ,postcode
  ,suburb
  ,brand
  ,service_station_name
  ,price
  from FIVETRAN_DATABASE.FUEL_PRICE.fuel_price
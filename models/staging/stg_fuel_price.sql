{{ config(materialized='view') }}

select 
  fuel_code
  ,postcode
  ,suburb
  ,brand
  ,service_station_name
  ,price
  ,to_date(substr(REPLACE(PRICE_UPDATED_DATE,'/','-'),1,10),'DD-MM-YYYY' )as new_date
from MODERN_DATA_STACK.S3.FUEL_PRICES 
  --from FIVETRAN_DATABASE.FUEL_PRICE.FUEL_PRICE
  where postcode is not null
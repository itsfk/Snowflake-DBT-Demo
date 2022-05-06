{{ config(materialized='view') }}


with get_date as (
  select year(to_date(price_updated_date)) as year
  ,month(to_date(price_updated_date)) as month
  ,day(to_date(price_updated_date)) as day
  ,fuel_code
  ,postcode
  ,suburb
  ,brand
  ,service_station_name
  ,price
  ,count(price)over(partition by year,month,day,fuel_code,postcode,suburb,brand,service_station_name) as number_of_price_change_per_day
  ,row_number() over(partition by  year,month,day,fuel_code,postcode,suburb,brand,service_station_name order by price) as price_rank_per_day
  ,round(avg(price) over(partition by  year,month,day,fuel_code,postcode,suburb,brand,service_station_name ),2) as avg_daily_price
  from FIVETRAN_DATABASE.FUEL_PRICE.fuel_price
  ),avg_month as (
  select year
  ,month
  ,day
  ,fuel_code
  ,postcode
  ,suburb
  ,brand
  ,service_station_name
 -- ,price
 -- ,number_of_price_change_per_day
 --,price_rank_per_day
  ,avg_daily_price
  ,round(avg(avg_daily_price) over(partition by year,month,fuel_code,postcode,suburb,brand,service_station_name ),2) as avg_monthly_price  
  from get_date
   where price_rank_per_day = 1
   
  ),pre_price as (
  select *
  ,lag(avg_daily_price,1)over (partition by fuel_code,postcode,suburb,brand,service_station_name order by year,month,day,fuel_code,postcode,suburb,brand,service_station_name) 
    as pre_day_price
  ,avg_daily_price - lag(avg_daily_price,1)over (partition by fuel_code,postcode,suburb,brand,service_station_name order by year,month,day,fuel_code,postcode,suburb,brand,service_station_name) 
    as diff_vs_pre_day_price
  from avg_month
  ),increse_rate as (
  
  select * 
  ,round(diff_vs_pre_day_price/pre_day_price * 100,2) as increase_rate
  from pre_price
  )
  
  select * from increse_rate 
  

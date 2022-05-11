{{ config(materialized='table') }}

with get_record as (

    select * from {{ ref('dim_fuel_price_record' )}}

), get_store as (

    select * from {{ ref('dim_store' )}}
), get_together as (

    select get_record.*
    ,get_store.postcode
    ,get_store.suburb
    ,get_store.brand

from get_record
left join get_store using (service_station_name)
), get_avg as (
    select *
    ,count(price)over(partition by new_date,fuel_code,postcode,suburb,brand,service_station_name) as number_of_price_change_per_day
    ,row_number() over(partition by  new_date,fuel_code,postcode,suburb,brand,service_station_name order by price) as price_rank_per_day
    ,round(avg(price) over(partition by  new_date,fuel_code,postcode,suburb,brand,service_station_name ),2) as avg_daily_price
    from get_together
  ),avg_month as (
  select 
  new_date
  ,year(new_date) as year
  ,month(new_date) as month
  ,day(new_date) as day
  ,fuel_code
  ,to_varchar(postcode) as postcode
  ,suburb
  ,brand
  ,service_station_name
 -- ,price
 -- ,number_of_price_change_per_day
 --,price_rank_per_day
  ,avg_daily_price
  ,round(avg(avg_daily_price) over(partition by year,month,fuel_code,postcode,suburb,brand,service_station_name ),2) as avg_monthly_price  
  from get_avg
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
  ), change_null as (
   select * 
  ,case when increase_rate IS NULL then 0 else increase_rate end AS increase_rate_with_no_null
  from increse_rate )

  select * from change_null 



  

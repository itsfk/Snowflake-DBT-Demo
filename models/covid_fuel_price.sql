 {{ config(materialized='table') }}

 with get_covid as (

    select * from {{ ref('fct_covid' )}}
 ),get_fct_fuel as (
    select * from {{ ref('fct_fuel_price')}}
 )
 select 
  get_covid.DATE_TESTED
 ,get_fct_fuel.postcode
 ,get_fct_fuel.suburb
 ,get_fct_fuel.brand
 ,get_fct_fuel.service_station_name
,get_fct_fuel.fuel_code
 ,get_fct_fuel.avg_daily_price
 ,get_fct_fuel.avg_monthly_price
  ,get_fct_fuel.increase_rate
 --,get_fct_fuel.increase_rate_with_no_null
 ,get_covid.number_of_cases as number_of_cases_per_day
  from get_fct_fuel 
inner join get_covid 
ON get_fct_fuel.new_date = get_covid.DATE_TESTED
 --nvl(get_fct_fuel.full_date,'1995-01-05') = nvl(get_covid.DATE_TESTED,'1995-01-05')
and  
 get_fct_fuel.postcode = get_covid.postcode
order by 2,3,4,5,6


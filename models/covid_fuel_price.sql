 {{ config(materialized='table') }}

 with get_covid as (

    select * from {{ ref('fct_covid' )}}
 ),get_fct_fuel as (
    select * from {{ ref('fct_fuel_price')}}
 ),join_together as (
     select get_fct_fuel.*
     ,get_covid.number_of_cases
    from get_fct_fuel f
    inner join get_covid c
    on f.postcode = c.postcpde and f.full_date = c.DATE_TESTED
 )

 select * from join_together
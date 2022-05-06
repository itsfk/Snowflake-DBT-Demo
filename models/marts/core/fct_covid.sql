{{ config(materialized='table') }}


with data as (

    select * from {{ ref('stg_covid' )}}

), factcovid as (
 SELECT  NOTIFICATION_DATE as DATE_TESTED
,postcode 
 ,COUNT(CONFIRMED_CASES_COUNT) as Number_of_Cases
from data 
group by NOTIFICATION_DATE,postcode
)
select * from factcovid 

{{ config(materialized='table') }}


with data as (

    select * from {{ ref('stg_covid' )}}

),no_null as (
select *
, CASE WHEN postcode = 'None' THEN 'NULL' ELSE postcode END as postcode
FROM data 
WHERE POSTCODE is Not null 

), factcovid as (
 SELECT  NOTIFICATION_DATE as DATE_TESTED
 ,COUNT(CONFIRMED_CASES_COUNT) as Number_of_Cases
from no_null
group by NOTIFICATION_DATE
)
select *  from factcovid 
{{ config(materialized='table') }}


with data as (

    select * from {{ ref('stg_covid' )}}

),

factcovid as (
 SELECT COUNT(cc.CONFIRMED_CASES_COUNT) over(partition by cc.NOTIFICATION_DATE, cc.postcode) as Number_of_Cases,cc.NOTIFICATION_DATE as Date_Tested, CASE
    WHEN cc.postcode = 'None' THEN 'NULL' 
    ELSE postcode
    END as postcode
FROM data cc
WHERE cc.POSTCODE is Not null 
)
select *  from factcovid
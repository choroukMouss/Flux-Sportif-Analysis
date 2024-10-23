

{{ config(materialized='table') }}

with source_data as (

    select *
    from {{ source('sales_data', 'products') }}

) 

select * from source_data;

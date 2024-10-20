
{{ config(materialized='table') }}

with source_data as (

    select 
    DATE(ORDER_CREATE_DATE) AS dates, 
    SITE_COUNTRY , 
    STORE_BRAND, 
    SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_SALE_PRICE AS FLOAT)) AS gross_revenue,
    SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_NET_PRICE AS FLOAT)) AS net_revenue,
    SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_SALE_PRICE AS FLOAT)) / COUNT(DISTINCT ORDER_ID) AS average_order_value,
    SUM(CAST(ITEM_QTY AS FLOAT)) AS items_sold

    FROM   {{ source('sales_data', 'transactions') }} 
    GROUP BY
        DATE(ORDER_CREATE_DATE),
        SITE_COUNTRY,
        STORE_BRAND
    ORDER BY
        dates, site_country, store_brand

) 

select * from source_data

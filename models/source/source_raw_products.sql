{{ config(alias='raw_products') }}

SELECT
    *  EXCLUDE(PRODUCT_GENDER)  , 
            CASE 
                WHEN RIGHT(UPPER(Product_gender), 1) = 'S' THEN LEFT(UPPER(Product_gender), LENGTH(Product_gender) - 1)
                WHEN UPPER(Product_gender) is null or UPPER(Product_gender) = '' THEN 'UNKNOWN'
                ELSE UPPER(Product_gender)  
            END AS Product_gender
FROM 
    {{ source('Sales', 'products') }}


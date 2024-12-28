/*
  Modèle : products_performance

  Description :
    Ce modèle calcule les performances des produits vendus en fonction de plusieurs dimensions
    (date, pays du site, marque, genre, et département du produit) et génère des métriques agrégées telles que :
    
    - Gross_revenue : Revenu brut calculé comme la somme des prix de vente des articles vendus.
    - Net_revenue : Revenu net calculé comme la somme des prix nets après application des réductions.
    - Average_value : Valeur moyenne des articles vendus, basée sur le prix net.
    - Items_sold : Nombre total d'articles vendus.

    Les résultats sont agrégés par jour (date), pays du site, marque de produit, genre du produit, et département du produit.


*/


{{ config(alias='products_performance') }}

with source_data as (

select  
    EXTRACT(DAY FROM DATE(t.ORDER_CREATE_DATE)) as date,
    p.Product_brand,
    Product_gender,
    p.Product_department,
    SUM( CAST(t.ITEM_SALE_PRICE AS FLOAT) * CAST(t.ITEM_QTY AS FLOAT)) AS gross_revenue,  
    SUM( CAST(t.ITEM_NET_PRICE AS FLOAT)  * CAST(t.ITEM_QTY AS FLOAT)) AS net_revenue, 
    AVG(t.ITEM_NET_PRICE) AS average_value, 
    SUM(t.ITEM_QTY) AS items_sold  
from  {{ ref('source_raw_transactions') }} as t
LEFT JOIN  {{ ref('source_raw_products') }} as p
on p.ITEM_ID = t.ITEM_ID
group by date,Product_brand,Product_gender,Product_department

) 

select * from source_data

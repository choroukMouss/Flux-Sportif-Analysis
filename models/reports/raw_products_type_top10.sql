/*
  Modèle : products_type_top10

  Description :

    Ce modèle identifie les 10 types de produits les plus performants en termes de revenu brut (`gross_revenue`)
    par pays et par mois. Les données sont agrégées selon les dimensions suivantes :
    
    - Date (mois) : La date de la commande, agrégée par mois.
    - Site_country : Le pays du site d'achat.
    - Product_type : Le type de produit vendu.

    La métrique calculée est :
    - Gross_revenue : Revenu brut, correspondant à la somme des prix de vente des articles vendus par type de produit.

    Seuls les 10 types de produits générant le plus de revenu brut par mois et par pays sont affichés.

*/

{{ config(alias='products_type_top10') }}

with source_data as (
    select 
        EXTRACT(MONTH FROM DATE(T.ORDER_CREATE_DATE)) AS month,
        SITE_COUNTRY,
        PRODUCT_TYPE,
        SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_SALE_PRICE AS FLOAT)) AS gross_revenue
    from 
        {{ref('source_raw_transactions')}}   as t
    join 
        {{ref('source_raw_products')}} as p
    on t.ITEM_ID = p.ITEM_ID
    group by 
        month,
        SITE_COUNTRY,
        PRODUCT_TYPE
),
ranked_products as (
    select *,
        ROW_NUMBER() OVER (PARTITION BY month, SITE_COUNTRY ORDER BY gross_revenue DESC) as rank
    from source_data
)
select 
    month,
    SITE_COUNTRY,
    PRODUCT_TYPE,
    gross_revenue
from 
    ranked_products
where 
    rank <= 10
order by 
    month,
    SITE_COUNTRY,
    gross_revenue DESC

/*
  Modèle : products_department_top10

  Description :

    Ce modèle calcule le revenu brut pour chaque département de produit par mois et par pays.
    Il ne conserve que le top 10 des départements en fonction du revenu brut par pays et par mois.

  Réalisé par : Chorouk MOUSSAOUI

*/

{{ config(alias='products_department_top10') }}

with source_data as (
    select 
        EXTRACT(MONTH FROM DATE(ORDER_CREATE_DATE)) AS month,  -- Obtenir uniquement les mois
        SITE_COUNTRY,
        PRODUCT_DEPARTMENT,
        SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_SALE_PRICE AS FLOAT)) AS gross_revenue
    from 
        {{ ref('source_raw_transactions') }} as t
    join 
        {{ ref('source_raw_products') }} as p
        on t.ITEM_ID = p.ITEM_ID
    group by 
        month, 
        SITE_COUNTRY,
        PRODUCT_DEPARTMENT
),
ranked_departments as (
    select *,
        ROW_NUMBER() OVER (PARTITION BY month, SITE_COUNTRY ORDER BY gross_revenue DESC) as rank  -- Classer par revenu brut
    from source_data
)
select 
    month,
    SITE_COUNTRY,
    PRODUCT_DEPARTMENT,
    gross_revenue
from 
    ranked_departments
where 
    rank <= 10  -- Garder seulement le top 10 des départements
order by 
    month,
    SITE_COUNTRY,
    gross_revenue DESC

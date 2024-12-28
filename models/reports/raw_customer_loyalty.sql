/*
  Modèle : customer_loyalty

  Description :

    Ce modèle évalue la fidélité des clients en calculant le nombre moyen de commandes,
    le taux de fidélité des clients ayant réalisé au moins 2 achats,
    et la valeur moyenne des revenus des clients fidèles.

*/
{{ config(alias='customer_loyalty') }}

with source_data as (
    select 
        COUNTRY as user_country,
        t.EMAIL_SHA ,
        COUNT(t.ORDER_ID) AS total_purchases,
        SUM(CAST(t.ITEM_QTY AS FLOAT) * CAST(t.ITEM_NET_PRICE AS FLOAT)) AS total_revenue
    from 
        {{ref('source_raw_users')}} as u
    join 
        {{ ref('source_raw_transactions') }}  as t
    on 
        u.EMAIL_SHA = t.EMAIL_SHA  -- Assurez-vous que la clé de jointure est correcte
    group by 
        user_country , t.EMAIL_SHA
)

select 
    user_country,
    AVG(total_purchases) AS mean_purchases,  -- Nombre moyen de commandes
    SUM(CASE WHEN total_purchases >= 2 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS loyal_rate,  -- Taux de fidélité
    AVG(CASE WHEN total_purchases >= 2 THEN total_revenue ELSE NULL END) AS loyal_mean_value  -- Valeur moyenne des clients fidèles
from 
    source_data
group by 
    user_country
order by 
    user_country

/*
  Modèle : global_performance
  Description :

    Ce modèle calcule les indicateurs de performance globaux des ventes en agrégeant les données issues de la table `transactions`
    selon plusieurs dimensions (date de la commande, pays du site, marque du magasin). Les métriques calculées incluent :

    - Gross_revenue : Revenu brut, calculé comme la somme des quantités vendues multipliées par le prix de vente unitaire.
    - Net_revenue : Revenu net, calculé comme la somme des quantités vendues multipliées par le prix net (après réductions).
    - Average_order_value : Valeur moyenne par commande, obtenue en divisant le revenu brut total par le nombre de commandes distinctes.
    - Items_sold : Quantité totale d'articles vendus.

    Les résultats sont agrégés par date, pays du site et marque du magasin, et triés selon ces dimensions.
 
  Réalisé par : Chorouk MOUSSAOUI
*/

{{ config(alias='global_performance') }}

with source_data as (

    select 
    EXTRACT(DAY FROM DATE(ORDER_CREATE_DATE)) as date,
    SITE_COUNTRY , 
    STORE_BRAND, 
    SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_SALE_PRICE AS FLOAT)) AS gross_revenue,
    SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_NET_PRICE AS FLOAT)) AS net_revenue,
    SUM(CAST(ITEM_QTY AS FLOAT) * CAST(ITEM_SALE_PRICE AS FLOAT)) / COUNT(DISTINCT ORDER_ID) AS average_order_value,
    SUM(CAST(ITEM_QTY AS FLOAT)) AS items_sold

    FROM  {{ref('source_raw_transactions')}}
    GROUP BY
        date,
        SITE_COUNTRY,
        STORE_BRAND
    ORDER BY
        date, site_country, store_brand

) 

select * from source_data

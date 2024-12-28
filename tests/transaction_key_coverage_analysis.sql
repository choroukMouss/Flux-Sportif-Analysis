/*
  Modèle : transaction_key_coverage_analysis

  Description :

    Cette requête calcule le pourcentage de couverture des utilisateurs et des produits basé sur 
    les transactions de la table source_raw_transactions, afin de déterminer si la couverture des 
    clés est à 100 %.

  Réalisé par : Chorouk MOUSSAOUI
*/

WITH coverage AS (

    SELECT 
        'Transactions' as table_source,
        (COUNT(t.EMAIL_SHA) * 100.0 / COUNT(*)) AS Users_coverage_percentage,
        (COUNT(t.ITEM_ID) * 100.0 / COUNT(*)) AS Products_coverage_percentage
    FROM 
          {{ ref('source_raw_transactions') }} t
    LEFT JOIN 
          {{ ref('source_raw_users') }}  u ON t.EMAIL_SHA = u.EMAIL_SHA
    LEFT JOIN 
          {{ ref('source_raw_products') }}  p ON t.ITEM_ID = p.ITEM_ID
    WHERE 
        u.EMAIL_SHA IS NOT NULL OR p.ITEM_ID IS NOT NULL

)

select * from coverage where Users_coverage_percentage < 100.000000
                       and Products_coverage_percentage < 100.000000

{{ config(alias='raw_transactions') }}

SELECT
    *  EXCLUDE(SHIPPING_CITY)  ,
    CASE 
        WHEN UPPER(SHIPPING_CITY) LIKE 'LONDON%' THEN 'LONDON' 
        WHEN UPPER(SHIPPING_CITY) LIKE '%VILA NOVA DE FAMALICAO%' THEN 'VILA NOVA DE FAMALICAO' 
        WHEN UPPER(SHIPPING_CITY) LIKE '%SAO DOMINGOS DE RANA%' THEN 'SAO DOMINGOS DE RANA' 
        WHEN UPPER(SHIPPING_CITY) LIKE '%MOITA%' THEN 'MOITA DO RIBATEJO' 
        WHEN UPPER(SHIPPING_CITY) LIKE '%LISBON%' THEN 'LISBON' 
        WHEN UPPER(SHIPPING_CITY) LIKE '%OEIRAS%' or UPPER(SHIPPING_CITY) LIKE 'ALGES%' THEN 'OEIRAS' --Algers c'est un village de la ville oeiras
        ELSE UPPER(SHIPPING_CITY)
    END AS SHIPPING_CITY

FROM 
    {{ source('Sales', 'transactions') }}



/*

Problème dans la colonne shipping_city :

En analysant les données, j'ai remarqué que certaines valeurs de la colonne shipping_city 
apparaissent sous plusieurs formes. Parfois, le nom de la ville est suivi du code postal ou 
d’une commune associée (par exemple, city - code postal ou city - commune). 

Cette incohérence ne respecte pas la forme normalisée de la ville seule et complique l'analyse des données par localité. 

Exemple : MOITA - codepostale 
Algés est un quartier qui fait partie de la ville Oeiras

*/
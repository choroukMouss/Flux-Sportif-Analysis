{{ config(alias='raw_users') }}

with source_data as (
    SELECT
    PRIMARY_KEY ,EMAIL_SHA, UPPER(GENDER) AS GENDER ,UPPER(INFERRED_GENDER) as INFERRED_GENDER ,UPPER(REGION) as REGION ,
            CASE 
                WHEN UPPER(COUNTRY) IN ('BE', 'BEL') THEN 'BE'  -- Normaliser pour la Belgique
                WHEN UPPER(COUNTRY) IN ('FR', 'FRA') THEN 'FR'   --  la France
                WHEN UPPER(COUNTRY) IN ('US', 'USA') THEN 'US'   --  les États-Unis
                WHEN UPPER(COUNTRY) IN ('EU', 'EUR') THEN 'EU'   -- l'Union Européenne
                WHEN UPPER(COUNTRY) IN ('PR', 'POR') THEN 'PR'   --  Portugal
                WHEN UPPER(COUNTRY) IN ('DE', 'DEU') THEN 'DE'   --  l'Allemagne
                WHEN UPPER(COUNTRY) IN ('CO', 'COU') THEN 'CO'   -- Colombie
                WHEN UPPER(COUNTRY) IN ('UK','U.K', 'UNI', 'UND') THEN 'UK'  -- Royaume-Uni
                WHEN UPPER(COUNTRY) IN ('EG', 'ENG') THEN 'EG'   -- England
                WHEN UPPER(COUNTRY) IN ('SPA', 'SN') THEN 'SN'  
                WHEN UPPER(COUNTRY) IN ('ES', 'ESP') THEN 'ES'  
                WHEN UPPER(COUNTRY) IN ('GR', 'GER') THEN 'GR'   --Greece
                WHEN UPPER(COUNTRY) IN ('IT', 'ITA') THEN 'IT'   --Italie
                WHEN UPPER(COUNTRY)  IN ('XX','OTH') OR UPPER(COUNTRY) IS NULL OR UPPER(COUNTRY) ='' OR UPPER(COUNTRY) = '0' THEN 'UNKNOWN'
                ELSE UPPER(COUNTRY) 
            END AS COUNTRY,POSTAL_CODE,
            UPPER(STATUS) as STATUS , 
            IFF(EXTRACT(YEAR from date(BIRTHDATE)) < '1900' or  EXTRACT(YEAR from date(BIRTHDATE)) > EXTRACT(YEAR FROM CURRENT_DATE)   , NULL, BIRTHDATE) as BIRTHDATE
    FROM 
        {{ source('Sales', 'users') }}
) 
select * from source_data
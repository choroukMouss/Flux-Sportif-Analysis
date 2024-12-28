SELECT 
    GENDER ,
    COUNT(t.ORDER_ID) AS total_purchases,
    SUM(CAST(t.ITEM_QTY AS FLOAT) * CAST(t.ITEM_NET_PRICE AS FLOAT)) AS total_spent
FROM 
    {{ref('source_raw_users')}} u
LEFT JOIN 
    {{ref('source_raw_transactions')}} t ON u.EMAIL_SHA = t.EMAIL_SHA
GROUP BY 
    GENDER
ORDER BY 
    total_purchases DESC
with transactions as (
    select
        SHIPPING_CITY,
        SHIPPING_POSTAL_CODE,
        SHIPPING_COUNTRY
    from {{ ref('source_raw_transactions') }}  
),

seeds as (
    select
        city,
        postal_code,
        country
    from {{ ref('city_postal_country') }}  -- Référence au fichier seed
)
, final as (

    select
        t.shipping_city,
        t.shipping_postal_code,
        t.shipping_country,
        s.city as matched_city,
        case 
            when t.shipping_city is not null and t.shipping_postal_code = s.postal_code and t.shipping_country = s.country then 'Valid'
            else 'Invalid'
        end as overall_status
    from transactions t
    left join seeds s
        on t.shipping_city LIKE '%' || s.city || '%'
        
) 

select * from final 
where overall_status = 'Invalid' 
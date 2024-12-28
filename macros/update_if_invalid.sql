{% macro update_if_invalid(seeds_table='city_postal_country' ) %}

    {% set update_query %}
    
        update {{ ref(source_raw_transactions) }} t
        set SHIPPING_CITY = (
            select s.city
            from {{ ref(seeds_table) }} s
            where t.SHIPPING_CITY LIKE '%' || s.city || '%'
            and t.SHIPPING_POSTAL_CODE = s.postal_code
            and t.SHIPPING_COUNTRY = s.country
            limit 1
        )
        where exists (
            select 1
            from {{ ref(seeds_table) }} s
            where t.SHIPPING_CITY LIKE '%' || s.city || '%'
            and t.SHIPPING_POSTAL_CODE = s.postal_code
            and t.SHIPPING_COUNTRY = s.country
        )
        -- vérifie si une ou plusieurs lignes existent dans la table seeds qui correspondent aux critères spécifiés.
    {% endset %}

    {{ run_query(update_query) }}

{% endmacro %}

SELECT
    country_name, 
    total_declined_usd
FROM {{ ref('int_globepay__declined_summary_by_country') }}
WHERE 
    total_declined_usd > 25000000
ORDER BY 
    total_declined_usd DESC
SELECT
    country_name,
    country_code,
    currency_code,
    total_declined_usd,
    total_declined_count
FROM {{ ref('int_globepay__declined_summary_by_country') }}
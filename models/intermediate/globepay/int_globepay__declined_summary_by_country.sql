WITH payments AS (
    SELECT
        *
    FROM {{ ref('int_globepay__converted_payments_and_chargebacks') }}
),

declined_summary AS (
    SELECT
        country_name,
        country_code,
        currency_code,
        -- Summing our standardized USD amount for all declined transactions
        SUM(amount_usd) AS total_declined_usd,
        COUNT(*) AS total_declined_count
    FROM payments
    WHERE
        payment_state = 'DECLINED'
    GROUP BY
        country_name,
        country_code,
        currency_code
        
)

SELECT
    country_name,
    country_code,
    currency_code,
    total_declined_usd,
    total_declined_count
FROM declined_summary
WITH exchange_rate_parsing AS (
    SELECT
        payment_id,
        payment_status,
        source,
        internal_reference_id,
        created_at,
        payment_state,
        is_cvv_provided,
        amount_local,
        country_code,
        currency_code,
        exchange_rates_json,
        -- Use PARSE_JSON and GET to dynamically pull the rate
        CAST(GET(PARSE_JSON(exchange_rates_json), currency_code) AS FLOAT) AS exchange_rate_to_usd
    FROM {{ ref('stg_globepay__acceptance') }}
),

conversion_to_usd AS (
    SELECT
        payment_id,
        payment_status,
        source,
        internal_reference_id,
        created_at,
        payment_state,
        is_cvv_provided,
        amount_local,
        country_code,
        currency_code,
        
        -- Business Logic: divide local amount by the specific rate and round to 2 decimal points
        -- We wrap the CASE in a ROUND() function for clean reporting
        ROUND(
            CASE 
                WHEN exchange_rate_to_usd IS NOT NULL AND exchange_rate_to_usd > 0 
                THEN (amount_local / exchange_rate_to_usd)
                ELSE 0 
            END, 
            2
        ) AS amount_usd

    FROM exchange_rate_parsing
)

SELECT 
    payment_id,
    payment_status,
    source,
    internal_reference_id,
    created_at,
    payment_state,
    is_cvv_provided,
    amount_local,
    country_code,
    currency_code,
    amount_usd
FROM conversion_to_usd
WITH source AS (
    SELECT 
        *
    FROM {{ source('globepay', 'globepay_acceptance_report') }}
),

renamed AS (
    SELECT
        external_ref as payment_id,
        status,
        source,
        ref as internal_reference_id,
        date_time as created_at,
        state,
        cvv_provided as is_cvv_provided,
        amount as amount_local,
        country as country_code,
        currency as currency_code,
        -- Keep rates as JSON for the intermediate layer to handle conversion
        rates as exchange_rates_json

    FROM source
)

SELECT * FROM renamed
WITH source AS (
    SELECT 
        *
    FROM {{ source('globepay', 'globepay_acceptance_report') }}
),

renamed AS (
    SELECT
        external_ref AS payment_id,
        status AS payment_status,
        source,
        ref AS internal_reference_id,
        date_time AS created_at,
        state AS payment_state,
        cvv_provided AS is_cvv_provided,
        amount AS amount_local,
        country AS country_code,
        currency AS currency_code,
        -- Keep rates as JSON for the intermediate layer to handle conversion
        rates AS exchange_rates_json

    FROM source
)

SELECT * FROM renamed
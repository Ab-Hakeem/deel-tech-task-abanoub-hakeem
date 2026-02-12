WITH source AS (
    SELECT 
        *
    FROM {{ source('globepay', 'globepay_chargeback_report') }}
),

renamed AS (
    SELECT
        external_ref as payment_id,
        status,
        source,
        chargeback
        
    FROM source
)

SELECT * FROM renamed
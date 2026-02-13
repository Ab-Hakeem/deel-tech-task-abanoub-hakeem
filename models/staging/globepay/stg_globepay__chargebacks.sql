WITH source AS (
    SELECT 
        *
    FROM {{ source('globepay', 'globepay_chargeback_report') }}
),

renamed AS (
    SELECT
        external_ref AS payment_id,
        status AS chargeback_status,
        source,
        chargeback
        
    FROM source
)

SELECT * FROM renamed
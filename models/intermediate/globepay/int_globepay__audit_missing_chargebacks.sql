WITH payments AS (
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
        exchange_rates_json
    FROM {{ ref('stg_globepay__acceptance') }}
),

chargebacks AS (
    SELECT
        payment_id,
        chargeback_status,
        source,
        chargeback
    FROM {{ ref('stg_globepay__chargebacks') }}
),

country_mapping AS (
    SELECT
        country_code,
        country_name
    FROM {{ ref('globepay_country_mapping') }}
),

-- Finding payments that do NOT have a match in the chargeback CTE
missing_from_source AS (
    SELECT
        payments.payment_id,
        payments.created_at,
        payments.payment_state,
        payments.amount_local,
        payments.country_code
    FROM payments payments
    WHERE NOT EXISTS (
        SELECT 1 
        FROM chargebacks chargebacks
        WHERE chargebacks.payment_id = payments.payment_id
    )
)

SELECT
    missing_from_source.payment_id,
    missing_from_source.created_at,
    missing_from_source.payment_state,
    missing_from_source.amount_local,
    country_mapping.country_name,
    'Audit Gap: Record present in Acceptance but missing from Chargeback report' AS audit_note
FROM missing_from_source missing_from_source
LEFT JOIN country_mapping country_mapping
    ON missing_from_source.country_code = country_mapping.country_code
WITH converted_payments AS (
    SELECT 
        converted.payment_id,
        converted.payment_status,
        converted.source,
        converted.internal_reference_id,
        converted.created_at,
        converted.payment_state,
        converted.is_cvv_provided,
        converted.amount_local,
        converted.country_code,
        converted.currency_code,
        converted.amount_usd
    FROM {{ ref('int_payments__converted') }} converted
),

chargebacks AS (
    SELECT 
        chargeback.payment_id,
        chargeback.chargeback_status,
        chargeback.source,
        chargeback.chargeback
    FROM {{ ref('stg_globepay__chargebacks') }} chargeback
),

country_lookup AS (
    SELECT
        country_code,
        country_name
    FROM {{ ref('stg_globepay__country_mapping') }} country
),

joined_payments_and_chargebacks AS (
    SELECT
        converted_payments.payment_id,
        converted_payments.payment_status,
        converted_payments.source,
        converted_payments.internal_reference_id,
        converted_payments.created_at,
        converted_payments.payment_state,
        converted_payments.is_cvv_provided,
        converted_payments.amount_local,
        converted_payments.country_code,
        COALESCE(country_lookup.country_name, 'Unknown') as country_name,
        converted_payments.currency_code,
        converted_payments.amount_usd,
        -- If a match is found in the chargeback report, use that value, else default to FALSE
        COALESCE(chargebacks.chargeback, false) AS is_chargeback,
        -- Business Logic for Question 3: 
        -- If the payment_id does not exist in the chargeback table, the record is "missing"
        CASE 
            WHEN chargebacks.payment_id IS NULL THEN true 
            ELSE false 
        END AS is_missing_chargeback_record,
        chargebacks.chargeback_status

    FROM converted_payments converted_payments
    LEFT JOIN chargebacks chargebacks 
        ON converted_payments.payment_id = chargebacks.payment_id
    LEFT JOIN country_lookup country_lookup
        ON converted_payments.country_code = country_lookup.country_code
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
    country_name,
    currency_code,
    amount_usd,
    is_chargeback,
    is_missing_chargeback_record,
    chargeback_status
FROM joined_payments_and_chargebacks
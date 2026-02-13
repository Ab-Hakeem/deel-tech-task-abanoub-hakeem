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
FROM {{ ref('int_globepay__converted_payments_and_chargebacks') }}
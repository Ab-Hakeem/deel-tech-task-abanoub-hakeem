SELECT
    payment_id,
    created_at,
    payment_state,
    amount_local,
    country_name,
    audit_note
FROM {{ ref('int_globepay__audit_missing_chargebacks') }}
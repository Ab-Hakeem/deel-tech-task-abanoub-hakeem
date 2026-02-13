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
        country_name,
        currency_code,
        amount_usd,
        is_chargeback,
        is_missing_chargeback_record,
        chargeback_status
    FROM {{ ref('int_globepay__converted_payments_and_chargebacks') }}
),

base_metrics as (
    SELECT
        created_at,
        CASE 
            WHEN payment_state = 'ACCEPTED' THEN 1 
            ELSE 0
        END AS is_accepted
    FROM payments
),

aggregations as (
    -- Daily
    SELECT 
        DATE_TRUNC('day', created_at) AS event_date, 
        'day' AS time_grain,
        COUNT(*) AS total_attempts,
        SUM(is_accepted) AS accepted_attempts
    FROM base_metrics
    GROUP BY 
        event_date,
        time_grain

    UNION ALL

    -- Weekly
    SELECT 
        DATE_TRUNC('week', created_at) AS event_date, 
        'week' AS time_grain,
        COUNT(*) AS total_attempts,
        SUM(is_accepted) AS accepted_attempts
    FROM base_metrics
    GROUP BY 
        event_date,
        time_grain

    UNION ALL

    -- Monthly
    SELECT 
        DATE_TRUNC('month', created_at) AS event_date, 
        'month' AS time_grain,
        COUNT(*) AS total_attempts,
        SUM(is_accepted) AS accepted_attempts
    FROM base_metrics 
    GROUP BY 
        event_date,
        time_grain

    UNION ALL

    -- Quarterly
    SELECT 
        DATE_TRUNC('quarter', created_at) AS event_date, 
        'quarter' AS time_grain,
        COUNT(*) AS total_attempts,
        sum(is_accepted) AS accepted_attempts
    FROM base_metrics
    GROUP BY 
        event_date,
        time_grain

    UNION ALL

    -- Yearly
    SELECT 
        DATE_TRUNC('year', created_at) AS event_date, 
        'year' AS time_grain,
        COUNT(*) AS total_attempts,
        SUM(is_accepted) AS accepted_attempts
    FROM base_metrics
    GROUP BY 
        event_date,
        time_grain
)

SELECT
    event_date,
    time_grain,
    total_attempts,
    accepted_attempts,
    ROUND((accepted_attempts * 1.0) / NULLIF(total_attempts, 0), 4) AS acceptance_rate
FROM aggregations
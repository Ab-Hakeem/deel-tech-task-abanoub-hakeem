SELECT
    event_date,
    time_grain,
    total_attempts,
    accepted_attempts,
    acceptance_rate
FROM {{ ref('int_globepay__agg_acceptance_trends') }}
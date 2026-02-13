WITH source AS (
    SELECT 
        *
    FROM {{ source('globepay', 'globepay_country_mapping') }}
),

renamed AS (
    SELECT
        country_code,
        country_name

    FROM source
)

SELECT * FROM renamed
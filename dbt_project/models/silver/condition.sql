{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ source('bronze', 'condition') }}
WHERE code_display IS NOT NULL

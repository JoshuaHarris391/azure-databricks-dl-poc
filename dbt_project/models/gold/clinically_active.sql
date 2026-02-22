{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ ref('condition') }}
WHERE code_display IS NOT NULL
  AND clinical_status = 'active'

{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT
    id,
    sha2(family_name, 256) AS family_name_hashed,
    sha2(given_names, 256) AS given_names_hashed,
    birth_date
FROM {{ source('bronze', 'patient') }}
WHERE family_name IS NOT NULL

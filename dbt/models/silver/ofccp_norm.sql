{{ config(materialized='table') }}

-- Silver: normalized OFCCP compliance evaluations
SELECT
    contractor_name,
    {{ normalize_name('contractor_name') }} AS name_normalized,
    street AS address,
    city,
    state,
    LEFT(REGEXP_REPLACE(zip, '[^0-9]', '', 'g'), 5) AS zip5,
    naics_code,
    date_received,
    date_resolved,
    resolution_type,
    violations_found,
    amount_of_relief,
    class_members
FROM {{ ref('stg_ofccp_evaluations') }}
WHERE contractor_name IS NOT NULL

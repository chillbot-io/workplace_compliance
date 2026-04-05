{{ config(materialized='table') }}

-- Silver: normalized OFLC labor condition applications
SELECT
    employer_name,
    {{ normalize_name('employer_name') }} AS name_normalized,
    address,
    city,
    state,
    zip5,
    naics_code,
    received_date,
    decision_date,
    case_status,
    wage_offered,
    prevailing_wage,
    total_workers,
    full_time_position,
    visa_class,
    job_title,
    soc_code
FROM {{ ref('stg_oflc_disclosure') }}
WHERE employer_name IS NOT NULL

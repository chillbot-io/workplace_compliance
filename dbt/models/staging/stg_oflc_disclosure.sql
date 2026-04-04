{{ config(materialized='view') }}

-- Staging: OFLC labor condition applications (H-1B, H-2A, H-2B, etc.)
SELECT
    employer_name,
    employer_address1 AS address,
    employer_city AS city,
    employer_state AS state,
    LEFT(REGEXP_REPLACE(employer_postal_code, '[^0-9]', '', 'g'), 5) AS zip5,
    naics_code,
    CAST(received_date AS DATE) AS received_date,
    CAST(decision_date AS DATE) AS decision_date,
    case_status,
    COALESCE(CAST(wage_rate_of_pay_from AS NUMERIC), 0) AS wage_offered,
    COALESCE(CAST(prevailing_wage AS NUMERIC), 0) AS prevailing_wage,
    COALESCE(CAST(total_workers AS INTEGER), 0) AS total_workers,
    full_time_position,
    visa_class,
    job_title,
    soc_code,
    soc_title
FROM raw_oflc_disclosure
WHERE employer_name IS NOT NULL

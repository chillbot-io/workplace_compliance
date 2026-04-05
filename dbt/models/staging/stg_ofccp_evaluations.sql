{{ config(materialized='view') }}

-- Staging: OFCCP compliance evaluations of federal contractors
SELECT
    contractor_name,
    street,
    city,
    state,
    zip,
    naics_code,
    CAST(date_received AS DATE) AS date_received,
    CAST(date_resolved AS DATE) AS date_resolved,
    resolution_type,
    violations_found,
    COALESCE(CAST(amount_of_relief AS NUMERIC), 0) AS amount_of_relief,
    COALESCE(CAST(number_of_class_members AS INTEGER), 0) AS class_members
FROM raw_ofccp_evaluations
WHERE contractor_name IS NOT NULL

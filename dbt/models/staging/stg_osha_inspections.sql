-- Load raw OSHA inspections from DuckDB bronze table
SELECT
    activity_nr,
    estab_name,
    site_address,
    site_city,
    site_state,
    site_zip,
    naics_code,
    CAST(open_date AS DATE) AS open_date,
    CAST(close_case_date AS DATE) AS close_case_date,
    insp_type,
    owner_type
FROM raw_osha_inspections
WHERE estab_name IS NOT NULL

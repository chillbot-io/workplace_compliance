-- Load raw OSHA violations from DuckDB bronze table
-- v4 API column names: initial_penalty/current_penalty (not "penalty")
SELECT
    activity_nr,
    citation_id,
    viol_type,
    CAST(gravity AS INTEGER) AS gravity,
    CAST(nr_instances AS INTEGER) AS nr_instances,
    CAST(initial_penalty AS NUMERIC) AS initial_penalty,
    CAST(current_penalty AS NUMERIC) AS current_penalty,
    CAST(abate_date AS DATE) AS abate_date,
    CAST(issuance_date AS DATE) AS issuance_date
FROM raw_osha_violations

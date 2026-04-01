-- Load raw WHD compliance actions from DuckDB bronze table
SELECT
    case_id,
    trade_nm,
    legal_name,
    street_addr_1_txt,
    city_nm,
    st_cd,
    zip_cd,
    naics_code_description,
    CAST(findings_start_date AS DATE) AS findings_start_date,
    CAST(findings_end_date AS DATE) AS findings_end_date,
    CAST(bw_amt AS NUMERIC) AS bw_amt,
    CAST(ee_violtd_cnt AS INTEGER) AS ee_violtd_cnt
FROM raw_whd_actions
WHERE trade_nm IS NOT NULL OR legal_name IS NOT NULL

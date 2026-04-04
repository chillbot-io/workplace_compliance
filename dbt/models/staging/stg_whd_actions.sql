{{ config(materialized='view') }}

-- Load raw WHD compliance actions from DuckDB bronze table
-- v4 API column names differ from v2: cty_nm (not city_nm), bw_atp_amt (not bw_amt)
SELECT
    case_id,
    trade_nm,
    legal_name,
    street_addr_1_txt,
    cty_nm AS city_nm,
    st_cd,
    zip_cd,
    naics_code_description,
    CAST(findings_start_date AS DATE) AS findings_start_date,
    CAST(findings_end_date AS DATE) AS findings_end_date,
    COALESCE(CAST(bw_atp_amt AS NUMERIC), 0) AS bw_amt,
    COALESCE(CAST(ee_violtd_cnt AS INTEGER), 0) AS ee_violtd_cnt
FROM raw_whd_actions
WHERE trade_nm IS NOT NULL OR legal_name IS NOT NULL

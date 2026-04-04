-- Load raw WHD compliance actions from DuckDB bronze table
-- Returns empty result set if raw_whd_actions hasn't been loaded yet
-- (WHD ingestion is weekly, not nightly — table may not exist on first runs)
{% set table_exists = run_query("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_whd_actions'").columns[0][0] > 0 %}

{% if table_exists %}
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
{% else %}
-- Empty stub — WHD data not yet loaded
SELECT
    NULL::VARCHAR AS case_id,
    NULL::VARCHAR AS trade_nm,
    NULL::VARCHAR AS legal_name,
    NULL::VARCHAR AS street_addr_1_txt,
    NULL::VARCHAR AS city_nm,
    NULL::VARCHAR AS st_cd,
    NULL::VARCHAR AS zip_cd,
    NULL::VARCHAR AS naics_code_description,
    NULL::DATE AS findings_start_date,
    NULL::DATE AS findings_end_date,
    0::NUMERIC AS bw_amt,
    0::INTEGER AS ee_violtd_cnt
WHERE 1=0
{% endif %}

{{ config(materialized='table') }}

-- Silver: normalized WHD compliance actions

SELECT
    case_id,
    COALESCE(trade_nm, legal_name) AS employer_name,
    {{ normalize_name('COALESCE(trade_nm, legal_name)') }} AS name_normalized,
    legal_name,
    street_addr_1_txt AS address,
    city_nm AS city,
    st_cd AS state,
    LEFT(REGEXP_REPLACE(zip_cd, '[^0-9]', '', 'g'), 5) AS zip5,
    naics_code_description,
    findings_start_date,
    findings_end_date,
    COALESCE(bw_amt, 0) AS backwages,
    COALESCE(ee_violtd_cnt, 0) AS employees_violated
FROM {{ ref('stg_whd_actions') }}
WHERE COALESCE(trade_nm, legal_name) IS NOT NULL

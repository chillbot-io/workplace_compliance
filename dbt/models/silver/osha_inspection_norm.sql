{{ config(materialized='table') }}

-- Silver: normalized OSHA inspections with name cleaning
-- Joins inspections to violations for per-inspection aggregates

WITH inspections AS (
    SELECT * FROM {{ ref('stg_osha_inspections') }}
),

violations AS (
    SELECT * FROM {{ ref('stg_osha_violations') }}
),

violation_agg AS (
    SELECT
        activity_nr,
        COUNT(*) AS violation_count,
        SUM(CASE WHEN viol_type = 'W' THEN 1 ELSE 0 END) AS willful_count,
        SUM(CASE WHEN viol_type = 'R' THEN 1 ELSE 0 END) AS repeat_count,
        SUM(CASE WHEN viol_type = 'S' THEN 1 ELSE 0 END) AS serious_count,
        SUM(CASE WHEN viol_type IN ('O', 'U') THEN 1 ELSE 0 END) AS other_count,
        SUM(COALESCE(current_penalty, initial_penalty, 0)) AS total_penalties,
        AVG(gravity) AS avg_gravity
    FROM violations
    GROUP BY activity_nr
)

SELECT
    i.activity_nr,
    i.estab_name,
    {{ normalize_name('i.estab_name') }} AS name_normalized,
    i.site_address,
    i.site_city,
    i.site_state,
    LEFT(REGEXP_REPLACE(i.site_zip, '[^0-9]', '', 'g'), 5) AS zip5,
    i.naics_code,
    LEFT(i.naics_code, 4) AS naics_4digit,
    i.open_date,
    i.close_case_date,
    i.insp_type,
    pa.address_key,
    COALESCE(v.violation_count, 0) AS violation_count,
    COALESCE(v.willful_count, 0) AS willful_count,
    COALESCE(v.repeat_count, 0) AS repeat_count,
    COALESCE(v.serious_count, 0) AS serious_count,
    COALESCE(v.other_count, 0) AS other_count,
    COALESCE(v.total_penalties, 0) AS total_penalties,
    v.avg_gravity
FROM inspections i
LEFT JOIN violation_agg v ON i.activity_nr = v.activity_nr
LEFT JOIN osha_address_keys pa ON i.site_address = pa.raw_address

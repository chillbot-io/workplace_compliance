{{ config(
    materialized='table',
    unique_key='employer_id'
) }}

-- Gold: employer-level risk profile using deterministic entity resolution.
--
-- Profiles are created from ALL data sources (OSHA, WHD, MSHA), not just OSHA.
-- An employer with WHD wage violations but no OSHA inspections gets its own profile.
--
-- Matching: name_normalized + state + zip5 = one employer profile.
-- Parent company rollup is display-only (never merges profiles).

-- ══════════════════════════════════════════════════════════
-- STEP 1: Build the universe of all employer keys from all sources
-- ══════════════════════════════════════════════════════════

WITH osha AS (
    SELECT * FROM {{ ref('osha_inspection_norm') }}
    WHERE name_normalized IS NOT NULL
      AND name_normalized NOT IN ('UNKNOWNINVALID ESTABLISHMENT', 'INVALID ESTABLISHMENT',
                                   'NA', 'NONE', 'TEST', 'TBD', 'NO NAME')
      AND LENGTH(name_normalized) > 2
),

whd AS (
    SELECT * FROM {{ ref('whd_norm') }}
    WHERE name_normalized IS NOT NULL
      AND LENGTH(name_normalized) > 2
),

-- All unique employer keys from every source
all_employer_keys AS (
    SELECT DISTINCT
        name_normalized || '|' || COALESCE(site_state, '') || '|' || COALESCE(zip5, '') AS employer_key,
        name_normalized,
        site_state AS state,
        zip5
    FROM osha

    UNION

    SELECT DISTINCT
        name_normalized || '|' || COALESCE(state, '') || '|' || COALESCE(zip5, '') AS employer_key,
        name_normalized,
        state,
        zip5
    FROM whd
),

-- Generate stable employer_id UUIDs from employer_key
employer_ids AS (
    SELECT
        employer_key,
        name_normalized,
        state,
        zip5,
        CAST(
            SUBSTR(MD5(employer_key), 1, 8) || '-' ||
            SUBSTR(MD5(employer_key), 9, 4) || '-' ||
            SUBSTR(MD5(employer_key), 13, 4) || '-' ||
            SUBSTR(MD5(employer_key), 17, 4) || '-' ||
            SUBSTR(MD5(employer_key), 21, 12)
        AS VARCHAR) AS employer_id
    FROM all_employer_keys
),

-- ══════════════════════════════════════════════════════════
-- STEP 2: Aggregate each source per employer
-- ══════════════════════════════════════════════════════════

-- OSHA 5-year aggregation
osha_with_key AS (
    SELECT o.*,
        name_normalized || '|' || COALESCE(site_state, '') || '|' || COALESCE(zip5, '') AS employer_key
    FROM osha o
    WHERE open_date >= CURRENT_DATE - INTERVAL '5 years'
),
osha_agg AS (
    SELECT
        ok.employer_key,
        ARG_MIN(ok.estab_name, -EXTRACT(EPOCH FROM ok.open_date)) AS employer_name,
        ARG_MIN(ok.site_address, -EXTRACT(EPOCH FROM ok.open_date)) AS address,
        ARG_MIN(ok.site_city, -EXTRACT(EPOCH FROM ok.open_date)) AS city,
        ARG_MIN(ok.naics_code, -EXTRACT(EPOCH FROM ok.open_date)) AS naics_code,
        ARG_MIN(LEFT(ok.naics_code, 4), -EXTRACT(EPOCH FROM ok.open_date)) AS naics_4digit,
        COUNT(DISTINCT ok.activity_nr) AS osha_inspections_5yr,
        SUM(ok.violation_count) AS osha_violations_5yr,
        SUM(ok.willful_count) AS osha_willful_count_5yr,
        SUM(ok.repeat_count) AS osha_repeat_count_5yr,
        SUM(ok.serious_count) AS osha_serious_count_5yr,
        SUM(ok.other_count) AS osha_other_count_5yr,
        SUM(ok.total_penalties) AS osha_penalty_total_5yr,
        MAX(ok.open_date) AS osha_last_inspection_date,
        AVG(ok.avg_gravity) AS osha_avg_gravity
    FROM osha_with_key ok
    GROUP BY ok.employer_key
),

-- OSHA trend (1yr and 3yr)
osha_1yr AS (
    SELECT employer_key, SUM(violation_count) AS violations_1yr
    FROM osha_with_key WHERE open_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY employer_key
),
osha_3yr AS (
    SELECT employer_key, SUM(violation_count) AS violations_3yr
    FROM osha_with_key WHERE open_date >= CURRENT_DATE - INTERVAL '3 years'
    GROUP BY employer_key
),

-- WHD aggregation
whd_with_key AS (
    SELECT w.*,
        w.name_normalized || '|' || COALESCE(w.state, '') || '|' || COALESCE(w.zip5, '') AS employer_key
    FROM whd w
    WHERE w.findings_end_date >= CURRENT_DATE - INTERVAL '5 years'
),
whd_agg AS (
    SELECT
        employer_key,
        ARG_MIN(employer_name, -EXTRACT(EPOCH FROM findings_end_date)) AS whd_employer_name,
        ARG_MIN(address, -EXTRACT(EPOCH FROM findings_end_date)) AS whd_address,
        ARG_MIN(city, -EXTRACT(EPOCH FROM findings_end_date)) AS whd_city,
        COUNT(DISTINCT case_id) AS whd_cases_5yr,
        SUM(backwages) AS whd_backwages_total,
        SUM(employees_violated) AS whd_ee_violated_total
    FROM whd_with_key
    GROUP BY employer_key
),

-- MSHA aggregation (by name + state, no zip)
msha_agg AS (
    SELECT
        mk.name_normalized,
        mk.state,
        COUNT(DISTINCT mk.violation_no) AS msha_violations_5yr,
        SUM(mk.proposed_penalty) AS msha_assessed_penalties
    FROM {{ ref('msha_violation_norm') }} mk
    WHERE mk.name_normalized IS NOT NULL
      AND LENGTH(mk.name_normalized) > 1
      AND mk.violation_date >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY mk.name_normalized, mk.state
),

-- ══════════════════════════════════════════════════════════
-- STEP 3: Build unified employer profiles
-- ══════════════════════════════════════════════════════════

employer_base AS (
    SELECT
        ei.employer_id,
        ei.employer_key,
        ei.name_normalized,
        -- Use OSHA identity fields first, fall back to WHD
        COALESCE(oa.employer_name, wa.whd_employer_name) AS employer_name,
        COALESCE(oa.address, wa.whd_address) AS address,
        COALESCE(oa.city, wa.whd_city) AS city,
        ei.state,
        ei.zip5,
        oa.naics_code,
        oa.naics_4digit,
        -- OSHA fields (0 if no OSHA data)
        COALESCE(oa.osha_inspections_5yr, 0) AS osha_inspections_5yr,
        COALESCE(oa.osha_violations_5yr, 0) AS osha_violations_5yr,
        COALESCE(oa.osha_willful_count_5yr, 0) AS osha_willful_count_5yr,
        COALESCE(oa.osha_repeat_count_5yr, 0) AS osha_repeat_count_5yr,
        COALESCE(oa.osha_serious_count_5yr, 0) AS osha_serious_count_5yr,
        COALESCE(oa.osha_other_count_5yr, 0) AS osha_other_count_5yr,
        COALESCE(oa.osha_penalty_total_5yr, 0) AS osha_penalty_total_5yr,
        oa.osha_last_inspection_date,
        oa.osha_avg_gravity,
        -- WHD fields (0 if no WHD data)
        COALESCE(wa.whd_cases_5yr, 0) AS whd_cases_5yr,
        COALESCE(wa.whd_backwages_total, 0) AS whd_backwages_total,
        COALESCE(wa.whd_ee_violated_total, 0) AS whd_ee_violated_total,
        -- MSHA fields (0 if no MSHA data)
        COALESCE(msha.msha_violations_5yr, 0) AS msha_violations_5yr,
        COALESCE(msha.msha_assessed_penalties, 0) AS msha_assessed_penalties,
        -- Trend
        COALESCE(t1.violations_1yr, 0) AS violations_1yr,
        COALESCE(t3.violations_3yr, 0) AS violations_3yr
    FROM employer_ids ei
    LEFT JOIN osha_agg oa ON ei.employer_key = oa.employer_key
    LEFT JOIN whd_agg wa ON ei.employer_key = wa.employer_key
    LEFT JOIN msha_agg msha ON ei.name_normalized = msha.name_normalized AND ei.state = msha.state
    LEFT JOIN osha_1yr t1 ON ei.employer_key = t1.employer_key
    LEFT JOIN osha_3yr t3 ON ei.employer_key = t3.employer_key
),

-- ══════════════════════════════════════════════════════════
-- STEP 4: Parent company + location count + scoring
-- ══════════════════════════════════════════════════════════

parent_raw AS (
    SELECT e.employer_id, pc.parent_name,
           ROW_NUMBER() OVER (
               PARTITION BY e.employer_id
               ORDER BY LENGTH(pc.name_pattern) DESC
           ) AS rn
    FROM employer_base e
    INNER JOIN {{ ref('parent_companies') }} pc
        ON e.name_normalized LIKE pc.name_pattern || '%'
),
parent_match AS (
    SELECT employer_id, parent_name FROM parent_raw WHERE rn = 1
),

location_counts AS (
    SELECT
        e.employer_id,
        COALESCE(pm.parent_name, e.name_normalized) AS group_key,
        COUNT(*) OVER (PARTITION BY COALESCE(pm.parent_name, e.name_normalized)) AS location_count
    FROM employer_base e
    LEFT JOIN parent_match pm ON e.employer_id = pm.employer_id
)

SELECT
    e.employer_id,
    e.employer_name,
    e.name_normalized,
    e.address,
    e.city,
    e.state,
    e.zip5,
    e.naics_code,
    e.naics_4digit,

    -- OSHA
    e.osha_inspections_5yr,
    e.osha_violations_5yr,
    e.osha_willful_count_5yr,
    e.osha_repeat_count_5yr,
    e.osha_serious_count_5yr,
    e.osha_other_count_5yr,
    e.osha_penalty_total_5yr,
    e.osha_last_inspection_date,
    e.osha_avg_gravity,

    -- WHD
    e.whd_cases_5yr,
    e.whd_backwages_total,
    e.whd_ee_violated_total,

    -- MSHA
    e.msha_violations_5yr,
    e.msha_assessed_penalties,

    -- Parent + location count
    pm.parent_name,
    COALESCE(lc.location_count, 1) AS location_count,

    -- Risk tier (combines all sources)
    CASE
        WHEN e.osha_willful_count_5yr >= 1                             THEN 'HIGH'
        WHEN e.osha_repeat_count_5yr >= 3                              THEN 'HIGH'
        WHEN e.osha_penalty_total_5yr > 100000                         THEN 'HIGH'
        WHEN e.whd_backwages_total > 100000                            THEN 'HIGH'
        WHEN e.whd_ee_violated_total > 100                             THEN 'HIGH'
        WHEN e.osha_inspections_5yr >= 5
             AND e.osha_violations_5yr >= 10                           THEN 'ELEVATED'
        WHEN e.whd_cases_5yr >= 3
             AND e.whd_backwages_total > 10000                         THEN 'ELEVATED'
        WHEN e.osha_violations_5yr >= 10                               THEN 'MEDIUM'
        WHEN e.osha_inspections_5yr BETWEEN 2 AND 4                    THEN 'MEDIUM'
        WHEN e.osha_violations_5yr BETWEEN 3 AND 9                     THEN 'MEDIUM'
        WHEN e.whd_cases_5yr >= 2                                      THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,

    -- Risk score (0-100)
    LEAST(100, GREATEST(0,
        LEAST(50, e.osha_willful_count_5yr * 30)
      + LEAST(30, e.osha_repeat_count_5yr * 15)
      + LEAST(20, e.osha_serious_count_5yr * 3)
      + LEAST(5, e.osha_other_count_5yr * 0.5)
      + LEAST(15, e.osha_penalty_total_5yr / 10000.0)
      + LEAST(8, e.whd_backwages_total / 10000.0)
      + LEAST(4, e.whd_cases_5yr * 1.5)
      + LEAST(3, e.whd_ee_violated_total / 25.0)
    )) AS risk_score,

    -- Trend signal
    CASE
        WHEN e.violations_1yr > e.violations_3yr / 3.0 * 1.5
             AND e.violations_3yr >= 3                                 THEN 'WORSENING'
        WHEN e.violations_1yr < e.violations_3yr / 3.0 * 0.5
             AND e.violations_3yr >= 3                                 THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS trend_signal,

    -- Confidence tier
    CASE
        WHEN e.osha_inspections_5yr >= 3 THEN 'HIGH'
        WHEN e.osha_inspections_5yr >= 1 OR e.whd_cases_5yr >= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence_tier,

    -- SVEP flag
    CASE
        WHEN e.osha_willful_count_5yr >= 1 THEN true
        WHEN e.osha_repeat_count_5yr >= 1 THEN true
        WHEN e.osha_serious_count_5yr >= 3
             AND e.osha_penalty_total_5yr > 50000 THEN true
        ELSE false
    END AS svep_flag,

    -- NAICS description
    n.naics_title AS naics_description

FROM employer_base e
LEFT JOIN {{ ref('naics_2022') }} n ON e.naics_code = n.naics_code
LEFT JOIN parent_match pm ON e.employer_id = pm.employer_id
LEFT JOIN location_counts lc ON e.employer_id = lc.employer_id

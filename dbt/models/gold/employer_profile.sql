{{ config(
    materialized='table',
    unique_key='employer_id'
) }}

-- Gold: employer-level risk profile using deterministic entity resolution.
--
-- Matching strategy (precision-first):
--   Pass 1: Group by name_normalized + state + zip5
--           → catches name variations (WALMART INC vs WAL-MART)
--   Pass 2: Within same state + zip5, merge profiles sharing same address_key
--           → catches different names at same physical location
--           (WALMART vs WALMART SUPERCENTER 5432 at same address)
--
-- Each profile = one physical establishment at one address.
-- Parent company rollup is display-only (never merges profiles).

WITH osha AS (
    SELECT * FROM {{ ref('osha_inspection_norm') }}
    WHERE name_normalized IS NOT NULL
      AND name_normalized NOT IN ('UNKNOWN', 'UNKNOWN CONTRACTOR', 'UNKNOWN EMPLOYER',
                                   'UNKNOWNINVALID ESTABLISHMENT', 'INVALID ESTABLISHMENT',
                                   'NA', 'NONE', 'TEST', 'TBD', 'NO NAME')
      AND LENGTH(name_normalized) > 2
),

-- Pass 1: Assign each inspection a location_key (name + state + zip)
osha_with_location AS (
    SELECT
        o.*,
        -- Primary grouping key: normalized name + state + zip
        name_normalized || '|' || COALESCE(site_state, '') || '|' || COALESCE(zip5, '') AS location_key,
        -- Address key for pass 2 merge (may be NULL if address parsing failed)
        address_key
    FROM osha o
),

-- Pass 2: Find cases where different location_keys share the same address_key + state + zip
-- These are different name variants at the same physical address
-- Pick the location_key with most inspections as the canonical key for each address group
address_groups AS (
    SELECT
        address_key,
        site_state,
        zip5,
        location_key,
        COUNT(*) AS inspection_count
    FROM osha_with_location
    WHERE address_key IS NOT NULL
    GROUP BY address_key, site_state, zip5, location_key
),
address_ranked AS (
    SELECT
        address_key, site_state, zip5, location_key AS canonical_key,
        ROW_NUMBER() OVER (
            PARTITION BY address_key, site_state, zip5
            ORDER BY inspection_count DESC
        ) AS rn
    FROM address_groups
),
address_canonical AS (
    SELECT address_key, site_state, zip5, canonical_key
    FROM address_ranked
    WHERE rn = 1
),
-- Map each location_key to its canonical key (if it shares an address with another key)
address_merge AS (
    SELECT DISTINCT
        ag.location_key,
        ac.canonical_key
    FROM address_groups ag
    JOIN address_canonical ac
        ON ag.address_key = ac.address_key
        AND ag.site_state = ac.site_state
        AND ag.zip5 = ac.zip5
    WHERE ag.location_key != ac.canonical_key
),

-- Build final employer_key: use canonical key from address merge, fall back to location_key
osha_with_employer AS (
    SELECT
        o.*,
        COALESCE(am.canonical_key, o.location_key) AS employer_key
    FROM osha_with_location o
    LEFT JOIN address_merge am ON o.location_key = am.location_key
),

-- Generate stable employer_id UUIDs from employer_key
-- Using MD5 hash formatted as UUID for deterministic, reproducible IDs
employer_ids AS (
    SELECT DISTINCT
        employer_key,
        CAST(
            SUBSTR(MD5(employer_key), 1, 8) || '-' ||
            SUBSTR(MD5(employer_key), 9, 4) || '-' ||
            SUBSTR(MD5(employer_key), 13, 4) || '-' ||
            SUBSTR(MD5(employer_key), 17, 4) || '-' ||
            SUBSTR(MD5(employer_key), 21, 12)
        AS VARCHAR) AS employer_id
    FROM osha_with_employer
),

-- 5-year window
osha_5yr AS (
    SELECT
        e.*,
        ei.employer_id
    FROM osha_with_employer e
    JOIN employer_ids ei ON e.employer_key = ei.employer_key
    WHERE e.open_date >= CURRENT_DATE - INTERVAL '5 years'
),

-- 1-year and 3-year for trend signal
osha_1yr AS (
    SELECT * FROM osha_5yr WHERE open_date >= CURRENT_DATE - INTERVAL '1 year'
),
osha_3yr AS (
    SELECT * FROM osha_5yr WHERE open_date >= CURRENT_DATE - INTERVAL '3 years'
),

-- Aggregate per employer_id
employer_osha AS (
    SELECT
        employer_id,
        ARG_MIN(estab_name, -EXTRACT(EPOCH FROM open_date)) AS employer_name,
        ARG_MIN(name_normalized, -EXTRACT(EPOCH FROM open_date)) AS name_normalized,
        ARG_MIN(site_address, -EXTRACT(EPOCH FROM open_date)) AS address,
        ARG_MIN(site_city, -EXTRACT(EPOCH FROM open_date)) AS city,
        ARG_MIN(site_state, -EXTRACT(EPOCH FROM open_date)) AS state,
        ARG_MIN(zip5, -EXTRACT(EPOCH FROM open_date)) AS zip5,
        ARG_MIN(naics_code, -EXTRACT(EPOCH FROM open_date)) AS naics_code,
        ARG_MIN(naics_4digit, -EXTRACT(EPOCH FROM open_date)) AS naics_4digit,
        COUNT(DISTINCT activity_nr) AS osha_inspections_5yr,
        SUM(violation_count) AS osha_violations_5yr,
        SUM(willful_count) AS osha_willful_count_5yr,
        SUM(repeat_count) AS osha_repeat_count_5yr,
        SUM(serious_count) AS osha_serious_count_5yr,
        SUM(other_count) AS osha_other_count_5yr,
        SUM(total_penalties) AS osha_penalty_total_5yr,
        MAX(open_date) AS osha_last_inspection_date,
        AVG(avg_gravity) AS osha_avg_gravity
    FROM osha_5yr
    WHERE employer_id IS NOT NULL
    GROUP BY employer_id
),

trend_1yr AS (
    SELECT employer_id, SUM(violation_count) AS violations_1yr
    FROM osha_1yr
    GROUP BY employer_id
),
trend_3yr AS (
    SELECT employer_id, SUM(violation_count) AS violations_3yr
    FROM osha_3yr
    GROUP BY employer_id
),

-- WHD: deterministic matching to OSHA profiles by name_normalized + state + zip5
-- WHD records join to the same employer_key as OSHA records at the same location
whd_with_key AS (
    SELECT
        w.*,
        w.name_normalized || '|' || COALESCE(w.state, '') || '|' || COALESCE(w.zip5, '') AS location_key
    FROM {{ ref('whd_norm') }} w
    WHERE w.name_normalized IS NOT NULL
      AND LENGTH(w.name_normalized) > 1
),
whd_with_employer AS (
    SELECT
        w.*,
        COALESCE(ei.employer_id, CAST(
            SUBSTR(MD5(w.location_key), 1, 8) || '-' ||
            SUBSTR(MD5(w.location_key), 9, 4) || '-' ||
            SUBSTR(MD5(w.location_key), 13, 4) || '-' ||
            SUBSTR(MD5(w.location_key), 17, 4) || '-' ||
            SUBSTR(MD5(w.location_key), 21, 12)
        AS VARCHAR)) AS employer_id
    FROM whd_with_key w
    LEFT JOIN employer_ids ei ON w.location_key = ei.employer_key
),
whd_agg AS (
    SELECT
        employer_id,
        COUNT(DISTINCT case_id) AS whd_cases_5yr,
        SUM(backwages) AS whd_backwages_total,
        SUM(employees_violated) AS whd_ee_violated_total
    FROM whd_with_employer
    WHERE findings_end_date >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY employer_id
),

-- MSHA: deterministic matching by name_normalized + state
-- MSHA doesn't have zip, so match on name + state only
msha_with_key AS (
    SELECT
        m.*,
        m.name_normalized || '|' || COALESCE(m.state, '') AS msha_key
    FROM {{ ref('msha_violation_norm') }} m
    WHERE m.name_normalized IS NOT NULL
      AND LENGTH(m.name_normalized) > 1
),
msha_agg AS (
    SELECT
        mk.name_normalized,
        mk.state,
        COUNT(DISTINCT mk.violation_no) AS msha_violations_5yr,
        SUM(mk.assessed_penalty) AS msha_assessed_penalties
    FROM msha_with_key mk
    WHERE mk.violation_date >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY mk.name_normalized, mk.state
),

-- OFCCP: compliance evaluations matched by name + state + zip
ofccp_agg AS (
    SELECT
        name_normalized,
        state,
        zip5,
        COUNT(*) AS ofccp_evaluations,
        SUM(CASE WHEN violations_found = 'Y' THEN 1 ELSE 0 END) > 0 AS ofccp_violations_found
    FROM {{ ref('ofccp_norm') }}
    WHERE date_resolved >= CURRENT_DATE - INTERVAL '5 years'
      AND name_normalized IS NOT NULL
      AND LENGTH(name_normalized) > 1
    GROUP BY name_normalized, state, zip5
),

-- OFLC: labor condition application counts by employer
oflc_agg AS (
    SELECT
        name_normalized,
        state,
        zip5,
        COUNT(*) AS oflc_lca_count
    FROM {{ ref('oflc_norm') }}
    WHERE received_date >= CURRENT_DATE - INTERVAL '5 years'
      AND name_normalized IS NOT NULL
      AND LENGTH(name_normalized) > 1
    GROUP BY name_normalized, state, zip5
),

-- Parent company matching (display-only, never merges profiles)
-- Curated list of ~90 national chains with prefix matching
-- Small enough that LIKE scan is instant
parent_raw AS (
    SELECT e.employer_id, pc.parent_name,
           -- Rank by longest pattern match (more specific = better)
           ROW_NUMBER() OVER (
               PARTITION BY e.employer_id
               ORDER BY LENGTH(pc.name_pattern) DESC
           ) AS rn
    FROM employer_osha e
    INNER JOIN parent_companies pc
        ON e.name_normalized LIKE pc.name_pattern || '%'
),
parent_match AS (
    SELECT employer_id, parent_name FROM parent_raw WHERE rn = 1
),

-- Location count per parent or per normalized name
location_counts AS (
    SELECT
        e.employer_id,
        COALESCE(pm.parent_name, e.name_normalized) AS group_key,
        COUNT(*) OVER (PARTITION BY COALESCE(pm.parent_name, e.name_normalized)) AS location_count
    FROM employer_osha e
    LEFT JOIN parent_match pm ON e.employer_id = pm.employer_id
)

SELECT
    e.*,

    -- WHD fields
    COALESCE(w.whd_cases_5yr, 0) AS whd_cases_5yr,
    COALESCE(w.whd_backwages_total, 0) AS whd_backwages_total,
    COALESCE(w.whd_ee_violated_total, 0) AS whd_ee_violated_total,

    -- MSHA fields (matched by name + state — MSHA doesn't have zip)
    COALESCE(msha.msha_violations_5yr, 0) AS msha_violations_5yr,
    COALESCE(msha.msha_assessed_penalties, 0) AS msha_assessed_penalties,

    -- OFCCP fields (matched by name + state + zip)
    COALESCE(ofccp.ofccp_evaluations, 0) AS ofccp_evaluations,
    COALESCE(ofccp.ofccp_violations_found, false) AS ofccp_violations_found,

    -- OFLC fields (matched by name + state + zip)
    COALESCE(oflc.oflc_lca_count, 0) AS oflc_lca_count,

    -- Parent company name (display-only)
    pm.parent_name,

    -- Related locations sharing same parent or normalized name
    COALESCE(lc.location_count, 1) AS location_count,

    -- Risk tier (combines OSHA + WHD signals)
    CASE
        WHEN COALESCE(e.osha_willful_count_5yr, 0) >= 1                    THEN 'HIGH'
        WHEN COALESCE(e.osha_repeat_count_5yr, 0) >= 3                     THEN 'HIGH'
        WHEN COALESCE(e.osha_penalty_total_5yr, 0) > 100000                THEN 'HIGH'
        WHEN COALESCE(w.whd_backwages_total, 0) > 100000                   THEN 'HIGH'
        WHEN COALESCE(w.whd_ee_violated_total, 0) > 100                    THEN 'HIGH'
        WHEN COALESCE(e.osha_inspections_5yr, 0) >= 5
             AND COALESCE(e.osha_violations_5yr, 0) >= 10                  THEN 'ELEVATED'
        WHEN COALESCE(w.whd_cases_5yr, 0) >= 3
             AND COALESCE(w.whd_backwages_total, 0) > 10000                THEN 'ELEVATED'
        WHEN COALESCE(e.osha_violations_5yr, 0) >= 10                      THEN 'MEDIUM'
        WHEN COALESCE(e.osha_inspections_5yr, 0) BETWEEN 2 AND 4           THEN 'MEDIUM'
        WHEN COALESCE(e.osha_violations_5yr, 0) BETWEEN 3 AND 9            THEN 'MEDIUM'
        WHEN COALESCE(w.whd_cases_5yr, 0) >= 2                             THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,

    -- Risk score (0-100 weighted sum, combines OSHA + WHD)
    LEAST(100, GREATEST(0,
        LEAST(50, COALESCE(e.osha_willful_count_5yr, 0) * 30)
      + LEAST(30, COALESCE(e.osha_repeat_count_5yr, 0) * 15)
      + LEAST(20, COALESCE(e.osha_serious_count_5yr, 0) * 3)
      + LEAST(5, COALESCE(e.osha_other_count_5yr, 0) * 0.5)
      + LEAST(15, COALESCE(e.osha_penalty_total_5yr, 0) / 10000.0)
      + LEAST(8, COALESCE(w.whd_backwages_total, 0) / 10000.0)
      + LEAST(4, COALESCE(w.whd_cases_5yr, 0) * 1.5)
      + LEAST(3, COALESCE(w.whd_ee_violated_total, 0) / 25.0)
    )) AS risk_score,

    -- Trend signal
    CASE
        WHEN COALESCE(t1.violations_1yr, 0) > COALESCE(t3.violations_3yr, 0) / 3.0 * 1.5
             AND COALESCE(t3.violations_3yr, 0) >= 3                       THEN 'WORSENING'
        WHEN COALESCE(t1.violations_1yr, 0) < COALESCE(t3.violations_3yr, 0) / 3.0 * 0.5
             AND COALESCE(t3.violations_3yr, 0) >= 3                       THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS trend_signal,

    -- Confidence tier
    CASE
        WHEN e.osha_inspections_5yr >= 3 THEN 'HIGH'
        WHEN e.osha_inspections_5yr >= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence_tier,

    -- SVEP flag
    CASE
        WHEN COALESCE(e.osha_willful_count_5yr, 0) >= 1 THEN true
        WHEN COALESCE(e.osha_repeat_count_5yr, 0) >= 1 THEN true
        WHEN COALESCE(e.osha_serious_count_5yr, 0) >= 3
             AND COALESCE(e.osha_penalty_total_5yr, 0) > 50000 THEN true
        ELSE false
    END AS svep_flag,

    -- NAICS description from seed
    n.naics_title AS naics_description

FROM employer_osha e
LEFT JOIN whd_agg w ON e.employer_id = w.employer_id
LEFT JOIN msha_agg msha ON e.name_normalized = msha.name_normalized AND e.state = msha.state
LEFT JOIN ofccp_agg ofccp ON e.name_normalized = ofccp.name_normalized AND e.state = ofccp.state AND e.zip5 = ofccp.zip5
LEFT JOIN oflc_agg oflc ON e.name_normalized = oflc.name_normalized AND e.state = oflc.state AND e.zip5 = oflc.zip5
LEFT JOIN trend_1yr t1 ON e.employer_id = t1.employer_id
LEFT JOIN trend_3yr t3 ON e.employer_id = t3.employer_id
LEFT JOIN {{ ref('naics_2022') }} n ON e.naics_code = n.naics_code
LEFT JOIN parent_match pm ON e.employer_id = pm.employer_id
LEFT JOIN location_counts lc ON e.employer_id = lc.employer_id

{{ config(
    materialized='table',
    unique_key='employer_id'
) }}

-- Gold: employer-level risk profile aggregated by Splink cluster.
-- Each cluster_id maps to a stable employer_id via cluster_id_mapping.
-- If entity resolution hasn't run yet, falls back to per-establishment profiles.

WITH cluster_map AS (
    -- Join Splink clusters to stable employer_id UUIDs
    SELECT
        ec.unique_id,
        ec.cluster_id,
        cm.employer_id
    FROM employer_clusters ec
    JOIN cluster_id_mapping cm
        ON CAST(ec.cluster_id AS VARCHAR) = CAST(cm.cluster_id AS VARCHAR)
),

osha AS (
    SELECT * FROM {{ ref('osha_inspection_norm') }}
),

-- Join inspections to their cluster employer_id
osha_with_employer AS (
    SELECT
        o.*,
        COALESCE(c.employer_id, CAST(o.activity_nr AS VARCHAR)) AS employer_id
    FROM osha o
    LEFT JOIN cluster_map c ON o.activity_nr = c.unique_id
),

-- 5-year window
osha_5yr AS (
    SELECT * FROM osha_with_employer
    WHERE open_date >= CURRENT_DATE - INTERVAL '5 years'
),

-- 1-year and 3-year for trend signal
osha_1yr AS (
    SELECT * FROM osha_with_employer WHERE open_date >= CURRENT_DATE - INTERVAL '1 year'
),
osha_3yr AS (
    SELECT * FROM osha_with_employer WHERE open_date >= CURRENT_DATE - INTERVAL '3 years'
),

-- Aggregate per employer_id (across all locations in the cluster)
employer_osha AS (
    SELECT
        employer_id,
        -- Use most common values for identity fields
        -- DuckDB: use ARG_MIN to get column value from the row with most recent inspection
        ARG_MIN(estab_name, -EXTRACT(EPOCH FROM open_date)) AS employer_name,
        ARG_MIN(name_normalized, -EXTRACT(EPOCH FROM open_date)) AS name_normalized,
        ARG_MIN(site_address, -EXTRACT(EPOCH FROM open_date)) AS address,
        ARG_MIN(site_city, -EXTRACT(EPOCH FROM open_date)) AS city,
        ARG_MIN(site_state, -EXTRACT(EPOCH FROM open_date)) AS state,
        ARG_MIN(zip5, -EXTRACT(EPOCH FROM open_date)) AS zip5,
        ARG_MIN(naics_code, -EXTRACT(EPOCH FROM open_date)) AS naics_code,
        ARG_MIN(naics_4digit, -EXTRACT(EPOCH FROM open_date)) AS naics_4digit,
        -- 5yr aggregates
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

-- Map employer names to parent companies via seed table
-- Uses normalized name prefix matching against known parent→subsidiary patterns
parent_match AS (
    SELECT
        e.employer_id,
        pc.parent_name
    FROM employer_osha e
    INNER JOIN {{ ref('parent_companies') }} pc
        ON e.name_normalized LIKE pc.name_pattern || '%'
),

-- Count locations per parent (if matched) or per normalized name (if not)
-- This gives "347 Walmart locations" instead of "12 WALMART STORE" locations
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

    -- Parent company name (NULL if no parent match — this is a standalone employer)
    pm.parent_name,

    -- Related locations sharing same parent or normalized name
    COALESCE(lc.location_count, 1) AS location_count,

    -- Risk tier
    CASE
        WHEN COALESCE(e.osha_willful_count_5yr, 0) >= 1                    THEN 'HIGH'
        WHEN COALESCE(e.osha_repeat_count_5yr, 0) >= 3                     THEN 'HIGH'
        WHEN COALESCE(e.osha_penalty_total_5yr, 0) > 100000                THEN 'HIGH'
        WHEN COALESCE(e.osha_inspections_5yr, 0) >= 5
             AND COALESCE(e.osha_violations_5yr, 0) >= 10                  THEN 'ELEVATED'
        WHEN COALESCE(e.osha_violations_5yr, 0) >= 10                      THEN 'MEDIUM'
        WHEN COALESCE(e.osha_inspections_5yr, 0) BETWEEN 2 AND 4           THEN 'MEDIUM'
        WHEN COALESCE(e.osha_violations_5yr, 0) BETWEEN 3 AND 9            THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,

    -- Risk score (0-100 weighted sum)
    LEAST(100, GREATEST(0,
        COALESCE(e.osha_willful_count_5yr, 0) * 25
      + COALESCE(e.osha_repeat_count_5yr, 0) * 10
      + COALESCE(e.osha_serious_count_5yr, 0) * 5
      + COALESCE(e.osha_other_count_5yr, 0) * 1
      + LEAST(20, COALESCE(e.osha_penalty_total_5yr, 0) / 10000.0)
    )) AS risk_score,

    -- Trend signal
    CASE
        WHEN COALESCE(t1.violations_1yr, 0) > COALESCE(t3.violations_3yr, 0) / 3.0 * 1.5
             AND COALESCE(t3.violations_3yr, 0) >= 3                       THEN 'WORSENING'
        WHEN COALESCE(t1.violations_1yr, 0) < COALESCE(t3.violations_3yr, 0) / 3.0 * 0.5
             AND COALESCE(t3.violations_3yr, 0) >= 3                       THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS trend_signal,

    -- Confidence tier — how confident are we in this employer's identity?
    CASE
        WHEN e.osha_inspections_5yr >= 3 THEN 'HIGH'
        WHEN e.osha_inspections_5yr >= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence_tier,

    -- SVEP flag (OSHA Severe Violator Enforcement Program)
    -- Criteria based on OSHA's actual SVEP designation:
    -- 1. Any willful or repeat violation
    -- 2. High-gravity serious violations with fatality/catastrophe inspection
    -- 3. Repeat serious violations (3+)
    -- We flag employers meeting criteria 1 or 3 (no fatality data in current dataset)
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
LEFT JOIN trend_1yr t1 ON e.employer_id = t1.employer_id
LEFT JOIN trend_3yr t3 ON e.employer_id = t3.employer_id
LEFT JOIN {{ ref('naics_2022') }} n ON e.naics_code = n.naics_code
LEFT JOIN parent_match pm ON e.employer_id = pm.employer_id
LEFT JOIN location_counts lc ON e.employer_id = lc.employer_id

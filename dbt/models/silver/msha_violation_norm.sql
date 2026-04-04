{{ config(materialized='table') }}

-- Silver: normalized MSHA violations joined to mine info
-- Links violations to operator names and locations for entity matching

WITH violations AS (
    SELECT * FROM {{ ref('stg_msha_violations') }}
),

mines AS (
    SELECT * FROM {{ ref('stg_msha_mines') }}
)

SELECT
    v.event_no,
    v.mine_id,
    v.violation_no,
    v.violator_name,
    {{ normalize_name('COALESCE(v.violator_name, m.operator_name)') }} AS name_normalized,
    v.violation_date,
    v.issue_date,
    v.section_of_act,
    v.sig_sub,
    v.proposed_penalty,
    v.amount_paid,
    v.negligence,
    m.mine_name,
    m.mine_status,
    m.operator_name,
    m.controller_name,
    m.state,
    m.county
FROM violations v
LEFT JOIN mines m ON v.mine_id = m.mine_id
WHERE COALESCE(v.violator_name, m.operator_name) IS NOT NULL

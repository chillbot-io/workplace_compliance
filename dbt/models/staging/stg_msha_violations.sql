{{ config(materialized='view') }}

-- Staging: raw MSHA violations from bulk download
SELECT
    EVENT_NO AS event_no,
    MINE_ID AS mine_id,
    VIOLATION_NO AS violation_no,
    VIOLATOR_NAME AS violator_name,
    CAST(VIOLATION_OCCUR_DT AS DATE) AS violation_date,
    CAST(VIOLATION_ISSUE_DT AS DATE) AS issue_date,
    SECTION_OF_ACT AS section_of_act,
    SIG_SUB AS sig_sub,
    COALESCE(CAST(PROPOSED_PENALTY AS NUMERIC), 0) AS proposed_penalty,
    COALESCE(CAST(AMOUNT_PAID AS NUMERIC), 0) AS amount_paid,
    NEGLIGENCE AS negligence
FROM raw_msha_violations

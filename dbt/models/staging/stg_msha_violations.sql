{{ config(materialized='view') }}

-- Staging: raw MSHA violations from bulk download
-- MSHA dates are MM/DD/YYYY format, need explicit parsing
SELECT
    EVENT_NO AS event_no,
    MINE_ID AS mine_id,
    VIOLATION_NO AS violation_no,
    VIOLATOR_NAME AS violator_name,
    TRY_CAST(STRPTIME(VIOLATION_OCCUR_DT, '%m/%d/%Y') AS DATE) AS violation_date,
    TRY_CAST(STRPTIME(VIOLATION_ISSUE_DT, '%m/%d/%Y') AS DATE) AS issue_date,
    SECTION_OF_ACT AS section_of_act,
    SIG_SUB AS sig_sub,
    COALESCE(TRY_CAST(PROPOSED_PENALTY AS NUMERIC), 0) AS proposed_penalty,
    COALESCE(TRY_CAST(AMOUNT_PAID AS NUMERIC), 0) AS amount_paid,
    NEGLIGENCE AS negligence
FROM raw_msha_violations

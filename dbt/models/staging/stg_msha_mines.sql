{{ config(materialized='view') }}

-- Staging: MSHA mine registry — links mine_id to operator name and location
SELECT
    MINE_ID AS mine_id,
    CURRENT_MINE_NAME AS mine_name,
    CURRENT_MINE_STATUS AS mine_status,
    CURRENT_OPERATOR_NAME AS operator_name,
    CURRENT_CONTROLLER_NAME AS controller_name,
    STATE AS state,
    FIPS_CNTY_NM AS county,
    PRIMARY_SIC AS sic_code,
    CAST(LATITUDE AS DOUBLE) AS latitude,
    CAST(LONGITUDE AS DOUBLE) AS longitude
FROM raw_msha_mines

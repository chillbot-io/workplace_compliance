{{ config(materialized='view') }}

-- Staging: OSHA ITA 300A summary data (establishment-level injury/illness reports)
-- Source: manually downloaded CSVs from osha.gov/Establishment-Specific-Injury-and-Illness-Data
-- Key value: EIN + company_name + DART/TRIR inputs

SELECT
    id AS ita_record_id,
    establishment_id AS ita_establishment_id,
    TRIM(establishment_name) AS establishment_name,
    TRIM(ein) AS ein,
    TRIM(company_name) AS company_name,
    TRIM(street_address) AS street_address,
    TRIM(city) AS city,
    UPPER(TRIM(state)) AS state,
    LEFT(REGEXP_REPLACE(COALESCE(zip_code, ''), '[^0-9]', '', 'g'), 5) AS zip5,
    TRIM(naics_code) AS naics_code,
    TRIM(industry_description) AS industry_description,
    TRY_CAST(annual_average_employees AS INTEGER) AS annual_average_employees,
    TRY_CAST(total_hours_worked AS NUMERIC) AS total_hours_worked,
    -- Injury/illness counts for DART and TRIR
    COALESCE(TRY_CAST(total_deaths AS INTEGER), 0) AS total_deaths,
    COALESCE(TRY_CAST(total_dafw_cases AS INTEGER), 0) AS total_dafw_cases,
    COALESCE(TRY_CAST(total_djtr_cases AS INTEGER), 0) AS total_djtr_cases,
    COALESCE(TRY_CAST(total_other_cases AS INTEGER), 0) AS total_other_cases,
    COALESCE(TRY_CAST(total_dafw_days AS INTEGER), 0) AS total_dafw_days,
    COALESCE(TRY_CAST(total_djtr_days AS INTEGER), 0) AS total_djtr_days,
    COALESCE(TRY_CAST(total_injuries AS INTEGER), 0) AS total_injuries,
    COALESCE(TRY_CAST(total_skin_disorders AS INTEGER), 0) AS total_skin_disorders,
    COALESCE(TRY_CAST(total_respiratory_conditions AS INTEGER), 0) AS total_respiratory_conditions,
    COALESCE(TRY_CAST(total_poisonings AS INTEGER), 0) AS total_poisonings,
    COALESCE(TRY_CAST(total_hearing_loss AS INTEGER), 0) AS total_hearing_loss,
    COALESCE(TRY_CAST(total_other_illnesses AS INTEGER), 0) AS total_other_illnesses,
    TRY_CAST(year_filing_for AS INTEGER) AS year_filing_for,
    TRIM(establishment_type) AS establishment_type,
    TRIM(size) AS size_category,
    TRY_CAST(no_injuries_illnesses AS BOOLEAN) AS no_injuries_illnesses
FROM raw_ita_summary
WHERE establishment_name IS NOT NULL
  AND TRIM(establishment_name) != ''

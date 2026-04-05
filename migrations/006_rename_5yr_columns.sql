-- 006_rename_5yr_columns.sql — Rename _5yr columns to reflect all-time aggregation

-- OSHA columns
ALTER TABLE employer_profile RENAME COLUMN osha_inspections_5yr TO osha_inspections;
ALTER TABLE employer_profile RENAME COLUMN osha_violations_5yr TO osha_violations;

-- WHD columns
ALTER TABLE employer_profile RENAME COLUMN whd_cases_5yr TO whd_cases;

-- MSHA columns
ALTER TABLE employer_profile RENAME COLUMN msha_violations_5yr TO msha_violations;

-- NLRB (future, but rename now while we're at it)
ALTER TABLE employer_profile RENAME COLUMN nlrb_cases_5yr TO nlrb_cases;

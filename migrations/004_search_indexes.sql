-- 004_search_indexes_and_columns.sql — Search improvements
-- Supports new search endpoint filters and pre-computed location count

-- Indexes for zip, state, city filters
CREATE INDEX IF NOT EXISTS idx_ep_zip ON employer_profile (zip);
CREATE INDEX IF NOT EXISTS idx_ep_state ON employer_profile (state);
CREATE INDEX IF NOT EXISTS idx_ep_city_trgm ON employer_profile USING gin (city gin_trgm_ops);

-- Pre-computed location count: how many employer_ids share the same normalized name
-- Populated by pipeline (gold model). Used by API to show "1 of 347 Walmart locations"
ALTER TABLE employer_profile ADD COLUMN IF NOT EXISTS location_count INTEGER DEFAULT 1;

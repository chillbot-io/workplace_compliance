-- 003_restrict_metabase.sql — Restrict metabase_user to analytical tables only.
-- Security fix: metabase_user previously had SELECT on ALL tables including
-- customers (password hashes), api_keys (key hashes), password_reset_tokens,
-- and subscriptions (signing secrets).

-- Revoke broad access
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM metabase_user;

-- Grant only analytical tables
GRANT SELECT ON
    employer_profile,
    inspection_history,
    risk_snapshots,
    pipeline_runs,
    pipeline_errors,
    api_usage,
    feedback
TO metabase_user;

-- Future tables default to no access for metabase_user
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM metabase_user;

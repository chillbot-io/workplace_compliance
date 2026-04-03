-- 002_add_constraints.sql — Add missing FK constraints, indexes, and ON DELETE behavior
-- Addresses audit findings: orphan records, missing indexes on FKs, cascading deletes.

------------------------------------------------------------
-- Missing indexes
------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_pipeline_errors_run_id ON pipeline_errors (run_id);
CREATE INDEX IF NOT EXISTS idx_insp_employer_date ON inspection_history (employer_id, inspection_date DESC);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_customer ON batch_jobs (customer_id);
CREATE INDEX IF NOT EXISTS idx_feedback_employer ON feedback (employer_id);
CREATE INDEX IF NOT EXISTS idx_review_queue_decision ON review_queue (decision) WHERE decision IS NULL;

------------------------------------------------------------
-- ON DELETE behavior for tables missing it
------------------------------------------------------------

-- subscriptions: cascade on customer delete
ALTER TABLE subscriptions
    DROP CONSTRAINT IF EXISTS subscriptions_customer_id_fkey,
    ADD CONSTRAINT subscriptions_customer_id_fkey
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE;

-- batch_jobs: cascade on customer delete
ALTER TABLE batch_jobs
    DROP CONSTRAINT IF EXISTS batch_jobs_customer_id_fkey,
    ADD CONSTRAINT batch_jobs_customer_id_fkey
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE;

-- api_key_audit_log: set null on customer delete (keep audit trail)
ALTER TABLE api_key_audit_log
    DROP CONSTRAINT IF EXISTS api_key_audit_log_customer_id_fkey,
    ADD CONSTRAINT api_key_audit_log_customer_id_fkey
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL;

-- api_usage: set null on customer delete (keep usage history)
ALTER TABLE api_usage
    DROP CONSTRAINT IF EXISTS api_usage_customer_id_fkey,
    ADD CONSTRAINT api_usage_customer_id_fkey
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL;

------------------------------------------------------------
-- CHECK constraints on enum-like columns
------------------------------------------------------------
ALTER TABLE inspection_history
    ADD CONSTRAINT chk_inspection_agency
        CHECK (agency IN ('OSHA', 'WHD', 'MSHA', 'EPA', 'FMCSA', 'OFCCP', 'NLRB', 'OFLC'));

ALTER TABLE batch_jobs
    DROP CONSTRAINT IF EXISTS batch_jobs_status_check,
    ADD CONSTRAINT batch_jobs_status_check
        CHECK (status IN ('pending', 'processing', 'completed', 'failed'));

ALTER TABLE feedback
    DROP CONSTRAINT IF EXISTS feedback_type_check,
    ADD CONSTRAINT feedback_type_check
        CHECK (type IN ('incorrect_match', 'missing_data', 'wrong_employer', 'other'));

------------------------------------------------------------
-- Grants for pipeline_user and metabase_user on new objects
------------------------------------------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    employer_profile, inspection_history, risk_snapshots,
    pipeline_runs, pipeline_errors, cluster_id_mapping, review_queue
TO pipeline_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;

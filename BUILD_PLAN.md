# Employer Compliance API — Build Plan

**Created:** April 2026
**Architecture Reference:** `osha_compliance_arch_v6.md` (v6.2)

---

## Build Sequence Overview

```
Phase 0: Infrastructure          (Day 1-2)   ← everything blocks on this
Phase 1: Pipeline — OSHA + WHD   (Day 3-7)   ← data before API
Phase 2: API Core                (Day 8-12)  ← serve the data
Phase 3: Billing + Dashboard     (Day 13-17) ← gate access, self-serve
Phase 4: Expansion + Hardening   (Day 18-22) ← widen the moat
```

Critical path: **Infrastructure → Pipeline → API → Billing**. Nothing runs without Postgres. The API is useless without data. Billing gates access to a working product.

---

## Phase 0: Infrastructure (Day 1-2)

Everything else blocks on this.

### Tasks

| # | Task | Details | Exit Check |
|---|------|---------|------------|
| 0.1 | Provision Hetzner servers | API: CPX31 (8GB). Pipeline: AX52 (64GB, 8-core). | Both servers SSH-accessible |
| 0.2 | Configure vSwitch | Private VLAN, API=10.0.0.1, Pipeline=10.0.0.2 | `ping 10.0.0.1` from pipeline server |
| 0.3 | Install Postgres 16 | API server. `listen_addresses = '127.0.0.1, 10.0.0.1'`. `pg_hba.conf` with scram-sha-256. | `psql -U postgres` works locally |
| 0.4 | Run init_db.sql | Create `stablelabel` DB + 3 roles (api, pipeline_user, metabase_user) | All 3 roles exist with correct grants |
| 0.5 | Deploy pgBouncer | Production `pgbouncer.ini` from arch doc §3.3. Bind 127.0.0.1:6432. | `psql -h 127.0.0.1 -p 6432 -U api stablelabel` connects |
| 0.6 | Docker Compose skeletons | `docker-compose.api.yml` + `docker-compose.pipeline.yml`. Minimal — just Postgres, pgBouncer, and placeholder app containers. | `docker compose up` starts without errors on both servers |
| 0.7 | Configure .env files | `.env.api` on API server, `.env.pipeline` on pipeline server. Generate RSA keypair, CSRF_SECRET. | Env vars load correctly in containers |
| 0.8 | Migration framework | `migrations/migrate.py` + `001_init.sql` (all 17 tables). Run at container startup. | `SELECT * FROM schema_migrations` shows version 1 applied |
| 0.9 | Cross-server connectivity | Pipeline server → API server Postgres via vSwitch with `sslmode=require`. | `psql` from pipeline server: `SELECT 1` returns on 10.0.0.1:5432 |
| 0.10 | Firewall rules | API: allow 10.0.0.2 on 5432, deny all else on 5432/6432. Both: allow 80/443. | `ufw status` confirms rules. Port scan from outside fails. |
| 0.11 | nginx stub | TLS termination with Let's Encrypt. Reverse proxy to localhost:8000. | `curl https://yourdomain.com` returns 502 (no app yet — expected) |

### Phase 0 Exit Criteria

- [ ] Pipeline server can `INSERT INTO employer_profile_staging ...` on API server's Postgres
- [ ] `migrate.py` runs idempotently — all tables created
- [ ] Both Docker Compose stacks start cleanly
- [ ] TLS terminates at nginx

---

## Phase 1: Pipeline — OSHA + WHD (Day 3-7)

The API is useless without data. Build the pipeline first with the two DOL sources.

> **PREREQUISITE:** Register at dataportal.dol.gov immediately. API key activation takes up to 24 hours.

### Tasks

| # | Task | Details | Exit Check |
|---|------|---------|------------|
| 1.1 | Bronze ingestion — DOL | `pipeline/ingest_dol.py`: fetch OSHA Inspections, OSHA Violations, WHD Compliance Actions via DOL API v2. Write raw Parquet to `/data/bronze/`. | Parquet files in `/data/bronze/osha_inspections/`, `osha_violations/`, `whd_actions/` |
| 1.2 | DuckDB setup | Create `/data/duckdb/employer_compliance.duckdb`. Load bronze Parquet into DuckDB tables. | `SELECT COUNT(*) FROM osha_inspections` returns >0 |
| 1.3 | dbt seeds | `naics_2022.csv`, `insp_type.csv`, `viol_type.csv`, `fmcsa_basic_labels.csv`. Download NAICS from census.gov. | `dbt seed` succeeds. `SELECT * FROM naics_2022 LIMIT 5` works. |
| 1.4 | dbt Silver — name normalization | `models/silver/osha_inspection_norm.sql`: uppercase, strip non-alphanumeric, expand abbreviations, remove corp suffixes. `name_normalized` column. | Spot-check: "WALMART INC." → "WALMART", "McDonald's Corp" → "MCDONALDS" |
| 1.5 | Address parsing | `pipeline/parse_addresses.py` using `usaddress`. Generate `address_key` (STREET_NUMBER\|STREET_NAME\|ZIP5). Handle `RepeatedLabelError` → NULL. | `SELECT address_key FROM osha_parsed_addresses WHERE address_key IS NOT NULL LIMIT 10` returns clean keys |
| 1.6 | dbt Silver — WHD normalization | Same name/address normalization for WHD. Join on `naics_code_description` to seed table. | WHD records have `name_normalized` and `address_key` |
| 1.7 | dbt Gold — risk scoring | `models/gold/employer_profile.sql`: risk_tier CASE, risk_score formula, trend_signal, confidence_tier. All COALESCE-wrapped. | `SELECT risk_tier, COUNT(*) FROM employer_profile GROUP BY 1` returns all 4 tiers |
| 1.8 | Entity resolution — Splink | `pipeline/entity_resolution.py`. Start with 2 blocking rules (zip5, state+name prefix). Threshold 0.80/0.85. cluster_id_mapping with stable UUIDs. | `SELECT COUNT(DISTINCT employer_id) FROM cluster_id_mapping` — reasonable number (not 1, not equal to raw record count) |
| 1.9 | Validate Splink manually | Sample 50 clusters. Check: are merged records actually the same employer? Are distinct employers kept separate? | Manual review passes — no obvious false merges or missed matches |
| 1.10 | Add 3rd blocking rule | `name_prefix + naics_4digit` for multi-geography employers. Re-run and compare. | National chains (Walmart, Amazon) cluster correctly across states |
| 1.11 | sync.py — shadow-table swap | DuckDB → Postgres binary COPY into `employer_profile_staging`, index, atomic swap. | `SELECT COUNT(*) FROM employer_profile` on API server Postgres matches DuckDB |
| 1.12 | validate_sync.py | Row count: DuckDB vs Postgres. Fail if >0.1% mismatch. | Script runs, prints "PASS", exits 0 |
| 1.13 | run_pipeline.sh | Full orchestration: flock, start/fail/success logging, dead-letter on partial failures, dispatch_webhooks placeholder. | `bash run_pipeline.sh` runs end-to-end. `pipeline_runs` shows status=completed. |
| 1.14 | Seed test_fixtures | `seeds/test_fixtures.sql`: 50 hand-curated employers across all tiers/edge cases. | `SELECT COUNT(*) FROM test_fixtures` = 50 |
| 1.15 | Pipeline cron | Cron entry: `0 2 * * *` with flock + cron_alert.sh wrapper. | Runs overnight, check `pipeline_runs` next morning |

### Phase 1 Exit Criteria

- [ ] `employer_profile` table has real OSHA+WHD data on the API server
- [ ] `SELECT * FROM employer_profile LIMIT 10` from API server returns real employer profiles
- [ ] Entity resolution produces reasonable clusters (manual validation)
- [ ] Pipeline runs end-to-end unattended via cron
- [ ] `test_fixtures` seeded with 50 employers

### Phase 1 Risks

| Risk | Mitigation |
|------|-----------|
| DOL API key not activated | Register immediately — 24h lead time |
| Splink threshold tuning | Budget 2-3 days. Validate manually before wiring sync. Start with 2 blocking rules, add 3rd after visual validation. |
| DuckDB memory pressure | Set `memory_limit='40GB'` and `threads=16` on AX52. Monitor with `PRAGMA memory_limit`. |
| Address parsing failures | NULL address_key is acceptable — records still match on name/NAICS. Log warning count. |

---

## Phase 2: API Core (Day 8-12)

Now serve the data.

### Tasks

| # | Task | Details | Exit Check |
|---|------|---------|------------|
| 2.1 | FastAPI skeleton | `api/main.py`: structured logging (structlog), Sentry integration, CORS headers, lifespan with asyncpg pool. | `uvicorn api.main:app` starts, logs JSON |
| 2.2 | Auth middleware | `api/auth.py`: `verify_key()` — SHA-256 hash, lookup by key_id, check status/expiration. `check_scope()` — FastAPI Depends. `check_monthly_quota()` — atomic row-counting from api_usage. | Hardcoded test key returns 200. Bad key returns 401. |
| 2.3 | GET /v1/health | 4-check health endpoint (DB, data_loaded, pipeline_recent, pipeline_status). 200 healthy / 503 degraded. | `curl /v1/health` returns 200 with employer_profiles_count > 0 |
| 2.4 | GET /v1/employers | Search: pg_trgm fuzzy name, EIN exact, address_key boost. Ranking priority chain. possible_matches capped at 10. | `curl /v1/employers?name=walmart` returns ranked results |
| 2.5 | GET /v1/employers/{id} | Direct lookup by employer_id UUID. 301 redirect for superseded IDs. 404 for unknown. | `curl /v1/employers/{uuid}` returns full profile JSON |
| 2.6 | GET /v1/employers/{id}/inspections | Free, not metered. Paginated inspection history from `inspection_history` table. | Returns inspection list with pagination envelope |
| 2.7 | Test key routing | `emp_test_` prefix routes to `test_fixtures` table. No quota consumption. | Test key returns fixture data. Production key returns real data. |
| 2.8 | Response headers | `X-Data-Freshness`, `X-Data-Age-Hours`, `X-Lookups-Remaining`, `X-Lookups-Limit`. | Headers present on all responses |
| 2.9 | Error responses | Unified format: `{error, message, resets_at, documentation}`. 401/403/404/429 all consistent. | Error responses match spec |
| 2.10 | nginx config | Reverse proxy to localhost:8000. Rate limit headers passed through. | `curl https://yourdomain.com/v1/health` works over TLS |
| 2.11 | Deploy script | `deploy.sh`: pull image, swap container, health check, rollback on failure. | Deploy + rollback tested manually |

### Phase 2 Exit Criteria

- [ ] Can `curl` the API over HTTPS with a test key and get a real employer profile
- [ ] Search, direct lookup, and inspections endpoints all working
- [ ] Auth middleware rejects bad/expired/disabled keys correctly
- [ ] Health check returns accurate status
- [ ] Deploy + rollback cycle tested

---

## Phase 3: Billing + Dashboard (Day 13-17)

Gate access. Let people self-serve.

### Tasks

| # | Task | Details | Exit Check |
|---|------|---------|------------|
| 3.1 | Stripe products | Create products + prices: free (5/mo), starter, pro, enterprise. | Products visible in Stripe dashboard |
| 3.2 | Stripe webhook handler | `api/billing.py`: checkout.session.completed → create customer + API key. Idempotency via `stripe_webhook_events`. | Stripe test webhook creates customer + key in DB |
| 3.3 | Signup flow | POST /auth/signup: argon2id hash, email verification token, Resend email. | Signup → email received → verify link works |
| 3.4 | Login flow | POST /auth/login: verify argon2id, issue RS256 JWT as HttpOnly cookie. Rate limited 10/min. | Login returns Set-Cookie header. Cookie works on dashboard routes. |
| 3.5 | Password reset | POST /auth/forgot-password + POST /auth/reset-password. 1h token expiry. Rate limited 3/min. | Full reset flow works end-to-end |
| 3.6 | Dashboard key management | GET/POST /dashboard/keys, POST /dashboard/keys/{id}/rotate, DELETE /dashboard/keys/{id}. Raw key shown once. | Create key → see raw key → key works on API → rotate → old key works 48h |
| 3.7 | CSRF middleware | Double-submit cookie on /dashboard/ routes. CSRF_SECRET from env var. | POST without CSRF token → 403. POST with token → succeeds. |
| 3.8 | Rate limiting | Per-key (100/min), per-IP (60/min), auth endpoints (custom per-route). | Exceed limit → 429 with Retry-After header |
| 3.9 | Metabase | Connect metabase_user (read-only). Build 4 core dashboards: top violators, industry risk, geographic heatmap, trend analysis. | Metabase accessible at /metabase, dashboards populated |
| 3.10 | Free tier | Default plan=free, monthly_limit=5. Stripe checkout optional (no CC required for free). | Sign up without CC → get key → 5 lookups work → 6th returns 429 |
| 3.11 | API key audit log | Log create/rotate/revoke events to `api_key_audit_log`. | Audit entries appear for all key lifecycle events |

### Phase 3 Exit Criteria

- [ ] Full signup → verify → login → create key → make API call flow works
- [ ] Stripe webhook provisions keys automatically on checkout
- [ ] Free tier: 5 lookups with no CC
- [ ] Dashboard is CSRF-protected, rate-limited
- [ ] Metabase dashboards show real data

**Ship to first real user after Phase 3.** OSHA+WHD alone is more than anyone can get today. Get feedback before expanding.

---

## Phase 4: Expansion + Hardening (Day 18-22)

Widen the moat. Production-harden.

### Tasks

| # | Task | Details | Exit Check |
|---|------|---------|------------|
| 4.1 | MSHA ingestion | `ingest_dol.py` expansion: MSHA Mines + Violations. Add to dbt Silver/Gold. | MSHA data in employer_profile |
| 4.2 | FMCSA ingestion | Bulk CSV download from ai.fmcsa.dot.gov/SMS. Rate-limited 0.5s/req. Into DuckDB. | FMCSA data visible in profiles (not yet in risk_tier — Phase 3 sources) |
| 4.3 | Batch endpoint | POST /v1/employers/batch. <=25 sync, >25 async (R2 + job polling). Cap 500. | Batch of 10 returns sync. Batch of 50 returns job_id + poll URL. |
| 4.4 | Webhook subscriptions | POST/GET/PATCH/DELETE /v1/subscriptions. HMAC-SHA256 signing. HTTPS-only callback. | Create subscription → trigger pipeline → receive webhook at callback URL |
| 4.5 | dispatch_webhooks.py | Diff employer_profile vs _prev. Fire to subscribers. Fail-fast on 4xx. Disable after 3 failures. | Risk tier change → webhook fires → payload matches spec |
| 4.6 | Risk history endpoint | GET /v1/employers/{id}/risk-history. Nightly snapshots from `risk_snapshots`. | Returns trend data with timestamps |
| 4.7 | Industry benchmarks | GET /v1/industries/{naics4} + GET /v1/industries/naics-codes. | NAICS lookup returns citation rates and industry stats |
| 4.8 | Backup to R2 | `rclone copy` (NOT sync): bronze, Postgres dumps, DuckDB checkpoints, operational config. 4 AM daily. | Backup in R2. Test restore on a scratch server. |
| 4.9 | Cron hardening | cron_alert.sh on all jobs. Disk monitoring every 6h. Snapshot retention (prune_snapshots.sh). Bronze compaction monthly. Usage reset 1st of month. | All cron jobs wrapped with alerting. Slack notification on any failure. |
| 4.10 | Key rotation cron | `rotate_keys.py` hourly: expire rotating_out keys past 48h window. | Keys transition active → rotating_out → revoked on schedule |
| 4.11 | UptimeRobot | Monitor /v1/health. 5-min intervals. Alert on 503. | UptimeRobot dashboard shows green |

### Phase 4 Exit Criteria

- [ ] 4 data sources live (OSHA, WHD, MSHA, FMCSA)
- [ ] Batch, webhooks, risk history, industry benchmarks all functional
- [ ] Pipeline runs nightly unattended with alerting
- [ ] Backups verified restorable
- [ ] All cron jobs monitored

---

## Future Phases (Not Scoped)

- **Phase 2 sources:** EPA ECHO, SAM debarment, OFCCP, NLRB, OFLC
- **Phase 2 pipeline:** libpostal replaces usaddress, supervised Splink training from review_queue
- **FMCSA in risk_tier:** After validating signal quality with CY2024 data
- **Azure Container Apps migration:** Same Docker images, swap compose for ACA manifests
- **Bulk export:** Data licensing access mode
- **Review queue UI:** Human labeling workflow for Splink drift monitoring

---

## Key Principles

1. **Data before API, API before billing.** Don't build gates before there's something to gate.
2. **Ship after Phase 3.** OSHA+WHD is already a product nobody else has. Get feedback early.
3. **Splink is the critical path.** Budget extra time. Validate manually before trusting it.
4. **No premature optimization.** Skip FMCSA risk_tier integration until signal quality is validated. Skip libpostal until usaddress proves insufficient.
5. **Test with emp_test_ keys during development.** Don't build real auth until endpoints work.

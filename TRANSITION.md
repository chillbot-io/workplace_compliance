# FastDOL Transition Document

**Date:** April 3, 2026
**Purpose:** Complete context transfer for continuing development

---

## 1. What FastDOL Is

FastDOL is a B2B API product that aggregates federal employer enforcement data (OSHA, WHD, MSHA, EPA, FMCSA) into normalized employer risk profiles. Customers query by employer name, EIN, or address and get back a structured compliance risk profile with risk tier, risk score, violation history, and trend signals.

**Target buyers:** Insurance underwriters, staffing/PEO firms, compliance consultants, supply chain teams.

**Business model:** API-first with tiered pricing:
- Free: $0, 50 lookups/mo
- Starter: $79/mo, 1,000 lookups
- Growth: $249/mo, 5,000 lookups  
- Pro: $599/mo, 25,000 lookups
- Enterprise: custom

**Live at:** https://api.fastdol.com

---

## 2. Architecture

### Two-Server Setup (Hetzner Cloud)

**API Server (scrubiq):**
- IP: 88.198.218.234 (public), 10.0.0.2 (vSwitch)
- Location: Nuremberg
- Spec: CPX42, 8 vCPU, 16GB RAM
- Runs: Postgres 16, pgBouncer, FastAPI (uvicorn on port 8001), nginx (TLS), Metabase (Docker, port 3000)
- systemd service: `fastdol-api.service`
- Env file: `/opt/employer-compliance/.env.api`

**Pipeline Server (ubuntu-32gb-nbg1-1):**
- IP: 46.224.150.38 (public), 10.0.0.3 (vSwitch)
- Location: Nuremberg
- Spec: CCX33, 8 vCPU, 32GB RAM
- Runs: DuckDB, dbt, Splink, pipeline scripts
- Docker with `iptables: false`
- Env file: `/opt/employer-compliance/.env.pipeline`

**Connected via:** Hetzner Cloud Network (10.0.0.0/24)

### Data Flow

```
DOL API v4 → Parquet (bronze) → DuckDB → dbt staging → dbt silver (normalize) 
→ Splink (entity resolution) → post-Splink name merge → dbt gold (risk scoring) 
→ shadow-table swap to Postgres → FastAPI serves via pgBouncer
```

### Key Credentials (all placeholder — real ones in .env files on servers)

- Postgres users: `api` (password1), `pipeline_user` (password2), `metabase_user` (password3)
- DOL API key: in `.env.pipeline` as `DOL_API_KEY`
- Stripe: test mode, keys in `.env.api`
- Resend: key in `.env.api`, domain `fastdol.com` verified
- JWT: RSA keys at `/etc/employer-compliance/jwt_private.pem` and `jwt_public.pem`
- GoDaddy DNS for fastdol.com → 88.198.218.234

**IMPORTANT:** All passwords are still defaults (password1/2/3). Must change before production launch (Block 6.2 in launch agenda).

---

## 3. Repository Structure

```
workplace_compliance/
├── api/
│   ├── main.py              # FastAPI app, health check, CORS, Sentry, middleware
│   ├── auth.py              # API key verification, scope checking, quota enforcement
│   ├── csrf.py              # Double-submit cookie CSRF middleware
│   ├── email.py             # Resend integration (verification + password reset emails)
│   └── routes/
│       ├── auth.py          # Signup, login, verify, forgot-password, reset-password
│       ├── dashboard.py     # API key CRUD (create, rotate, revoke)
│       ├── billing.py       # Stripe checkout, webhook handler
│       └── employers.py     # Search, lookup, batch, risk-history, feedback, industry
├── pipeline/
│   ├── ingest_dol.py        # DOL API v4 ingestion with burst rate limiting
│   ├── load_bronze.py       # Load parquet into DuckDB
│   ├── parse_addresses.py   # usaddress → address_key
│   ├── entity_resolution.py # Splink + post-Splink name merge
│   ├── sync.py              # Shadow-table swap DuckDB → Postgres
│   ├── validate_sync.py     # Row count validation
│   ├── validate_data.py     # Data quality checks (null rates, joins, distributions)
│   └── run_pipeline.sh      # 9-step orchestration script
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── seeds/
│   │   ├── insp_type.csv
│   │   ├── viol_type.csv
│   │   └── naics_2022.csv   # 2,012 NAICS codes from Census.gov
│   ├── macros/
│   │   └── normalize_name.sql  # Name normalization (DuckDB RE2 regex with 'g' flag)
│   └── models/
│       ├── staging/         # Raw table wrappers
│       ├── silver/          # Normalized: osha_inspection_norm, whd_norm
│       └── gold/            # employer_profile (risk tier, score, trend, SVEP, confidence)
├── migrations/
│   ├── migrate.py           # Sequential migration runner
│   ├── 001_init.sql         # 18 tables
│   ├── 002_add_constraints.sql
│   └── 003_restrict_metabase.sql
├── scripts/
│   ├── init_db.sql          # Postgres role creation
│   ├── deploy.sh            # Atomic deploy with rollback
│   ├── fastdol-api.service  # systemd service (uses EnvironmentFile)
│   ├── cron_alert.sh        # Slack webhook on cron failure
│   ├── crontab.pipeline     # All cron entries (NOT YET INSTALLED)
│   ├── rotate_keys.py       # Expire rotating_out keys past 48h
│   ├── reset_monthly_usage.py
│   ├── check_disk.sh
│   ├── check_health.sh
│   └── backup.sh
├── nginx.conf               # TLS, rate limiting, security headers
├── pgbouncer.ini
├── Dockerfile               # API server
├── Dockerfile.pipeline
├── docker-compose.api.yml
├── docker-compose.pipeline.yml
├── requirements.txt         # API dependencies (pinned versions)
├── requirements.pipeline.txt
├── BUILD_PLAN.md
├── LAUNCH_AGENDA.md
├── LAUNCH_PLAN.md           # Business strategy, go-to-market
└── osha_compliance_arch_v6.md  # Original architecture doc (v6.2)
```

---

## 4. Current Data State

### What's in DuckDB (pipeline server: /data/duckdb/employer_compliance.duckdb)

| Table | Records | Notes |
|-------|---------|-------|
| raw_osha_inspections | 2,485,000 | Full OSHA inspection history |
| raw_osha_violations | 400,000 | Linked to inspections by activity_nr |
| raw_whd_actions | 62,000 | Partial — rate limited, needs more |
| osha_inspection_norm | 2,485,000 | Silver: normalized names, joined violations |
| employer_clusters | 2,484,991 | Splink output |
| cluster_id_mapping | 1,204,491 | After post-Splink name merge |
| employer_profile | 249,472 | Gold: risk-scored profiles |
| er_input | 2,484,991 | Entity resolution input |
| naics_2022 | 2,012 | NAICS code → description seed |

### What's in Postgres (API server)

| Table | Records | Notes |
|-------|---------|-------|
| employer_profile | 249,472 | Synced from DuckDB Gold |
| All other tables | Empty or minimal | Auth tables have test data |

### Risk Distribution

- HIGH: 2,610 employers
- ELEVATED: 302
- MEDIUM: 77,344
- LOW: 169,216 (approximate)

### Data Quality

- Violation join: 100% (all 400k violations linked to inspections)
- Name normalization: working (strips INC/LLC/CORP/LTD/DBA, trailing numbers)
- Entity resolution: Splink clusters + post-Splink name merge (1.69M → 1.2M clusters)
- Known issue: National chains (Amazon, Walmart) still show as separate location-based clusters because different name variants (AMAZONCOM SERVICES, AMAZON FULFILLMENT, etc.) don't merge without EIN-based linking

---

## 5. API Endpoints (all live at api.fastdol.com)

### Public
- `GET /v1/health` — system health with pipeline stats, data freshness

### Employer Data (requires X-Api-Key header)
- `GET /v1/employers?name=&ein=&state=&naics=` — fuzzy search (pg_trgm)
- `GET /v1/employers/{id}` — direct lookup (301 redirect for superseded IDs)
- `GET /v1/employers/{id}/inspections` — free, not metered
- `GET /v1/employers/{id}/risk-history` — nightly snapshots
- `POST /v1/employers/batch` — sync <=25 items (>25 returns 501)
- `POST /v1/employers/{id}/feedback` — report bad matches
- `GET /v1/industries/{naics4}` — industry benchmarks
- `GET /v1/industries/naics-codes` — NAICS code discovery

### Auth
- `POST /auth/signup` — argon2id hash, email verification via Resend
- `GET /auth/verify?token=` — verify email, issue first API key
- `POST /auth/login` — RS256 JWT as HttpOnly cookie (8h expiry)
- `POST /auth/forgot-password` — always 202
- `POST /auth/reset-password` — token-based

### Dashboard (JWT cookie auth + CSRF)
- `GET /dashboard/keys` — list keys
- `POST /dashboard/keys` — create key (shown once, max 5)
- `POST /dashboard/keys/{id}/rotate` — 48h NIST window
- `DELETE /dashboard/keys/{id}` — immediate revoke

### Billing
- `POST /billing/checkout` — create Stripe checkout session
- `POST /webhooks/stripe` — handle checkout.session.completed, subscription.deleted
- `GET /billing/success` / `GET /billing/cancel`

---

## 6. What We Were Working On (Interrupted)

### Immediate Task: WHD Data Ingestion (Block 1.7)

WHD (Wage & Hour Division) ingestion got 62,000 records but stopped due to rate limits. WHD has significantly more records. Need to:

1. Restart WHD ingestion to get remaining records
2. Load into DuckDB
3. Build WHD Silver model (already exists at `dbt/models/silver/whd_norm.sql` but couldn't run because raw_whd_actions table didn't exist)
4. Integrate WHD data into Gold model employer profiles

**WHD does NOT have EIN** — we discovered this during the session. The arch doc assumed it did, but the DOL v4 API doesn't expose EIN for any dataset.

### Next Task: SAM.gov EIN Integration

We identified SAM.gov as the best free source for EIN data (millions of federal contractor entities with EIN). The user was in the process of registering for a SAM.gov API key. Plan:

1. Get SAM.gov API key
2. Download entity extract (bulk CSV with EIN, legal name, address, NAICS)
3. Match SAM entities to OSHA records via name + address
4. Use EIN as canonical entity key for parent company resolution

This solves the "Amazon shows as 50 separate locations" problem.

---

## 7. DOL API v4 Rate Limiting (Critical Knowledge)

The DOL API (apiprod.dol.gov/v4) has undocumented rate limits:

- **Burst limit:** ~13 requests before 429/500/502 errors
- **Reset window:** ~60 seconds of silence
- **Key placement:** OSHA endpoints accept header auth, WHD requires query param. We send both.
- **Page size:** OSHA allows 5,000 records/request, WHD allows 1,000 (5,000 returns 413)
- **Our strategy:** Burst 10 requests, cooldown 65s. On 429, wait 120s + escalating backoff.
- **Per-source page_size:** Defined in SOURCES dict in ingest_dol.py
- **Checkpoint:** Saves parquet every 5,000 records via atomic rename (temp file → final file)
- **Resume:** On restart, loads existing parquet and resumes from that offset

---

## 8. Security Posture

5 security audits were conducted and 30+ issues fixed across 4 commits:

### Fixed
- Test key backdoor locked down (blocked in production)
- User enumeration on signup removed
- CSRF on dashboard + billing endpoints
- JWT fails hard in production if RSA keys missing
- Scope escalation prevented (only admins can create admin:all keys)
- Stripe webhook idempotency atomic (INSERT ON CONFLICT DO NOTHING)
- API key moved from URL query params to HTTP header (was leaking in logs)
- nginx TLS hardening (TLSv1.2+, strong ciphers, OCSP stapling)
- Rate limiting on all nginx locations
- Metabase grants restricted to analytical tables only
- Password max length 128 (prevents argon2 DoS)
- Input length limits on all Pydantic models
- Backup script excludes .env files
- Atomic parquet checkpoint writes
- rotating_out keys checked at request time

### Not Fixed (lower priority)
- Systemd service runs as root (has hardening directives but still root user)
- Docker base images not digest-pinned
- No Content-Security-Policy fine-tuning
- pgBouncer connection logging disabled
- Log rotation not configured
- No test infrastructure (zero tests)

---

## 9. Ops Status

### Crontab (NOT INSTALLED — file exists at scripts/crontab.pipeline)
- Pipeline: 2 AM daily
- Health check: 8:30 AM daily
- Key rotation: hourly
- Backup: 4 AM daily
- Disk check: every 6h
- Monthly usage reset: 1st of month

### Backups
- Local only (/data/backups, 7-day retention)
- R2 offsite NOT configured
- pg_dump + DuckDB checkpoint + config (excluding .env)

### Monitoring
- Sentry SDK initialized (needs real DSN in .env.api)
- UptimeRobot NOT configured
- structlog JSON logging to stdout

---

## 10. Launch Agenda (see LAUNCH_AGENDA.md)

### Block 1: Data Quality — IN PROGRESS
- [x] 1.1 Verify violation join (400k correctly linked)
- [x] 1.2 Fix entity resolution (name merge, normalization)
- [x] 1.3 Add zip field
- [x] 1.4 Add naics_2022.csv seed (in dbt, Gold model updated)
- [x] 1.5 Add confidence_tier
- [x] 1.6 Add svep_flag
- [ ] 1.7 WHD integration (62k downloaded, need more + integration)
- [x] 1.8 Data quality checks (validate_data.py)
- [ ] 1.9 Manual validation against osha.gov
- [ ] 1.10 Full pipeline re-run with all fixes
- [ ] 1.11 Install crontab

### Block 2: Website — NOT STARTED
- 2A: Marketing site (landing, demo widget, pricing)
- 2B: Auth pages (signup, login, verify, reset)
- 2C: Developer dashboard (API keys, usage, billing)
- 2D: Docs site (API reference, getting started, examples)

### Block 3: Entity Resolution Improvement
- WHD integration (name matching)
- SAM.gov EIN bridge (user registering for API key)
- Parent company seed table (top 500)

### Block 4: HIL Review UI
### Block 5: Ops Hardening
### Block 6: Pre-Launch Polish

---

## 11. Key Files to Read First

1. `LAUNCH_AGENDA.md` — what's left to build
2. `LAUNCH_PLAN.md` — business strategy, pricing, go-to-market
3. `api/main.py` — app entry point, all routes registered here
4. `api/auth.py` — how API key auth works
5. `pipeline/ingest_dol.py` — DOL API ingestion with rate limiting
6. `pipeline/entity_resolution.py` — Splink + post-Splink name merge
7. `dbt/models/gold/employer_profile.sql` — risk scoring logic
8. `pipeline/sync.py` — shadow-table swap to Postgres
9. `osha_compliance_arch_v6.md` — original architecture doc (v6.2 with our amendments)

---

## 12. Known Issues / Gotchas

1. **WHD staging model errors on every dbt run** because `raw_whd_actions` doesn't always exist. Use `dbt run --select staging silver` and ignore the WHD error if no WHD data is loaded.

2. **DuckDB single-writer lock** — only one process can write to the DuckDB file at a time. Kill any stuck Python processes before running pipeline steps: `kill -9 $(lsof -t /data/duckdb/employer_compliance.duckdb)`

3. **Postgres table ownership** — `employer_profile` is owned by `pipeline_user` (not `api`). After any manual table recreation, run: `ALTER TABLE employer_profile OWNER TO pipeline_user;` and `GRANT SELECT ON employer_profile TO api;`

4. **Uvicorn runs on port 8001** (not 8000) because port 8000 is used by an existing openlabels project on scrubiq.

5. **nginx config in repo uses port 8000** — the deployed version on scrubiq was sed'd to 8001. When redeploying nginx config, run: `sed 's/127.0.0.1:8000/127.0.0.1:8001/g' nginx.conf > /etc/nginx/sites-available/fastdol`

6. **Splink v4.0.6 bug** — EM training fails with `ValueError: Expected sql condition to refer to one column`. Workaround: skip EM, use only `estimate_u_using_random_sampling`. See entity_resolution.py.

7. **DOL API key** must be sent as BOTH header AND query param. WHD endpoint rejects header-only auth. See `fetch_one_page` in ingest_dol.py.

8. **`openpyxl` needed on pipeline server** for NAICS seed generation. Install: `pip install openpyxl`

9. **Previous pipeline server (65.21.49.4) was compromised** via exposed Docker daemon. Malware was found and removed, server was deleted. Current pipeline server has Docker `iptables: false` and UFW deny-all defaults.

10. **Hetzner abuse report** was filed against the old pipeline server IP. If you reuse that IP, it may still be blocked. Current pipeline server uses a different IP (46.224.150.38).

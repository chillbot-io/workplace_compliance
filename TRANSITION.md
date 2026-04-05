# FastDOL Transition Document

**Date:** April 5, 2026
**Session:** Claude Code session_01Sp4DDSTfgDuwrNDvZQ45N6
**Purpose:** Complete context transfer for continuing development

---

## 1. What FastDOL Is

FastDOL is a B2B data API that aggregates federal DOL (Department of Labor) enforcement data into normalized employer risk profiles. Customers query by employer name, zip, or state and get back risk scores, inspection history, and violation detail.

**Target buyers:** Insurance underwriters, staffing agencies, compliance consultants, supply chain teams.

**Live at:**
- Website: https://www.fastdol.com (Vercel, dark navy theme)
- API: https://api.fastdol.com

---

## 2. What Was Done This Session

### Major Architecture Changes
- **Replaced Splink with deterministic entity resolution** — name_normalized + state + zip5 grouping. No probabilistic matching. Each profile = one employer at one location.
- **Removed address merge** — was falsely merging different businesses at same address (e.g., 114 companies at a Las Vegas hotel merged into one profile). CRITICAL bug found and fixed.
- **All-time aggregation** — changed from 5-year window to all-time. All employer records since 1972 are in the database. Risk scores use time-decay (100% for <3yr, 80% for 3-4yr, 60% for 4-5yr, 40% for 5yr+).
- **Multi-source profiles** — profiles created from OSHA AND WHD data. WHD-only employers (restaurants, janitorial) now have their own profiles. Previously 97.8% of WHD data was invisible.
- **Renamed _5yr fields** — all field names updated across entire stack (gold model, sync, API, web) to remove misleading "_5yr" suffix since data is now all-time.

### Data Sources
- **OSHA Inspections:** 2.5M records (DOL API v4, nightly refresh)
- **OSHA Violations:** 1.8M records (DOL API v4, nightly refresh)
- **WHD Enforcement:** 355K records (DOL API v4, weekly refresh)
- **MSHA Mine Safety:** 3M violations, 1.1M inspections, 91K mines (bulk download from arlweb.msha.gov, weekly refresh)
- **OFCCP:** staging/silver models built, blocked on data.dol.gov download URLs
- **OFLC:** staging/silver models built, blocked on data.dol.gov download URLs
- **SEC EDGAR EIN:** dead end — API doesn't expose EIN
- **SAM.gov EIN:** dead end — public key doesn't get EIN

### Current Data State
- **2,163,628 employer profiles** (all-time, from OSHA + WHD)
- **38,055 with parent company matching** (134-entry curated seed)
- **1,508 with MSHA data matched**
- Zero duplicate employer_ids
- Zero null employer names

### Entity Resolution
- **Deterministic only** — `name_normalized + state + zip5` = one profile
- **No Splink, no probabilistic matching** — precision > recall
- **Name normalization:** strips leading numbers, corp suffixes (INC, LLC, CORP, etc.), trailing numbers, non-alphanumeric chars
- **Parent company matching:** 134-entry curated seed, prefix matching, longest match wins. Display-only, never merges profiles.

### Risk Scoring
- **All-time aggregation with time-decay:**
  - 100% weight for activity < 3 years old
  - 80% for 3-4 years
  - 60% for 4-5 years
  - 40% for 5+ years (never zero — history matters)
- **OSHA components:** willful (30pts), repeat (15pts), serious (3pts), other (0.5pts), penalties (up to 15pts)
- **WHD components:** back wages (up to 8pts), cases (up to 4pts), employees violated (up to 3pts)
- **Risk tiers:** HIGH (willful, >$100k penalties/backwages), ELEVATED, MEDIUM, LOW
- **Calibrated to OSHA penalty ratios** (willful/repeat 10x serious)

### Website (Next.js 16, Tailwind, Vercel)
- Dark navy theme with violet accents
- Landing page with demo search (per-IP rate limited, 3 results max)
- Auth flow: signup → email verification → API key shown once → search
- Employer detail page with clickable inspection reports
  - Each inspection expandable to show violation-level detail
  - Citation ID, type badge (Willful/Repeat/Serious/Other), gravity, penalty
  - Initial vs current penalty (shows strikethrough if reduced)
- CSV bulk upload page
- Pricing page with 4 tiers
- API docs with Swagger/ReDoc links
- 38 security issues fixed from audit (middleware, CSRF, input validation, etc.)

### Pipeline
- **8-step nightly pipeline** (no more Splink step):
  1. Ingest OSHA from DOL API
  2. Load bronze into DuckDB
  3. dbt seed + staging + silver
  4. Parse addresses
  5. dbt gold (deterministic matching + scoring)
  6. Data quality gate (blocks sync on critical failure)
  7. Sync to Postgres (shadow-table swap)
  8. Validate sync
- **Weekly:** WHD enforcement data refresh
- **Weekly:** MSHA bulk download refresh
- **Monthly:** SEC subsidiary data + parent company seed reload
- **Crontab installed** on pipeline server

### Inspection Detail
- `inspection_detail` table: 354,753 inspections synced to Postgres
- `violation_detail` table: 631,211 violations synced to Postgres
- API endpoint: `GET /v1/employers/{id}/inspections`
- API endpoint: `GET /v1/inspections/{activity_nr}/violations`
- Detail page loads violations on-demand when inspection card is clicked

---

## 3. What Needs to Be Done Next (Morning Session)

### P0 — Must fix before launch
1. **Ground truth validation** — run `validate_ground_truth.py`, manually check 10+ employers against osha.gov. Nobody has verified our data against the source yet.
2. **MSHA columns in Postgres sync** — `sync.py` doesn't push `msha_violations` or `msha_assessed_penalties`. API returns zeros for everyone.
3. **End-to-end signup flow test** — nobody has actually signed up, verified email, got API key, and searched through the website.
4. **ENV=production on API server** — verify test keys are blocked.
5. **Deploy latest code** — API server needs git pull + restart. Vercel needs merge to main.

### P1 — Before launch
6. **Change default passwords** (password1/2/3 in Postgres)
7. **Stripe test → live mode** (new keys, webhook endpoint)
8. **Resend email verification** — is it actually sending?
9. **E&O insurance** — get the $620/yr Plus plan
10. **Analytics** — add Plausible or PostHog so you know who visits

### P2 — After launch
11. **OFCCP + OFLC data** — find download URLs on data.dol.gov
12. **More parent company entries** — auto-detect from data
13. **MSHA geocoding** — map lat/long to zip5 for better matching
14. **HIL review queue UI** — for edge case review
15. **Python SDK**
16. **Grafana monitoring**

---

## 4. Known Issues / Gotchas

1. **DuckDB single-writer lock** — kill stuck processes: `kill -9 $(lsof -t /data/duckdb/employer_compliance.duckdb)`
2. **Uvicorn runs on port 8001** (not 8000) — port 8000 used by openlabels
3. **nginx config uses port 8000** — deployed version sed'd to 8001
4. **Database is named `stablelabel`** not `employer_compliance` — legacy name
5. **DOL API key** must be sent as BOTH header AND query param (WHD requires query param)
6. **WHD offset 130000** returns persistent 502 — bad batch skip logic handles it
7. **Postgres table ownership** — `employer_profile` owned by `pipeline_user`, migrations need that user
8. **MSHA dates are MM/DD/YYYY** — staging model uses STRPTIME to parse
9. **OFCCP/OFLC NOT on DOL API v4** — need bulk download from data.dol.gov
10. **Splink is REMOVED** — `entity_resolution.py` still exists but is not called. Pipeline uses deterministic matching in dbt gold model.
11. **parent_companies.csv** — 134-entry curated seed, loads via dbt seed
12. **Migration 006** renames _5yr columns — must run on API server before deploying
13. **2.1M profiles** — all-time data, most are historical with no recent activity. This is correct.
14. **NAICS null rate 14.2%** — many older inspections and WHD records don't have NAICS codes

---

## 5. API Endpoints

### Employer Data (X-Api-Key header)
- `GET /v1/employers?name=&zip=&state=&naics=&limit=&offset=` — search
- `GET /v1/employers/parent?name=` — parent company rollup
- `GET /v1/employers/{id}` — direct lookup
- `GET /v1/employers/{id}/inspections` — inspection history
- `GET /v1/employers/{id}/risk-history` — risk snapshots
- `GET /v1/inspections/{activity_nr}/violations` — violation detail
- `POST /v1/employers/batch` — bulk lookup (100 sync)
- `POST /v1/employers/upload-csv` — CSV upload → CSV results
- `POST /v1/employers/{id}/feedback` — report bad matches
- `GET /v1/industries/{naics4}` — industry benchmarks
- `GET /v1/industries/naics-codes` — NAICS code discovery

### Auth
- `POST /auth/signup`, `GET /auth/verify`, `POST /auth/login`
- `POST /auth/forgot-password`, `POST /auth/reset-password`

### Dashboard
- `GET /dashboard/keys`, `POST /dashboard/keys`
- `POST /dashboard/keys/{id}/rotate`, `DELETE /dashboard/keys/{id}`

### Billing
- `POST /billing/checkout`, `POST /webhooks/stripe`

### System
- `GET /v1/health` — public health check

---

## 6. Infrastructure

```
Pipeline Server (CCX33)                    API Server (CPX42)
46.224.150.38 / 10.0.0.3                  88.198.218.234 / 10.0.0.2
8 vCPU, 32GB RAM                          8 vCPU, 16GB RAM

/opt/employer-compliance/                  nginx (TLS) → uvicorn :8001
  pipeline/  (ingestion + ETL)             PostgreSQL 16 ← pgBouncer
  dbt/       (transforms + seeds)          Metabase (:3000)
  .env.pipeline                            .env.api

/data/                                     systemd: fastdol-api.service
  bronze/    (raw Parquet)
  duckdb/    (employer_compliance.duckdb)
  backups/   (7-day retention)
  dq_snapshots/ (daily quality metrics)

Website: Vercel (www.fastdol.com)
  web/ directory in monorepo
  Env vars: API_URL, DEMO_API_KEY

Connected via Hetzner vSwitch (10.0.0.0/24)
```

---

## 7. Key Files

1. `PIPELINE_ARCHITECTURE.md` — diagram, schedule, risk scoring (needs update)
2. `PRE_LAUNCH_CHECKLIST.md` — deployment + security steps
3. `dbt/models/gold/employer_profile.sql` — THE core file. Deterministic matching, all-time aggregation, time-decay scoring, multi-source profiles
4. `pipeline/ingest_dol.py` — OSHA/WHD ingestion with rate limiting + bad batch skip
5. `pipeline/ingest_msha.py` — MSHA bulk download
6. `pipeline/sync.py` — DuckDB → Postgres shadow-table swap + inspection detail sync
7. `pipeline/validate_data.py` — data quality gate (blocks sync on critical)
8. `api/routes/employers.py` — all employer endpoints + parent rollup
9. `web/src/app/page.tsx` — landing page with demo search
10. `web/src/app/employers/[id]/page.tsx` — employer detail with inspection reports

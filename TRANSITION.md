# FastDOL Transition Document

**Date:** April 4, 2026
**Session:** Claude Code session_01Sp4DDSTfgDuwrNDvZQ45N6
**Purpose:** Complete context transfer for continuing development

---

## 1. What Was Done This Session

### Data Pipeline Fixes
- **WHD offset bug fixed** — `ingest_dol.py` was incrementing offset by 5000 (global PAGE_SIZE) instead of 1000 (WHD page_size), skipping 4/5 of records
- **Bad batch skip logic** — After 3 consecutive failures at same offset, skips ahead and retries with smaller page sizes at the end
- **Added `--source` CLI filter** — `python ingest_dol.py whd_actions` runs just one source
- **Added `naic_cd` to WHD fields** — was missing from ingestion
- **WHD fully downloaded** — 354,965 records loaded into DuckDB
- **WHD integrated into Splink** — entity resolution now clusters OSHA + WHD together (2.84M records)
- **WHD integrated into risk scoring** — gold model combines OSHA violations + WHD back wages/employees violated

### Entity Resolution Improvements
- **Added zip5 + address_key as Splink comparisons** — reduces duplicate profiles (was 10.7k dupes, now ~39)
- **Post-Splink name merge** — clusters sharing same normalized name merged across states
- **Clusters:** 1,538,088 → 1,132,995 after name merge
- **Profiles:** 190,873 (down from 249k due to dedup + junk filtering)

### Search & API Redesign
- **Search endpoint** — added zip filter, pagination (offset/limit), flat results list, risk_score desc sort
- **Lower similarity threshold** — 0.15 (0.1 for short names) to catch misspellings
- **Zero-result searches** — don't charge quota, return suggestions
- **Batch endpoint** — added state/zip/city filters, match confidence, sync limit 25→100
- **CSV upload endpoint** — `POST /v1/employers/upload-csv`, accepts CSV, returns CSV with risk profiles
- **Parent company rollup** — `GET /v1/employers/parent?name=Amazon`, aggregate risk across all locations
- **Parent endpoint** changed from path param to query param (URL encoding fix)
- **Risk note** added for 0-score employers ("no inspections ≠ safe")
- **data_notes** added to search responses (freshness, coverage, scoring methodology)

### Parent Company Matching
- **OpenSanctions SEC Exhibit 21 data** — 613k parent→subsidiary mappings downloaded
- **Manual overrides** — ~55 national chains (Amazon, Walmart, Target, FedEx, etc.) with prefix matching
- **Dual join strategy** — exact match for SEC data (fast), prefix match for manual overrides
- **Match rate:** 3.1% (5,989 of 190,873 profiles)
- **CSV sniffer issues** — parent_companies.csv too messy for dbt seed, loaded directly via `load_parent_companies.py`

### Risk Score Calibration
- **Weights recalibrated** to match OSHA penalty ratios:
  - Willful: 30 pts (cap 50) — was 25
  - Repeat: 15 pts (cap 30) — was 10
  - Serious: 3 pts (cap 20) — was 5
  - WHD back wages: up to 8 pts
  - WHD cases: up to 4 pts
  - WHD employees violated: up to 3 pts
- **Risk tier** now includes WHD signals (>$100k back wages = HIGH)

### Data Quality
- **Junk records filtered** — UNKNOWN, UNKNOWN CONTRACTOR, NA, NONE, TEST, TBD excluded from gold model
- **Name normalization** — strips leading numbers (`65318 AMAZON COM SERVICES` → `AMAZON COM SERVICES`)
- **NAICS 2017 codes added** — 139 codes from 2017 edition merged into seed (was 2,012, now 2,151)
- **DQ gate rebuilt** — 5 dimensions (completeness, freshness, consistency, distribution, referential), blocks sync on critical failure, saves daily snapshots for regression detection
- **ER validation script** — checks cluster sizes, over-merging, under-merging, known company spot checks
- **Ground truth validation script** — generates 30 employer samples for manual osha.gov spot-checking

### Pipeline Orchestration
- **Multi-schedule pipeline** — nightly (OSHA), weekly (WHD), monthly (SEC subsidiaries + NAICS)
- **run_pipeline.sh** — 9 steps, DQ gate before sync (step 7 blocks if critical fails)
- **run_weekly.sh** — WHD ingestion + bronze load
- **run_monthly.sh** — SEC subsidiaries + parent company load + NAICS update + seed reload
- **Crontab installed** on pipeline server

### Ops
- **API deployed** with new endpoints on API server
- **Migration 004 applied** — zip/state/city indexes, location_count, parent_name columns
- **Pre-launch checklist** created (PRE_LAUNCH_CHECKLIST.md)
- **Pipeline architecture doc** created (PIPELINE_ARCHITECTURE.md)

### Dead Ends
- **SAM.gov** — public API key doesn't expose EIN (FOUO access required)
- **SEC EDGAR XBRL** — EntityTaxIdentificationNumber not available via public API
- **EIN** — no free public source exists for employer tax IDs

---

## 2. Current Data State

### DuckDB (pipeline server: /data/duckdb/employer_compliance.duckdb)

| Table | Records | Notes |
|-------|---------|-------|
| raw_osha_inspections | 2,485,000 | Full history |
| raw_osha_violations | 1,805,000 | Linked by activity_nr |
| raw_whd_actions | 354,965 | Full WHD enforcement since FY2005 |
| osha_inspection_norm | ~2,485,000 | Silver: normalized names, joined violations |
| whd_norm | ~354,965 | Silver: normalized WHD data |
| employer_clusters | 2,839,808 | Splink output (OSHA + WHD combined) |
| cluster_id_mapping | 1,132,995 | Stable employer_id UUIDs |
| employer_profile | 190,912 | Gold: risk-scored profiles |
| parent_companies | 613,451 | SEC Exhibit 21 + manual overrides |
| naics_2022 | 2,151 | 2017 + 2022 editions |

### Postgres (API server)

| Table | Records |
|-------|---------|
| employer_profile | 190,873 | 

### Risk Distribution

- HIGH: 4,278 (2.2%)
- ELEVATED: 2,384 (1.2%)
- MEDIUM: 88,937 (46.6%)
- LOW: 95,313 (49.9%)

---

## 3. API Endpoints (all live at api.fastdol.com)

### Employer Data (requires X-Api-Key header)
- `GET /v1/employers?name=&zip=&state=&naics=` — fuzzy search, paginated, risk_score desc
- `GET /v1/employers/parent?name=` — parent company rollup (aggregate + locations)
- `GET /v1/employers/{id}` — direct lookup (301 redirect for superseded IDs)
- `GET /v1/employers/{id}/inspections` — inspection history (not metered)
- `GET /v1/employers/{id}/risk-history` — nightly risk snapshots
- `POST /v1/employers/batch` — bulk lookup (100 sync, state/zip/city filters)
- `POST /v1/employers/upload-csv` — CSV upload → CSV results (500 row limit)
- `POST /v1/employers/{id}/feedback` — report bad matches (not metered)
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
- `GET /billing/success`, `GET /billing/cancel`

### System
- `GET /v1/health` — public health check

---

## 4. Known Issues / Gotchas

1. **DuckDB single-writer lock** — kill stuck processes before pipeline: `kill -9 $(lsof -t /data/duckdb/employer_compliance.duckdb)`
2. **Uvicorn runs on port 8001** (not 8000) — port 8000 used by openlabels
3. **nginx config uses port 8000** — deployed version sed'd to 8001
4. **Database is named `stablelabel`** not `employer_compliance` — legacy name, works fine
5. **parent_companies.csv** can't load via dbt seed (CSV sniffer fails on special chars) — use `load_parent_companies.py` instead
6. **Splink v4.0.6** — EM training fails, use only `estimate_u_using_random_sampling`
7. **DOL API key** must be sent as BOTH header AND query param (WHD requires query param)
8. **WHD offset 130000** returns persistent 502 — bad batch skip logic handles it
9. **Postgres table ownership** — `employer_profile` owned by `pipeline_user`, migrations need that user
10. **39 duplicate employer_ids** in gold model from parent match join — deduplicated in sync.py

---

## 5. Pipeline Schedule (crontab installed)

| Schedule | Cron | Script |
|----------|------|--------|
| Nightly 2AM | `0 2 * * *` | `run_pipeline.sh` (OSHA + full pipeline) |
| Weekly Sun 1AM | `0 1 * * 0` | `run_weekly.sh` (WHD ingestion) |
| Monthly 1st | `0 0 1 * *` | `run_monthly.sh` (SEC subsidiaries + NAICS) |
| Backup 4AM | `0 4 * * *` | `backup.sh` |
| Health 8:30AM | `30 8 * * *` | `check_health.sh` |
| Key rotation | `0 * * * *` | `rotate_keys.py` |
| Disk check | `0 */6 * * *` | `check_disk.sh` |
| Usage reset | `5 0 1 * *` | `reset_monthly_usage.py` |

---

## 6. What's Next

### Immediate (next session)
1. **Website** — the only blocker to revenue:
   - Landing page (hero, value prop, data sources)
   - Search page (name + zip/state, results with risk profiles)
   - CSV bulk upload page (drag & drop, get results CSV)
   - Signup/login pages (calls existing auth API)
   - Pricing page (tiers, Stripe checkout)
   - API key display (shown once after verification)
   - Stack: Next.js + Tailwind + shadcn/ui, deploy to Vercel
2. **Ground truth validation** — run `validate_ground_truth.py`, spot-check 10+ employers against osha.gov
3. **Security hardening** — change default passwords, Stripe live mode, Sentry DSN (see PRE_LAUNCH_CHECKLIST.md)

### After first customers
4. **Additional data sources** (each ~1 day with existing pipeline framework):
   - MSHA (mine safety) — bulk download, pipe-delimited flat files
   - FMCSA (trucking) — REST API, free key via Login.gov
   - EPA ECHO (environmental) — bulk CSV, 1.5M facilities
   - OFCCP (federal contractor compliance) — DOL data catalog CSV
   - NLRB (labor relations) — GitHub scraper or web search tool
5. **HIL review queue UI** — review borderline Splink pairs (0.80-0.85 match probability)
6. **API documentation** — endpoint reference, getting started guide, code examples
7. **Python SDK** — thin wrapper over API (search, batch, upload, parent)
8. **Grafana monitoring** — pipeline metrics, API response times, DQ trends

### Key files to read first
1. `PIPELINE_ARCHITECTURE.md` — full pipeline diagram, schedule, risk scoring methodology
2. `PRE_LAUNCH_CHECKLIST.md` — every step to go live
3. `LAUNCH_AGENDA.md` — block-by-block launch plan
4. `api/routes/employers.py` — all employer endpoints
5. `dbt/models/gold/employer_profile.sql` — risk scoring logic
6. `pipeline/entity_resolution.py` — Splink + name merge
7. `pipeline/ingest_dol.py` — DOL API ingestion with rate limiting
8. `pipeline/validate_data.py` — data quality gate

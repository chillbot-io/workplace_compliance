# FastDOL Launch Agenda

**Created:** April 2026
**Target:** ~12 working days to launch

---

## Block 1: Data Quality (Days 1-3)

*Nothing else matters if the data is wrong.*

| # | Task | Effort | Depends On | Status |
|---|------|--------|-----------|--------|
| 1.1 | Verify violation→inspection join (are 400k violations linking?) | 30 min | — | |
| 1.2 | Fix entity resolution — merge same-name employers across locations | 3 hours | 1.1 | |
| 1.3 | Add zip field to sync/API response (currently null) | 15 min | — | |
| 1.4 | Add naics_2022.csv seed → naics_description in responses | 1 hour | — | |
| 1.5 | Add confidence_tier to Gold model | 30 min | — | |
| 1.6 | Add svep_flag to sync.py columns | 15 min | — | |
| 1.7 | WHD data ingestion + integrate into Gold model | 1 day | 1.2 | |
| 1.8 | Add data quality checks to pipeline (null rates, join rates, distributions) | 2 hours | 1.7 | |
| 1.9 | Manual validation — check 20 employers against osha.gov | 1 hour | 1.7 | |
| 1.10 | Full pipeline re-run with all fixes | 2 hours | 1.1-1.9 | |
| 1.11 | Install crontab on pipeline server (nightly runs) | 5 min | 1.10 | |

---

## Block 2: Web Frontend (Days 4-10)

*Customers need a UI, not curl.*

| # | Task | Effort | Depends On | Status |
|---|------|--------|-----------|--------|
| 2.1 | Next.js project setup (repo, Tailwind, shadcn/ui) | 2 hours | — | |
| 2.2 | Marketing landing page (hero, value prop, pricing table) | 1 day | 2.1 | |
| 2.3 | Signup + login pages (hit /auth endpoints) | Half day | 2.1 | |
| 2.4 | Dashboard home (usage stats, plan info, quick search) | 1 day | 2.3 | |
| 2.5 | API keys management page (create, rotate, revoke) | Half day | 2.3 | |
| 2.6 | Employer search page (type name → see risk profiles) | 1 day | 2.3 | |
| 2.7 | Billing page (Stripe customer portal integration) | Half day | 2.3 | |
| 2.8 | Metabase embedded dashboards (top violators, industry risk, heatmap) | 1 day | Metabase running | |
| 2.9 | Deploy frontend (Vercel or same nginx) | 2 hours | 2.2-2.7 | |

---

## Block 3: API Documentation (Days 8-9)

*Developers won't use an API without docs.*

| # | Task | Effort | Depends On | Status |
|---|------|--------|-----------|--------|
| 3.1 | API reference (all endpoints, request/response examples) | 1 day | Block 1 done | |
| 3.2 | Getting started guide (signup → first API call in 5 min) | 2 hours | 3.1 | |
| 3.3 | Authentication guide (API keys, scopes, test keys) | 1 hour | 3.1 | |
| 3.4 | Code examples (Python, JavaScript, curl) | 2 hours | 3.1 | |
| 3.5 | Deploy docs site (docs.fastdol.com or in-app) | 1 hour | 3.1-3.4 | |

---

## Block 4: HIL Review UI (Days 9-11)

*Improves entity resolution over time.*

| # | Task | Effort | Depends On | Status |
|---|------|--------|-----------|--------|
| 4.1 | Review UI page — two records side by side, Match/Non-Match/Skip buttons | 1 day | 2.1 | |
| 4.2 | Backend: GET /dashboard/review (next unreviewed pair) | 2 hours | — | |
| 4.3 | Backend: POST /dashboard/review/{id} (submit decision) | 1 hour | 4.2 | |
| 4.4 | Wire decisions back to pipeline (supervised Splink training when 200+ labels) | Half day | 4.1-4.3 | |

---

## Block 5: Operations Hardening (Days 10-11)

*Run unattended without breaking.*

| # | Task | Effort | Depends On | Status |
|---|------|--------|-----------|--------|
| 5.1 | test_fixtures seed data (50 hand-curated employers for sandbox) | 2 hours | Block 1 done | |
| 5.2 | End-to-end test of full flow (signup → verify → key → query → Stripe) | 1 hour | Block 2 done | |
| 5.3 | R2 offsite backups (currently local only) | Half day | — | |
| 5.4 | UptimeRobot on /v1/health | 15 min | — | |
| 5.5 | Sentry configured with real DSN | 15 min | — | |
| 5.6 | Deploy.sh tested end-to-end | 1 hour | — | |
| 5.7 | Metabase 4 core dashboards configured | 2 hours | Metabase running | |

---

## Block 6: Pre-Launch Polish (Day 12)

| # | Task | Effort | Depends On | Status |
|---|------|--------|-----------|--------|
| 6.1 | Remove all dev-mode fallbacks (HS256 JWT, test key bypass) | 30 min | — | |
| 6.2 | Change all passwords from password1/2/3 to strong passwords | 30 min | — | |
| 6.3 | Switch Stripe from test mode to live mode | 30 min | — | |
| 6.4 | Verify Resend sends from noreply@fastdol.com | 15 min | Done | |
| 6.5 | Security header check (securityheaders.com scan) | 15 min | — | |
| 6.6 | Load test API (50 concurrent requests, verify <500ms p95) | 1 hour | — | |
| 6.7 | Final manual QA — every endpoint, every page | 2 hours | Everything | |

---

## Post-Launch (Phase 4+)

| # | Task | Priority |
|---|------|----------|
| P1 | MSHA data ingestion (mining industry) | MEDIUM |
| P2 | FMCSA data ingestion (trucking industry) | MEDIUM |
| P3 | Async batch >25 items (R2 storage + job polling) | MEDIUM |
| P4 | Webhook subscriptions (4 CRUD endpoints + dispatch) | MEDIUM |
| P5 | Python + JavaScript SDKs | MEDIUM |
| P6 | EPA ECHO, OFCCP, NLRB, OFLC data sources | LOW |
| P7 | Supervised Splink training from review_queue labels | LOW |
| P8 | Bulk export / data licensing mode | LOW |
| P9 | Azure Container Apps migration | LOW |
| P10 | Test infrastructure (pytest, CI/CD, GitHub Actions) | LOW |
| P11 | Per-buyer Metabase accounts | LOW |

---

## Current State (as of April 3, 2026)

**What's working:**
- API live at https://api.fastdol.com with TLS
- 285,745 employer profiles (2,610 HIGH, 302 ELEVATED, 77,344 MEDIUM)
- 13 API endpoints (search, lookup, batch, risk-history, feedback, industry benchmarks, auth, dashboard, billing)
- Full auth flow (signup → email verification → login → JWT sessions)
- Stripe billing (Starter $79, Growth $249, Pro $599, Free 50/mo)
- Security audited (5 audits, 30+ fixes applied)
- Ops scripts ready (cron, backup, health check, disk monitoring)

**What's not working:**
- Entity resolution under-merging (same company at different addresses not grouped)
- Several response fields null (zip, naics_description, confidence_tier, svep_flag)
- No WHD data integrated
- No web frontend
- No API documentation
- No data quality validation in pipeline

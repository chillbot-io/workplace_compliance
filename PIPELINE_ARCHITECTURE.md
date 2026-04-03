# FastDOL Data Pipeline Architecture

## Data Sources & Update Frequency

```mermaid
flowchart TB
    subgraph Sources["External Data Sources"]
        DOL_OSHA["DOL API v4<br/>OSHA Inspections + Violations<br/>📅 Updated: Daily<br/>📊 ~2.5M inspections, ~400k violations"]
        DOL_WHD["DOL API v4<br/>WHD Enforcement Actions<br/>📅 Updated: Monthly<br/>📊 ~300k records"]
        SEC_EX21["OpenSanctions<br/>SEC Exhibit 21 Subsidiaries<br/>📅 Updated: Annually (10-K filings)<br/>📊 ~613k parent→subsidiary mappings"]
        SEC_EDGAR["SEC EDGAR XBRL<br/>Company EINs<br/>📅 Updated: Annually<br/>📊 ~8k public companies"]
    end

    subgraph Ingestion["Ingestion Scripts (Pipeline Server)"]
        ING_OSHA["ingest_dol.py osha_inspections osha_violations<br/>Burst rate limiting: 10 req/burst, 65s cooldown<br/>Checkpoint every 5k records"]
        ING_WHD["ingest_dol.py whd_actions<br/>Page size: 1000 (WHD limit)<br/>Checkpoint every 5k records"]
        ING_SUB["ingest_subsidiaries.py<br/>Downloads FtM JSON, extracts<br/>parent→subsidiary relationships"]
        ING_EIN["ingest_sec_ein.py<br/>Fetches EIN from XBRL company facts<br/>Rate limit: 10 req/sec"]
    end

    subgraph Bronze["Bronze Layer (Parquet files)"]
        B_OSHA_I["/data/bronze/osha_inspections/{date}/"]
        B_OSHA_V["/data/bronze/osha_violations/{date}/"]
        B_WHD["/data/bronze/whd_actions/{date}/"]
    end

    subgraph DuckDB["DuckDB (Pipeline Server)"]
        direction TB
        LOAD["load_bronze.py<br/>Parquet → DuckDB tables"]
        PARSE["parse_addresses.py<br/>USADDRESS → address_key"]

        subgraph dbt["dbt Transforms"]
            STAGING["Staging Models<br/>stg_osha_inspections<br/>stg_osha_violations<br/>stg_whd_actions"]
            SILVER["Silver Models<br/>osha_inspection_norm<br/>whd_norm<br/>(name normalization, NAICS join)"]
            SEEDS["Seeds<br/>naics_2022 (2,012 codes)<br/>insp_type, viol_type<br/>parent_companies (613k mappings)"]
            GOLD["Gold Model<br/>employer_profile<br/>(risk tier, risk score, trend,<br/>confidence, SVEP, parent_name,<br/>location_count)"]
        end

        SPLINK["entity_resolution.py<br/>Splink probabilistic matching<br/>+ post-Splink name merge<br/>→ cluster_id_mapping"]
    end

    subgraph Postgres["PostgreSQL (API Server via vSwitch)"]
        SYNC["sync.py<br/>Shadow-table swap<br/>(DuckDB → Postgres via COPY)"]
        EP["employer_profile<br/>~250k+ profiles"]
        RS["risk_snapshots<br/>Nightly snapshots for /risk-history"]
        VALIDATE["validate_sync.py + validate_data.py<br/>Row count verification<br/>Null rates, join rates, distributions"]
    end

    subgraph API["FastAPI (API Server)"]
        SEARCH["GET /v1/employers<br/>name + zip/state/naics search"]
        PARENT["GET /v1/employers/parent/{name}<br/>Parent company rollup"]
        BATCH["POST /v1/employers/batch<br/>Bulk lookup (up to 100 sync)"]
        UPLOAD["POST /v1/employers/upload-csv<br/>CSV bulk upload → CSV results"]
        DETAIL["GET /v1/employers/{id}<br/>Single employer lookup"]
    end

    %% Connections
    DOL_OSHA --> ING_OSHA --> B_OSHA_I & B_OSHA_V
    DOL_WHD --> ING_WHD --> B_WHD
    SEC_EX21 --> ING_SUB --> SEEDS
    SEC_EDGAR --> ING_EIN --> SEEDS

    B_OSHA_I & B_OSHA_V & B_WHD --> LOAD
    LOAD --> STAGING
    STAGING --> SILVER
    SILVER --> SPLINK
    SPLINK --> GOLD
    SEEDS --> GOLD
    PARSE -.-> SILVER

    GOLD --> SYNC --> EP & RS
    SYNC --> VALIDATE

    EP --> SEARCH & PARENT & BATCH & UPLOAD & DETAIL
```

## Pipeline Schedule

```
┌─────────────────────────────────────────────────────────────────┐
│                    PIPELINE SCHEDULE                             │
├─────────────────┬───────────────┬───────────────────────────────┤
│ Schedule        │ Cron          │ What Runs                     │
├─────────────────┼───────────────┼───────────────────────────────┤
│ NIGHTLY (2 AM)  │ 0 2 * * *     │ 1. ingest_dol.py (OSHA only)  │
│                 │               │ 2. load_bronze.py              │
│                 │               │ 3. dbt seed + staging + silver │
│                 │               │ 4. parse_addresses.py          │
│                 │               │ 5. entity_resolution.py        │
│                 │               │ 6. dbt gold                    │
│                 │               │ 7. sync.py → Postgres          │
│                 │               │ 8. validate_sync.py            │
│                 │               │ 9. validate_data.py            │
├─────────────────┼───────────────┼───────────────────────────────┤
│ WEEKLY (Sun 1AM)│ 0 1 * * 0     │ 1. ingest_dol.py whd_actions   │
│                 │               │ 2. load_bronze.py              │
│                 │               │    (then nightly picks up rest) │
├─────────────────┼───────────────┼───────────────────────────────┤
│ MONTHLY (1st)   │ 0 0 1 * *     │ 1. ingest_subsidiaries.py      │
│                 │               │ 2. ingest_sec_ein.py           │
│                 │               │    (then nightly picks up rest) │
├─────────────────┼───────────────┼───────────────────────────────┤
│ DAILY (4 AM)    │ 0 4 * * *     │ backup.sh (pg_dump + DuckDB)   │
├─────────────────┼───────────────┼───────────────────────────────┤
│ DAILY (8:30 AM) │ 30 8 * * *    │ check_health.sh                │
├─────────────────┼───────────────┼───────────────────────────────┤
│ HOURLY          │ 0 * * * *     │ rotate_keys.py                 │
├─────────────────┼───────────────┼───────────────────────────────┤
│ EVERY 6 HRS     │ 0 */6 * * *   │ check_disk.sh                  │
├─────────────────┼───────────────┼───────────────────────────────┤
│ MONTHLY (1st)   │ 0 0 1 * *     │ reset_monthly_usage.py         │
└─────────────────┴───────────────┴───────────────────────────────┘
```

## Infrastructure

```
┌─────────────────────────────────────────────────────┐
│              Pipeline Server (AX52)                  │
│              46.224.150.38 / 10.0.0.3                │
│              CCX33: 8 vCPU, 32GB RAM                 │
│                                                      │
│  ┌─────────────────────────────────────────────┐    │
│  │  /opt/employer-compliance/                   │    │
│  │    pipeline/     (ingestion + ETL scripts)   │    │
│  │    dbt/          (transforms + seeds)        │    │
│  │    .env.pipeline (DOL_API_KEY, DB creds)     │    │
│  └─────────────────────────────────────────────┘    │
│                                                      │
│  ┌─────────────────────────────────────────────┐    │
│  │  /data/                                      │    │
│  │    bronze/       (raw Parquet files)          │    │
│  │    duckdb/       (employer_compliance.duckdb) │    │
│  │    backups/      (local, 7-day retention)     │    │
│  └─────────────────────────────────────────────┘    │
│                                                      │
│  Cron: run_pipeline.sh (nightly)                     │
│         run_weekly.sh (Sundays)                      │
│         run_monthly.sh (1st of month)                │
└──────────────────────┬──────────────────────────────┘
                       │ vSwitch (10.0.0.0/24)
┌──────────────────────┴──────────────────────────────┐
│              API Server (CPX42)                       │
│              88.198.218.234 / 10.0.0.2               │
│              8 vCPU, 16GB RAM                        │
│                                                      │
│  nginx (TLS) → FastAPI (uvicorn :8001)               │
│  PostgreSQL 16 ← pgBouncer                           │
│  Metabase (:3000)                                    │
│                                                      │
│  systemd: fastdol-api.service                        │
└─────────────────────────────────────────────────────┘
```

## Data Flow Summary

1. **Ingestion** — Scripts fetch from external APIs, write Parquet to `/data/bronze/`
2. **Load** — `load_bronze.py` reads Parquet into DuckDB raw tables
3. **Transform** — dbt staging (rename columns) → silver (normalize names, addresses, NAICS) → gold (aggregate by employer, risk scoring)
4. **Entity Resolution** — Splink clusters similar establishments, `cluster_id_mapping` assigns stable UUIDs
5. **Enrichment** — Parent company seed maps subsidiaries to parent names, NAICS seed adds industry descriptions
6. **Sync** — Shadow-table swap: DuckDB gold → Postgres `employer_profile` via COPY + atomic RENAME
7. **Serve** — FastAPI reads from Postgres via pgBouncer, serves search/batch/upload/parent endpoints

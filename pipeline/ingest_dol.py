"""
pipeline/ingest_dol.py — Fetch OSHA Inspections, OSHA Violations, and WHD data from DOL API v2.
Writes raw Parquet files to /data/bronze/{source}/{date}/.

Usage:
    python pipeline/ingest_dol.py

Environment:
    DOL_API_KEY — API key from dataportal.dol.gov
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd

DOL_API_KEY = os.environ.get("DOL_API_KEY")
if not DOL_API_KEY:
    print("ERROR: DOL_API_KEY environment variable not set", file=sys.stderr)
    print("Register at https://dataportal.dol.gov to get a key", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://api.dol.gov/v2"
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/data/bronze"))
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# DOL API pagination limit
PAGE_SIZE = 200
RATE_LIMIT_DELAY = 0.5  # seconds between requests

SOURCES = {
    "osha_inspections": {
        "endpoint": "/Safety/Inspections",
        "fields": [
            "activity_nr", "estab_name", "site_address", "site_city",
            "site_state", "site_zip", "naics_code", "open_date",
            "close_case_date", "insp_type", "owner_type",
        ],
    },
    "osha_violations": {
        "endpoint": "/Safety/Violations",
        "fields": [
            "activity_nr", "citation_id", "viol_type", "gravity",
            "nr_instances", "penalty", "current_penalty", "abate_date",
            "issuance_date",
        ],
    },
    "whd_actions": {
        "endpoint": "/WHD/ComplianceActions",
        "fields": [
            "trade_nm", "legal_name", "street_addr_1_txt", "city_nm",
            "st_cd", "zip_cd", "naics_code_description",
            "findings_start_date", "findings_end_date",
            "bw_amt", "ee_violtd_cnt", "case_id",
        ],
    },
}


def fetch_source(name: str, config: dict) -> pd.DataFrame:
    """Fetch all records for a DOL API source with pagination."""
    endpoint = config["endpoint"]
    url = f"{BASE_URL}{endpoint}"
    headers = {"X-API-KEY": DOL_API_KEY}

    all_records = []
    offset = 0
    total_fetched = 0

    print(f"[{name}] Starting fetch from {endpoint}...")

    while True:
        params = {
            "$top": PAGE_SIZE,
            "$skip": offset,
            "$orderby": config["fields"][0],  # order by primary key field
        }

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[{name}] ERROR at offset {offset}: {e}", file=sys.stderr)
            if total_fetched > 0:
                print(f"[{name}] Continuing with {total_fetched} records fetched so far")
                break
            raise

        data = resp.json()

        # DOL API returns results in 'd' wrapper or directly as list
        if isinstance(data, dict) and "d" in data:
            records = data["d"].get("results", [])
        elif isinstance(data, dict) and "results" in data:
            records = data["results"]
        elif isinstance(data, list):
            records = data
        else:
            print(f"[{name}] Unexpected response format: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            break

        if not records:
            break

        all_records.extend(records)
        total_fetched += len(records)
        offset += PAGE_SIZE

        if total_fetched % 5000 == 0 or len(records) < PAGE_SIZE:
            print(f"[{name}] Fetched {total_fetched} records...")

        if len(records) < PAGE_SIZE:
            break

        time.sleep(RATE_LIMIT_DELAY)

    print(f"[{name}] Complete: {total_fetched} total records")

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    # Keep only the fields we care about (plus any extras the API returns)
    available_fields = [f for f in config["fields"] if f in df.columns]
    return df[available_fields]


def save_parquet(df: pd.DataFrame, source_name: str):
    """Save DataFrame as Parquet in bronze directory."""
    if df.empty:
        print(f"[{source_name}] WARNING: No records to save", file=sys.stderr)
        return

    out_dir = BRONZE_DIR / source_name / TODAY
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{source_name}.parquet"

    df.to_parquet(out_path, index=False)
    print(f"[{source_name}] Saved {len(df)} records to {out_path}")


def main():
    print(f"=== DOL Ingestion Starting — {TODAY} ===")
    start_time = time.time()
    errors = []

    for source_name, config in SOURCES.items():
        try:
            df = fetch_source(source_name, config)
            save_parquet(df, source_name)
        except Exception as e:
            print(f"[{source_name}] FAILED: {e}", file=sys.stderr)
            errors.append(source_name)

    elapsed = time.time() - start_time
    print(f"=== DOL Ingestion Complete — {elapsed:.1f}s ===")

    if errors:
        print(f"WARNING: {len(errors)} source(s) failed: {', '.join(errors)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

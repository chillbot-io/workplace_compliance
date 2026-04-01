"""
pipeline/ingest_dol.py — Fetch OSHA Inspections, OSHA Violations, and WHD data from DOL API v4.
Writes raw Parquet files to /data/bronze/{source}/{date}/.

DOL migrated from v2 (api.dol.gov) to v4 (apiprod.dol.gov) in late 2024.
v4 uses limit/offset pagination and API key as query parameter.

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

BASE_URL = "https://apiprod.dol.gov/v4/get"
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/data/bronze"))
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

PAGE_SIZE = 200
RATE_LIMIT_DELAY = 4.0  # seconds between requests — DOL v4 has undocumented rate limits (~15 req/min safe)
RATE_LIMIT_RETRIES = 5  # max retries on 429
RATE_LIMIT_BACKOFF = 60  # seconds to wait on 429 before retrying

SOURCES = {
    "osha_inspections": {
        "path": "osha/inspection",
        "sort_by": "open_date",
        "fields": [
            "activity_nr", "estab_name", "site_address", "site_city",
            "site_state", "site_zip", "naics_code", "open_date",
            "close_case_date", "insp_type", "owner_type",
        ],
    },
    "osha_violations": {
        "path": "osha/violation",
        "sort_by": "issuance_date",
        "fields": [
            "activity_nr", "citation_id", "viol_type", "gravity",
            "nr_instances", "initial_penalty", "current_penalty", "abate_date",
            "issuance_date",
        ],
    },
    "whd_actions": {
        "path": "whd/enforcement",
        "sort_by": "findings_end_date",
        "fields": [
            "trade_nm", "legal_name", "street_addr_1_txt", "cty_nm",
            "st_cd", "zip_cd", "naics_code_description",
            "findings_start_date", "findings_end_date",
            "bw_atp_amt", "ee_violtd_cnt", "case_id",
        ],
    },
}


def fetch_source(name: str, config: dict) -> pd.DataFrame:
    """Fetch all records for a DOL API v4 source with limit/offset pagination."""
    api_path = config["path"]
    url = f"{BASE_URL}/{api_path}/json"

    all_records = []
    offset = 0
    total_fetched = 0

    print(f"[{name}] Starting fetch from {url}...")

    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "sort": "desc",
            "sort_by": config.get("sort_by", "load_dt"),
            "X-API-KEY": DOL_API_KEY,
        }

        resp = None
        for retry in range(RATE_LIMIT_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=60)
                if resp.status_code == 429:
                    wait = RATE_LIMIT_BACKOFF * (retry + 1)
                    print(f"[{name}] Rate limited (429) at offset {offset}, waiting {wait}s (retry {retry + 1}/{RATE_LIMIT_RETRIES})...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if retry < RATE_LIMIT_RETRIES - 1:
                    wait = RATE_LIMIT_BACKOFF * (retry + 1)
                    print(f"[{name}] Request error at offset {offset}: {e}, retrying in {wait}s...")
                    time.sleep(wait)
                    continue
                print(f"[{name}] ERROR at offset {offset}: {e}", file=sys.stderr)
                if total_fetched > 0:
                    print(f"[{name}] Continuing with {total_fetched} records fetched so far")
                    resp = None
                    break
                raise

        if resp is None or resp.status_code == 429:
            print(f"[{name}] Stopping after {total_fetched} records (rate limit exhausted)")
            break

        data = resp.json()

        # v4 API wraps results in "data" key
        if isinstance(data, dict) and "data" in data:
            records = data["data"]
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
    # Keep only the fields we care about
    available_fields = [f for f in config["fields"] if f in df.columns]
    if available_fields:
        return df[available_fields]
    return df


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

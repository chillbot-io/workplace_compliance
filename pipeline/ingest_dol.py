"""
pipeline/ingest_dol.py — Fetch OSHA Inspections, OSHA Violations, and WHD data from DOL API v4.
Writes raw Parquet files to /data/bronze/{source}/{date}/.

Rate limit strategy: DOL v4 allows 16 requests per ~30-second window.
We fire 14 requests in a burst (with safety margin), then sleep 35 seconds.
This yields ~2800 records per cycle, ~5600 records/minute.

Saves checkpoints every CHECKPOINT_INTERVAL records so progress survives crashes.
Resumes from existing parquet if present.

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

import requests as req
import pandas as pd

DOL_API_KEY = os.environ.get("DOL_API_KEY")
if not DOL_API_KEY:
    print("ERROR: DOL_API_KEY environment variable not set", file=sys.stderr)
    print("Register at https://dataportal.dol.gov to get a key", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://apiprod.dol.gov/v4/get"
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/data/bronze"))
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

PAGE_SIZE = 5000          # DOL API allows up to 10k records or 5MB — 5k fits under 5MB limit
BURST_SIZE = 10           # requests per burst (13 actual limit, generous safety margin)
BURST_COOLDOWN = 65       # seconds of SILENCE after successful burst
RATE_LIMIT_WAIT = 120     # seconds to wait after hitting 429 (must be long enough to fully reset)
MAX_RETRIES = 10          # max consecutive 429s before giving up
CHECKPOINT_INTERVAL = 5000

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


def get_parquet_path(source_name: str) -> Path:
    out_dir = BRONZE_DIR / source_name / TODAY
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{source_name}.parquet"


def load_existing(source_name: str) -> pd.DataFrame:
    path = get_parquet_path(source_name)
    if path.exists():
        df = pd.read_parquet(path)
        print(f"[{source_name}] Resuming — {len(df)} records already on disk")
        return df
    return pd.DataFrame()


def save_checkpoint(df: pd.DataFrame, source_name: str):
    path = get_parquet_path(source_name)
    df.to_parquet(path, index=False)
    print(f"[{source_name}] Checkpoint: {len(df)} records saved")


def fetch_one_page(url: str, params: dict, source_name: str) -> tuple[list | None, bool]:
    """Fetch a single page. Returns (records, hit_rate_limit).
    On 429: returns (None, True) — caller must wait BURST_COOLDOWN before ANY request."""
    try:
        resp = req.get(url, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503):
            print(f"[{source_name}] {resp.status_code} at offset {params.get('offset')} — treating as rate limit")
            return None, True
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "data" in data:
            return data["data"], False
        return (data if isinstance(data, list) else []), False
    except req.RequestException as e:
        print(f"[{source_name}] Error at offset {params.get('offset')}: {e}", file=sys.stderr)
        return None, False


def fetch_source(name: str, config: dict) -> pd.DataFrame:
    """Fetch all records using burst strategy: BURST_SIZE requests, then cooldown."""
    api_path = config["path"]
    url = f"{BASE_URL}/{api_path}/json"

    existing_df = load_existing(name)
    all_records = existing_df.to_dict("records") if not existing_df.empty else []
    offset = len(all_records)
    total_fetched = len(all_records)
    last_checkpoint = total_fetched
    done = False

    print(f"[{name}] Fetching from offset {offset}...")

    consecutive_429s = 0

    while not done:
        burst_start = time.time()
        burst_count = 0

        # Fire a burst of requests
        for _ in range(BURST_SIZE):
            params = {
                "limit": PAGE_SIZE,
                "offset": offset,
                "sort": "desc",
                "sort_by": config.get("sort_by", "load_dt"),
                "X-API-KEY": DOL_API_KEY,
            }

            records, hit_limit = fetch_one_page(url, params, name)

            if hit_limit:
                # Rate limited — stop burst, wait LONGER than normal cooldown
                consecutive_429s += 1
                if consecutive_429s >= MAX_RETRIES:
                    print(f"[{name}] Too many consecutive 429s, stopping")
                    done = True
                else:
                    print(f"[{name}] Rate limited, waiting {RATE_LIMIT_WAIT}s (attempt {consecutive_429s}/{MAX_RETRIES})...")
                    time.sleep(RATE_LIMIT_WAIT)
                break

            consecutive_429s = 0  # reset on success

            if records is None:
                # Network error — save what we have
                done = True
                break

            if not records:
                # No more data
                done = True
                break

            all_records.extend(records)
            total_fetched += len(records)
            offset += PAGE_SIZE
            burst_count += 1

            if len(records) < PAGE_SIZE:
                done = True
                break

            # Small delay within burst
            time.sleep(0.3)

        # Progress update
        if burst_count > 0:
            print(f"[{name}] {total_fetched} records ({burst_count} pages in {time.time()-burst_start:.1f}s)")

        # Checkpoint
        if total_fetched - last_checkpoint >= CHECKPOINT_INTERVAL:
            df = pd.DataFrame(all_records)
            available = [f for f in config["fields"] if f in df.columns]
            save_checkpoint(df[available] if available else df, name)
            last_checkpoint = total_fetched

        # Cooldown — ONLY after a successful burst, NOT after a rate limit hit
        # (rate limit already waited RATE_LIMIT_WAIT inside the burst loop)
        if not done and burst_count > 0:
            print(f"[{name}] Cooldown {BURST_COOLDOWN}s (no requests)...")
            time.sleep(BURST_COOLDOWN)

    print(f"[{name}] Complete: {total_fetched} total records")

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    available = [f for f in config["fields"] if f in df.columns]
    return df[available] if available else df


def save_parquet(df: pd.DataFrame, source_name: str):
    if df.empty:
        print(f"[{source_name}] WARNING: No records to save", file=sys.stderr)
        return
    path = get_parquet_path(source_name)
    df.to_parquet(path, index=False)
    print(f"[{source_name}] Final save: {len(df)} records to {path}")


def main():
    print(f"=== DOL Ingestion Starting — {TODAY} ===")
    start_time = time.time()
    errors = []

    source_list = list(SOURCES.items())
    for idx, (source_name, config) in enumerate(source_list):
        # Wait between sources to let rate limit fully reset
        if idx > 0:
            print(f"=== Waiting {RATE_LIMIT_WAIT}s between sources ===")
            time.sleep(RATE_LIMIT_WAIT)
        try:
            df = fetch_source(source_name, config)
            save_parquet(df, source_name)
        except Exception as e:
            print(f"[{source_name}] FAILED: {e}", file=sys.stderr)
            errors.append(source_name)

    elapsed = time.time() - start_time
    hours = elapsed / 3600
    print(f"=== DOL Ingestion Complete — {hours:.1f}h ({elapsed:.0f}s) ===")

    if errors:
        print(f"WARNING: {len(errors)} source(s) failed: {', '.join(errors)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

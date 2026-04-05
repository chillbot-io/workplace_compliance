"""
pipeline/ingest_msha.py — Download MSHA mine safety data from DOL Open Government portal.

Downloads pipe-delimited flat files for:
  - Mines: mine locations, operators, NAICS codes, status
  - Violations: violations issued from inspections since 2000
  - Inspections: inspection records since 2000

Source: https://arlweb.msha.gov/opengovernmentdata/ogimsha.asp
No API key required. Files updated weekly (Fridays).

Output: /data/bronze/msha/{date}/

Usage:
    python pipeline/ingest_msha.py
"""

import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests as req

BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/data/bronze"))
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# MSHA Open Government Data download URLs
DATASETS = {
    "msha_mines": {
        "url": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip",
        "delimiter": "|",
        "fields": [
            "MINE_ID", "MINE_NAME", "MINE_TYPE", "MINE_STATUS",
            "STATE", "FIPS_CNTY_CD", "FIPS_CNTY_NM",
            "PRIMARY_SIC", "PRIMARY_SIC_DESC", "PRIMARY_CANVASS_CD",
            "CURRENT_MINE_NAME", "CURRENT_MINE_TYPE", "CURRENT_MINE_STATUS",
            "CURRENT_CONTROLLER_ID", "CURRENT_CONTROLLER_NAME",
            "CURRENT_OPERATOR_ID", "CURRENT_OPERATOR_NAME",
            "LATITUDE", "LONGITUDE",
            "AVG_MINE_EMP_CNT", "AVG_MINE_EMP_CNT_COAL", "AVG_MINE_EMP_CNT_MNM",
        ],
    },
    "msha_violations": {
        "url": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Violations.zip",
        "delimiter": "|",
        "fields": [
            "EVENT_NO", "MINE_ID", "VIOLATION_NO", "VIOLATOR_ID",
            "VIOLATOR_NAME", "VIOLATION_OCCUR_DT", "VIOLATION_ISSUE_DT",
            "SECTION_OF_ACT", "SIG_SUB", "PROPOSED_PENALTY",
            "AMOUNT_PAID", "ASSESSED_PENALTY",
            "NEGLIGENCE", "GRAVITY", "INJ_ILLNESS",
        ],
    },
    "msha_inspections": {
        "url": "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Inspections.zip",
        "delimiter": "|",
        "fields": [
            "EVENT_NO", "MINE_ID", "INSPECTION_BEGIN_DT", "INSPECTION_END_DT",
            "INSP_TYPE_CD", "INSPECTED_LAST_DT",
            "TOTAL_VIOLATIONS", "TOTAL_S_AND_S_VIOLATIONS",
        ],
    },
}

USER_AGENT = "FastDOL/1.0 (compliance data aggregation)"


def download_dataset(name: str, config: dict) -> pd.DataFrame:
    """Download a zip file from MSHA and extract the pipe-delimited data."""
    url = config["url"]
    print(f"[{name}] Downloading from {url}...")

    resp = req.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
    resp.raise_for_status()

    # Extract zip
    z = zipfile.ZipFile(BytesIO(resp.content))
    file_names = z.namelist()
    print(f"[{name}] Zip contains: {file_names}")

    # Read the first txt/csv file
    data_file = [f for f in file_names if f.endswith(('.txt', '.csv'))][0]

    with z.open(data_file) as f:
        df = pd.read_csv(
            f,
            delimiter=config["delimiter"],
            dtype=str,
            on_bad_lines="skip",
            encoding="latin-1",
        )

    print(f"[{name}] Read {len(df):,} rows, {len(df.columns)} columns")

    # Keep only fields we need (if they exist)
    available = [c for c in config["fields"] if c in df.columns]
    if available:
        df = df[available]

    return df


def save_parquet(df: pd.DataFrame, name: str):
    """Save dataframe as parquet in bronze directory."""
    out_dir = BRONZE_DIR / name / TODAY
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"[{name}] Saved {len(df):,} rows to {path}")


def main():
    print(f"=== MSHA Data Download — {TODAY} ===\n")
    start = time.time()

    for name, config in DATASETS.items():
        try:
            df = download_dataset(name, config)
            save_parquet(df, name)
        except Exception as e:
            print(f"[{name}] FAILED: {e}", file=sys.stderr)

    elapsed = time.time() - start
    print(f"\n=== MSHA Download Complete — {elapsed:.0f}s ===")


if __name__ == "__main__":
    main()

"""
pipeline/ingest_sec_ein.py — Extract EIN data from SEC EDGAR for public companies.

Downloads companyfacts.zip (XBRL data) from SEC EDGAR and extracts
EntityTaxIdentificationNumber for each CIK. Produces a CSV seed file
that can be used to bridge OSHA employers to their EIN via name matching.

Output: dbt/seeds/sec_ein_bridge.csv
    columns: cik, company_name, ein, sic_code

Usage:
    python pipeline/ingest_sec_ein.py

No API key required — SEC EDGAR is free and public.
Rate limit: 10 requests/sec. We download one bulk file so this is fine.
"""

import json
import os
import sys
import zipfile
from pathlib import Path

import requests as req
import pandas as pd

# SEC requires a User-Agent header with contact info
USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "FastDOL/1.0 (compliance data aggregation; contact@fastdol.com)"
)

SUBMISSIONS_URL = "https://efts.sec.gov/LATEST/search-index?q=%22EntityTaxIdentificationNumber%22&dateRange=custom&startdt=2024-01-01&enddt=2026-12-31&forms=10-K"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANYFACTS_BASE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

OUTPUT_PATH = Path("dbt/seeds/sec_ein_bridge.csv")


def download_company_tickers() -> pd.DataFrame:
    """Download the SEC company tickers list (CIK + name + ticker)."""
    print("Downloading company tickers list...")
    resp = req.get(
        COMPANY_TICKERS_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"}, ...}
    rows = list(data.values())
    df = pd.DataFrame(rows)
    df.rename(columns={"cik_str": "cik", "title": "company_name"}, inplace=True)
    print(f"  {len(df)} companies in SEC tickers list")
    return df


def fetch_ein_for_cik(cik: int) -> str | None:
    """Fetch EIN from SEC XBRL company facts for a given CIK."""
    url = COMPANYFACTS_BASE.format(cik=str(cik).zfill(10))
    try:
        resp = req.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()

        # EIN is under dei:EntityTaxIdentificationNumber
        dei = data.get("facts", {}).get("dei", {})
        ein_data = dei.get("EntityTaxIdentificationNumber", {})
        units = ein_data.get("units", {})

        # Usually under "pure" or first available unit
        for unit_key, filings in units.items():
            if filings:
                # Get most recent filing's value
                latest = sorted(filings, key=lambda x: x.get("end", ""), reverse=True)
                if latest:
                    return latest[0].get("val")

        return None
    except Exception as e:
        return None


def main():
    print("=== SEC EDGAR EIN Extraction ===")

    # Step 1: Get company list
    tickers_df = download_company_tickers()

    # Step 2: Fetch EIN for each company
    # This will take a while (~8000 companies at 10 req/sec = ~15 min)
    import time

    results = []
    total = len(tickers_df)
    batch_count = 0

    for idx, row in tickers_df.iterrows():
        cik = row["cik"]
        name = row["company_name"]

        ein = fetch_ein_for_cik(cik)
        if ein:
            results.append({
                "cik": cik,
                "company_name": name.upper(),
                "ein": ein,
                "ticker": row.get("ticker", ""),
            })

        batch_count += 1

        # Progress update every 100
        if batch_count % 100 == 0:
            print(f"  [{batch_count}/{total}] {len(results)} EINs found so far...")

        # Rate limit: 10 req/sec → sleep 0.1s between requests
        time.sleep(0.1)

    print(f"\nExtracted {len(results)} EINs from {total} companies")

    if not results:
        print("WARNING: No EINs found", file=sys.stderr)
        sys.exit(1)

    # Step 3: Save as dbt seed CSV
    df = pd.DataFrame(results)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

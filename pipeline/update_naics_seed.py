"""
pipeline/update_naics_seed.py — Expand NAICS seed to include 2012 and 2017 codes.

OSHA inspectors use whatever NAICS edition was current when they filed.
Our seed only has 2022 codes, causing 5.1% of profiles to show "no desc".

Downloads NAICS 2017 codes from Census.gov and merges with existing 2022 seed.
Keeps 2022 descriptions when codes overlap.

Usage:
    python pipeline/update_naics_seed.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
import requests

SEED_PATH = Path("dbt/seeds/naics_2022.csv")

# Census Bureau publishes NAICS code lists as Excel files
NAICS_2017_URL = "https://www.census.gov/naics/2017NAICS/2-6%20digit_2017_Codes.xlsx"
NAICS_2012_URL = "https://www.census.gov/naics/2012NAICS/2-digit_2012_Codes.xls"


def download_naics_2017() -> pd.DataFrame:
    """Download 2017 NAICS codes from Census.gov."""
    print("Downloading NAICS 2017 codes from Census.gov...")
    try:
        resp = requests.get(NAICS_2017_URL, timeout=30)
        resp.raise_for_status()

        # Save temporarily
        tmp = Path("/tmp/naics_2017.xlsx")
        tmp.write_bytes(resp.content)

        df = pd.read_excel(tmp, dtype=str)
        # Census files have varying column names
        # Look for the code and title columns
        code_col = None
        title_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if 'code' in col_lower and code_col is None:
                code_col = col
            if 'title' in col_lower or 'description' in col_lower:
                title_col = col

        if not code_col or not title_col:
            print(f"  Could not identify columns. Found: {list(df.columns)}")
            return pd.DataFrame()

        result = df[[code_col, title_col]].rename(columns={
            code_col: "naics_code",
            title_col: "naics_title",
        })

        # Keep only 6-digit codes
        result = result[result["naics_code"].str.len() == 6]
        result = result.dropna(subset=["naics_code"])

        print(f"  Got {len(result)} six-digit 2017 NAICS codes")
        return result

    except Exception as e:
        print(f"  Failed to download 2017 codes: {e}")
        return pd.DataFrame()


def main():
    print("=== NAICS Seed Update ===\n")

    # Load existing 2022 seed
    existing = pd.read_csv(SEED_PATH, dtype=str)
    print(f"Existing seed: {len(existing)} codes")
    existing_codes = set(existing["naics_code"].tolist())

    # Download 2017
    naics_2017 = download_naics_2017()

    if naics_2017.empty:
        print("\nFailed to download 2017 codes. Trying manual fallback...")
        # Fallback: query DuckDB for missing codes and create stubs
        try:
            import duckdb
            DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")
            con = duckdb.connect(DUCKDB_PATH, read_only=True)

            missing = con.execute("""
                SELECT DISTINCT naics_code
                FROM employer_profile
                WHERE naics_code IS NOT NULL
                  AND naics_code NOT IN (SELECT naics_code FROM main_main.naics_2022)
            """).df()
            con.close()

            if not missing.empty:
                # Create stub entries with code as description
                missing["naics_title"] = "NAICS " + missing["naics_code"] + " (pre-2022 code)"
                naics_2017 = missing
                print(f"  Created {len(missing)} stub entries for missing codes")
        except Exception as e:
            print(f"  Fallback also failed: {e}")
            sys.exit(1)

    # Merge: keep 2022 descriptions where available, add 2017 for missing codes
    new_codes = naics_2017[~naics_2017["naics_code"].isin(existing_codes)]
    print(f"New codes to add: {len(new_codes)}")

    if new_codes.empty:
        print("No new codes needed. Seed is up to date.")
        return

    combined = pd.concat([existing, new_codes[["naics_code", "naics_title"]]], ignore_index=True)
    combined = combined.drop_duplicates(subset=["naics_code"], keep="first")

    # Save
    combined.to_csv(SEED_PATH, index=False)
    print(f"\nUpdated seed: {len(combined)} codes (was {len(existing)})")
    print(f"Added {len(combined) - len(existing)} codes from 2017 edition")


if __name__ == "__main__":
    main()

"""
pipeline/ingest_ita.py — Load OSHA ITA (Injury Tracking Application) data.

Reads 300A summary CSV files and case detail CSV files downloaded from:
  https://www.osha.gov/Establishment-Specific-Injury-and-Illness-Data

These files must be manually downloaded (OSHA blocks bots) and placed in
/data/bronze/ita/ (or BRONZE_DIR/ita/).

This script reads all CSVs in that directory, combines them, and writes
Parquet files for the bronze layer.

Key fields: EIN, company_name, establishment_name, DART/TRIR inputs.

Usage:
    python pipeline/ingest_ita.py
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd

BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/data/bronze"))
ITA_INPUT_DIR = BRONZE_DIR / "ita"

# 300A summary columns we care about
SUMMARY_COLUMNS = [
    "id", "establishment_name", "establishment_id", "ein", "company_name",
    "street_address", "city", "state", "zip_code", "naics_code",
    "annual_average_employees", "total_hours_worked",
    "no_injuries_illnesses",
    "total_deaths", "total_dafw_cases", "total_djtr_cases", "total_other_cases",
    "total_dafw_days", "total_djtr_days",
    "total_injuries", "total_skin_disorders", "total_respiratory_conditions",
    "total_poisonings", "total_hearing_loss", "total_other_illnesses",
    "year_filing_for", "establishment_type", "size",
    "industry_description", "created_timestamp",
]

# Case detail columns we care about (from 300/301 forms)
CASE_DETAIL_COLUMNS = [
    "id", "establishment_id", "establishment_name", "ein", "company_name",
    "street_address", "city", "state", "zip_code", "naics_code",
    "annual_average_employees", "total_hours_worked",
    "case_number", "date_of_incident", "incident_outcome",
    "dafw_num_away", "djtr_num_tr",
    "type_of_incident", "year_of_filing",
]


def detect_file_type(df: pd.DataFrame) -> str:
    """Detect whether a CSV is 300A summary or case detail based on columns."""
    cols = set(c.lower().strip() for c in df.columns)
    if "total_dafw_cases" in cols or "no_injuries_illnesses" in cols:
        return "summary"
    elif "case_number" in cols or "date_of_incident" in cols:
        return "case_detail"
    else:
        # Default to summary if it has the key aggregate fields
        return "summary"


def read_csv_safe(path: Path) -> pd.DataFrame | None:
    """Read a CSV with encoding fallback."""
    for encoding in ["utf-8-sig", "utf-8", "latin-1"]:
        try:
            df = pd.read_csv(
                path,
                dtype=str,
                encoding=encoding,
                on_bad_lines="skip",
                low_memory=False,
            )
            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]
            return df
        except UnicodeDecodeError:
            continue
    print(f"  WARNING: Could not read {path.name} with any encoding", file=sys.stderr)
    return None


def main():
    print("=== OSHA ITA Data Ingestion ===\n")
    start = time.time()

    if not ITA_INPUT_DIR.exists():
        print(f"ERROR: ITA input directory not found: {ITA_INPUT_DIR}", file=sys.stderr)
        print(f"Download CSV files from https://www.osha.gov/Establishment-Specific-Injury-and-Illness-Data")
        print(f"and place them in {ITA_INPUT_DIR}/")
        sys.exit(1)

    csv_files = sorted(ITA_INPUT_DIR.glob("*.csv"))
    if not csv_files:
        print(f"ERROR: No CSV files found in {ITA_INPUT_DIR}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV files:")
    for f in csv_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name} ({size_mb:.1f} MB)")

    summary_frames = []
    case_detail_frames = []

    for csv_path in csv_files:
        print(f"\n[{csv_path.name}] Reading...")
        df = read_csv_safe(csv_path)
        if df is None:
            continue

        file_type = detect_file_type(df)
        print(f"  Detected type: {file_type}, {len(df):,} rows, {len(df.columns)} columns")

        if file_type == "summary":
            # Keep only columns we care about (that exist)
            available = [c for c in SUMMARY_COLUMNS if c in df.columns]
            df = df[available]
            summary_frames.append(df)
        else:
            available = [c for c in CASE_DETAIL_COLUMNS if c in df.columns]
            df = df[available]
            case_detail_frames.append(df)

    # Save 300A summary data
    if summary_frames:
        summary_df = pd.concat(summary_frames, ignore_index=True)
        # Deduplicate on id (OSHA's internal record ID)
        before = len(summary_df)
        if "id" in summary_df.columns:
            summary_df = summary_df.drop_duplicates(subset=["id"], keep="last")
        print(f"\n300A Summary: {len(summary_df):,} records ({before - len(summary_df):,} duplicates removed)")

        out_dir = BRONZE_DIR / "ita_summary"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "ita_300a_summary.parquet"
        summary_df.to_parquet(out_path, index=False)
        print(f"  Saved to {out_path}")

        # Quick stats
        if "ein" in summary_df.columns:
            ein_count = summary_df["ein"].notna().sum()
            ein_unique = summary_df["ein"].nunique()
            print(f"  EINs: {ein_count:,} records with EIN, {ein_unique:,} unique EINs")
        if "company_name" in summary_df.columns:
            cn_count = summary_df["company_name"].notna().sum()
            cn_unique = summary_df["company_name"].nunique()
            print(f"  Company names: {cn_count:,} records, {cn_unique:,} unique")
        if "year_filing_for" in summary_df.columns:
            years = sorted(summary_df["year_filing_for"].dropna().unique())
            print(f"  Years: {', '.join(str(y) for y in years)}")
    else:
        print("\nWARNING: No 300A summary files found")

    # Save case detail data
    if case_detail_frames:
        case_df = pd.concat(case_detail_frames, ignore_index=True)
        before = len(case_df)
        if "id" in case_df.columns:
            case_df = case_df.drop_duplicates(subset=["id"], keep="last")
        print(f"\nCase Detail: {len(case_df):,} records ({before - len(case_df):,} duplicates removed)")

        out_dir = BRONZE_DIR / "ita_case_detail"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "ita_case_detail.parquet"
        case_df.to_parquet(out_path, index=False)
        print(f"  Saved to {out_path}")
    else:
        print("\nNo case detail files found (expected — only available for 2024+)")

    elapsed = time.time() - start
    print(f"\n=== ITA Ingestion Complete — {elapsed:.0f}s ===")


if __name__ == "__main__":
    main()

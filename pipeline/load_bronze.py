"""
pipeline/load_bronze.py — Load bronze Parquet files into DuckDB tables.
Run after ingest_dol.py and before dbt.

Usage:
    python pipeline/load_bronze.py
"""

import os
import sys
from pathlib import Path

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")
BRONZE_DIR = Path(os.environ.get("BRONZE_DIR", "/data/bronze"))


def find_latest_parquet(source_name: str) -> Path | None:
    """Find the most recent Parquet file for a source."""
    source_dir = BRONZE_DIR / source_name
    if not source_dir.exists():
        return None

    # Get most recent date directory
    date_dirs = sorted(source_dir.iterdir(), reverse=True)
    for d in date_dirs:
        parquet_files = list(d.glob("*.parquet"))
        if parquet_files:
            return parquet_files[0]
    return None


def main():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("SET memory_limit='20GB'")
    con.execute("SET threads=8")

    sources = [
        "osha_inspections", "osha_violations", "whd_actions",
        "msha_mines", "msha_violations", "msha_inspections",
    ]

    for source in sources:
        parquet_path = find_latest_parquet(source)
        if parquet_path is None:
            print(f"[{source}] WARNING: No parquet files found in {BRONZE_DIR / source}")
            continue

        table_name = f"raw_{source}"
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[{source}] Loaded {count} records into {table_name}")

    con.close()
    print("Bronze load complete.")


if __name__ == "__main__":
    main()

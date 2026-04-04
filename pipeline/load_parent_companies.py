"""
pipeline/load_parent_companies.py — Load parent_companies.csv directly into DuckDB.
Bypasses dbt seed which chokes on the CSV sniffer with 613k rows of messy SEC data.

Usage:
    python pipeline/load_parent_companies.py
"""

import os
import duckdb
import pandas as pd
from pathlib import Path

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")
CSV_PATH = Path("dbt/seeds/parent_companies.csv")


def main():
    print(f"Loading parent_companies from {CSV_PATH}...")

    df = pd.read_csv(CSV_PATH)
    print(f"  Read {len(df)} rows, columns: {list(df.columns)}")

    con = duckdb.connect(DUCKDB_PATH)

    # Drop and recreate in the same schema dbt seeds use
    con.execute("DROP TABLE IF EXISTS main_main.parent_companies")
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS main_main
    """)

    con.execute("""
        CREATE TABLE main_main.parent_companies (
            name_pattern VARCHAR,
            parent_name VARCHAR,
            match_type VARCHAR
        )
    """)

    con.execute("INSERT INTO main_main.parent_companies SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM main_main.parent_companies").fetchone()[0]
    print(f"  Loaded {count:,} rows into main_main.parent_companies")

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()

"""
pipeline/sync.py — Shadow-table swap: DuckDB → Postgres.
Reads employer_profile from DuckDB Gold layer, writes to Postgres via binary COPY,
then performs an atomic table swap.

Usage:
    python pipeline/sync.py
"""

import os
import sys
from datetime import date
from io import StringIO
from uuid import uuid4

import duckdb
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")


def sync():
    pipeline_run_id = os.environ.get("PIPELINE_RUN_ID", str(uuid4()))
    snapshot_date = date.today().isoformat()

    # Connect to DuckDB
    duck = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Read Gold employer profiles
    df = duck.execute("SELECT * FROM employer_profile").df()
    duck.close()

    if df.empty:
        print("ERROR: employer_profile is empty in DuckDB — aborting sync", file=sys.stderr)
        sys.exit(1)

    print(f"Syncing {len(df)} employer profiles to Postgres...")

    # employer_id comes from dbt Gold model (via cluster_id_mapping)
    # Convert to string if it's not already
    df["employer_id"] = df["employer_id"].astype(str)
    df["snapshot_date"] = snapshot_date
    df["pipeline_run_id"] = pipeline_run_id

    # Map DuckDB columns to Postgres employer_profile columns
    pg_columns = [
        "employer_id", "snapshot_date", "pipeline_run_id",
        "employer_name", "address", "city", "state", "naics_code",
        "osha_inspections_5yr", "osha_violations_5yr",
        "osha_serious_willful", "osha_total_penalties",
        "osha_open_date_latest", "osha_avg_gravity",
        "risk_tier", "risk_score", "trend_signal",
    ]

    # Build the dataframe with Postgres column names
    pg_df = df.rename(columns={
        "employer_name": "employer_name",
        "address": "address",
        "city": "city",
        "state": "state",
        "naics_code": "naics_code",
        "osha_inspections_5yr": "osha_inspections_5yr",
        "osha_violations_5yr": "osha_violations_5yr",
        "osha_willful_count_5yr": "osha_serious_willful",
        "osha_penalty_total_5yr": "osha_total_penalties",
        "osha_last_inspection_date": "osha_open_date_latest",
        "osha_avg_gravity": "osha_avg_gravity",
        "risk_tier": "risk_tier",
        "risk_score": "risk_score",
        "trend_signal": "trend_signal",
    })

    # Ensure all required columns exist
    for col in pg_columns:
        if col not in pg_df.columns:
            pg_df[col] = None

    pg_df = pg_df[pg_columns]

    # Cast integer columns — DuckDB SUM() produces floats
    int_cols = [
        "osha_inspections_5yr", "osha_violations_5yr", "osha_serious_willful",
    ]
    for col in int_cols:
        if col in pg_df.columns:
            pg_df[col] = pg_df[col].fillna(0).astype(int)

    # Connect to Postgres
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        # Step 1: Create staging table
        cur.execute("DROP TABLE IF EXISTS employer_profile_staging")
        cur.execute("CREATE TABLE employer_profile_staging (LIKE employer_profile INCLUDING ALL)")

        # Step 2: COPY data into staging via CSV stream
        buffer = StringIO()
        pg_df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
        buffer.seek(0)

        columns_str = ", ".join(pg_columns)
        cur.copy_expert(
            f"COPY employer_profile_staging ({columns_str}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buffer,
        )

        cur.execute("SELECT COUNT(*) FROM employer_profile_staging")
        staging_count = cur.fetchone()[0]
        print(f"Staging table loaded: {staging_count} rows")

        # Step 3: Atomic swap
        cur.execute("DROP TABLE IF EXISTS employer_profile_prev")
        cur.execute("BEGIN")
        cur.execute("ALTER TABLE employer_profile RENAME TO employer_profile_prev")
        cur.execute("ALTER TABLE employer_profile_staging RENAME TO employer_profile")
        cur.execute("COMMIT")

        # Step 4: Recreate the latest view
        cur.execute("DROP VIEW IF EXISTS employer_profile_latest")
        cur.execute("""
            CREATE VIEW employer_profile_latest AS
            SELECT DISTINCT ON (employer_id) *
            FROM employer_profile
            ORDER BY employer_id, snapshot_date DESC
        """)

        conn.commit()
        print(f"Shadow-table swap complete. {staging_count} profiles live.")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sync()

"""
pipeline/validate_sync.py — Post-sync validation.
Compares DuckDB row count vs Postgres row count.
Fails if mismatch exceeds 0.1%.
"""

import os
import sys

import duckdb
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")


def main():
    # DuckDB count
    duck = duckdb.connect(DUCKDB_PATH, read_only=True)
    duck_count = duck.execute("SELECT COUNT(*) FROM employer_profile").fetchone()[0]
    duck.close()

    # Postgres count
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM employer_profile")
    pg_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"DuckDB: {duck_count} rows")
    print(f"Postgres: {pg_count} rows")

    if duck_count == 0:
        print("FAIL: DuckDB employer_profile is empty", file=sys.stderr)
        sys.exit(1)

    if pg_count == 0:
        print("FAIL: Postgres employer_profile is empty", file=sys.stderr)
        sys.exit(1)

    drift = abs(duck_count - pg_count) / duck_count
    print(f"Drift: {drift:.4%}")

    if drift > 0.001:
        print(f"FAIL: Row count drift {drift:.4%} exceeds 0.1% threshold", file=sys.stderr)
        sys.exit(1)

    print("PASS: Row counts match within tolerance.")


if __name__ == "__main__":
    main()

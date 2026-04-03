"""
pipeline/validate_data.py — Data quality checks run after each pipeline stage.
Checks null rates, join rates, distributions, and flags anomalies.

Usage:
    python pipeline/validate_data.py
"""

import os
import sys

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")

CHECKS = []
WARNINGS = []
FAILURES = []


def check(name, passed, detail=""):
    if passed:
        CHECKS.append(f"  PASS: {name}")
    else:
        FAILURES.append(f"  FAIL: {name} — {detail}")


def warn(name, detail=""):
    WARNINGS.append(f"  WARN: {name} — {detail}")


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    # --- Bronze checks ---
    print("=== Bronze Layer ===")
    for table in ["raw_osha_inspections", "raw_osha_violations"]:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            check(f"{table} exists", count > 0, f"got {count} rows")
            if count == 0:
                check(f"{table} has data", False, "table is empty")
        except Exception:
            check(f"{table} exists", False, "table not found")

    # --- Silver checks ---
    print("=== Silver Layer ===")
    try:
        total = con.execute("SELECT COUNT(*) FROM osha_inspection_norm").fetchone()[0]
        check("osha_inspection_norm has data", total > 0, f"{total} rows")

        # Null rate on key fields
        for col in ["name_normalized", "site_state", "naics_code", "open_date"]:
            null_count = con.execute(f"SELECT COUNT(*) FROM osha_inspection_norm WHERE {col} IS NULL").fetchone()[0]
            null_rate = null_count / total if total > 0 else 0
            if null_rate > 0.5:
                check(f"{col} null rate < 50%", False, f"{null_rate:.1%} null")
            elif null_rate > 0.2:
                warn(f"{col} null rate is {null_rate:.1%}")
            else:
                check(f"{col} null rate OK", True)

        # Violation join rate
        with_violations = con.execute("SELECT COUNT(*) FROM osha_inspection_norm WHERE violation_count > 0").fetchone()[0]
        violation_rate = with_violations / total if total > 0 else 0
        check("violation join rate > 1%", violation_rate > 0.01, f"{violation_rate:.1%} have violations")

        total_violations = con.execute("SELECT SUM(violation_count) FROM osha_inspection_norm").fetchone()[0]
        raw_violations = con.execute("SELECT COUNT(*) FROM raw_osha_violations").fetchone()[0]
        if raw_violations > 0:
            join_rate = total_violations / raw_violations if raw_violations > 0 else 0
            check("violation join completeness > 90%", join_rate > 0.9, f"{join_rate:.1%} of violations joined")

    except Exception as e:
        check("osha_inspection_norm exists", False, str(e))

    # --- Gold checks ---
    print("=== Gold Layer ===")
    try:
        total = con.execute("SELECT COUNT(*) FROM employer_profile").fetchone()[0]
        check("employer_profile has data", total > 0, f"{total} profiles")

        # Risk tier distribution
        tiers = con.execute("SELECT risk_tier, COUNT(*) FROM employer_profile GROUP BY 1").df()
        tier_dict = dict(zip(tiers.iloc[:, 0], tiers.iloc[:, 1]))
        check("has HIGH risk employers", tier_dict.get("HIGH", 0) > 0, f"{tier_dict.get('HIGH', 0)} HIGH")
        check("has MEDIUM risk employers", tier_dict.get("MEDIUM", 0) > 0, f"{tier_dict.get('MEDIUM', 0)} MEDIUM")
        check("LOW is majority", tier_dict.get("LOW", 0) > total * 0.5, f"{tier_dict.get('LOW', 0)} LOW")

        # Null checks on key output fields
        for col in ["employer_name", "risk_tier", "employer_id"]:
            null_count = con.execute(f"SELECT COUNT(*) FROM employer_profile WHERE {col} IS NULL").fetchone()[0]
            check(f"{col} not null", null_count == 0, f"{null_count} nulls")

        # Entity resolution check
        cluster_count = con.execute("SELECT COUNT(*) FROM cluster_id_mapping").fetchone()[0]
        check("cluster_id_mapping populated", cluster_count > 0, f"{cluster_count} mappings")

    except Exception as e:
        check("employer_profile exists", False, str(e))

    # --- Summary ---
    print()
    for c in CHECKS:
        print(c)
    for w in WARNINGS:
        print(w)
    for f in FAILURES:
        print(f)

    print(f"\n=== {len(CHECKS)} passed, {len(WARNINGS)} warnings, {len(FAILURES)} failures ===")

    if FAILURES:
        print("\nDATA QUALITY CHECK FAILED")
        sys.exit(1)
    else:
        print("\nDATA QUALITY CHECK PASSED")


if __name__ == "__main__":
    main()

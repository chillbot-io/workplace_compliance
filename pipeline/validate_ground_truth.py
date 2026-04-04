"""
pipeline/validate_ground_truth.py — Ground truth validation against OSHA.gov.

Picks a sample of employers across risk tiers and sizes, outputs their
key metrics so you can manually verify against:
  https://www.osha.gov/ords/imis/establishment.html

Run this, then spot-check each employer on osha.gov.

Usage:
    python pipeline/validate_ground_truth.py
"""

import os
import json
from datetime import date
from pathlib import Path

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")
REPORT_DIR = Path(os.environ.get("GT_REPORT_DIR", "/data/gt_reports"))


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    print("=== Ground Truth Validation Sample ===\n")
    print("Verify each employer below against OSHA Establishment Search:")
    print("  https://www.osha.gov/ords/imis/establishment.html\n")

    samples = []

    # 1. Well-known national companies (easy to verify)
    print("--- National Companies (verify inspection counts) ---\n")
    known = [
        "WALMART", "AMAZON", "TARGET", "HOME DEPOT", "MCDONALDS",
        "FEDEX", "TYSON", "TESLA", "COSTCO", "DOLLAR GENERAL",
    ]
    for name in known:
        row = con.execute(f"""
            SELECT
                employer_name, name_normalized, state, zip5, naics_code,
                osha_inspections_5yr, osha_violations_5yr,
                osha_penalty_total_5yr, risk_tier, risk_score,
                osha_last_inspection_date
            FROM employer_profile
            WHERE name_normalized LIKE '{name}%'
            ORDER BY osha_inspections_5yr DESC
            LIMIT 1
        """).fetchone()

        if row:
            entry = format_entry(row)
            samples.append(entry)
            print_entry(entry)

    # 2. HIGH risk employers (most important to get right)
    print("\n--- HIGH Risk Employers (verify violations + penalties) ---\n")
    high_risk = con.execute("""
        SELECT
            employer_name, name_normalized, state, zip5, naics_code,
            osha_inspections_5yr, osha_violations_5yr,
            osha_penalty_total_5yr, risk_tier, risk_score,
            osha_last_inspection_date
        FROM employer_profile
        WHERE risk_tier = 'HIGH'
        ORDER BY risk_score DESC
        LIMIT 10
    """).fetchall()

    for row in high_risk:
        entry = format_entry(row)
        samples.append(entry)
        print_entry(entry)

    # 3. Random MEDIUM risk (spot check the middle)
    print("\n--- Random MEDIUM Risk (spot check) ---\n")
    medium = con.execute("""
        SELECT
            employer_name, name_normalized, state, zip5, naics_code,
            osha_inspections_5yr, osha_violations_5yr,
            osha_penalty_total_5yr, risk_tier, risk_score,
            osha_last_inspection_date
        FROM employer_profile
        WHERE risk_tier = 'MEDIUM'
        ORDER BY RANDOM()
        LIMIT 5
    """).fetchall()

    for row in medium:
        entry = format_entry(row)
        samples.append(entry)
        print_entry(entry)

    # 4. Random LOW risk (should have minimal violations)
    print("\n--- Random LOW Risk (should have few/no violations) ---\n")
    low = con.execute("""
        SELECT
            employer_name, name_normalized, state, zip5, naics_code,
            osha_inspections_5yr, osha_violations_5yr,
            osha_penalty_total_5yr, risk_tier, risk_score,
            osha_last_inspection_date
        FROM employer_profile
        WHERE risk_tier = 'LOW' AND osha_inspections_5yr >= 1
        ORDER BY RANDOM()
        LIMIT 5
    """).fetchall()

    for row in low:
        entry = format_entry(row)
        samples.append(entry)
        print_entry(entry)

    # Save report
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"ground_truth_{date.today().isoformat()}.json"
    with open(report_path, "w") as f:
        json.dump(samples, f, indent=2, default=str)
    print(f"\nSample saved to {report_path}")
    print(f"\nTotal: {len(samples)} employers to verify")

    con.close()


def format_entry(row):
    return {
        "employer_name": row[0],
        "name_normalized": row[1],
        "state": row[2],
        "zip": row[3],
        "naics": row[4],
        "osha_inspections_5yr": row[5],
        "osha_violations_5yr": row[6],
        "osha_penalties_5yr": float(row[7]) if row[7] else 0,
        "risk_tier": row[8],
        "risk_score": float(row[9]) if row[9] else 0,
        "last_inspection": str(row[10]) if row[10] else None,
    }


def print_entry(e):
    print(f"  {e['employer_name']}")
    print(f"    State: {e['state']}  ZIP: {e['zip']}  NAICS: {e['naics']}")
    print(f"    Inspections (5yr): {e['osha_inspections_5yr']}  Violations: {e['osha_violations_5yr']}  Penalties: ${e['osha_penalties_5yr']:,.0f}")
    print(f"    Risk: {e['risk_tier']} ({e['risk_score']})  Last inspection: {e['last_inspection']}")
    print()


if __name__ == "__main__":
    main()

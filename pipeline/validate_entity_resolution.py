"""
pipeline/validate_entity_resolution.py — Audit Splink entity resolution quality.

Samples clusters and checks for:
1. Over-merging: different companies incorrectly clustered together
2. Under-merging: same company name at different locations not clustered
3. Cluster size distribution: are there suspiciously large clusters?

Outputs a report with flagged clusters for manual review.

Usage:
    python pipeline/validate_entity_resolution.py
"""

import os
import sys
import json
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")
REPORT_DIR = Path(os.environ.get("ER_REPORT_DIR", "/data/er_reports"))


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    print("=== Entity Resolution Validation ===\n")

    # ─── 1. CLUSTER SIZE DISTRIBUTION ────────────────────────

    print("[1/5] Cluster size distribution...")

    cluster_sizes = con.execute("""
        SELECT cluster_id, COUNT(*) as member_count
        FROM employer_clusters
        GROUP BY cluster_id
        ORDER BY member_count DESC
    """).df()

    total_clusters = len(cluster_sizes)
    total_records = cluster_sizes["member_count"].sum()

    print(f"  Total clusters: {total_clusters:,}")
    print(f"  Total records:  {total_records:,}")
    print(f"  Singletons (1 record):  {(cluster_sizes['member_count'] == 1).sum():,}")
    print(f"  Pairs (2 records):      {(cluster_sizes['member_count'] == 2).sum():,}")
    print(f"  3-10 records:           {((cluster_sizes['member_count'] >= 3) & (cluster_sizes['member_count'] <= 10)).sum():,}")
    print(f"  11-50 records:          {((cluster_sizes['member_count'] >= 11) & (cluster_sizes['member_count'] <= 50)).sum():,}")
    print(f"  51-100 records:         {((cluster_sizes['member_count'] >= 51) & (cluster_sizes['member_count'] <= 100)).sum():,}")
    print(f"  >100 records:           {(cluster_sizes['member_count'] > 100).sum():,}")

    # Flag suspiciously large clusters
    large_clusters = cluster_sizes[cluster_sizes["member_count"] > 50]
    if len(large_clusters) > 0:
        print(f"\n  ⚠ {len(large_clusters)} clusters with >50 members (potential over-merging)")

    # ─── 2. OVER-MERGE CHECK — LARGE CLUSTERS ────────────────

    print("\n[2/5] Over-merge check — inspecting largest clusters...")

    top_clusters = cluster_sizes.head(20)
    over_merge_flags = []

    for _, row in top_clusters.iterrows():
        cid = row["cluster_id"]
        members = con.execute(f"""
            SELECT DISTINCT ec.unique_id, o.estab_name, o.site_state, o.naics_code
            FROM employer_clusters ec
            JOIN osha_inspection_norm o ON ec.unique_id = o.activity_nr
            WHERE ec.cluster_id = '{cid}'
            LIMIT 20
        """).df()

        if members.empty:
            continue

        # Check if cluster has multiple distinct normalized names
        unique_names = members["estab_name"].dropna().unique()
        unique_states = members["site_state"].dropna().unique()
        unique_naics = members["naics_code"].dropna().unique()

        # Flag if names look very different (potential over-merge)
        if len(unique_names) > 1:
            # Simple check: do the first words match?
            first_words = set()
            for name in unique_names:
                words = str(name).upper().split()
                if words:
                    first_words.add(words[0])

            if len(first_words) > 3:
                over_merge_flags.append({
                    "cluster_id": str(cid),
                    "member_count": int(row["member_count"]),
                    "unique_names": [str(n) for n in unique_names[:10]],
                    "unique_states": [str(s) for s in unique_states],
                    "flag": "MULTIPLE_DISTINCT_NAMES",
                })
                print(f"  ⚠ Cluster {cid} ({row['member_count']} members): {len(first_words)} distinct name prefixes")
                for n in unique_names[:5]:
                    print(f"      {n}")

    if not over_merge_flags:
        print("  ✓ No obvious over-merging in top 20 clusters")

    # ─── 3. UNDER-MERGE CHECK — SAME NAME, DIFFERENT CLUSTERS ─

    print("\n[3/5] Under-merge check — same name in different clusters...")

    # Find normalized names that appear in multiple clusters
    under_merge = con.execute("""
        SELECT
            o.name_normalized,
            COUNT(DISTINCT ec.cluster_id) AS cluster_count,
            COUNT(DISTINCT o.activity_nr) AS record_count
        FROM employer_clusters ec
        JOIN osha_inspection_norm o ON ec.unique_id = o.activity_nr
        WHERE o.name_normalized IS NOT NULL
          AND LENGTH(o.name_normalized) > 3
        GROUP BY o.name_normalized
        HAVING COUNT(DISTINCT ec.cluster_id) > 5
        ORDER BY cluster_count DESC
        LIMIT 30
    """).df()

    if not under_merge.empty:
        print(f"  Names split across many clusters (potential under-merging):")
        for _, row in under_merge.head(15).iterrows():
            print(f"    {row['name_normalized']:40s}  {row['cluster_count']:>4} clusters, {row['record_count']:>5} records")
    else:
        print("  ✓ No obvious under-merging found")

    # ─── 4. KNOWN COMPANY SPOT CHECK ─────────────────────────

    print("\n[4/5] Known company spot check...")

    known_companies = [
        "WALMART", "AMAZON", "TARGET", "MCDONALDS", "STARBUCKS",
        "HOME DEPOT", "LOWES", "COSTCO", "KROGER", "DOLLAR GENERAL",
        "FEDEX", "TYSON", "CARGILL", "WASTE MANAGEMENT", "TESLA",
    ]

    for company in known_companies:
        result = con.execute(f"""
            SELECT
                COUNT(DISTINCT ec.cluster_id) AS clusters,
                COUNT(DISTINCT o.activity_nr) AS inspections,
                COUNT(DISTINCT o.site_state) AS states
            FROM osha_inspection_norm o
            LEFT JOIN employer_clusters ec ON o.activity_nr = ec.unique_id
            WHERE o.name_normalized LIKE '%{company}%'
        """).fetchone()

        clusters, inspections, states = result
        status = "⚠" if clusters > inspections * 0.8 else "✓"  # Too many clusters = under-merging
        print(f"  {status} {company:20s}  {inspections:>5} inspections across {states:>2} states in {clusters:>5} clusters")

    # ─── 5. PROFILE COVERAGE CHECK ───────────────────────────

    print("\n[5/5] Profile coverage...")

    total_inspections = con.execute("SELECT COUNT(*) FROM osha_inspection_norm").fetchone()[0]
    in_clusters = con.execute("SELECT COUNT(*) FROM employer_clusters").fetchone()[0]
    in_profiles = con.execute("SELECT COUNT(*) FROM employer_profile").fetchone()[0]

    print(f"  OSHA inspections:     {total_inspections:>10,}")
    print(f"  In Splink clusters:   {in_clusters:>10,}  ({100*in_clusters/total_inspections:.1f}%)")
    print(f"  Employer profiles:    {in_profiles:>10,}")
    print(f"  Compression ratio:    {total_inspections/in_profiles:.1f}x  (inspections per profile)")

    # ─── SAVE REPORT ─────────────────────────────────────────

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "date": date.today().isoformat(),
        "total_clusters": total_clusters,
        "total_records": int(total_records),
        "over_merge_flags": over_merge_flags,
        "under_merge_top": under_merge.to_dict("records") if not under_merge.empty else [],
    }
    report_path = REPORT_DIR / f"er_validation_{date.today().isoformat()}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved to {report_path}")

    con.close()


if __name__ == "__main__":
    main()

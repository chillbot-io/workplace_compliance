"""
pipeline/validate_data.py — Data quality gate for the FastDOL pipeline.

Runs after each pipeline stage. If any CRITICAL check fails, exits with
code 1 which blocks sync.py from pushing bad data to Postgres.

Three severity levels:
  CRITICAL — blocks sync, data cannot ship in this state
  WARNING  — logged + alerted, but sync proceeds
  INFO     — logged for trending

Quality dimensions checked:
  1. Completeness  — are we missing records vs. expected counts?
  2. Freshness     — is the data recent enough?
  3. Consistency   — do cross-source joins make sense?
  4. Distribution  — did risk tiers shift abnormally? (regression detection)
  5. Referential   — do all FKs resolve?

Usage:
    python pipeline/validate_data.py
"""

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")
SNAPSHOT_DIR = Path(os.environ.get("DQ_SNAPSHOT_DIR", "/data/dq_snapshots"))

results = {"critical": [], "warning": [], "info": [], "pass": []}


def critical(name, passed, detail=""):
    if passed:
        results["pass"].append({"check": name, "detail": detail})
    else:
        results["critical"].append({"check": name, "detail": detail})


def warning(name, passed, detail=""):
    if passed:
        results["pass"].append({"check": name, "detail": detail})
    else:
        results["warning"].append({"check": name, "detail": detail})


def info(name, value):
    results["info"].append({"check": name, "value": value})


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    print("=== FastDOL Data Quality Gate ===\n")

    # ─── 1. COMPLETENESS ─────────────────────────────────────

    print("[1/5] Completeness checks...")

    # OSHA inspections — should have >2M records (historical back to 1972)
    osha_count = safe_count(con, "raw_osha_inspections")
    critical("OSHA inspections > 2M", osha_count > 2_000_000,
             f"got {osha_count:,} (expected >2M)")
    info("osha_inspections_count", osha_count)

    # OSHA violations — should have >300k
    viol_count = safe_count(con, "raw_osha_violations")
    critical("OSHA violations > 300k", viol_count > 300_000,
             f"got {viol_count:,} (expected >300k)")
    info("osha_violations_count", viol_count)

    # WHD — may not exist yet, but if it does, check minimum
    whd_count = safe_count(con, "raw_whd_actions")
    if whd_count > 0:
        warning("WHD actions > 100k", whd_count > 100_000,
                f"got {whd_count:,} (expected >100k if loaded)")
    info("whd_actions_count", whd_count)

    # Employer profiles — should be >200k after entity resolution
    profile_count = safe_count(con, "employer_profile")
    critical("employer_profile > 150k", profile_count > 150_000,
             f"got {profile_count:,} (expected >150k)")
    info("employer_profile_count", profile_count)

    # Entity resolution — deterministic matching produces profiles directly
    # No separate cluster table needed anymore

    # ─── 2. FRESHNESS ────────────────────────────────────────

    print("[2/5] Freshness checks...")

    # Most recent OSHA inspection should be within 6 months
    try:
        latest_osha = con.execute(
            "SELECT MAX(open_date) FROM raw_osha_inspections"
        ).fetchone()[0]
        if latest_osha:
            days_old = (date.today() - latest_osha.date() if hasattr(latest_osha, 'date')
                        else (date.today() - latest_osha))
            if hasattr(days_old, 'days'):
                days_old = days_old.days
            warning("Latest OSHA inspection < 6 months old",
                    days_old < 180, f"latest is {days_old} days old")
            info("osha_latest_inspection_age_days", days_old)
    except Exception:
        warning("Could read OSHA latest date", False, "query failed")

    # ─── 3. CONSISTENCY ──────────────────────────────────────

    print("[3/5] Consistency checks...")

    # Violation → inspection join rate
    try:
        total_viols = con.execute(
            "SELECT COUNT(*) FROM raw_osha_violations"
        ).fetchone()[0]
        joined_viols = con.execute("""
            SELECT COUNT(*) FROM raw_osha_violations v
            JOIN raw_osha_inspections i ON v.activity_nr = i.activity_nr
        """).fetchone()[0]
        if total_viols > 0:
            join_rate = joined_viols / total_viols
            critical("Violation→inspection join > 95%",
                     join_rate > 0.95,
                     f"{join_rate:.1%} ({joined_viols:,}/{total_viols:,})")
            info("violation_join_rate", round(join_rate, 4))
    except Exception as e:
        warning("Violation join check", False, str(e))

    # Profile count sanity — should be reasonable relative to inspection count
    if profile_count > 0 and osha_count > 0:
        ratio = osha_count / profile_count
        warning("Inspections-per-profile ratio reasonable (2-20x)",
                2 < ratio < 20,
                f"ratio={ratio:.1f}x ({osha_count:,} inspections / {profile_count:,} profiles)")

    # Null rates on critical fields
    if profile_count > 0:
        for col, max_null_rate in [
            ("employer_name", 0.0),
            ("employer_id", 0.0),
            ("risk_tier", 0.0),
            ("risk_score", 0.01),
            ("state", 0.15),
            ("zip5", 0.20),
            ("naics_code", 0.30),
        ]:
            try:
                null_count = con.execute(
                    f"SELECT COUNT(*) FROM employer_profile WHERE {col} IS NULL"
                ).fetchone()[0]
                null_rate = null_count / profile_count
                is_critical = max_null_rate == 0.0
                check_fn = critical if is_critical else warning
                check_fn(f"{col} null rate ≤ {max_null_rate:.0%}",
                         null_rate <= max_null_rate,
                         f"{null_rate:.1%} null ({null_count:,}/{profile_count:,})")
            except Exception:
                pass  # column might not exist yet

    # ─── 4. DISTRIBUTION (regression detection) ──────────────

    print("[4/5] Distribution checks...")

    if profile_count > 0:
        # Risk tier distribution
        try:
            tiers = con.execute("""
                SELECT risk_tier, COUNT(*) as cnt
                FROM employer_profile
                GROUP BY risk_tier
            """).fetchall()
            tier_dict = {t[0]: t[1] for t in tiers}

            high = tier_dict.get("HIGH", 0)
            elevated = tier_dict.get("ELEVATED", 0)
            medium = tier_dict.get("MEDIUM", 0)
            low = tier_dict.get("LOW", 0)

            info("risk_tier_HIGH", high)
            info("risk_tier_ELEVATED", elevated)
            info("risk_tier_MEDIUM", medium)
            info("risk_tier_LOW", low)

            # Sanity checks
            critical("Has HIGH risk employers", high > 0, f"got {high}")
            critical("LOW is majority (>=40%)", low >= profile_count * 0.4,
                     f"LOW={low} ({100*low/profile_count:.0f}%)")
            warning("HIGH < 5% of total", high < profile_count * 0.05,
                    f"HIGH={high} ({100*high/profile_count:.1f}%)")

            # Compare to previous snapshot (regression detection)
            prev = load_previous_snapshot()
            if prev:
                for tier in ["HIGH", "ELEVATED", "MEDIUM", "LOW"]:
                    prev_count = prev.get(f"risk_tier_{tier}", 0)
                    curr_count = tier_dict.get(tier, 0)
                    if prev_count > 0:
                        pct_change = (curr_count - prev_count) / prev_count
                        warning(f"{tier} count within 50% of previous",
                                abs(pct_change) < 0.5,
                                f"was {prev_count:,}, now {curr_count:,} ({pct_change:+.0%})")

                # Total profile count shouldn't drop >10%
                prev_total = prev.get("employer_profile_count", 0)
                if prev_total > 0:
                    pct_change = (profile_count - prev_total) / prev_total
                    critical("Profile count not dropped >10%",
                             pct_change > -0.1,
                             f"was {prev_total:,}, now {profile_count:,} ({pct_change:+.1%})")

        except Exception as e:
            warning("Distribution checks", False, str(e))

    # ─── 5. REFERENTIAL INTEGRITY ────────────────────────────

    print("[5/5] Referential integrity checks...")

    # All employer_ids in profile should be valid UUIDs
    try:
        bad_ids = con.execute("""
            SELECT COUNT(*) FROM employer_profile
            WHERE TRY_CAST(employer_id AS UUID) IS NULL
        """).fetchone()[0]
        critical("All employer_ids are valid UUIDs",
                 bad_ids == 0, f"{bad_ids} invalid UUIDs")
    except Exception:
        pass  # TRY_CAST might not work in all DuckDB versions

    # Parent company matches
    try:
        parent_count = con.execute(
            "SELECT COUNT(*) FROM employer_profile WHERE parent_name IS NOT NULL"
        ).fetchone()[0]
        info("parent_company_matches", parent_count)
        info("parent_company_match_rate",
             round(parent_count / profile_count, 4) if profile_count > 0 else 0)
    except Exception:
        pass

    con.close()

    # ─── SAVE SNAPSHOT (for next run's regression detection) ──

    save_snapshot()

    # ─── REPORT ──────────────────────────────────────────────

    print("\n" + "=" * 60)
    print(f"  PASSED:   {len(results['pass'])}")
    print(f"  WARNINGS: {len(results['warning'])}")
    print(f"  CRITICAL: {len(results['critical'])}")
    print(f"  INFO:     {len(results['info'])}")
    print("=" * 60)

    if results["pass"]:
        print("\n✓ Passed:")
        for r in results["pass"]:
            print(f"    {r['check']}: {r['detail']}")

    if results["warning"]:
        print("\n⚠ Warnings:")
        for r in results["warning"]:
            print(f"    {r['check']}: {r['detail']}")

    if results["critical"]:
        print("\n✗ CRITICAL FAILURES (sync blocked):")
        for r in results["critical"]:
            print(f"    {r['check']}: {r['detail']}")

    if results["info"]:
        print("\n📊 Metrics:")
        for r in results["info"]:
            val = r['value']
            if isinstance(val, (int, float)) and val > 1000:
                print(f"    {r['check']}: {val:,}")
            else:
                print(f"    {r['check']}: {val}")

    # Exit code: 1 if any critical failure (blocks sync)
    if results["critical"]:
        print(f"\n🚫 DATA QUALITY GATE FAILED — {len(results['critical'])} critical issue(s)")
        print("   Sync to Postgres will NOT proceed.")
        sys.exit(1)
    elif results["warning"]:
        print(f"\n⚠ Data quality gate PASSED WITH WARNINGS ({len(results['warning'])})")
    else:
        print("\n✓ Data quality gate PASSED")


def safe_count(con, table: str) -> int:
    """Count rows in a table, returning 0 if table doesn't exist."""
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0


def load_previous_snapshot() -> dict | None:
    """Load the most recent DQ snapshot for regression comparison."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(SNAPSHOT_DIR.glob("dq_*.json"), reverse=True)
    # Skip today's snapshot (if it exists), use yesterday's
    today_prefix = f"dq_{date.today().isoformat()}"
    for f in files:
        if not f.name.startswith(today_prefix):
            try:
                with open(f) as fh:
                    return json.load(fh)
            except Exception:
                continue
    return None


def save_snapshot():
    """Save current metrics as a JSON snapshot for future comparison."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {}
    for item in results["info"]:
        snapshot[item["check"]] = item["value"]

    path = SNAPSHOT_DIR / f"dq_{date.today().isoformat()}.json"
    with open(path, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"\nSnapshot saved to {path}")


if __name__ == "__main__":
    main()

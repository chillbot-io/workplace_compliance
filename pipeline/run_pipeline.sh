#!/bin/bash
# pipeline/run_pipeline.sh — Nightly pipeline orchestration
# Ingests OSHA data, transforms, resolves entities, syncs to Postgres.
# WHD ingestion is handled by run_weekly.sh (Sundays).
# SEC reference data is handled by run_monthly.sh (1st of month).
#
# Usage: bash pipeline/run_pipeline.sh
# Runs nightly via cron with flock to prevent overlapping runs.

set -euo pipefail

LOCK_FILE="/var/lock/pipeline.lock"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV="${PROJECT_DIR}/venv"
LOG_FILE="/var/log/pipeline-$(date +%Y%m%d).log"

# Prevent concurrent runs
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "Another pipeline run is already in progress. Exiting." | tee -a "$LOG_FILE"
    exit 1
fi

# Validate prerequisites
if [[ ! -f "${VENV}/bin/activate" ]]; then echo "Venv not found at ${VENV}"; exit 1; fi
if [[ ! -f "${PROJECT_DIR}/.env.pipeline" ]]; then echo "Missing .env.pipeline"; exit 1; fi

# Activate venv
source "${VENV}/bin/activate"

# Load environment
set -a
source "${PROJECT_DIR}/.env.pipeline"
set +a

export PIPELINE_RUN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")

echo "=== Pipeline Run Starting — $(date -u) — Run ID: ${PIPELINE_RUN_ID} ===" | tee -a "$LOG_FILE"

# Step 1: Ingest OSHA data only (WHD is weekly, SEC is monthly)
echo "[Step 1/9] Ingesting OSHA data from DOL API..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/ingest_dol.py" osha_inspections osha_violations 2>&1 | tee -a "$LOG_FILE"

# Step 2: Load bronze into DuckDB
echo "[Step 2/9] Loading bronze into DuckDB..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/load_bronze.py" 2>&1 | tee -a "$LOG_FILE"

# Step 3: Run dbt staging + silver models (NOT gold — needs entity resolution first)
echo "[Step 3/9] Running dbt (staging + silver)..." | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}/dbt"
dbt seed --profiles-dir . 2>&1 | tee -a "$LOG_FILE"
dbt run --profiles-dir . --select staging silver 2>&1 | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}"

# Step 4: Parse addresses (needed for address_key matching in gold model)
echo "[Step 4/8] Parsing addresses..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/parse_addresses.py" 2>&1 | tee -a "$LOG_FILE"

# Step 5: Run dbt gold models (deterministic entity resolution built into SQL)
# No separate Splink step needed — matching is name_normalized + state + zip5
# with address_key merge for different names at same physical location
echo "[Step 5/8] Running dbt (gold)..." | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}/dbt"
dbt run --profiles-dir . --select gold 2>&1 | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}"

# Step 6: Data quality gate (MUST pass before sync — blocks bad data from shipping)
echo "[Step 6/8] Running data quality gate..." | tee -a "$LOG_FILE"
if ! python "${PROJECT_DIR}/pipeline/validate_data.py" 2>&1 | tee -a "$LOG_FILE"; then
    echo "!!! DATA QUALITY GATE FAILED — sync to Postgres blocked !!!" | tee -a "$LOG_FILE"
    echo "Review failures above. Fix data issues and re-run." | tee -a "$LOG_FILE"
    exit 1
fi

# Step 7: Sync to Postgres (shadow-table swap) — only runs if DQ passed
echo "[Step 7/8] Syncing to Postgres..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/sync.py" 2>&1 | tee -a "$LOG_FILE"

# Step 8: Validate sync (row count comparison DuckDB vs Postgres)
echo "[Step 8/8] Validating sync..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/validate_sync.py" 2>&1 | tee -a "$LOG_FILE"

echo "=== Pipeline Run Complete — $(date -u) ===" | tee -a "$LOG_FILE"

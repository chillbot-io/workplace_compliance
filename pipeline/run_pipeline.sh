#!/bin/bash
# pipeline/run_pipeline.sh — Full pipeline orchestration
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

# Step 1: Ingest from DOL API
echo "[Step 1/8] Ingesting from DOL API..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/ingest_dol.py" 2>&1 | tee -a "$LOG_FILE"

# Step 2: Load bronze into DuckDB
echo "[Step 2/8] Loading bronze into DuckDB..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/load_bronze.py" 2>&1 | tee -a "$LOG_FILE"

# Step 3: Run dbt staging + silver models (NOT gold — needs entity resolution first)
echo "[Step 3/8] Running dbt (staging + silver)..." | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}/dbt"
dbt seed --profiles-dir . 2>&1 | tee -a "$LOG_FILE"
dbt run --profiles-dir . --select staging silver 2>&1 | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}"

# Step 4: Parse addresses
echo "[Step 4/8] Parsing addresses..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/parse_addresses.py" 2>&1 | tee -a "$LOG_FILE"

# Step 5: Entity resolution (Splink)
echo "[Step 5/8] Running entity resolution (Splink)..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/entity_resolution.py" 2>&1 | tee -a "$LOG_FILE"

# Step 6: Run dbt gold models (aggregates by Splink clusters)
echo "[Step 6/8] Running dbt (gold)..." | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}/dbt"
dbt run --profiles-dir . --select gold 2>&1 | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}"

# Step 7: Sync to Postgres (shadow-table swap)
echo "[Step 7/8] Syncing to Postgres..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/sync.py" 2>&1 | tee -a "$LOG_FILE"

# Step 8: Validate sync
echo "[Step 8/8] Validating sync..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/validate_sync.py" 2>&1 | tee -a "$LOG_FILE"

echo "=== Pipeline Run Complete — $(date -u) ===" | tee -a "$LOG_FILE"

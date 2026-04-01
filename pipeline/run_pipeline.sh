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

# Activate venv
source "${VENV}/bin/activate"

# Load environment
set -a
source "${PROJECT_DIR}/.env.pipeline"
set +a

export PIPELINE_RUN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")

echo "=== Pipeline Run Starting — $(date -u) — Run ID: ${PIPELINE_RUN_ID} ===" | tee -a "$LOG_FILE"

# Step 1: Ingest from DOL API
echo "[Step 1/6] Ingesting from DOL API..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/ingest_dol.py" 2>&1 | tee -a "$LOG_FILE"

# Step 2: Load bronze into DuckDB
echo "[Step 2/6] Loading bronze into DuckDB..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/load_bronze.py" 2>&1 | tee -a "$LOG_FILE"

# Step 3: Run dbt (seeds + models)
echo "[Step 3/6] Running dbt..." | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}/dbt"
dbt seed --profiles-dir . 2>&1 | tee -a "$LOG_FILE"
dbt run --profiles-dir . 2>&1 | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}"

# Step 4: Parse addresses
echo "[Step 4/6] Parsing addresses..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/parse_addresses.py" 2>&1 | tee -a "$LOG_FILE"

# Step 5: Sync to Postgres (shadow-table swap)
echo "[Step 5/6] Syncing to Postgres..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/sync.py" 2>&1 | tee -a "$LOG_FILE"

# Step 6: Validate sync
echo "[Step 6/6] Validating sync..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/validate_sync.py" 2>&1 | tee -a "$LOG_FILE"

echo "=== Pipeline Run Complete — $(date -u) ===" | tee -a "$LOG_FILE"

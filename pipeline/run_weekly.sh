#!/bin/bash
# pipeline/run_weekly.sh — Weekly pipeline tasks (Sundays at 1 AM)
# Fetches WHD data, loads bronze. The nightly run handles the rest
# (dbt, entity resolution, gold, sync).
#
# Usage: bash pipeline/run_weekly.sh

set -euo pipefail

LOCK_FILE="/var/lock/pipeline-weekly.lock"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV="${PROJECT_DIR}/venv"
LOG_FILE="/var/log/pipeline-weekly-$(date +%Y%m%d).log"

# Prevent concurrent runs
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "Another weekly pipeline run is already in progress. Exiting." | tee -a "$LOG_FILE"
    exit 1
fi

# Also check if nightly pipeline is running
if ! flock -n 201 200>/var/lock/pipeline.lock; then
    echo "Nightly pipeline is running. Skipping weekly run." | tee -a "$LOG_FILE"
    exit 1
fi

# Validate prerequisites
if [[ ! -f "${VENV}/bin/activate" ]]; then echo "Venv not found at ${VENV}"; exit 1; fi
if [[ ! -f "${PROJECT_DIR}/.env.pipeline" ]]; then echo "Missing .env.pipeline"; exit 1; fi

# Activate venv + load env
source "${VENV}/bin/activate"
set -a
source "${PROJECT_DIR}/.env.pipeline"
set +a

echo "=== Weekly Pipeline Starting — $(date -u) ===" | tee -a "$LOG_FILE"

# Step 1: Ingest WHD data
echo "[Weekly 1/2] Ingesting WHD enforcement data..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/ingest_dol.py" whd_actions 2>&1 | tee -a "$LOG_FILE"

# Step 2: Load into DuckDB
echo "[Weekly 2/2] Loading bronze into DuckDB..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/load_bronze.py" 2>&1 | tee -a "$LOG_FILE"

echo "=== Weekly Pipeline Complete — $(date -u) ===" | tee -a "$LOG_FILE"
echo "WHD data refreshed. Nightly run will integrate into gold model." | tee -a "$LOG_FILE"

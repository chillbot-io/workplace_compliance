#!/bin/bash
# pipeline/run_monthly.sh — Monthly pipeline tasks (1st of month at midnight)
# Refreshes slow-moving reference data: SEC subsidiaries + EINs.
# The nightly run handles integrating the updated seeds into gold.
#
# Usage: bash pipeline/run_monthly.sh

set -euo pipefail

LOCK_FILE="/var/lock/pipeline-monthly.lock"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV="${PROJECT_DIR}/venv"
LOG_FILE="/var/log/pipeline-monthly-$(date +%Y%m%d).log"

# Prevent concurrent runs
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "Another monthly pipeline run is already in progress. Exiting." | tee -a "$LOG_FILE"
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

echo "=== Monthly Pipeline Starting — $(date -u) ===" | tee -a "$LOG_FILE"

# Step 1: Refresh SEC Exhibit 21 subsidiary data (parent company mappings)
echo "[Monthly 1/4] Downloading SEC Exhibit 21 subsidiaries..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/ingest_subsidiaries.py" 2>&1 | tee -a "$LOG_FILE"

# Step 2: Load parent_companies directly into DuckDB (bypasses dbt seed CSV issues)
echo "[Monthly 2/4] Loading parent companies into DuckDB..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/load_parent_companies.py" 2>&1 | tee -a "$LOG_FILE"

# Step 3: Update NAICS seed with any missing older codes
echo "[Monthly 3/4] Updating NAICS codes..." | tee -a "$LOG_FILE"
python "${PROJECT_DIR}/pipeline/update_naics_seed.py" 2>&1 | tee -a "$LOG_FILE"

# Step 4: Reload dbt seeds (excluding parent_companies — loaded directly above)
echo "[Monthly 4/4] Reloading dbt seeds..." | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}/dbt"
dbt seed --profiles-dir . --exclude parent_companies 2>&1 | tee -a "$LOG_FILE"
cd "${PROJECT_DIR}"

echo "=== Monthly Pipeline Complete — $(date -u) ===" | tee -a "$LOG_FILE"
echo "Reference data refreshed. Nightly run will rebuild gold model." | tee -a "$LOG_FILE"

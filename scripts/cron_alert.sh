#!/bin/bash
# scripts/cron_alert.sh — Wrapper for cron jobs that alerts on failure.
# Usage: cron_alert.sh "job-name" command arg1 arg2 ...
# Posts to ALERT_WEBHOOK_URL (Slack/generic webhook) on failure.

set -uo pipefail

JOB_NAME="${1:?Usage: cron_alert.sh <job-name> <command> [args...]}"
shift
LOG_FILE="/var/log/${JOB_NAME}.log"

echo "=== ${JOB_NAME} starting at $(date -u) ===" >> "$LOG_FILE"

if "$@" >> "$LOG_FILE" 2>&1; then
    echo "=== ${JOB_NAME} completed at $(date -u) ===" >> "$LOG_FILE"
else
    EXIT_CODE=$?
    echo "=== ${JOB_NAME} FAILED (exit ${EXIT_CODE}) at $(date -u) ===" >> "$LOG_FILE"

    if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
        LAST_LINES=$(tail -5 "$LOG_FILE" | sed 's/"/\\"/g')
        curl -sf -X POST "$ALERT_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\": \"🚨 Cron job '${JOB_NAME}' failed (exit ${EXIT_CODE})\\n\`\`\`${LAST_LINES}\`\`\`\"}" \
            > /dev/null 2>&1 || true
    fi

    exit $EXIT_CODE
fi

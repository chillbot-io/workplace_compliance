#!/bin/bash
# scripts/check_disk.sh — Alert if disk usage exceeds 80%.
# Runs every 6 hours via cron.

THRESHOLD=80

USAGE=$(df /data 2>/dev/null | awk 'NR==2 {print int($5)}')
if [ -z "$USAGE" ]; then
    USAGE=$(df / | awk 'NR==2 {print int($5)}')
fi

if [ "$USAGE" -ge "$THRESHOLD" ]; then
    echo "ALERT: Disk usage at ${USAGE}% (threshold: ${THRESHOLD}%)"
    if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
        curl -sf -X POST "$ALERT_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\": \"⚠️ Disk usage at ${USAGE}% on $(hostname) — threshold is ${THRESHOLD}%\"}" \
            > /dev/null 2>&1 || true
    fi
    exit 1
else
    echo "Disk usage: ${USAGE}% (OK)"
fi

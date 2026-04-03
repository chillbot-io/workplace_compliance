#!/bin/bash
# scripts/check_health.sh — Verify API health and pipeline freshness.
# Runs after pipeline + safety net at 8:30 AM via cron.

set -uo pipefail

HEALTH_URL="${HEALTH_URL:-https://api.fastdol.com/v1/health}"

RESPONSE=$(curl -sf "$HEALTH_URL" 2>/dev/null)
if [ $? -ne 0 ]; then
    echo "ALERT: Health check failed — API unreachable"
    if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
        curl -sf -X POST "$ALERT_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d '{"text": "🚨 FastDOL API health check failed — API unreachable"}' \
            > /dev/null 2>&1 || true
    fi
    exit 1
fi

STATUS=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null)

if [ "$STATUS" != "healthy" ]; then
    echo "ALERT: API status is '${STATUS}', not 'healthy'"
    if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
        curl -sf -X POST "$ALERT_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\": \"⚠️ FastDOL API status: ${STATUS}\\n\`\`\`${RESPONSE}\`\`\`\"}" \
            > /dev/null 2>&1 || true
    fi
    exit 1
else
    echo "Health check passed: ${STATUS}"
fi

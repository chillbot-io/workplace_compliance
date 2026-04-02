#!/bin/bash
# scripts/deploy.sh — Atomic deploy with rollback on health check failure
# Usage: ./scripts/deploy.sh <image_tag>
# Run on the API server.

set -euo pipefail

IMAGE_TAG="${1:?Usage: deploy.sh <image_tag>}"
PROJECT_DIR="${DEPLOY_DIR:-/opt/employer-compliance}"
IMAGE="ghcr.io/chillbot-io/workplace_compliance:${IMAGE_TAG}"
STATE_FILE="${PROJECT_DIR}/.current_image_tag"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.api.yml"
HEALTH_URL="${HEALTH_URL:-http://localhost:8000/v1/health}"
HEALTH_TIMEOUT=60  # seconds to wait for health check

# Record current image for rollback
PREV_TAG=""
if [ -f "$STATE_FILE" ]; then
    PREV_TAG=$(cat "$STATE_FILE")
fi

echo "Deploying ${IMAGE}..."
echo "${IMAGE_TAG}" > "$STATE_FILE"

# Pull and restart API service only
docker compose -f "$COMPOSE_FILE" pull api
docker compose -f "$COMPOSE_FILE" up -d api

# Poll health check with timeout
echo "Waiting for health check (timeout: ${HEALTH_TIMEOUT}s)..."
HEALTHY=false
ELAPSED=0
while [ "$ELAPSED" -lt "$HEALTH_TIMEOUT" ]; do
    if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
        HEALTHY=true
        break
    fi
    sleep 3
    ELAPSED=$((ELAPSED + 3))
    echo "  ... ${ELAPSED}s elapsed"
done

if [ "$HEALTHY" = true ]; then
    echo "Deploy successful: ${IMAGE_TAG} (healthy after ${ELAPSED}s)"
else
    echo "HEALTH CHECK FAILED after ${HEALTH_TIMEOUT}s — rolling back to ${PREV_TAG}"
    if [ -n "$PREV_TAG" ]; then
        echo "${PREV_TAG}" > "$STATE_FILE"
        docker compose -f "$COMPOSE_FILE" up -d api
        sleep 10
        if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
            echo "Rollback successful: ${PREV_TAG}"
        else
            echo "ROLLBACK ALSO FAILED — manual intervention required"
            exit 2
        fi
    else
        echo "No previous tag to roll back to — manual intervention required"
        exit 2
    fi
    exit 1
fi

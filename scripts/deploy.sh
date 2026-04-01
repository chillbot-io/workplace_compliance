#!/bin/bash
# scripts/deploy.sh — Atomic deploy with rollback on health check failure
# Usage: ./scripts/deploy.sh <image_tag>
# Run on the API server.

set -euo pipefail

IMAGE_TAG="${1:?Usage: deploy.sh <image_tag>}"
IMAGE="ghcr.io/chillbot-io/workplace_compliance:${IMAGE_TAG}"
STATE_FILE="/opt/employer-compliance/.current_image_tag"
COMPOSE_FILE="/opt/employer-compliance/docker-compose.api.yml"
HEALTH_URL="http://localhost:8000/v1/health"

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

# Wait for container to start
echo "Waiting for health check..."
sleep 5

# Health check (3 attempts)
HEALTHY=false
for i in 1 2 3; do
    if curl -sf "$HEALTH_URL" > /dev/null 2>&1; then
        HEALTHY=true
        break
    fi
    echo "Health check attempt ${i}/3 failed, retrying in 5s..."
    sleep 5
done

if [ "$HEALTHY" = true ]; then
    echo "Deploy successful: ${IMAGE_TAG}"
else
    echo "HEALTH CHECK FAILED — rolling back to ${PREV_TAG}"
    if [ -n "$PREV_TAG" ]; then
        echo "${PREV_TAG}" > "$STATE_FILE"
        docker compose -f "$COMPOSE_FILE" up -d api
        sleep 5
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

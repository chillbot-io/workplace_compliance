#!/bin/bash
# scripts/backup.sh — Backup bronze data, Postgres dump, and config to local backup dir.
# Runs daily at 4 AM via cron. Add rclone to R2 when ready.

set -euo pipefail

BACKUP_DIR="/data/backups/$(date +%Y-%m-%d)"
mkdir -p "$BACKUP_DIR"

echo "=== Backup starting at $(date -u) ==="

# 1. Postgres dump
echo "Dumping Postgres..."
PGPASSWORD="${PG_PASSWORD:?PG_PASSWORD not set}" PGSSLMODE=require \
    pg_dump -h 10.0.0.2 -U pipeline_user -d stablelabel \
    --no-owner --no-acl \
    -F c -f "$BACKUP_DIR/stablelabel.dump" 2>/dev/null || echo "WARNING: pg_dump failed"

# 2. DuckDB checkpoint
echo "Copying DuckDB..."
cp "${DUCKDB_PATH:-/data/duckdb/employer_compliance.duckdb}" "$BACKUP_DIR/" 2>/dev/null || echo "WARNING: DuckDB copy failed"

# 3. Config backup (exclude secrets — .env files contain credentials)
echo "Backing up config..."
tar -czf "$BACKUP_DIR/config.tar.gz" \
    --exclude='*.env.*' --exclude='.env*' \
    /opt/employer-compliance/docker-compose.pipeline.yml \
    /opt/employer-compliance/scripts/ \
    2>/dev/null || echo "WARNING: config backup failed"

# 4. Cleanup old backups (keep 7 days)
find /data/backups -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \; 2>/dev/null

BACKUP_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
echo "=== Backup complete: ${BACKUP_SIZE} at ${BACKUP_DIR} ==="

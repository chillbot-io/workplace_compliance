"""
api/main.py — FastAPI application entry point.
Minimal skeleton for Phase 0 — health check only.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)
log = structlog.get_logger()

# Connection pool — initialized on startup
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    dsn = os.environ.get("PG_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("No PG_DSN or DATABASE_URL set")
        raise RuntimeError("Set PG_DSN or DATABASE_URL")
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    log.info("database_pool_ready", dsn=dsn.split("@")[-1])  # log host only, not creds
    yield
    await pool.close()
    log.info("database_pool_closed")


app = FastAPI(title="Employer Compliance API", version="1.0", lifespan=lifespan)


@app.get("/v1/health")
async def health():
    """System health — public, no auth, no metering."""
    checks = {
        "database": "fail",
        "data_loaded": "fail",
        "pipeline_recent": "fail",
        "pipeline_status": "fail",
    }
    status_code = 503
    message = None

    try:
        async with pool.acquire() as con:
            # Check 1: database connectivity
            await con.fetchval("SELECT 1")
            checks["database"] = "ok"

            # Check 2: data loaded
            count = await con.fetchval("SELECT COUNT(*) FROM employer_profile")
            checks["data_loaded"] = "ok" if count and count > 0 else "fail"

            # Check 3 & 4: pipeline recency and status
            row = await con.fetchrow("""
                SELECT status, finished_at
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 1
            """)
            if row:
                checks["pipeline_status"] = "ok" if row["status"] in (
                    "completed", "completed_with_warnings"
                ) else "fail"

                if row["finished_at"]:
                    age_hours = (
                        datetime.now(timezone.utc) - row["finished_at"].replace(tzinfo=timezone.utc)
                    ).total_seconds() / 3600
                    checks["pipeline_recent"] = "ok" if age_hours <= 26 else "fail"

    except Exception as e:
        log.error("health_check_error", error=str(e))
        message = f"Database error: {type(e).__name__}"

    all_ok = all(v == "ok" for v in checks.values())
    if all_ok:
        status_code = 200

    body = {
        "status": "healthy" if all_ok else "degraded",
        "checks": checks,
        "api_version": "1.0",
    }
    if message:
        body["message"] = message

    return JSONResponse(body, status_code=status_code)

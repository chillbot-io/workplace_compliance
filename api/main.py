"""
api/main.py — FastAPI application entry point.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from api.auth import set_pool
from api.csrf import CSRFMiddleware
from api.routes.employers import router as employers_router
from api.routes.auth import router as auth_router
from api.routes.dashboard import router as dashboard_router
from api.routes.billing import router as billing_router

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
    set_pool(pool)  # share pool with auth module
    log.info("database_pool_ready", dsn=dsn.split("@")[-1])
    yield
    await pool.close()
    log.info("database_pool_closed")


app = FastAPI(title="Employer Compliance API", version="1.0", lifespan=lifespan)

# CSRF protection on dashboard routes
app.add_middleware(CSRFMiddleware)

# Register routes
app.include_router(employers_router)
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(billing_router)


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

    if not pool:
        return JSONResponse({"status": "unhealthy", "checks": checks, "api_version": "1.0"}, status_code=503)

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
        log.error("health_check_db_error", error=str(e))
        message = "Database unavailable"

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

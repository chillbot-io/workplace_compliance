"""
api/main.py — FastAPI application entry point.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import structlog
import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.auth import set_pool
from api.csrf import CSRFMiddleware
from api.routes.employers import router as employers_router
from api.routes.auth import router as auth_router
from api.routes.dashboard import router as dashboard_router
from api.routes.billing import router as billing_router
from api.routes.upload import router as upload_router

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)
log = structlog.get_logger()

# Sentry error tracking
sentry_dsn = os.environ.get("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        environment=os.environ.get("ENV", "development"),
        traces_sample_rate=0.1,
    )

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
    log.info("database_pool_ready")
    yield
    await pool.close()
    log.info("database_pool_closed")


app = FastAPI(title="FastDOL API", version="1.0", lifespan=lifespan)

# CORS — allow API consumers from any origin (API key auth, not cookie-based)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
    expose_headers=["X-Lookups-Remaining", "X-Lookups-Limit", "X-Data-Freshness", "X-Data-Age-Hours", "X-Billing-Note"],
)

# CSRF protection on dashboard routes
app.add_middleware(CSRFMiddleware)

# Register routes
app.include_router(employers_router)
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(billing_router)
app.include_router(upload_router)


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
    extra = {}

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
            extra["employer_profiles_count"] = count or 0

            # Check 3 & 4: pipeline recency and status
            row = await con.fetchrow("""
                SELECT status, finished_at, started_at
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 1
            """)
            if row:
                checks["pipeline_status"] = "ok" if row["status"] in (
                    "completed", "completed_with_warnings"
                ) else "fail"
                extra["last_pipeline_run"] = row["finished_at"].isoformat() if row["finished_at"] else None
                extra["last_pipeline_status"] = row["status"]

                if row["finished_at"]:
                    age_hours = (
                        datetime.now(timezone.utc) - row["finished_at"].replace(tzinfo=timezone.utc)
                    ).total_seconds() / 3600
                    checks["pipeline_recent"] = "ok" if age_hours <= 26 else "fail"
                    extra["data_age_hours"] = round(age_hours, 1)

    except Exception as e:
        log.error("health_check_db_error", error=str(e))
        message = "Database unavailable"

    all_ok = all(v == "ok" for v in checks.values())
    if all_ok:
        status_code = 200

    body = {
        "status": "healthy" if all_ok else "degraded",
        "checks": checks,
        **extra,
        "data_lag_note": "OSHA citations appear 3-8 months after inspection date",
        "api_version": "1.0",
    }
    if message:
        body["message"] = message

    return JSONResponse(body, status_code=status_code)

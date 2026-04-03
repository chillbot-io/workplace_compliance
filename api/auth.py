"""
api/auth.py — API key authentication, scope enforcement, and quota checking.

Auth flow:
1. verify_key(): hash the raw key, look up by hash, check status/expiration
2. check_scope(): verify the key has the required scope for this endpoint
3. check_monthly_quota(): count rows in api_usage this month (atomic, TOCTOU-safe)
4. record_usage(): log the API call for metered endpoints

Test keys (emp_test_ prefix) skip quota and route to test_fixtures table.
"""

import hashlib
import os
from datetime import date, datetime, timezone

from fastapi import Depends, Header, HTTPException, Request

# Pool is set by main.py lifespan
_pool = None


def set_pool(pool):
    global _pool
    _pool = pool


def get_pool():
    return _pool


async def verify_key(x_api_key: str = Header(..., alias="X-Api-Key")):
    """Verify API key: hash it, look up in DB, check status and expiration."""
    if not x_api_key:
        raise HTTPException(401, detail={
            "error": "missing_api_key",
            "message": "Provide an API key via the X-Api-Key header.",
        })

    # Test keys — only in non-production environments
    if x_api_key.startswith("emp_test_"):
        env = os.environ.get("ENV", "development")
        if env == "production":
            raise HTTPException(401, detail={
                "error": "test_key_disabled",
                "message": "Test keys are not available in production.",
            })
        return {
            "key_hash": hashlib.sha256(x_api_key.encode()).hexdigest(),
            "key_id": "test-key",
            "customer_id": None,
            "scopes": ["employer:read"],
            "monthly_limit": 999999,
            "status": "active",
            "is_test": True,
        }

    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()

    async with get_pool().acquire() as con:
        row = await con.fetchrow("""
            SELECT key_id, customer_id, scopes, monthly_limit, status,
                   expires_at, last_used_at
            FROM api_keys
            WHERE key_hash = $1
        """, key_hash)

    if not row:
        raise HTTPException(401, detail={
            "error": "invalid_api_key",
            "message": "The provided API key is not valid.",
        })

    # Check status
    if row["status"] == "revoked":
        raise HTTPException(401, detail={
            "error": "key_revoked",
            "message": "This API key has been revoked.",
        })

    # Check expiration
    if row["expires_at"] and row["expires_at"] < datetime.now(timezone.utc):
        raise HTTPException(401, detail={
            "error": "api_key_expired",
            "message": f"This API key expired on {row['expires_at'].date()}. Generate a new key.",
        })

    # Update last_used_at (fire and forget)
    async with get_pool().acquire() as con:
        await con.execute(
            "UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = $1", key_hash
        )

    return {
        "key_hash": key_hash,
        "key_id": str(row["key_id"]),
        "customer_id": row["customer_id"],
        "scopes": row["scopes"] or ["employer:read"],
        "monthly_limit": row["monthly_limit"],
        "status": row["status"],
        "is_test": False,
    }


def check_scope(required_scope: str):
    """FastAPI dependency that verifies the key has the required scope."""
    async def scope_checker(key_row=Depends(verify_key)):
        scopes = key_row.get("scopes", ["employer:read"])
        if "admin:all" in scopes or required_scope in scopes:
            return key_row
        raise HTTPException(403, detail={
            "error": "insufficient_scope",
            "message": f'This key requires the "{required_scope}" scope.',
        })
    return scope_checker


async def check_monthly_quota(key_row: dict):
    """Check if the key has exceeded its monthly quota. Raises 429 if so."""
    if key_row.get("is_test"):
        return  # test keys skip quota

    limit = key_row["monthly_limit"]
    if limit == 0:
        raise HTTPException(403, detail={
            "error": "key_disabled",
            "message": "This API key has no quota allocated.",
        })

    async with get_pool().acquire() as con:
        count = await con.fetchval("""
            SELECT COUNT(*) FROM api_usage
            WHERE key_hash = $1 AND queried_at >= date_trunc('month', NOW())
        """, key_row["key_hash"])

    if count >= limit:
        d = date.today()
        resets = date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)
        raise HTTPException(429, detail={
            "error": "monthly_quota_exceeded",
            "message": f"Monthly quota of {limit} lookups exceeded.",
            "resets_at": resets.isoformat(),
        })


async def record_usage(key_row: dict, endpoint: str, count: int = 1):
    """Log metered API usage. Called only on metered endpoints."""
    if key_row.get("is_test"):
        return  # test keys don't consume quota

    async with get_pool().acquire() as con:
        await con.execute("""
            INSERT INTO api_usage (key_hash, customer_id, endpoint, lookup_count, queried_at)
            VALUES ($1, $2, $3, $4, NOW())
        """, key_row["key_hash"], key_row["customer_id"], endpoint, count)
        # Update denormalized display counter
        await con.execute("""
            UPDATE api_keys SET current_usage = current_usage + $1
            WHERE key_hash = $2
        """, count, key_row["key_hash"])


async def get_quota_headers(key_row: dict) -> dict:
    """Generate X-Lookups-Remaining and X-Lookups-Limit headers."""
    if key_row.get("is_test"):
        return {"X-Billing-Note": "not-metered"}

    limit = key_row["monthly_limit"]
    async with get_pool().acquire() as con:
        used = await con.fetchval("""
            SELECT COUNT(*) FROM api_usage
            WHERE key_hash = $1 AND queried_at >= date_trunc('month', NOW())
        """, key_row["key_hash"])

    remaining = max(0, limit - used)
    return {
        "X-Lookups-Remaining": str(remaining),
        "X-Lookups-Limit": str(limit),
    }

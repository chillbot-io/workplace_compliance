"""
api/routes/dashboard.py — Dashboard key management.
Protected by JWT session cookie (not API key).
CSRF protection required on all POST/DELETE routes.
"""

import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.auth import get_pool
from api.routes.auth import get_current_user

router = APIRouter(prefix="/dashboard")


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, UUID):
            return str(obj)
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


class CreateKeyRequest(BaseModel):
    label: str = "default"
    scopes: list[str] = ["employer:read"]


## --- List Keys ---

@router.get("/keys")
async def list_keys(user=Depends(get_current_user)):
    """List all API keys for the current customer."""
    async with get_pool().acquire() as con:
        rows = await con.fetch("""
            SELECT key_id, key_prefix, label, scopes, monthly_limit, current_usage,
                   status, expires_at, rotation_expires_at, last_used_at, created_at
            FROM api_keys
            WHERE customer_id = $1 AND status != 'revoked'
            ORDER BY created_at DESC
        """, user["customer_id"])

    return JSONResponse(content={
        "data": [json.loads(json.dumps(dict(r), cls=CustomEncoder)) for r in rows],
        "total": len(rows),
    })


## --- Create Key ---

@router.post("/keys")
async def create_key(body: CreateKeyRequest, user=Depends(get_current_user)):
    """Generate a new API key. Raw key shown ONCE in response."""
    valid_scopes = {"employer:read", "batch:write", "subscriptions:manage", "admin:all"}
    for scope in body.scopes:
        if scope not in valid_scopes:
            raise HTTPException(400, detail={
                "error": "invalid_scope",
                "message": f"Invalid scope: {scope}. Valid: {', '.join(valid_scopes)}",
            })

    # Check key limit (max 5 per customer)
    async with get_pool().acquire() as con:
        key_count = await con.fetchval("""
            SELECT COUNT(*) FROM api_keys
            WHERE customer_id = $1 AND status != 'revoked'
        """, user["customer_id"])

        if key_count >= 5:
            raise HTTPException(400, detail={
                "error": "key_limit_reached",
                "message": "Maximum 5 active API keys per account.",
            })

        # Get customer's plan limit
        customer = await con.fetchrow(
            "SELECT monthly_limit FROM customers WHERE id = $1", user["customer_id"]
        )

        # Generate key
        raw_key = f"emp_live_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        key_prefix = raw_key[:8]

        key_id = await con.fetchval("""
            INSERT INTO api_keys (customer_id, key_hash, key_prefix, label, scopes, monthly_limit, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'active')
            RETURNING key_id
        """, user["customer_id"], key_hash, key_prefix, body.label, body.scopes,
            customer["monthly_limit"])

        # Audit log
        await con.execute("""
            INSERT INTO api_key_audit_log (key_id, customer_id, action, performed_by)
            VALUES ($1, $2, 'created', $3)
        """, key_id, user["customer_id"], user["email"])

    return JSONResponse(content={
        "key_id": str(key_id),
        "api_key": raw_key,
        "key_prefix": key_prefix,
        "label": body.label,
        "scopes": body.scopes,
        "message": "Save this key now. It will not be shown again.",
    })


## --- Rotate Key ---

@router.post("/keys/{key_id}/rotate")
async def rotate_key(key_id: str, user=Depends(get_current_user)):
    """Rotate a key. Old key works for 48h, new key is immediately active."""
    async with get_pool().acquire() as con:
        # Verify ownership
        old_key = await con.fetchrow("""
            SELECT id, customer_id, scopes, monthly_limit, label, status
            FROM api_keys
            WHERE key_id = $1::uuid AND customer_id = $2
        """, key_id, user["customer_id"])

        if not old_key:
            raise HTTPException(404, detail={
                "error": "key_not_found",
                "message": "API key not found.",
            })

        if old_key["status"] != "active":
            raise HTTPException(400, detail={
                "error": "key_not_active",
                "message": f"Cannot rotate a key with status '{old_key['status']}'.",
            })

        # Mark old key as rotating_out (48h NIST window)
        rotation_expires = datetime.now(timezone.utc) + timedelta(hours=48)
        await con.execute("""
            UPDATE api_keys SET status = 'rotating_out', rotation_expires_at = $1
            WHERE key_id = $2::uuid
        """, rotation_expires, key_id)

        # Generate new key with same settings
        raw_key = f"emp_live_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        key_prefix = raw_key[:8]

        new_key_id = await con.fetchval("""
            INSERT INTO api_keys (customer_id, key_hash, key_prefix, label, scopes, monthly_limit, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'active')
            RETURNING key_id
        """, user["customer_id"], key_hash, key_prefix,
            f"{old_key['label']} (rotated)", old_key["scopes"], old_key["monthly_limit"])

        # Audit log
        await con.execute("""
            INSERT INTO api_key_audit_log (key_id, customer_id, action, performed_by)
            VALUES ($1, $2, 'rotated', $3)
        """, new_key_id, user["customer_id"], user["email"])

    return JSONResponse(content={
        "new_key_id": str(new_key_id),
        "api_key": raw_key,
        "old_key_id": key_id,
        "old_key_expires": rotation_expires.isoformat(),
        "message": "New key active immediately. Old key will work for 48 more hours.",
    })


## --- Revoke Key ---

@router.delete("/keys/{key_id}")
async def revoke_key(key_id: str, user=Depends(get_current_user)):
    """Immediately revoke an API key."""
    async with get_pool().acquire() as con:
        result = await con.execute("""
            UPDATE api_keys SET status = 'revoked'
            WHERE key_id = $1::uuid AND customer_id = $2 AND status != 'revoked'
        """, key_id, user["customer_id"])

        if result == "UPDATE 0":
            raise HTTPException(404, detail={
                "error": "key_not_found",
                "message": "API key not found or already revoked.",
            })

        # Audit log
        await con.execute("""
            INSERT INTO api_key_audit_log (key_id, customer_id, action, performed_by)
            VALUES ($1::uuid, $2, 'revoked', $3)
        """, key_id, user["customer_id"], user["email"])

    return {"status": "revoked", "key_id": key_id}

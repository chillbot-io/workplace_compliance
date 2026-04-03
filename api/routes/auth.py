"""
api/routes/auth.py — Signup, login, email verification, password reset.

Uses argon2id for password hashing (not bcrypt — OWASP 2024 recommendation).
RS256 JWT stored as HttpOnly cookie for dashboard sessions.
API key auth (X-Api-Key header) is separate — handled in api/auth.py.
"""

import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from fastapi import APIRouter, HTTPException, Response, Request
from pydantic import BaseModel, EmailStr

from api.auth import get_pool

router = APIRouter(prefix="/auth")

# Argon2id config per arch doc (finding #16)
ph = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=4)

# JWT config — RS256 asymmetric
JWT_PRIVATE_KEY_PATH = os.environ.get("JWT_PRIVATE_KEY_PATH", "/etc/employer-compliance/jwt_private.pem")
JWT_PUBLIC_KEY_PATH = os.environ.get("JWT_PUBLIC_KEY_PATH", "/etc/employer-compliance/jwt_public.pem")
JWT_ALGORITHM = "RS256"
JWT_EXPIRY_HOURS = 8
JWT_ISSUER = "employer-compliance-api"

# Load keys (fallback to HS256 with secret for development)
_private_key = None
_public_key = None
_use_hs256 = False

try:
    with open(JWT_PRIVATE_KEY_PATH, "r") as f:
        _private_key = f.read()
    with open(JWT_PUBLIC_KEY_PATH, "r") as f:
        _public_key = f.read()
except FileNotFoundError:
    # Dev mode — fall back to HS256 with random secret
    _private_key = secrets.token_hex(32)
    _public_key = _private_key
    _use_hs256 = True
    print("WARNING: JWT RSA keys not found, using HS256 dev mode")


def _jwt_encode(payload: dict) -> str:
    alg = "HS256" if _use_hs256 else JWT_ALGORITHM
    return jwt.encode(payload, _private_key, algorithm=alg)


def _jwt_decode(token: str) -> dict:
    alg = "HS256" if _use_hs256 else JWT_ALGORITHM
    return jwt.decode(token, _public_key, algorithms=[alg], issuer=JWT_ISSUER)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


## --- Models ---

class SignupRequest(BaseModel):
    email: EmailStr
    password: str
    company_name: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


## --- Signup ---

@router.post("/signup")
async def signup(body: SignupRequest):
    """Create account with email + password. Sends verification email."""
    if len(body.password) < 8:
        raise HTTPException(400, detail={
            "error": "weak_password",
            "message": "Password must be at least 8 characters.",
        })

    password_hash = ph.hash(body.password)

    async with get_pool().acquire() as con:
        # Check if email already exists
        existing = await con.fetchval(
            "SELECT id FROM customers WHERE email = $1", body.email
        )
        if existing:
            raise HTTPException(409, detail={
                "error": "email_exists",
                "message": "An account with this email already exists.",
            })

        # Create customer
        customer_id = await con.fetchval("""
            INSERT INTO customers (email, password_hash, company_name, plan, monthly_limit)
            VALUES ($1, $2, $3, 'free', 5)
            RETURNING id
        """, body.email, password_hash, body.company_name)

        # Generate verification token
        raw_token = secrets.token_urlsafe(32)
        token_hash = _hash_token(raw_token)
        expires_at = datetime.utcnow() + timedelta(hours=24)

        await con.execute("""
            INSERT INTO email_verifications (customer_id, token_hash, expires_at)
            VALUES ($1, $2, $3)
        """, customer_id, token_hash, expires_at)

    # TODO: Send verification email via Resend
    # For now, return the token in the response (dev mode)
    return {
        "status": "created",
        "message": "Account created. Please verify your email.",
        "customer_id": customer_id,
        # Remove this in production — token should only be in the email
        "_dev_verification_token": raw_token,
    }


## --- Email Verification ---

@router.get("/verify")
async def verify_email(token: str, response: Response):
    """Verify email address and activate account."""
    token_hash = _hash_token(token)

    async with get_pool().acquire() as con:
        row = await con.fetchrow("""
            SELECT id, customer_id, expires_at, used
            FROM email_verifications
            WHERE token_hash = $1
        """, token_hash)

        if not row:
            raise HTTPException(400, detail={
                "error": "invalid_token",
                "message": "Invalid or expired verification token.",
            })

        if row["used"]:
            raise HTTPException(400, detail={
                "error": "token_used",
                "message": "This verification link has already been used.",
            })

        if row["expires_at"] < datetime.utcnow():
            raise HTTPException(400, detail={
                "error": "token_expired",
                "message": "This verification link has expired. Please request a new one.",
            })

        # Mark token as used
        await con.execute(
            "UPDATE email_verifications SET used = true WHERE id = $1", row["id"]
        )

        # Generate first API key for the customer
        raw_key = f"emp_live_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        key_prefix = raw_key[:8]

        await con.execute("""
            INSERT INTO api_keys (customer_id, key_hash, key_prefix, label, scopes, monthly_limit, status)
            VALUES ($1, $2, $3, 'default', '{employer:read}', 5, 'active')
        """, row["customer_id"], key_hash, key_prefix)

        # Log audit event
        key_id = await con.fetchval(
            "SELECT key_id FROM api_keys WHERE key_hash = $1", key_hash
        )
        await con.execute("""
            INSERT INTO api_key_audit_log (key_id, customer_id, action, performed_by)
            VALUES ($1, $2, 'created', 'system:email_verification')
        """, key_id, row["customer_id"])

    return {
        "status": "verified",
        "message": "Email verified. Your API key is below. Save it now — it won't be shown again.",
        "api_key": raw_key,
    }


## --- Login ---

@router.post("/login")
async def login(body: LoginRequest, response: Response):
    """Login with email + password. Returns JWT as HttpOnly cookie."""
    async with get_pool().acquire() as con:
        row = await con.fetchrow(
            "SELECT id, password_hash, role, email FROM customers WHERE email = $1",
            body.email,
        )

    if not row:
        raise HTTPException(401, detail={
            "error": "invalid_credentials",
            "message": "Invalid email or password.",
        })

    try:
        ph.verify(row["password_hash"], body.password)
    except VerifyMismatchError:
        raise HTTPException(401, detail={
            "error": "invalid_credentials",
            "message": "Invalid email or password.",
        })

    # Check if argon2 params need rehashing
    if ph.check_needs_rehash(row["password_hash"]):
        new_hash = ph.hash(body.password)
        async with get_pool().acquire() as con:
            await con.execute(
                "UPDATE customers SET password_hash = $1 WHERE id = $2",
                new_hash, row["id"],
            )

    # Issue JWT
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(row["id"]),
        "role": row["role"],
        "email": row["email"],
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=JWT_EXPIRY_HOURS)).timestamp()),
        "iss": JWT_ISSUER,
        "jti": secrets.token_hex(16),
    }
    token = _jwt_encode(payload)

    # Set as HttpOnly cookie
    response.set_cookie(
        key="session",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=JWT_EXPIRY_HOURS * 3600,
        path="/",
    )

    return {"status": "ok", "message": "Logged in.", "role": row["role"]}


## --- Forgot Password ---

@router.post("/forgot-password")
async def forgot_password(body: ForgotPasswordRequest):
    """Request password reset. Always returns 202 (don't leak if email exists)."""
    async with get_pool().acquire() as con:
        customer = await con.fetchrow(
            "SELECT id FROM customers WHERE email = $1", body.email
        )

        if customer:
            raw_token = secrets.token_urlsafe(32)
            token_hash = _hash_token(raw_token)
            expires_at = datetime.utcnow() + timedelta(hours=1)

            await con.execute("""
                INSERT INTO password_reset_tokens (customer_id, token_hash, expires_at)
                VALUES ($1, $2, $3)
            """, customer["id"], token_hash, expires_at)

            # TODO: Send reset email via Resend
            # For now, return token in response (dev mode)
            return {
                "status": "sent",
                "message": "If an account exists with this email, a reset link has been sent.",
                "_dev_reset_token": raw_token,
            }

    # Always return 202 regardless — don't reveal whether email exists
    return {"status": "sent", "message": "If an account exists with this email, a reset link has been sent."}


## --- Reset Password ---

@router.post("/reset-password")
async def reset_password(body: ResetPasswordRequest):
    """Reset password using token from email."""
    if len(body.new_password) < 8:
        raise HTTPException(400, detail={
            "error": "weak_password",
            "message": "Password must be at least 8 characters.",
        })

    token_hash = _hash_token(body.token)

    async with get_pool().acquire() as con:
        row = await con.fetchrow("""
            SELECT id, customer_id, expires_at, used
            FROM password_reset_tokens
            WHERE token_hash = $1
        """, token_hash)

        if not row:
            raise HTTPException(400, detail={
                "error": "invalid_token",
                "message": "Invalid or expired reset token.",
            })

        if row["used"]:
            raise HTTPException(400, detail={
                "error": "token_used",
                "message": "This reset link has already been used.",
            })

        if row["expires_at"] < datetime.utcnow():
            raise HTTPException(400, detail={
                "error": "token_expired",
                "message": "This reset link has expired. Please request a new one.",
            })

        # Update password
        new_hash = ph.hash(body.new_password)
        await con.execute(
            "UPDATE customers SET password_hash = $1, updated_at = NOW() WHERE id = $2",
            new_hash, row["customer_id"],
        )

        # Mark token as used
        await con.execute(
            "UPDATE password_reset_tokens SET used = true WHERE id = $1", row["id"]
        )

    return {"status": "ok", "message": "Password has been reset. Please login with your new password."}


## --- JWT Session Helper (for dashboard routes) ---

async def get_current_user(request: Request) -> dict:
    """Extract and validate JWT from session cookie. Used by dashboard endpoints."""
    token = request.cookies.get("session")
    if not token:
        raise HTTPException(401, detail={
            "error": "not_authenticated",
            "message": "Please login to access the dashboard.",
        })

    try:
        payload = _jwt_decode(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, detail={
            "error": "session_expired",
            "message": "Your session has expired. Please login again.",
        })
    except jwt.InvalidTokenError:
        raise HTTPException(401, detail={
            "error": "invalid_session",
            "message": "Invalid session. Please login again.",
        })

    return {
        "customer_id": int(payload["sub"]),
        "role": payload["role"],
        "email": payload["email"],
    }

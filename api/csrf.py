"""
api/csrf.py — Double-submit cookie CSRF protection for dashboard routes.

API key endpoints (/v1/*) are exempt — X-Api-Key header is not auto-attached by browsers.
Dashboard endpoints (/dashboard/*) use cookie-based JWT sessions and need CSRF protection.

Pattern: On GET, set a csrf_token cookie (JS-readable). On POST/PUT/DELETE, require
the same token in the X-CSRF-Token header. Compare with hmac.compare_digest.
"""

import os
import secrets
import hmac

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response, JSONResponse


CSRF_COOKIE = "csrf_token"
CSRF_HEADER = "X-CSRF-Token"
CSRF_SECRET = os.environ.get("CSRF_SECRET", "").encode() or secrets.token_bytes(32)
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
PROTECTED_PREFIXES = ("/dashboard/",)


class CSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Only protect dashboard routes
        if not any(request.url.path.startswith(p) for p in PROTECTED_PREFIXES):
            return await call_next(request)

        # Safe methods — just ensure the cookie exists
        if request.method in SAFE_METHODS:
            response = await call_next(request)
            if CSRF_COOKIE not in request.cookies:
                token = secrets.token_urlsafe(32)
                response.set_cookie(
                    CSRF_COOKIE, token,
                    httponly=False,  # JS must read this to send in header
                    secure=True,
                    samesite="strict",
                    max_age=3600 * 8,  # matches JWT expiry
                )
            return response

        # On POST/PUT/DELETE: validate double-submit
        cookie_token = request.cookies.get(CSRF_COOKIE)
        header_token = request.headers.get(CSRF_HEADER)

        if not cookie_token or not header_token:
            return JSONResponse(
                status_code=403,
                content={"error": "csrf_missing", "message": "CSRF token required. Send the csrf_token cookie value in the X-CSRF-Token header."},
            )

        if not hmac.compare_digest(cookie_token, header_token):
            return JSONResponse(
                status_code=403,
                content={"error": "csrf_invalid", "message": "CSRF token mismatch."},
            )

        return await call_next(request)

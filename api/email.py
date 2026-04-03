"""
api/email.py — Send transactional emails via Resend.

Handles verification emails and password reset emails.
Uses onboarding@resend.dev in dev/test, noreply@fastdol.com in production.
"""

import os

import resend

resend.api_key = os.environ.get("RESEND_API_KEY", "")

FROM_EMAIL = os.environ.get("FROM_EMAIL", "FastDOL <onboarding@resend.dev>")
BASE_URL = os.environ.get("BASE_URL", "https://api.fastdol.com")


def send_verification_email(to_email: str, token: str):
    """Send email verification link after signup."""
    verify_url = f"{BASE_URL}/auth/verify?token={token}"

    resend.Emails.send({
        "from": FROM_EMAIL,
        "to": [to_email],
        "subject": "Verify your FastDOL account",
        "html": f"""
        <h2>Welcome to FastDOL</h2>
        <p>Click the link below to verify your email and get your API key:</p>
        <p><a href="{verify_url}" style="background:#2563eb;color:white;padding:12px 24px;text-decoration:none;border-radius:6px;display:inline-block;">Verify Email</a></p>
        <p>Or copy this URL: {verify_url}</p>
        <p>This link expires in 24 hours.</p>
        <p>— The FastDOL Team</p>
        """,
    })


def send_password_reset_email(to_email: str, token: str):
    """Send password reset link."""
    reset_url = f"{BASE_URL}/auth/reset-password?token={token}"

    resend.Emails.send({
        "from": FROM_EMAIL,
        "to": [to_email],
        "subject": "Reset your FastDOL password",
        "html": f"""
        <h2>Password Reset</h2>
        <p>Click the link below to reset your password:</p>
        <p><a href="{reset_url}" style="background:#2563eb;color:white;padding:12px 24px;text-decoration:none;border-radius:6px;display:inline-block;">Reset Password</a></p>
        <p>Or copy this URL: {reset_url}</p>
        <p>This link expires in 1 hour. If you didn't request this, ignore this email.</p>
        <p>— The FastDOL Team</p>
        """,
    })

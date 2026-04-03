"""
api/routes/billing.py — Stripe webhook handler and checkout session creation.

Handles:
- checkout.session.completed → upgrade customer plan + adjust API key limits
- customer.subscription.deleted → downgrade to free tier
- Idempotency via stripe_webhook_events table (finding #17)

Free tier (50 lookups/mo) is handled in code — no Stripe checkout needed.
"""

import hashlib
import json
import os
import secrets

import stripe
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from api.auth import get_pool

router = APIRouter()

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

# Price ID → (plan_name, monthly_limit)
TIER_MAP = {
    "price_1TI7QN3EcFqpAdAMNk2FkldO": ("starter", 1000),
    "price_1TI7R23EcFqpAdAM1RXpYSNb": ("growth", 5000),
    "price_1TI7Rc3EcFqpAdAMNw3paitK": ("pro", 25000),
}


## --- Create Checkout Session ---

@router.post("/billing/checkout")
async def create_checkout(request: Request):
    """Create a Stripe checkout session. Customer must be logged in."""
    from api.routes.auth import get_current_user
    user = await get_current_user(request)

    body = await request.json()
    price_id = body.get("price_id")

    if price_id not in TIER_MAP:
        raise HTTPException(400, detail={
            "error": "invalid_price",
            "message": f"Invalid price_id. Valid options: {list(TIER_MAP.keys())}",
        })

    async with get_pool().acquire() as con:
        customer = await con.fetchrow(
            "SELECT id, email, stripe_customer_id FROM customers WHERE id = $1",
            user["customer_id"],
        )

    # Create or reuse Stripe customer
    if customer["stripe_customer_id"]:
        stripe_customer_id = customer["stripe_customer_id"]
    else:
        stripe_customer = stripe.Customer.create(
            email=customer["email"],
            metadata={"fastdol_customer_id": str(customer["id"])},
        )
        stripe_customer_id = stripe_customer.id
        async with get_pool().acquire() as con:
            await con.execute(
                "UPDATE customers SET stripe_customer_id = $1 WHERE id = $2",
                stripe_customer_id, customer["id"],
            )

    # Create checkout session
    session = stripe.checkout.Session.create(
        customer=stripe_customer_id,
        payment_method_types=["card"],
        line_items=[{"price": price_id, "quantity": 1}],
        mode="subscription",
        success_url=f"https://api.fastdol.com/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url="https://api.fastdol.com/billing/cancel",
        metadata={"fastdol_customer_id": str(customer["id"])},
    )

    return JSONResponse(content={
        "checkout_url": session.url,
        "session_id": session.id,
    })


## --- Stripe Webhook ---

@router.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events with idempotency."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except ValueError:
        raise HTTPException(400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(400, detail="Invalid signature")

    # Idempotency check (finding #17)
    async with get_pool().acquire() as con:
        existing = await con.fetchval(
            "SELECT event_id FROM stripe_webhook_events WHERE event_id = $1",
            event["id"],
        )
        if existing:
            return JSONResponse(content={"status": "duplicate"})

        # Record event
        await con.execute(
            "INSERT INTO stripe_webhook_events (event_id, event_type) VALUES ($1, $2)",
            event["id"], event["type"],
        )

    # Handle events
    if event["type"] == "checkout.session.completed":
        await _handle_checkout_completed(event["data"]["object"])
    elif event["type"] == "customer.subscription.deleted":
        await _handle_subscription_deleted(event["data"]["object"])

    return JSONResponse(content={"status": "ok"})


async def _handle_checkout_completed(session):
    """Upgrade customer plan and API key limits after successful checkout."""
    stripe_customer_id = session.get("customer")
    if not stripe_customer_id:
        return

    # Get the price from the subscription
    subscription_id = session.get("subscription")
    if not subscription_id:
        return

    subscription = stripe.Subscription.retrieve(subscription_id)
    price_id = subscription["items"]["data"][0]["price"]["id"]

    plan_name, monthly_limit = TIER_MAP.get(price_id, ("free", 50))

    async with get_pool().acquire() as con:
        # Find customer by stripe_customer_id
        customer = await con.fetchrow(
            "SELECT id FROM customers WHERE stripe_customer_id = $1",
            stripe_customer_id,
        )

        if not customer:
            # Customer might not exist yet — find by metadata
            fastdol_id = session.get("metadata", {}).get("fastdol_customer_id")
            if fastdol_id:
                await con.execute(
                    "UPDATE customers SET stripe_customer_id = $1 WHERE id = $2",
                    stripe_customer_id, int(fastdol_id),
                )
                customer = {"id": int(fastdol_id)}
            else:
                return

        # Upgrade plan
        await con.execute("""
            UPDATE customers SET plan = $1, monthly_limit = $2, updated_at = NOW()
            WHERE id = $3
        """, plan_name, monthly_limit, customer["id"])

        # Update all active API keys with new limit
        await con.execute("""
            UPDATE api_keys SET monthly_limit = $1
            WHERE customer_id = $2 AND status = 'active'
        """, monthly_limit, customer["id"])


async def _handle_subscription_deleted(subscription):
    """Downgrade customer to free tier when subscription is cancelled."""
    stripe_customer_id = subscription.get("customer")
    if not stripe_customer_id:
        return

    async with get_pool().acquire() as con:
        customer = await con.fetchrow(
            "SELECT id FROM customers WHERE stripe_customer_id = $1",
            stripe_customer_id,
        )
        if not customer:
            return

        # Downgrade to free
        await con.execute("""
            UPDATE customers SET plan = 'free', monthly_limit = 50, updated_at = NOW()
            WHERE id = $1
        """, customer["id"])

        # Downgrade all active API keys
        await con.execute("""
            UPDATE api_keys SET monthly_limit = 50
            WHERE customer_id = $1 AND status = 'active'
        """, customer["id"])


## --- Success/Cancel Pages ---

@router.get("/billing/success")
async def billing_success(session_id: str):
    return JSONResponse(content={
        "status": "success",
        "message": "Payment successful! Your plan has been upgraded.",
        "session_id": session_id,
    })


@router.get("/billing/cancel")
async def billing_cancel():
    return JSONResponse(content={
        "status": "cancelled",
        "message": "Checkout was cancelled. Your plan has not changed.",
    })

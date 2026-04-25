"""Stripe billing endpoints — checkout, webhooks, customer portal."""
import os
import json
import logging
from typing import Optional

import stripe
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_auth, UserInfo

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/billing", tags=["billing"])

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRO_PRICE_ID = os.getenv("STRIPE_PRO_PRICE_ID", "")
STRIPE_ELITE_PRICE_ID = os.getenv("STRIPE_ELITE_PRICE_ID", "")
APP_URL = os.getenv("APP_URL", "http://localhost:8000")


class CheckoutRequest(BaseModel):
    tier: str  # "pro" or "elite"


@router.post("/checkout")
async def create_checkout(
    body: CheckoutRequest,
    user: UserInfo = Depends(require_auth),
):
    """Create a Stripe Checkout session for Pro or Elite subscription."""
    if body.tier not in ("pro", "elite"):
        raise HTTPException(400, "Invalid tier. Must be 'pro' or 'elite'.")

    price_id = STRIPE_PRO_PRICE_ID if body.tier == "pro" else STRIPE_ELITE_PRICE_ID
    if not price_id:
        raise HTTPException(500, f"Stripe price ID for '{body.tier}' not configured.")

    if not stripe.api_key:
        raise HTTPException(500, "Stripe not configured.")

    # Get or create Stripe customer
    customer_id = user.stripe_customer_id
    if not customer_id:
        customer = stripe.Customer.create(
            email=user.email,
            metadata={"user_id": str(user.user_id)},
        )
        customer_id = customer.id
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET stripe_customer_id=$1 WHERE id=$2",
                customer_id, user.user_id
            )

    try:
        session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            success_url=f"{APP_URL}/?subscription=success&tier={body.tier}",
            cancel_url=f"{APP_URL}/?upgrade={body.tier}",
            metadata={
                "user_id": str(user.user_id),
                "tier": body.tier,
            },
            subscription_data={
                "metadata": {
                    "user_id": str(user.user_id),
                    "tier": body.tier,
                }
            },
        )
    except stripe.error.StripeError as e:
        logger.error(f"Stripe error creating checkout: {e}")
        raise HTTPException(500, f"Stripe error: {str(e)}")

    # Telemetry: checkout funnel start
    try:
        from services.telemetry import emit
        await emit("checkout_start", user_id=user.user_id, tier=user.tier,
                   properties={"target_tier": body.tier})
    except Exception:
        pass

    return {"checkout_url": session.url, "session_id": session.id}


@router.post("/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    # HARDENED: Signature-Verification ist NICHT optional. Wenn kein Secret
    # konfiguriert ist, muss der Endpoint hart failen statt tier-upgrades
    # durchzuwinken — sonst ist das ein direkter Privilege-Escalation-Pfad
    # (ein Angreifer POSTet ein gefaktes subscription.updated-Event mit
    # metadata.tier="elite" und ist Elite-User ohne zu zahlen).
    if not STRIPE_WEBHOOK_SECRET:
        logger.error("STRIPE_WEBHOOK_SECRET not configured — refusing to process webhook.")
        raise HTTPException(500, "Webhook secret not configured.")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        raise HTTPException(400, "Invalid payload.")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(400, "Invalid signature.")

    event_type = event["type"]
    data = event["data"]["object"]

    if event_type in ("customer.subscription.created", "customer.subscription.updated"):
        await _handle_subscription_update(data)
    elif event_type == "customer.subscription.deleted":
        await _handle_subscription_deleted(data)
    elif event_type == "checkout.session.completed":
        # Subscription is already handled by subscription events— just emit
        # the funnel-success event so we can measure conversion.
        try:
            from services.telemetry import emit
            meta = data.get("metadata", {})
            uid = meta.get("user_id")
            await emit(
                "checkout_success",
                user_id=int(uid) if uid else None,
                tier=meta.get("tier"),
                properties={"session_id": data.get("id"), "amount_total": data.get("amount_total")},
            )
        except Exception:
            pass

    return {"received": True}


async def _handle_subscription_update(subscription: dict):
    """Update user tier based on active subscription."""
    sub_id = subscription.get("id")
    customer_id = subscription.get("customer")
    status_val = subscription.get("status")
    meta = subscription.get("metadata", {})
    tier = meta.get("tier", "")
    current_period_end = subscription.get("current_period_end")

    if not customer_id:
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            "SELECT id FROM users WHERE stripe_customer_id=$1", customer_id
        )
        if user_row is None:
            # Try from metadata
            user_id = meta.get("user_id")
            if user_id:
                user_row = await conn.fetchrow("SELECT id FROM users WHERE id=$1", int(user_id))

        if user_row is None:
            logger.warning(f"No user found for customer {customer_id}")
            return

        user_id = user_row["id"]

        # Convert unix timestamp to timestamptz if present
        period_end = None
        if current_period_end:
            from datetime import datetime, timezone
            period_end = datetime.fromtimestamp(int(current_period_end), tz=timezone.utc)

        # Upsert subscription
        await conn.execute("""
            INSERT INTO subscriptions (user_id, stripe_subscription_id, tier, status, current_period_end)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(stripe_subscription_id) DO UPDATE SET
                tier=EXCLUDED.tier, status=EXCLUDED.status,
                current_period_end=EXCLUDED.current_period_end
        """, user_id, sub_id, tier, status_val, period_end)

        # Update user tier based on the EFFECTIVE tier across all active
        # subscription sources (stripe + shop_bonus). Avoids downgrading a user
        # who still has an active shop-bonus when their Stripe sub lapses.
        if status_val == "active" and tier in ("pro", "elite"):
            await conn.execute("UPDATE users SET tier=$1 WHERE id=$2", tier, user_id)
        elif status_val not in ("active", "trialing"):
            await _recompute_effective_tier(conn, user_id)

        logger.info(f"Updated subscription for user {user_id}: tier={tier} status={status_val}")


async def _handle_subscription_deleted(subscription: dict):
    """Downgrade user to free tier when subscription is cancelled."""
    customer_id = subscription.get("customer")
    sub_id = subscription.get("id")

    if not customer_id:
        return

    pool = await get_pool()
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            "SELECT id FROM users WHERE stripe_customer_id=$1", customer_id
        )
        if user_row:
            user_id = user_row["id"]
            # First mark the Stripe subscription canceled, then recompute the
            # effective tier from any remaining active subs (e.g. shop_bonus).
            await conn.execute(
                "UPDATE subscriptions SET status='canceled' WHERE stripe_subscription_id=$1",
                sub_id,
            )
            await _recompute_effective_tier(conn, user_id)
            logger.info(f"Stripe sub canceled for user {user_id} — effective tier recomputed")


async def _recompute_effective_tier(conn, user_id: int):
    """Set users.tier to the highest tier of any active subscription row.

    Reads from the subscriptions table after updates have been applied. If no
    active row exists in any source, tier becomes 'free'. Tier hierarchy:
    elite > pro > free.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    row = await conn.fetchrow(
        """SELECT tier FROM subscriptions
           WHERE user_id=$1 AND status='active'
             AND (current_period_end IS NULL OR current_period_end > $2)
           ORDER BY CASE tier WHEN 'elite' THEN 2 WHEN 'pro' THEN 1 ELSE 0 END DESC
           LIMIT 1""",
        user_id, now,
    )
    new_tier = row["tier"] if row else "free"
    await conn.execute("UPDATE users SET tier=$1 WHERE id=$2", new_tier, user_id)


@router.get("/portal")
async def customer_portal(user: UserInfo = Depends(require_auth)):
    """Get Stripe customer portal URL for subscription management."""
    if not user.stripe_customer_id:
        raise HTTPException(400, "No active subscription found.")

    if not stripe.api_key:
        raise HTTPException(500, "Stripe not configured.")

    try:
        session = stripe.billing_portal.Session.create(
            customer=user.stripe_customer_id,
            return_url=f"{APP_URL}/",
        )
        return {"portal_url": session.url}
    except stripe.error.StripeError as e:
        raise HTTPException(500, f"Stripe error: {str(e)}")


@router.get("/plans")
async def get_plans():
    """Get available subscription plans (public)."""
    return {
        "plans": [
            {
                "id": "pro",
                "name": "Pro",
                "price_chf": 19,
                "price_label": "CHF 19/mo (Early Bird)",
                "regular_price": "CHF 29/mo",
                "features": [
                    "All sets (JP + EN)",
                    "Real-time data (15-min cache)",
                    "Full arbitrage scanner with profit calculations",
                    "Sealed product tracker with charts",
                    "EV Calculator",
                    "Weekly Market Digest",
                    "Historical price charts (30 days)",
                ],
            },
            {
                "id": "elite",
                "name": "Elite",
                "price_chf": 69,
                "price_label": "CHF 69/mo",
                "features": [
                    "Everything in Pro",
                    "Historical price charts (1 year)",
                    "Price alerts (email)",
                    "API access (coming soon)",
                    "Priority support",
                ],
            },
        ]
    }

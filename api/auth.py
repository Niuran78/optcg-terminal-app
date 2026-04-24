"""Authentication API endpoints — register, login, JWT."""
import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt as _bcrypt
from fastapi import APIRouter, Depends, HTTPException, status
from jose import jwt
from pydantic import BaseModel, EmailStr

from db.init import get_pool
from middleware.tier_gate import JWT_SECRET, JWT_ALGORITHM, get_current_user, UserInfo

router = APIRouter(prefix="/api/auth", tags=["auth"])
JWT_EXPIRE_DAYS = int(os.getenv("JWT_EXPIRE_DAYS", "30"))


# ─── Schemas ──────────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


# ─── Helpers ──────────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


def create_token(user_id: int) -> str:
    expire = datetime.utcnow() + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": str(user_id), "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def user_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "email": row["email"],
        "tier": row["tier"],
        "created_at": str(row["created_at"]),
    }


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    """Register a new user (free tier)."""
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters.")

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check existing
        existing = await conn.fetchval("SELECT id FROM users WHERE email=$1", body.email)
        if existing:
            raise HTTPException(409, "Email already registered.")

        password_hash = hash_password(body.password)
        user_id = await conn.fetchval(
            "INSERT INTO users (email, password_hash, tier) VALUES ($1, $2, 'free') RETURNING id",
            body.email, password_hash
        )

        # Fetch the inserted row
        row = await conn.fetchrow(
            "SELECT id, email, tier, created_at FROM users WHERE id=$1",
            user_id
        )

        token = create_token(row["id"])
        return TokenResponse(access_token=token, user=user_to_dict(row))


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    """Login with email and password."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, password_hash, tier, created_at FROM users WHERE email=$1",
            body.email
        )

    if row is None or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(401, "Invalid email or password.")

    token = create_token(row["id"])
    return TokenResponse(
        access_token=token,
        user={
            "id": row["id"],
            "email": row["email"],
            "tier": row["tier"],
            "created_at": str(row["created_at"]),
        }
    )


@router.get("/me")
async def me(user: UserInfo = Depends(get_current_user)):
    """Get current user info and tier."""
    if not user.is_authenticated:
        raise HTTPException(401, "Not authenticated.")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, tier, stripe_customer_id, created_at FROM users WHERE id=$1",
            user.user_id
        )
        if row is None:
            raise HTTPException(404, "User not found.")

        # Get active subscription
        sub = await conn.fetchrow(
            """SELECT tier, status, current_period_end FROM subscriptions
               WHERE user_id=$1 AND status='active' ORDER BY id DESC LIMIT 1""",
            user.user_id
        )

    return {
        "id": row["id"],
        "email": row["email"],
        "tier": row["tier"],
        "stripe_customer_id": row["stripe_customer_id"],
        "created_at": str(row["created_at"]),
        "subscription": dict(sub) if sub else None,
    }


# ─── Admin Endpoint ───────────────────────────────────────────────────────────

ADMIN_SECRET = os.getenv("ADMIN_SECRET", "optcg_admin_2026_blockreaction")

class AdminTierRequest(BaseModel):
    email: EmailStr
    tier: str  # free, pro, elite
    admin_secret: str

@router.post("/admin/set-tier")
async def admin_set_tier(body: AdminTierRequest):
    """Admin endpoint to manually set a user's tier."""
    if body.admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Invalid admin secret.")
    if body.tier not in ("free", "pro", "elite"):
        raise HTTPException(400, "Tier must be free, pro, or elite.")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, email, tier FROM users WHERE email=$1", body.email)
        if row is None:
            raise HTTPException(404, "User not found.")

        await conn.execute("UPDATE users SET tier=$1 WHERE email=$2", body.tier, body.email)

    return {"message": f"User {body.email} tier updated to {body.tier}"}


# ─── Shop Bonus: Terminal-Pro als Kaufprämie ───────────────────────────
#
# Regel (gemäss Holygrade 90-Tage-Strategie):
#   - Bestellung >= 300 EUR: 3 Monate Terminal-Pro gratis
#   - Bestellung >= 1000 EUR: 12 Monate Terminal-Pro gratis
#
# Workflow:
#   1) Kunde bestellt bei holygrade.com
#   2) Du triggerst diesen Endpoint manuell oder via Shopify-Webhook
#   3) Endpoint erstellt User falls noch nicht vorhanden (Temp-Passwort zum
#      Zurücksetzen) oder upgraded bestehenden User
#   4) Setzt tier auf 'pro' und trackt Ablaufdatum in subscriptions-Tabelle
#   5) Retournierts das Temp-Passwort für die Welcome-Mail

class ShopBonusRequest(BaseModel):
    email: EmailStr
    order_amount_eur: float
    order_id: str
    admin_secret: str


@router.post("/admin/shop-bonus")
async def shop_bonus(body: ShopBonusRequest):
    """Grant Terminal-Pro tier as a purchase bonus.

    - >= 300 EUR = 3 months Pro
    - >= 1000 EUR = 12 months Pro
    - < 300 EUR = no bonus (returns 400)
    """
    import secrets
    from datetime import datetime, timedelta, timezone
    from passlib.context import CryptContext

    if body.admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Invalid admin secret.")

    if body.order_amount_eur < 300:
        raise HTTPException(400, f"Order amount {body.order_amount_eur:.2f} EUR below bonus threshold (300 EUR).")

    # Bestimme Bonus-Dauer
    if body.order_amount_eur >= 1000:
        months = 12
        bonus_label = "12 months"
    else:
        months = 3
        bonus_label = "3 months"

    period_end = datetime.now(timezone.utc) + timedelta(days=months * 30)

    pool = await get_pool()
    pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
    temp_password = None

    bonus_sub_id = f"shop_bonus_{body.order_id}"

    async with pool.acquire() as conn:
        # IDEMPOTENCY GUARD — if this order_id was already processed, return
        # the existing result without granting another bonus. Shopify retries
        # webhooks on 5xx or timeout, so the same order_id can arrive 2–3×.
        duplicate = await conn.fetchrow(
            "SELECT id, user_id, current_period_end FROM subscriptions WHERE stripe_subscription_id=$1",
            bonus_sub_id,
        )
        if duplicate:
            return {
                "message": f"Order {body.order_id} already processed (idempotent).",
                "email": body.email,
                "user_action": "no_op_already_applied",
                "subscription_action": "no_op_duplicate",
                "pro_until": duplicate["current_period_end"].isoformat() if duplicate["current_period_end"] else None,
                "temp_password": None,
                "order_id": body.order_id,
                "order_amount_eur": body.order_amount_eur,
                "idempotent": True,
            }

        user_row = await conn.fetchrow("SELECT id, email, tier FROM users WHERE email=$1", body.email)

        if user_row is None:
            # Create user with a random temp password (customer resets via login flow)
            temp_password = secrets.token_urlsafe(12)
            pw_hash = pwd.hash(temp_password)
            user_row = await conn.fetchrow(
                "INSERT INTO users (email, password_hash, tier) VALUES ($1, $2, 'pro') RETURNING id, email, tier",
                body.email, pw_hash,
            )
            action = "created"
        else:
            # Upgrade existing user to pro (only if currently free)
            if user_row["tier"] == "free":
                await conn.execute("UPDATE users SET tier='pro' WHERE id=$1", user_row["id"])
                action = "upgraded_to_pro"
            else:
                action = f"kept_{user_row['tier']}"  # Don't downgrade elite to pro

        # Track the bonus in subscriptions (extends existing active period if any)
        existing_sub = await conn.fetchrow(
            """SELECT id, current_period_end FROM subscriptions
               WHERE user_id=$1 AND status='active'
               ORDER BY current_period_end DESC NULLS LAST LIMIT 1""",
            user_row["id"],
        )

        if existing_sub and existing_sub["current_period_end"] and existing_sub["current_period_end"] > datetime.now(timezone.utc):
            # Extend existing period: always add N months to whichever is later
            # (existing period_end or 'now'). This is additive + order-independent.
            base = existing_sub["current_period_end"]
            new_end = base + timedelta(days=months * 30)
            await conn.execute(
                "UPDATE subscriptions SET current_period_end=$1, stripe_subscription_id=$2 WHERE id=$3",
                new_end, bonus_sub_id, existing_sub["id"],
            )
            period_end = new_end
            sub_action = "extended"
        else:
            # Create new subscription entry (idempotency enforced by unique stripe_subscription_id)
            await conn.execute(
                """INSERT INTO subscriptions
                   (user_id, stripe_subscription_id, tier, status, current_period_end)
                   VALUES ($1, $2, 'pro', 'active', $3)
                   ON CONFLICT (stripe_subscription_id) DO NOTHING""",
                user_row["id"],
                bonus_sub_id,
                period_end,
            )
            sub_action = "created"

    return {
        "message": f"Shop bonus granted: {bonus_label} Terminal-Pro",
        "email": body.email,
        "user_action": action,
        "subscription_action": sub_action,
        "pro_until": period_end.isoformat(),
        "temp_password": temp_password,  # only set for newly-created users
        "order_id": body.order_id,
        "order_amount_eur": body.order_amount_eur,
    }

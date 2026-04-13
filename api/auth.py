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

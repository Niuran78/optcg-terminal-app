"""Authentication API endpoints — register, login, JWT."""
import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt as _bcrypt
import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, status
from jose import jwt
from pydantic import BaseModel, EmailStr

from db.init import DATABASE_PATH
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


def user_to_dict(row: aiosqlite.Row) -> dict:
    return {
        "id": row["id"],
        "email": row["email"],
        "tier": row["tier"],
        "created_at": row["created_at"],
    }


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    """Register a new user (free tier)."""
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters.")

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Check existing
        cursor = await db.execute("SELECT id FROM users WHERE email=?", (body.email,))
        existing = await cursor.fetchone()
        if existing:
            raise HTTPException(409, "Email already registered.")

        password_hash = hash_password(body.password)
        await db.execute(
            "INSERT INTO users (email, password_hash, tier) VALUES (?, ?, 'free')",
            (body.email, password_hash)
        )
        await db.commit()

        # Fetch the inserted row
        cursor = await db.execute(
            "SELECT id, email, tier, created_at FROM users WHERE email=?",
            (body.email,)
        )
        row = await cursor.fetchone()

        token = create_token(row["id"])
        return TokenResponse(access_token=token, user=user_to_dict(row))


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    """Login with email and password."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, email, password_hash, tier, created_at FROM users WHERE email=?",
            (body.email,)
        )
        row = await cursor.fetchone()

    if row is None or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(401, "Invalid email or password.")

    token = create_token(row["id"])
    return TokenResponse(
        access_token=token,
        user={
            "id": row["id"],
            "email": row["email"],
            "tier": row["tier"],
            "created_at": row["created_at"],
        }
    )


@router.get("/me")
async def me(user: UserInfo = Depends(get_current_user)):
    """Get current user info and tier."""
    if not user.is_authenticated:
        raise HTTPException(401, "Not authenticated.")

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, email, tier, stripe_customer_id, created_at FROM users WHERE id=?",
            (user.user_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "User not found.")

        # Get active subscription
        cursor = await db.execute(
            """SELECT tier, status, current_period_end FROM subscriptions
               WHERE user_id=? AND status='active' ORDER BY id DESC LIMIT 1""",
            (user.user_id,)
        )
        sub = await cursor.fetchone()

    return {
        "id": row["id"],
        "email": row["email"],
        "tier": row["tier"],
        "created_at": row["created_at"],
        "subscription": dict(sub) if sub else None,
    }

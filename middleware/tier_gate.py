"""Tier-based access control middleware for OPTCG Terminal.

Uses FastAPI Dependency Injection to check JWT token and user tier.
"""
import os
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from jose import JWTError, jwt

from db.init import get_pool

JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"

security = HTTPBearer(auto_error=False)

TIER_HIERARCHY = {
    "free": 0,
    "pro": 1,
    "elite": 2,
}


class UserInfo:
    """Represents an authenticated (or anonymous) user."""
    def __init__(self, user_id: Optional[int] = None, email: Optional[str] = None,
                 tier: str = "free", stripe_customer_id: Optional[str] = None):
        self.user_id = user_id
        self.email = email
        self.tier = tier
        self.stripe_customer_id = stripe_customer_id
        self.is_authenticated = user_id is not None

    @property
    def tier_level(self) -> int:
        return TIER_HIERARCHY.get(self.tier, 0)

    def can_access(self, required_tier: str) -> bool:
        required_level = TIER_HIERARCHY.get(required_tier, 0)
        return self.tier_level >= required_level


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> UserInfo:
    """
    Extract user from JWT token. Returns anonymous free-tier user if no token.
    Does NOT raise 401 — endpoints must check tier themselves if needed.
    """
    if credentials is None:
        return UserInfo()  # Anonymous free-tier user

    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            return UserInfo()
    except JWTError:
        return UserInfo()

    # Look up user in DB
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, email, tier, stripe_customer_id FROM users WHERE id=$1",
                int(user_id)
            )
            if row is None:
                return UserInfo()
            return UserInfo(
                user_id=row["id"],
                email=row["email"],
                tier=row["tier"],
                stripe_customer_id=row["stripe_customer_id"],
            )
    except Exception:
        return UserInfo()


async def require_pro(user: UserInfo = Depends(get_current_user)) -> UserInfo:
    """Dependency that requires Pro or Elite tier."""
    if not user.can_access("pro"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "PRO_REQUIRED",
                "message": "This feature requires a Pro or Elite subscription.",
                "upgrade_url": "/?upgrade=pro",
            }
        )
    return user


async def require_elite(user: UserInfo = Depends(get_current_user)) -> UserInfo:
    """Dependency that requires Elite tier."""
    if not user.can_access("elite"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "ELITE_REQUIRED",
                "message": "This feature requires an Elite subscription.",
                "upgrade_url": "/?upgrade=pro",
            }
        )
    return user


async def require_auth(user: UserInfo = Depends(get_current_user)) -> UserInfo:
    """Dependency that requires any authenticated user."""
    if not user.is_authenticated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": "AUTH_REQUIRED",
                "message": "Authentication required.",
            }
        )
    return user


def require_tier(tier: str):
    """Factory that returns a FastAPI dependency requiring the specified tier.
    Usage: user: UserInfo = Depends(require_tier('pro'))
    """
    async def _require_tier(user: UserInfo = Depends(get_current_user)) -> UserInfo:
        if not user.can_access(tier):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": f"{tier.upper()}_REQUIRED",
                    "message": f"This feature requires a {tier.capitalize()} or higher subscription.",
                    "upgrade_url": "/?upgrade=pro",
                }
            )
        return user
    return _require_tier

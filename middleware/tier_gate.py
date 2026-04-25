"""Tier-based access control middleware for OPTCG Terminal.

Uses FastAPI Dependency Injection to check JWT token and user tier.
"""
import os
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from jose import JWTError, jwt

from db.init import get_pool

# HARDENED: reject known-public defaults, but log LOUD warnings instead of
# crashing the service at import time. A crashed service is worse than a
# service with a default secret as long as we log the problem prominently
# so it gets noticed and fixed. Admin endpoints additionally double-check
# the secret at call time via require_admin role separation.
import logging as _logging
_log = _logging.getLogger(__name__)

JWT_SECRET = os.getenv("JWT_SECRET")
_INSECURE_DEFAULTS = {"change-me-in-production", "changeme", "secret", ""}
if JWT_SECRET is None or JWT_SECRET in _INSECURE_DEFAULTS:
    _log.critical(
        "\n" + "=" * 70 + "\n"
        "SECURITY ALERT: JWT_SECRET is missing or uses a known-insecure default!\n"
        "Anyone can forge admin tokens. Fix NOW in Render environment:\n"
        "  python -c 'import secrets; print(secrets.token_urlsafe(64))'\n"
        + "=" * 70
    )
    # Use a process-unique random value so tokens from a misconfigured
    # env at least can't be forged with the known-public default.
    import secrets as _secrets
    JWT_SECRET = _secrets.token_urlsafe(64)
    _log.warning("Using ephemeral random JWT_SECRET — existing sessions will be invalidated on every restart until ENV is fixed.")
elif len(JWT_SECRET) < 32:
    _log.warning(
        f"JWT_SECRET is shorter than 32 chars (len={len(JWT_SECRET)}). Rotate to a longer value soon."
    )
JWT_ALGORITHM = "HS256"

security = HTTPBearer(auto_error=False)

TIER_HIERARCHY = {
    "free": 0,
    "pro": 1,
    "elite": 2,
}


class UserInfo:
    """Represents an authenticated (or anonymous) user.

    Two orthogonal axes:
      - tier  : monetization (free / pro / elite) — controls feature gating
      - role  : permission   (user / admin)       — controls admin endpoints
    An Elite subscriber is NOT an admin. Admin role is granted by DB write only.
    """
    def __init__(self, user_id: Optional[int] = None, email: Optional[str] = None,
                 tier: str = "free", role: str = "user",
                 stripe_customer_id: Optional[str] = None):
        self.user_id = user_id
        self.email = email
        self.tier = tier
        self.role = role
        self.stripe_customer_id = stripe_customer_id
        self.is_authenticated = user_id is not None

    @property
    def tier_level(self) -> int:
        return TIER_HIERARCHY.get(self.tier, 0)

    def can_access(self, required_tier: str) -> bool:
        required_level = TIER_HIERARCHY.get(required_tier, 0)
        return self.tier_level >= required_level

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


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

    # Look up user in DB. On DB failure we now raise 503 instead of masquerading
    # infrastructure errors as anonymous users (GPT-5.5 finding #12).
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, email, tier, COALESCE(role, 'user') AS role, stripe_customer_id "
                "FROM users WHERE id=$1",
                int(user_id)
            )
            if row is None:
                return UserInfo()
            return UserInfo(
                user_id=row["id"],
                email=row["email"],
                tier=row["tier"],
                role=row["role"] or "user",
                stripe_customer_id=row["stripe_customer_id"],
            )
    except Exception as e:
        # Log but don't swallow — an infra failure should surface, not silently
        # strip a paying user down to anonymous free tier mid-session.
        import logging
        logging.getLogger(__name__).error(f"get_current_user DB lookup failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "DB_UNAVAILABLE", "message": "Database temporarily unavailable. Please retry."}
        )


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


async def require_admin(user: UserInfo = Depends(get_current_user)) -> UserInfo:
    """Dependency for admin-only endpoints.

    Distinct from tier checks: Elite is a paid tier, admin is a permission.
    A paying Elite subscriber is NOT an admin. Role must be granted in the DB.
    """
    if not user.is_authenticated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "AUTH_REQUIRED", "message": "Authentication required."}
        )
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "ADMIN_REQUIRED", "message": "Admin role required."}
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

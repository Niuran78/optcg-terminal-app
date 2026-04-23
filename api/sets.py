"""Sets/Episodes API endpoints."""
from fastapi import APIRouter, Depends, Query

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_auth, UserInfo
from services import opcg_api

router = APIRouter(prefix="/api/sets", tags=["sets"])


@router.get("")
async def list_sets(
    language: str = Query(None, description="Filter by language: JP or EN"),
    user: UserInfo = Depends(require_auth),
):
    """
    List all sets/episodes.
    Free tier: latest 3 sets only.
    Pro/Elite: all sets.
    """
    sets = await opcg_api.get_sets(tier=user.tier)

    if language:
        sets = [s for s in sets if s.get("language", "").upper() == language.upper()]

    # Free tier: only 3 latest sets
    if not user.can_access("pro"):
        sets = sets[:3]
        return {
            "sets": sets,
            "total": len(sets),
            "tier_limited": True,
            "message": "Free tier shows latest 3 sets. Upgrade to Pro for all sets.",
        }

    return {
        "sets": sets,
        "total": len(sets),
        "tier_limited": False,
    }


@router.get("/{set_id}")
async def get_set(set_id: str, user: UserInfo = Depends(require_auth)):
    """Get details for a specific set."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM sets WHERE api_id=$1", set_id
        )
        if row is None:
            # Try fetching from API
            sets = await opcg_api.get_sets(tier=user.tier)
            for s in sets:
                if s.get("api_id") == set_id:
                    return s
            from fastapi import HTTPException
            raise HTTPException(404, f"Set {set_id} not found.")
        return dict(row)

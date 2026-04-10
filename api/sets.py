"""Sets/Episodes API endpoints."""
import aiosqlite
from fastapi import APIRouter, Depends, Query

from db.init import DATABASE_PATH
from middleware.tier_gate import get_current_user, UserInfo
from services import opcg_api

router = APIRouter(prefix="/api/sets", tags=["sets"])


@router.get("")
async def list_sets(
    language: str = Query(None, description="Filter by language: JP or EN"),
    user: UserInfo = Depends(get_current_user),
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
async def get_set(set_id: str, user: UserInfo = Depends(get_current_user)):
    """Get details for a specific set."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM sets WHERE api_id=?", (set_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            # Try fetching from API
            sets = await opcg_api.get_sets(tier=user.tier)
            for s in sets:
                if s.get("api_id") == set_id:
                    return s
            from fastapi import HTTPException
            raise HTTPException(404, f"Set {set_id} not found.")
        return dict(row)

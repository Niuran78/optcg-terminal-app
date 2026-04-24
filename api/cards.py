"""Cards API endpoints."""
from fastapi import APIRouter, Depends, Query, HTTPException

from middleware.tier_gate import get_current_user, require_auth, UserInfo
from services import opcg_api

router = APIRouter(prefix="/api/cards", tags=["cards"])


@router.get("/search")
async def search_cards(
    q: str = Query(..., min_length=2, description="Card name search query"),
    user: UserInfo = Depends(require_auth),
):
    """Search cards by name. Available to all tiers."""
    results = await opcg_api.search_cards(q, tier=user.tier)
    return {
        "query": q,
        "results": results,
        "count": len(results),
    }


@router.get("/set/{set_id}")
async def get_cards_for_set(
    set_id: str,
    sort: str = Query("price_highest", description="Sort order"),
    user: UserInfo = Depends(require_auth),
):
    """
    Get all cards for a set.
    Free tier: latest 3 sets only.
    Pro/Elite: all sets.
    """
    # Check free tier set restriction
    if not user.can_access("pro"):
        from services import opcg_api as api
        all_sets = await api.get_sets(tier="free")
        allowed_ids = {str(s.get("api_id")) for s in all_sets[:3]}
        if set_id not in allowed_ids:
            raise HTTPException(
                403,
                detail={
                    "error": "PRO_REQUIRED",
                    "message": "This set requires a Pro subscription.",
                    "upgrade_url": "/?upgrade=pro",
                }
            )

    cards = await opcg_api.get_cards(set_id, tier=user.tier)

    # Sort options
    if sort == "price_highest":
        cards.sort(key=lambda c: c.get("_cardmarket_price") or 0, reverse=True)
    elif sort == "price_lowest":
        cards.sort(key=lambda c: c.get("_cardmarket_price") or 0)
    elif sort == "name":
        cards.sort(key=lambda c: (c.get("name") or c.get("card_name") or ""))

    return {
        "set_id": set_id,
        "cards": cards,
        "count": len(cards),
    }


@router.get("/{card_id}/history")
async def get_card_history(
    card_id: str,
    days: int = Query(30, ge=1, le=365),
    user: UserInfo = Depends(require_auth),
):
    """Get price history for a card. Pro: 30 days, Elite: 1 year."""
    if not user.can_access("pro"):
        raise HTTPException(
            403,
            detail={
                "error": "PRO_REQUIRED",
                "message": "Price history requires a Pro subscription.",
                "upgrade_url": "/?upgrade=pro",
            }
        )

    # Elite gets up to 365 days, Pro gets max 30
    if not user.can_access("elite"):
        days = min(days, 30)

    history = await opcg_api.get_price_history(card_id, "card", days=days)
    return {
        "card_id": card_id,
        "days": days,
        "history": history,
    }

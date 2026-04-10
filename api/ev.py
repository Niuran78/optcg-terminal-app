"""EV Calculator API endpoints — Pro tier required."""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from db.init import DATABASE_PATH
from middleware.tier_gate import get_current_user, UserInfo, require_pro
from services import opcg_api
from services.ev_engine import calculate_ev, calculate_custom_ev, PULL_RATES

router = APIRouter(prefix="/api/ev", tags=["ev"])


class CustomEVRequest(BaseModel):
    set_id: str
    language: str = "JP"
    box_cost: float
    custom_pull_rates: Optional[dict] = None


@router.get("/calculate/{set_id}")
async def calculate_set_ev(
    set_id: str,
    box_cost: Optional[float] = Query(None, description="Override box cost in EUR"),
    user: UserInfo = Depends(require_pro),
):
    """
    Calculate EV for a set based on current card prices.
    Pro tier required.
    """
    import aiosqlite
    # Get set info
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets WHERE api_id=?", (set_id,))
        set_row = await cursor.fetchone()

    if set_row is None:
        # Try to find set via API data
        sets = await opcg_api.get_sets(tier=user.tier)
        set_data = next((s for s in sets if str(s.get("api_id")) == set_id), None)
        if not set_data:
            raise HTTPException(404, f"Set {set_id} not found.")
    else:
        set_data = dict(set_row)

    # Fetch cards
    cards = await opcg_api.get_cards(set_id, tier=user.tier)
    if not cards:
        raise HTTPException(404, "No card data available for this set.")

    # Get products to find box price if not provided
    if box_cost is None:
        products = await opcg_api.get_products(set_id, tier=user.tier)
        # Find booster box product
        box = None
        for p in products:
            name_lower = (p.get("name") or p.get("product_name") or "").lower()
            if "booster box" in name_lower or "booster display" in name_lower or "box" in name_lower:
                box = p
                break
        if box:
            box_cost = box.get("_cardmarket_price")

        if box_cost is None:
            box_cost = 0.0  # Will show N/A verdict

    language = set_data.get("language", "JP")

    result = calculate_ev(
        set_id=set_id,
        set_name=set_data.get("name", "Unknown"),
        language=language,
        cards=cards,
        box_cost=float(box_cost),
    )

    if result is None:
        raise HTTPException(422, "Insufficient price data to calculate EV for this set.")

    return {
        "ev": result.to_dict(),
        "set": set_data,
        "pull_rates": PULL_RATES.get(language.upper(), PULL_RATES["JP"]),
    }


@router.post("/custom")
async def custom_ev(
    body: CustomEVRequest,
    user: UserInfo = Depends(require_pro),
):
    """
    Calculate EV with custom box cost and optional custom pull rates.
    Pro tier required.
    """
    import aiosqlite
    # Get set info
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets WHERE api_id=?", (body.set_id,))
        set_row = await cursor.fetchone()

    set_data = dict(set_row) if set_row else {"api_id": body.set_id, "name": "Unknown"}
    language = body.language or set_data.get("language", "JP")

    # Fetch cards
    cards = await opcg_api.get_cards(body.set_id, tier=user.tier)
    if not cards:
        raise HTTPException(404, "No card data available for this set.")

    result = calculate_ev(
        set_id=body.set_id,
        set_name=set_data.get("name", "Custom"),
        language=language,
        cards=cards,
        box_cost=body.box_cost,
    )

    if result is None:
        raise HTTPException(422, "Insufficient price data to calculate EV.")

    ev_dict = result.to_dict()
    if body.custom_pull_rates:
        ev_dict["note"] = "Custom pull rates applied."

    return {
        "ev": ev_dict,
        "set": set_data,
        "box_cost_used": body.box_cost,
    }


@router.get("/pull-rates")
async def get_pull_rates():
    """Get the current pull rate configuration (public endpoint)."""
    return {
        "JP": {
            **PULL_RATES["JP"],
            "description": "Japanese booster box (10 packs)",
        },
        "EN": {
            **PULL_RATES["EN"],
            "description": "English booster box (24 packs)",
        },
        "notes": [
            "Pull rates are community-sourced estimates and may vary by set.",
            "SR = Super Rare, SEC = Secret Rare, L_AA = Leader Alternate Art, MANGA = Manga Alternate Art",
            "R = Rare, UC = Uncommon, C = Common",
        ]
    }

"""Sealed Products API endpoints."""
import asyncio
import aiosqlite
from fastapi import APIRouter, Depends, Query, HTTPException

from db.init import DATABASE_PATH
from middleware.tier_gate import get_current_user, UserInfo, require_pro
from services import opcg_api

router = APIRouter(prefix="/api/sealed", tags=["sealed"])


@router.get("/products")
async def list_sealed_products(
    language: str = Query(None, description="Filter by language: JP or EN"),
    set_id: str = Query(None, description="Filter by set ID"),
    sort: str = Query("price_highest", description="Sort order"),
    user: UserInfo = Depends(get_current_user),
):
    """
    List all sealed products with current prices.
    Free: latest 3 sets.
    Pro/Elite: all sets.
    """
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets ORDER BY release_date DESC")
        all_sets = await cursor.fetchall()

    sets_list = [dict(row) for row in all_sets]

    if language:
        sets_list = [s for s in sets_list if s.get("language", "").upper() == language.upper()]

    if set_id:
        sets_list = [s for s in sets_list if s.get("api_id") == set_id]

    # Free tier: latest 3 sets
    if not user.can_access("pro"):
        sets_list = sets_list[:3]

    all_products = []

    async def fetch_products(set_info: dict):
        sid = set_info["api_id"]
        try:
            products = await opcg_api.get_products(sid, tier=user.tier)
            for p in products:
                p["set_id"] = sid
                p["set_name"] = set_info.get("name", "")
                p["set_language"] = set_info.get("language", "EN")
                p["set_code"] = set_info.get("code", "")
            return products
        except Exception:
            return []

    batch_size = 5
    for i in range(0, len(sets_list), batch_size):
        batch = sets_list[i:i + batch_size]
        batch_results = await asyncio.gather(*[fetch_products(s) for s in batch])
        for results in batch_results:
            all_products.extend(results)

    # Sort
    if sort == "price_highest":
        all_products.sort(key=lambda p: p.get("_cardmarket_price") or 0, reverse=True)
    elif sort == "price_lowest":
        all_products.sort(key=lambda p: p.get("_cardmarket_price") or 0)
    elif sort == "name":
        all_products.sort(key=lambda p: p.get("name") or p.get("product_name") or "")

    return {
        "products": all_products,
        "total": len(all_products),
        "tier": user.tier,
        "tier_limited": not user.can_access("pro"),
    }


@router.get("/products/{product_id}/history")
async def get_product_history(
    product_id: str,
    days: int = Query(30, ge=1, le=365),
    user: UserInfo = Depends(require_pro),
):
    """
    Get price history for a sealed product.
    Pro: 30 days.
    Elite: 1 year.
    """
    if not user.can_access("elite"):
        days = min(days, 30)

    history = await opcg_api.get_price_history(product_id, "product", days=days)

    return {
        "product_id": product_id,
        "days": days,
        "history": history,
        "data_points": len(history),
    }


@router.get("/tracker")
async def sealed_tracker(
    language: str = Query(None, description="Filter by language: JP or EN"),
    user: UserInfo = Depends(require_pro),
):
    """
    Enhanced sealed product tracker with price change data.
    Requires Pro or Elite.
    """
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets ORDER BY release_date DESC")
        all_sets = await cursor.fetchall()

    sets_list = [dict(row) for row in all_sets]
    if language:
        sets_list = [s for s in sets_list if s.get("language", "").upper() == language.upper()]

    results = []
    for set_info in sets_list:
        sid = set_info["api_id"]
        try:
            products = await opcg_api.get_products(sid, tier=user.tier)
            for p in products:
                prod_id = str(p.get("id", p.get("_id", p.get("code", ""))))
                history = await opcg_api.get_price_history(prod_id, "product", days=7)
                cm_price = p.get("_cardmarket_price")
                tcp_price = p.get("_tcgplayer_price")

                # Calculate 7-day change
                price_7d_ago = None
                if history and len(history) > 1:
                    price_7d_ago = history[0].get("cardmarket_price")

                change_7d = None
                change_7d_pct = None
                if cm_price and price_7d_ago and price_7d_ago > 0:
                    change_7d = cm_price - price_7d_ago
                    change_7d_pct = (change_7d / price_7d_ago) * 100

                results.append({
                    "id": prod_id,
                    "name": p.get("name") or p.get("product_name") or "Unknown",
                    "set_id": sid,
                    "set_name": set_info.get("name", ""),
                    "set_language": set_info.get("language", "EN"),
                    "cardmarket_price": cm_price,
                    "tcgplayer_price": tcp_price,
                    "change_7d": round(change_7d, 2) if change_7d is not None else None,
                    "change_7d_pct": round(change_7d_pct, 1) if change_7d_pct is not None else None,
                    "history": history[-30:],  # Last 30 data points for sparkline
                })
        except Exception:
            continue

    results.sort(key=lambda x: x.get("cardmarket_price") or 0, reverse=True)
    return {
        "products": results,
        "total": len(results),
    }

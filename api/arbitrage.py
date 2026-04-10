"""Arbitrage scanner API endpoints."""
import asyncio
import json
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException

import aiosqlite

from db.init import DATABASE_PATH
from middleware.tier_gate import get_current_user, UserInfo
from services import opcg_api
from services.arbitrage_engine import analyze_items

router = APIRouter(prefix="/api/arbitrage", tags=["arbitrage"])


async def _load_items_from_cache(set_id: str, item_type: str) -> list[dict]:
    """Load all items for a set from the SQLite cache (no live API call)."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        if item_type == "product":
            cursor = await db.execute(
                "SELECT * FROM products_cache WHERE set_api_id=?", (set_id,)
            )
            rows = await cursor.fetchall()
            result = []
            for row in rows:
                item = json.loads(row["product_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                result.append(item)
            return result
        else:
            cursor = await db.execute(
                "SELECT * FROM cards_cache WHERE set_api_id=?", (set_id,)
            )
            rows = await cursor.fetchall()
            result = []
            for row in rows:
                item = json.loads(row["card_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                from services.opcg_api import _classify_region
                item["_region"] = _classify_region(row["cardmarket_price"], row["tcgplayer_price"])
                result.append(item)
            return result


@router.get("/scanner")
async def arbitrage_scanner(
    limit: int = Query(50, ge=1, le=200),
    min_profit: float = Query(0.0, description="Minimum profit in EUR"),
    signal: Optional[str] = Query(None, description="Filter by signal: BUY_EU, BUY_US, WATCH, NEUTRAL"),
    language: Optional[str] = Query(None, description="Filter by language: JP or EN"),
    item_type: str = Query("product", description="'product' or 'card'"),
    user: UserInfo = Depends(get_current_user),
):
    """
    Top arbitrage opportunities across all sets.
    Reads from pre-seeded SQLite cache — no live API calls during requests.
    Free: top 10 from 3 latest sets, cards only show signal.
    Pro/Elite: all sets, full profit calculations.
    """
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets ORDER BY release_date DESC")
        all_sets = await cursor.fetchall()

    sets_list = [dict(row) for row in all_sets]

    if language:
        sets_list = [s for s in sets_list if s.get("language", "").upper() == language.upper()]

    # Free tier: latest 3 sets only
    if not user.can_access("pro"):
        sets_list = sets_list[:3]

    # Fetch items from cache for each set concurrently
    all_results = []

    async def fetch_set_items_cached(set_info: dict):
        set_id = set_info["api_id"]
        try:
            items = await _load_items_from_cache(set_id, item_type)
            # If cache is empty for this set, fall back to live fetch (populates cache)
            if not items:
                if item_type == "product":
                    items = await opcg_api.get_products(set_id, tier=user.tier)
                else:
                    items = await opcg_api.get_cards(set_id, tier=user.tier)

            analyzed = analyze_items(items, item_type=item_type)
            for item in analyzed:
                item["set_id"] = set_id
                item["set_name"] = set_info.get("name", "")
                item["set_language"] = set_info.get("language", "EN")
            return analyzed
        except Exception:
            return []

    # Batch fetch sets concurrently (cache reads are fast — larger batches ok)
    batch_size = 10
    for i in range(0, len(sets_list), batch_size):
        batch = sets_list[i:i + batch_size]
        batch_results = await asyncio.gather(*[fetch_set_items_cached(s) for s in batch])
        for results in batch_results:
            all_results.extend(results)

    # Filter
    if signal:
        all_results = [r for r in all_results if r.get("signal") == signal.upper()]
    if min_profit > 0:
        all_results = [r for r in all_results if r.get("profit_eur", 0) >= min_profit]

    # Sort by profit descending
    all_results.sort(key=lambda x: x.get("profit_eur", 0) or 0, reverse=True)

    # Free tier: top 10 only, hide detailed profit
    if not user.can_access("pro"):
        all_results = all_results[:10]
        for item in all_results:
            item["profit_eur"] = None
            item["profit_pct"] = None
            item["cost_breakdown"] = None

    return {
        "opportunities": all_results[:limit],
        "total": len(all_results),
        "tier": user.tier,
        "tier_limited": not user.can_access("pro"),
        "sets_scanned": len(sets_list),
        "item_type": item_type,
    }


@router.get("/set/{set_id}")
async def arbitrage_for_set(
    set_id: str,
    item_type: str = Query("product", description="'product' or 'card'"),
    user: UserInfo = Depends(get_current_user),
):
    """Arbitrage analysis for a specific set. Reads from cache first."""
    # Check free tier set restriction
    if not user.can_access("pro"):
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT api_id FROM sets ORDER BY release_date DESC LIMIT 3"
            )
            allowed = await cursor.fetchall()
            allowed_ids = {row["api_id"] for row in allowed}

        if set_id not in allowed_ids:
            raise HTTPException(
                403,
                detail={
                    "error": "PRO_REQUIRED",
                    "message": "This set requires a Pro subscription.",
                    "upgrade_url": "/login.html#upgrade",
                }
            )

    # Try cache first, fall back to live fetch
    items = await _load_items_from_cache(set_id, item_type)
    if not items:
        if item_type == "product":
            items = await opcg_api.get_products(set_id, tier=user.tier)
        else:
            items = await opcg_api.get_cards(set_id, tier=user.tier)

    analyzed = analyze_items(items, item_type=item_type)

    # Get set info
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets WHERE api_id=?", (set_id,))
        set_info = await cursor.fetchone()

    set_data = dict(set_info) if set_info else {"api_id": set_id}

    for item in analyzed:
        item["set_id"] = set_id
        item["set_name"] = set_data.get("name", "")
        item["set_language"] = set_data.get("language", "EN")

    return {
        "set": set_data,
        "opportunities": analyzed,
        "total": len(analyzed),
        "item_type": item_type,
        "tier": user.tier,
    }

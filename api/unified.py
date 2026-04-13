"""Unified card browser API — merges EN (TCG Price Lookup) and EU (Cardmarket) prices.

Endpoints:
    GET /api/cards/browse   — browse all cards with EN + EU prices
    GET /api/cards/sealed   — browse sealed products with EU prices
    GET /api/cards/arbitrage — find EN↔EU arbitrage opportunities
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from db.init import get_pool
from middleware.tier_gate import get_current_user, UserInfo
from services.card_aggregator import USD_TO_EUR, EUR_TO_USD

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cards", tags=["cards"])

# Free tier: access limited to the 3 most recent main sets
FREE_TIER_SET_LIMIT = 3
FREE_TIER_SETS = ["OP09", "OP08", "OP07"]  # Updated as new sets release

# Valid sort columns for cards_unified
CARD_SORT_COLUMNS = {
    "en_tcgplayer_market",
    "en_tcgplayer_low",
    "en_ebay_avg_7d",
    "eu_cardmarket_7d_avg",
    "eu_cardmarket_30d_avg",
    "eu_cardmarket_lowest",
    "name",
    "card_id",
    "rarity",
    "set_code",
}

SEALED_SORT_COLUMNS = {
    "eu_price",
    "eu_30d_avg",
    "eu_7d_avg",
    "product_name",
    "set_code",
    "product_type",
}


def _row_to_card(row: dict) -> dict:
    """Convert a cards_unified DB row to a clean API response dict."""
    return {
        "card_id": row.get("card_id"),
        "name": row.get("name"),
        "set_code": row.get("set_code"),
        "set_name": row.get("set_name"),
        "rarity": row.get("rarity"),
        "variant": row.get("variant"),
        "image_url": row.get("image_url"),
        "en_prices": {
            "tcgplayer_market_usd": row.get("en_tcgplayer_market"),
            "tcgplayer_low_usd": row.get("en_tcgplayer_low"),
            "ebay_avg_7d_usd": row.get("en_ebay_avg_7d"),
            "source": row.get("en_source", "TCG Price Lookup"),
            "updated_at": str(row.get("en_updated_at")) if row.get("en_updated_at") else None,
        },
        "eu_prices": {
            "cardmarket_7d_avg_eur": row.get("eu_cardmarket_7d_avg"),
            "cardmarket_30d_avg_eur": row.get("eu_cardmarket_30d_avg"),
            "cardmarket_lowest_eur": row.get("eu_cardmarket_lowest"),
            "source": row.get("eu_source", "Cardmarket"),
            "updated_at": str(row.get("eu_updated_at")) if row.get("eu_updated_at") else None,
        },
        "ids": {
            "tcg_price_lookup_id": row.get("tcg_price_lookup_id"),
            "rapidapi_card_id": row.get("rapidapi_card_id"),
            "tcgplayer_id": row.get("tcgplayer_id"),
            "cardmarket_id": row.get("cardmarket_id"),
        },
    }


def _row_to_sealed(row: dict) -> dict:
    """Convert a sealed_unified DB row to a clean API response dict."""
    return {
        "product_name": row.get("product_name"),
        "set_code": row.get("set_code"),
        "set_name": row.get("set_name"),
        "product_type": row.get("product_type"),
        "image_url": row.get("image_url"),
        "eu_prices": {
            "price_eur": row.get("eu_price"),
            "avg_30d_eur": row.get("eu_30d_avg"),
            "avg_7d_eur": row.get("eu_7d_avg"),
            "trend": row.get("eu_trend"),
            "source": row.get("eu_source", "Cardmarket"),
            "updated_at": str(row.get("eu_updated_at")) if row.get("eu_updated_at") else None,
        },
        "rapidapi_product_id": row.get("rapidapi_product_id"),
    }


def _arbitrage_calc(card_row: dict, min_profit_pct: float) -> Optional[dict]:
    """Calculate EN→EU arbitrage for a single card.

    Buys at EN TCGPlayer market (USD), sells at EU Cardmarket 7d avg (EUR).
    Deducts TCGPlayer seller fee (13%), EU shipping ($22 → €20.24), CM fee (5%).

    Returns a dict if profitable above min_profit_pct, else None.
    """
    en_usd = card_row.get("en_tcgplayer_market")
    eu_eur = card_row.get("eu_cardmarket_7d_avg")

    if not en_usd or not eu_eur or en_usd <= 0 or eu_eur <= 0:
        return None

    # Convert EN price to EUR
    en_eur = en_usd * USD_TO_EUR

    # Selling on Cardmarket: subtract CM seller fee (5%)
    cm_fee_pct = 0.05
    # Shipping EU→US or buying EN and shipping to EU:
    # Model: buyer in EU buys EN card from TCGPlayer, ships it to EU
    # Cost: en_eur (buy) + shipping + any TCGPlayer fees
    # Revenue: eu_eur * (1 - cm_fee) if re-selling on CM
    tcg_fee_pct = 0.13
    shipping_usd = 22.0  # US to EU, per shipment (amortized)
    shipping_eur = shipping_usd * USD_TO_EUR

    # Net cost to acquire EN card (including TCGPlayer buyer's premium is
    # already in market price; if selling we'd pay 13%)
    # Scenario: Buy on TCGPlayer at market, resell on Cardmarket
    cost_eur = en_eur + shipping_eur  # acquisition + shipping
    revenue_eur = eu_eur * (1 - cm_fee_pct)
    profit_eur = revenue_eur - cost_eur
    profit_pct = (profit_eur / cost_eur) * 100 if cost_eur > 0 else 0

    if profit_pct < min_profit_pct:
        return None

    return {
        "card_id": card_row.get("card_id"),
        "name": card_row.get("name"),
        "set_code": card_row.get("set_code"),
        "set_name": card_row.get("set_name"),
        "rarity": card_row.get("rarity"),
        "variant": card_row.get("variant"),
        "image_url": card_row.get("image_url"),
        "en_tcgplayer_market_usd": en_usd,
        "en_tcgplayer_market_eur": round(en_eur, 2),
        "eu_cardmarket_7d_avg_eur": eu_eur,
        "profit_eur": round(profit_eur, 2),
        "profit_pct": round(profit_pct, 2),
        "cost_eur": round(cost_eur, 2),
        "revenue_eur": round(revenue_eur, 2),
        "signal": "BUY" if profit_pct >= 15 else "WATCH",
        "sources": {
            "en": "TCG Price Lookup",
            "eu": "Cardmarket",
        },
    }


# ─── Browse cards ─────────────────────────────────────────────────────────────

@router.get("/browse")
async def browse_cards(
    set_code: Optional[str] = Query(None, description="Filter by set code, e.g. OP01"),
    search: Optional[str] = Query(None, min_length=2, description="Card name search"),
    rarity: Optional[str] = Query(None, description="Filter by rarity"),
    sort: str = Query("en_tcgplayer_market", description="Sort column"),
    order: str = Query("desc", description="asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: UserInfo = Depends(get_current_user),
):
    """Browse all cards with EN + EU prices from both sources."""
    # Validate sort/order
    if sort not in CARD_SORT_COLUMNS:
        sort = "en_tcgplayer_market"
    order_sql = "DESC" if order.lower() != "asc" else "ASC"

    # Free tier: limit to latest sets
    allowed_sets: Optional[list[str]] = None
    if not user.can_access("pro"):
        allowed_sets = FREE_TIER_SETS

    conditions = []
    params: list = []
    param_idx = 1  # asyncpg uses $1, $2, ...

    if allowed_sets is not None:
        placeholders = ",".join(f"${param_idx + i}" for i in range(len(allowed_sets)))
        conditions.append(f"set_code IN ({placeholders})")
        params.extend(allowed_sets)
        param_idx += len(allowed_sets)

    if set_code:
        if allowed_sets and set_code.upper() not in allowed_sets:
            raise HTTPException(
                403,
                detail={
                    "error": "PRO_REQUIRED",
                    "message": "Access to this set requires a Pro subscription.",
                    "upgrade_url": "/login.html#upgrade",
                },
            )
        conditions.append(f"set_code = ${param_idx}")
        params.append(set_code.upper())
        param_idx += 1

    if search:
        conditions.append(f"name LIKE ${param_idx}")
        params.append(f"%{search}%")
        param_idx += 1

    if rarity:
        conditions.append(f"rarity = ${param_idx}")
        params.append(rarity)
        param_idx += 1

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    count_query = f"SELECT COUNT(*) FROM cards_unified {where_clause}"
    data_query = (
        f"SELECT * FROM cards_unified {where_clause} "
        f"ORDER BY {sort} {order_sql} NULLS LAST "
        f"LIMIT ${param_idx} OFFSET ${param_idx + 1}"
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval(count_query, *params)
        rows = await conn.fetch(data_query, *params, limit, offset)

    cards = [_row_to_card(dict(row)) for row in rows]

    # Free tier: mask some price details
    if not user.can_access("pro"):
        for card in cards:
            card["en_prices"]["ebay_avg_7d_usd"] = None  # eBay prices = Trader plan
            card["en_prices"]["tcgplayer_low_usd"] = None

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "tier": user.tier,
        "cards": cards,
    }


# ─── Browse sealed ────────────────────────────────────────────────────────────

@router.get("/sealed")
async def browse_sealed(
    set_code: Optional[str] = Query(None, description="Filter by set code"),
    product_type: Optional[str] = Query(None, description="case, booster_box, booster"),
    sort: str = Query("eu_price", description="Sort column"),
    order: str = Query("desc", description="asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: UserInfo = Depends(get_current_user),
):
    """Browse sealed products with EU Cardmarket prices."""
    if sort not in SEALED_SORT_COLUMNS:
        sort = "eu_price"
    order_sql = "DESC" if order.lower() != "asc" else "ASC"

    allowed_sets: Optional[list[str]] = None
    if not user.can_access("pro"):
        allowed_sets = FREE_TIER_SETS

    conditions = []
    params: list = []
    param_idx = 1

    if allowed_sets is not None:
        placeholders = ",".join(f"${param_idx + i}" for i in range(len(allowed_sets)))
        conditions.append(f"set_code IN ({placeholders})")
        params.extend(allowed_sets)
        param_idx += len(allowed_sets)

    if set_code:
        if allowed_sets and set_code.upper() not in allowed_sets:
            raise HTTPException(
                403,
                detail={
                    "error": "PRO_REQUIRED",
                    "message": "Access to this set requires a Pro subscription.",
                    "upgrade_url": "/login.html#upgrade",
                },
            )
        conditions.append(f"set_code = ${param_idx}")
        params.append(set_code.upper())
        param_idx += 1

    if product_type:
        conditions.append(f"product_type = ${param_idx}")
        params.append(product_type.lower())
        param_idx += 1

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    count_query = f"SELECT COUNT(*) FROM sealed_unified {where_clause}"
    data_query = (
        f"SELECT * FROM sealed_unified {where_clause} "
        f"ORDER BY {sort} {order_sql} NULLS LAST "
        f"LIMIT ${param_idx} OFFSET ${param_idx + 1}"
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval(count_query, *params)
        rows = await conn.fetch(data_query, *params, limit, offset)

    products = [_row_to_sealed(dict(row)) for row in rows]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "tier": user.tier,
        "products": products,
    }


# ─── Arbitrage scanner ────────────────────────────────────────────────────────

@router.get("/arbitrage")
async def arbitrage_scanner(
    set_code: Optional[str] = Query(None, description="Filter by set code"),
    min_profit_pct: float = Query(5.0, ge=0.0, description="Minimum profit percentage"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: UserInfo = Depends(get_current_user),
):
    """Find EN↔EU arbitrage opportunities.

    Compares en_tcgplayer_market (USD, converted to EUR) vs
    eu_cardmarket_7d_avg (EUR) and calculates profit after fees/shipping.

    Free tier: limited to the 3 latest sets.
    Pro+: all sets.
    """
    allowed_sets: Optional[list[str]] = None
    if not user.can_access("pro"):
        allowed_sets = FREE_TIER_SETS

    conditions = [
        "en_tcgplayer_market IS NOT NULL",
        "eu_cardmarket_7d_avg IS NOT NULL",
        "en_tcgplayer_market > 0",
        "eu_cardmarket_7d_avg > 0",
    ]
    params: list = []
    param_idx = 1

    if allowed_sets is not None:
        placeholders = ",".join(f"${param_idx + i}" for i in range(len(allowed_sets)))
        conditions.append(f"set_code IN ({placeholders})")
        params.extend(allowed_sets)
        param_idx += len(allowed_sets)

    if set_code:
        if allowed_sets and set_code.upper() not in allowed_sets:
            raise HTTPException(
                403,
                detail={
                    "error": "PRO_REQUIRED",
                    "message": "Access to this set requires a Pro subscription.",
                    "upgrade_url": "/login.html#upgrade",
                },
            )
        conditions.append(f"set_code = ${param_idx}")
        params.append(set_code.upper())
        param_idx += 1

    where_clause = "WHERE " + " AND ".join(conditions)
    query = (
        f"SELECT * FROM cards_unified {where_clause} "
        f"ORDER BY (eu_cardmarket_7d_avg - en_tcgplayer_market * {USD_TO_EUR}) DESC "
        f"LIMIT 1000"  # Fetch more than needed; filter by profit_pct in Python
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    opportunities = []
    for row in rows:
        arb = _arbitrage_calc(dict(row), min_profit_pct)
        if arb:
            opportunities.append(arb)

    # Sort by profit_pct desc, then paginate
    opportunities.sort(key=lambda x: x["profit_pct"], reverse=True)
    total = len(opportunities)
    page = opportunities[offset: offset + limit]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "min_profit_pct": min_profit_pct,
        "tier": user.tier,
        "fx_rate": {"usd_to_eur": USD_TO_EUR, "eur_to_usd": EUR_TO_USD},
        "opportunities": page,
    }

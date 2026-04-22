"""Unified card browser API — merges EN (TCG Price Lookup) and EU (Cardmarket) prices.

Endpoints:
    GET /api/cards/browse        — browse all cards with EN + EU prices
    GET /api/cards/sealed        — browse sealed products with EU prices
    GET /api/cards/arbitrage     — find EN↔EU arbitrage opportunities
    GET /api/cards/price-history — daily price history for a card
"""
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, HTTPException

from db.init import get_pool
from middleware.tier_gate import get_current_user, UserInfo
from services.card_aggregator import USD_TO_EUR, EUR_TO_USD

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cards", tags=["cards"])

# Free tier: access limited to the 3 most recent main sets
FREE_TIER_SET_LIMIT = 3
FREE_TIER_SETS = ["OP15", "OP14", "OP13"]  # Latest 3 booster sets

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
    """Convert a cards_unified DB row to a flat API response dict.

    Fields are kept flat so the frontend can access card.en_tcgplayer_market
    directly without nesting.
    """
    return {
        "card_id": row.get("card_id"),
        "name": row.get("name"),
        "set_code": row.get("set_code"),
        "set_name": row.get("set_name"),
        "rarity": row.get("rarity"),
        "variant": row.get("variant"),
        "image_url": row.get("image_url"),
        # EN prices (flat)
        "en_tcgplayer_market": row.get("en_tcgplayer_market"),
        "en_tcgplayer_low": row.get("en_tcgplayer_low"),
        "en_ebay_avg_7d": row.get("en_ebay_avg_7d"),
        "en_source": row.get("en_source", "TCG Price Lookup"),
        "en_updated_at": str(row.get("en_updated_at")) if row.get("en_updated_at") else None,
        # EU prices (flat)
        "eu_cardmarket_7d_avg": row.get("eu_cardmarket_7d_avg"),
        "eu_cardmarket_30d_avg": row.get("eu_cardmarket_30d_avg"),
        "eu_cardmarket_lowest": row.get("eu_cardmarket_lowest"),
        "eu_source": row.get("eu_source", "Cardmarket"),
        "eu_updated_at": str(row.get("eu_updated_at")) if row.get("eu_updated_at") else None,
        # IDs
        "tcg_price_lookup_id": row.get("tcg_price_lookup_id"),
        "rapidapi_card_id": row.get("rapidapi_card_id"),
        "tcgplayer_id": row.get("tcgplayer_id"),
        "cardmarket_id": row.get("cardmarket_id"),
        "pricecharting_id": row.get("pricecharting_id"),
        "links": _build_card_links(row),
    }


def _build_card_links(row: dict) -> dict:
    """Inline shim to services.marketplace_urls — avoid extra import at top."""
    from services.marketplace_urls import build_card_links
    return build_card_links(row)


def _build_sealed_links(row: dict) -> dict:
    from services.marketplace_urls import build_sealed_links
    return build_sealed_links(row)


def _row_to_sealed(row: dict) -> dict:
    """Convert a sealed_unified DB row to a flat API response dict."""
    return {
        "product_name": row.get("product_name"),
        "set_code": row.get("set_code"),
        "set_name": row.get("set_name"),
        "product_type": row.get("product_type"),
        "image_url": row.get("image_url"),
        "links": _build_sealed_links(row),
        "eu_price": row.get("eu_price"),
        "eu_30d_avg": row.get("eu_30d_avg"),
        "eu_7d_avg": row.get("eu_7d_avg"),
        "eu_trend": row.get("eu_trend"),
        "eu_source": row.get("eu_source", "Cardmarket"),
        "eu_updated_at": str(row.get("eu_updated_at")) if row.get("eu_updated_at") else None,
        "rapidapi_product_id": row.get("rapidapi_product_id"),
        "language": row.get("language") or "JP",
        "en_price_usd": row.get("en_price_usd"),
    }


def _jp_en_arbitrage_calc(row: dict, min_profit_pct: float) -> Optional[dict]:
    """Calculate JP→EN arbitrage for a single card-variant.

    Model: Buy the JP version (from Japan, e.g. Yuyutei/Cardrush), ship to EU,
    sell as JP-sealed or JP-single on Cardmarket/eBay to collectors.
    """
    jp_usd = row.get("jp_price_usd")
    en_usd = row.get("en_price_usd")
    if not jp_usd or not en_usd or jp_usd <= 0 or en_usd <= 0:
        return None

    jp_eur = jp_usd * USD_TO_EUR
    en_eur = en_usd * USD_TO_EUR

    # Costs: JP price + ~15% shipping/customs from Japan
    shipping_import_pct = 0.15
    cost_eur = jp_eur * (1 + shipping_import_pct)

    # Revenue: Sell at EN-collector price MINUS CM fees (5%)
    # Note: JP cards usually sell at ~70-85% of EN equivalent (not full EN price)
    # because JP-sellers attract JP-collectors, not EN-players.
    # We use 0.75 as realistic collector-market discount.
    jp_to_en_sell_factor = 0.75
    revenue_eur = en_eur * jp_to_en_sell_factor * (1 - 0.05)

    profit_eur = revenue_eur - cost_eur
    profit_pct = (profit_eur / cost_eur * 100) if cost_eur > 0 else 0

    if profit_pct < min_profit_pct:
        return None

    spread_ratio = en_eur / jp_eur
    signal = "BUY" if profit_pct >= 20 else "WATCH"

    # Build links (use JP-specific cardmarket URL when possible)
    from services.marketplace_urls import (
        tcgplayer_url, cardmarket_card_url, pricecharting_url,
    )
    name = row.get("name")
    cid = row.get("card_id")
    sc = row.get("set_code")
    var = row.get("variant")
    links = {
        "tcgplayer": tcgplayer_url(row.get("en_tcgplayer_id")),
        "cardmarket_en": cardmarket_card_url(name, cid, sc, var, language="EN"),
        "cardmarket_jp": cardmarket_card_url(name, cid, sc, var, language="JP"),
        "pricecharting_jp": pricecharting_url(row.get("jp_pricecharting_id")),
        "pricecharting_en": pricecharting_url(row.get("en_pricecharting_id")),
    }

    return {
        "card_id": cid,
        "name": name,
        "set_code": sc,
        "set_name": row.get("set_name"),
        "rarity": row.get("rarity"),
        "variant": var,
        "image_url": row.get("image_url"),
        "jp_price_usd": round(jp_usd, 2),
        "jp_price_eur": round(jp_eur, 2),
        "en_price_usd": round(en_usd, 2),
        "en_price_eur": round(en_eur, 2),
        "eu_price_eur": round(en_eur, 2),          # kept for UI back-compat
        "spread_ratio": round(spread_ratio, 2),
        "cost_eur": round(cost_eur, 2),
        "revenue_eur": round(revenue_eur, 2),
        "profit_eur": round(profit_eur, 2),
        "profit_pct": round(profit_pct, 2),
        "signal": signal,
        "sell_market": "Cardmarket (EN buyers pay premium)",
        "buy_market": "Yuyutei / Cardrush (JP)",
        "links": links,
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

    # Sanity filter: skip cards where EU/EN ratio is wildly inconsistent.
    # A real Normal-variant card should trade within 0.3–3× range across markets.
    # Ratios above 10× almost always indicate mismatched variants in our data
    # (Normal-row in DB actually storing Alt-Art price). These are not real
    # arbitrage — they're data bugs that would mislead the user.
    ratio = eu_eur / en_eur if en_eur > 0 else 0
    if ratio > 10 or ratio < 0.1:
        return None

    # Also require a sane 30d confirmation (if present) — 7d spike alone is noise
    eu_30d = card_row.get("eu_cardmarket_30d_avg")
    if eu_30d is not None and eu_30d > 0:
        if eu_eur / eu_30d > 3 or eu_30d / eu_eur > 3:
            return None  # 7d vs 30d divergence > 3× = unreliable

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

    signal = "BUY" if profit_pct >= 15 else "WATCH"

    return {
        "card_id": card_row.get("card_id"),
        "name": card_row.get("name"),
        "set_code": card_row.get("set_code"),
        "set_name": card_row.get("set_name"),
        "rarity": card_row.get("rarity"),
        "variant": card_row.get("variant"),
        "image_url": card_row.get("image_url"),
        # Flat field names expected by frontend
        "en_price_usd": en_usd,
        "en_price_eur": round(en_eur, 2),
        "eu_price_eur": eu_eur,
        "profit_eur": round(profit_eur, 2),
        "profit_pct": round(profit_pct, 2),
        "cost_eur": round(cost_eur, 2),
        "revenue_eur": round(revenue_eur, 2),
        "signal": signal,
        "sell_market": "TCGPlayer",
        "buy_market": "Cardmarket",
        "links": _build_card_links(card_row),
    }


# ─── Market Summary ───────────────────────────────────────────────────────────

@router.get("/market-summary")
async def market_summary():
    """Public endpoint: aggregate stats from cards_unified."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_cards = await conn.fetchval("SELECT COUNT(*) FROM cards_unified")
        total_sets = await conn.fetchval("SELECT COUNT(DISTINCT set_code) FROM cards_unified WHERE set_code IS NOT NULL")
        cards_with_eu = await conn.fetchval("SELECT COUNT(*) FROM cards_unified WHERE eu_cardmarket_7d_avg IS NOT NULL")
        cards_with_en = await conn.fetchval("SELECT COUNT(*) FROM cards_unified WHERE en_tcgplayer_market IS NOT NULL")

        top_eu = await conn.fetchrow(
            "SELECT card_id, name, eu_cardmarket_7d_avg FROM cards_unified "
            "WHERE eu_cardmarket_7d_avg IS NOT NULL "
            "ORDER BY eu_cardmarket_7d_avg DESC LIMIT 1"
        )

        last_updated = await conn.fetchval(
            "SELECT MAX(eu_updated_at) FROM cards_unified WHERE eu_updated_at IS NOT NULL"
        )

    top_eu_card = None
    if top_eu:
        top_eu_card = {
            "card_id": top_eu["card_id"],
            "name": top_eu["name"],
            "eu_cardmarket_7d_avg": top_eu["eu_cardmarket_7d_avg"],
        }

    return {
        "total_cards": total_cards,
        "total_sets": total_sets,
        "cards_with_eu_prices": cards_with_eu,
        "cards_with_en_prices": cards_with_en,
        "top_eu_card": top_eu_card,
        "last_updated": str(last_updated) if last_updated else None,
    }


# ─── Price History ────────────────────────────────────────────────────────────

@router.get("/price-history/{card_id}")
async def card_price_history(
    card_id: str = Path(..., description="Card ID, e.g. OP01-001"),
    variant: str = Query("Normal", description="Card variant"),
    days: int = Query(30, ge=7, le=365, description="Number of days of history"),
    user: UserInfo = Depends(get_current_user),
):
    """Daily price history for a card from daily_price_snapshots.

    Free tier: capped at 7 days.
    Pro+: up to 365 days.
    """
    # Free tier: 7 days only
    if not user.can_access("pro"):
        days = min(days, 7)

    since = date.today() - timedelta(days=days)

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Card metadata (match by card_id + variant)
        card = await conn.fetchrow(
            "SELECT id, card_id, name, set_code, set_name, rarity, variant, image_url, "
            "en_tcgplayer_market, eu_cardmarket_7d_avg, eu_cardmarket_30d_avg "
            "FROM cards_unified WHERE card_id = $1 AND variant = $2 LIMIT 1",
            card_id, variant,
        )
        if not card:
            # Fallback: try without variant filter
            card = await conn.fetchrow(
                "SELECT id, card_id, name, set_code, set_name, rarity, variant, image_url, "
                "en_tcgplayer_market, eu_cardmarket_7d_avg, eu_cardmarket_30d_avg "
                "FROM cards_unified WHERE card_id = $1 LIMIT 1",
                card_id,
            )
        if not card:
            raise HTTPException(404, detail="Card not found")

        # Snapshot history — join via card_unified_id, column is snap_date
        rows = await conn.fetch(
            "SELECT snap_date, en_tcgplayer_market, en_tcgplayer_low, "
            "eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest "
            "FROM daily_price_snapshots "
            "WHERE card_unified_id = $1 AND snap_date >= $2 "
            "ORDER BY snap_date ASC",
            card["id"], since,
        )

    history = [
        {
            "date": str(row["snap_date"]),
            "en_tcgplayer_market": row["en_tcgplayer_market"],
            "en_tcgplayer_low": row["en_tcgplayer_low"],
            "eu_cardmarket_7d_avg": row["eu_cardmarket_7d_avg"],
            "eu_cardmarket_30d_avg": row["eu_cardmarket_30d_avg"],
            "eu_cardmarket_lowest": row["eu_cardmarket_lowest"],
        }
        for row in rows
    ]

    # Fallback: if no snapshots yet, return current prices as single data point
    if not history:
        history = [
            {
                "date": str(date.today()),
                "en_tcgplayer_market": card["en_tcgplayer_market"],
                "en_tcgplayer_low": None,
                "eu_cardmarket_7d_avg": card["eu_cardmarket_7d_avg"],
                "eu_cardmarket_30d_avg": card["eu_cardmarket_30d_avg"],
                "eu_cardmarket_lowest": None,
            }
        ]

    # ── Compute technical + TCG-specific indicators ──
    from services.indicators import build_indicators
    from services.set_meta import SET_RELEASE_DATES

    set_release = SET_RELEASE_DATES.get((card["set_code"] or "").upper())
    indicators = build_indicators(
        history=history,
        current={"eu_cardmarket_7d_avg": card["eu_cardmarket_7d_avg"]},
        set_release_date=set_release,
    )

    return {
        "card_id": card["card_id"],
        "name": card["name"],
        "set_code": card["set_code"],
        "set_name": card["set_name"],
        "rarity": card["rarity"],
        "variant": variant,
        "image_url": card["image_url"],
        "current_eu_price": card["eu_cardmarket_7d_avg"],
        "current_en_price_usd": card["en_tcgplayer_market"],
        "days": days,
        "tier": user.tier,
        "history": history,
        "indicators": indicators,
    }


# ─── Browse cards ─────────────────────────────────────────────────────────────

@router.get("/browse")
async def browse_cards(
    set_code: Optional[str] = Query(None, description="Filter by set code, e.g. OP01"),
    search: Optional[str] = Query(None, min_length=2, description="Card name search"),
    rarity: Optional[str] = Query(None, description="Filter by rarity"),
    sort: str = Query("eu_cardmarket_7d_avg", description="Sort column"),
    order: str = Query("desc", description="asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: UserInfo = Depends(get_current_user),
):
    """Browse all cards with EN + EU prices from both sources."""
    # Validate sort/order
    if sort not in CARD_SORT_COLUMNS:
        sort = "eu_cardmarket_7d_avg"
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
            card["en_ebay_avg_7d"] = None  # eBay prices = Trader plan
            card["en_tcgplayer_low"] = None

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
        params.append(product_type.lower().replace("_", " "))
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
    """Find JP↔EN arbitrage opportunities.

    For each card_id + variant that exists in BOTH Japanese and English,
    computes the price spread. JP is typically cheaper (sourced from Japan)
    while EN commands premium in Western collector markets.

    Ranking: highest absolute EUR spread first.
    Free tier: limited to the 3 latest sets.
    Pro+: all sets.
    """
    allowed_sets: Optional[list[str]] = None
    if not user.can_access("pro"):
        allowed_sets = FREE_TIER_SETS

    params: list = []
    param_idx = 1
    set_filter = ""

    if allowed_sets is not None:
        placeholders = ",".join(f"${param_idx + i}" for i in range(len(allowed_sets)))
        set_filter = f"AND jp.set_code IN ({placeholders})"
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
        set_filter += f" AND jp.set_code = ${param_idx}"
        params.append(set_code.upper())
        param_idx += 1

    query = f"""
        SELECT
            jp.card_id, jp.variant, jp.name, jp.set_code, jp.set_name,
            jp.rarity, jp.image_url,
            jp.pc_price_usd AS jp_price_usd,
            en.pc_price_usd AS en_price_usd,
            jp.tcgplayer_id AS jp_tcgplayer_id,
            jp.cardmarket_id AS jp_cardmarket_id,
            en.tcgplayer_id AS en_tcgplayer_id,
            en.cardmarket_id AS en_cardmarket_id,
            jp.pricecharting_id AS jp_pricecharting_id,
            en.pricecharting_id AS en_pricecharting_id
        FROM cards_unified jp
        JOIN cards_unified en
          ON en.card_id = jp.card_id
         AND en.variant = jp.variant
         AND en.language = 'EN'
        WHERE jp.language = 'JP'
          AND jp.pc_price_usd > 1.0
          AND en.pc_price_usd > 1.0
          {set_filter}
        ORDER BY (en.pc_price_usd - jp.pc_price_usd) DESC
        LIMIT 1000
    """

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    opportunities = []
    for row in rows:
        arb = _jp_en_arbitrage_calc(dict(row), min_profit_pct)
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

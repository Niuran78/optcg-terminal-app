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
from pydantic import BaseModel

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_auth, UserInfo
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
    "cm_live_trend",
    "cm_live_lowest",
    "cm_live_available",
    "expected_value_eur",
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
        # EU prices (flat) — reference derived from PriceCharting/TCGPlayer
        "eu_cardmarket_7d_avg": row.get("eu_cardmarket_7d_avg"),
        "eu_cardmarket_30d_avg": row.get("eu_cardmarket_30d_avg"),
        "eu_cardmarket_lowest": row.get("eu_cardmarket_lowest"),
        "eu_source": row.get("eu_source", "Cardmarket"),
        "eu_updated_at": str(row.get("eu_updated_at")) if row.get("eu_updated_at") else None,
        # LIVE Cardmarket prices (scraped directly) — authoritative when present
        "cm_live_trend":      row.get("cm_live_trend"),
        "cm_live_30d_avg":    row.get("cm_live_30d_avg"),
        "cm_live_7d_avg":     row.get("cm_live_7d_avg"),
        "cm_live_lowest":     row.get("cm_live_lowest"),
        "cm_live_available":  row.get("cm_live_available"),
        "cm_live_url":        row.get("cm_live_url"),
        "cm_live_status":     row.get("cm_live_status"),
        "cm_live_updated_at": str(row.get("cm_live_updated_at")) if row.get("cm_live_updated_at") else None,
        # JP-side live prices
        "jp_cm_live_trend":   row.get("jp_cm_live_trend"),
        "jp_cm_live_30d_avg": row.get("jp_cm_live_30d_avg"),
        "jp_cm_live_lowest":  row.get("jp_cm_live_lowest"),
        "jp_cm_live_available": row.get("jp_cm_live_available"),
        "jp_cm_live_url":     row.get("jp_cm_live_url"),
        "jp_cm_live_status":  row.get("jp_cm_live_status"),
        # IDs
        "tcg_price_lookup_id": row.get("tcg_price_lookup_id"),
        "rapidapi_card_id": row.get("rapidapi_card_id"),
        "tcgplayer_id": row.get("tcgplayer_id"),
        "cardmarket_id": row.get("cardmarket_id"),
        "pricecharting_id": row.get("pricecharting_id") or row.get("en_pricecharting_id"),
        # JP + EN sibling prices (populated by browse JOIN)
        "jp_pc_price_usd": row.get("jp_pc_price_usd"),
        "en_pc_price_usd": row.get("en_pc_price_usd"),
        "jp_pricecharting_id": row.get("jp_pricecharting_id"),
        "en_pricecharting_id": row.get("en_pricecharting_id"),
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
    """Convert a sealed_unified DB row to a flat API response dict.

    Live Cardmarket prices (cm_live_trend) take precedence over the legacy
    eu_price field (which is PriceCharting USD * live_fx, so not an actual
    EU market price). Users see the real number whenever we have one.
    """
    cm_live_trend = row.get("cm_live_trend")
    has_live = cm_live_trend is not None

    # Effective price: live trend first, else the legacy reference
    effective_price = cm_live_trend if has_live else row.get("eu_price")
    price_source = "Cardmarket LIVE" if has_live else row.get("eu_source", "Reference (PriceCharting)")

    # Spread % = (trend - lowest) / trend * 100. Only meaningful when both live.
    cm_live_lowest = row.get("cm_live_lowest")
    spread_pct = None
    if cm_live_trend and cm_live_lowest and cm_live_trend > 0:
        try:
            spread_pct = round((cm_live_trend - cm_live_lowest) / cm_live_trend * 100.0, 1)
        except Exception:
            spread_pct = None

    # EV fields (persisted nightly by services.sealed_ev)
    ev_eur = row.get("expected_value_eur")
    if ev_eur is not None:
        try:
            ev_eur = float(ev_eur)
        except Exception:
            ev_eur = None
    ev_pct = None
    ev_minus_box = None
    if ev_eur is not None and cm_live_trend and cm_live_trend > 0:
        ev_minus_box = round(ev_eur - float(cm_live_trend), 2)
        ev_pct = round((ev_eur - float(cm_live_trend)) / float(cm_live_trend) * 100.0, 1)

    return {
        "product_name": row.get("product_name"),
        "set_code": row.get("set_code"),
        "set_name": row.get("set_name"),
        "product_type": row.get("product_type"),
        "image_url": row.get("image_url"),
        "links": _build_sealed_links(row),
        # Primary price fields (the ones the frontend should use first)
        "eu_price": effective_price,
        "eu_30d_avg": row.get("cm_live_30d_avg") if has_live else row.get("eu_30d_avg"),
        "eu_7d_avg": row.get("cm_live_7d_avg") if has_live else row.get("eu_7d_avg"),
        "eu_trend": row.get("eu_trend"),
        "eu_source": price_source,
        "eu_updated_at": str(row.get("cm_live_updated_at") or row.get("eu_updated_at")) if (row.get("cm_live_updated_at") or row.get("eu_updated_at")) else None,
        # Explicit live-data fields for transparency in the UI
        "cm_live_trend":     cm_live_trend,
        "cm_live_30d_avg":   row.get("cm_live_30d_avg"),
        "cm_live_7d_avg":    row.get("cm_live_7d_avg"),
        "cm_live_lowest":    cm_live_lowest,
        "cm_live_available": row.get("cm_live_available"),
        "cm_live_url":       row.get("cm_live_url"),
        "cm_live_status":    row.get("cm_live_status"),
        "cm_live_updated_at": str(row.get("cm_live_updated_at")) if row.get("cm_live_updated_at") else None,
        "spread_pct":        spread_pct,
        "has_live":          has_live,
        # EV (estimate)
        "ev_eur":             round(ev_eur, 2) if ev_eur is not None else None,
        "ev_minus_box":       ev_minus_box,
        "ev_pct":             ev_pct,
        "ev_computed_at":     str(row.get("ev_computed_at")) if row.get("ev_computed_at") else None,
        "ev_label":           "estimate",
        # Keep legacy/reference fields for fallback display
        "reference_eu_price": row.get("eu_price"),
        "rapidapi_product_id": row.get("rapidapi_product_id"),
        "language": row.get("language") or "JP",
        "en_price_usd": row.get("en_price_usd"),
    }


def _jp_en_arbitrage_calc(row: dict, min_profit_pct: float) -> Optional[dict]:
    """Calculate JP→EN arbitrage for a single card-variant.

    Model: Buy the JP version on Cardmarket JP, sell the EN version on
    Cardmarket EN. Live Cardmarket prices are used whenever available,
    falling back to PriceCharting-derived EUR only if live data is missing.
    """
    # Prefer live Cardmarket EUR prices (scraped)
    jp_eur = row.get("jp_cm_live_trend")
    en_eur = row.get("en_cm_live_trend")
    # Fallback: derive from PriceCharting USD * FX
    if jp_eur is None:
        jp_usd = row.get("jp_price_usd")
        jp_eur = jp_usd * USD_TO_EUR if jp_usd else None
    if en_eur is None:
        en_usd = row.get("en_price_usd")
        en_eur = en_usd * USD_TO_EUR if en_usd else None

    if not jp_eur or not en_eur or jp_eur <= 0 or en_eur <= 0:
        return None

    # Keep legacy *_usd fields for the response payload (UI shows them)
    jp_usd = (jp_eur / USD_TO_EUR) if USD_TO_EUR else jp_eur
    en_usd = (en_eur / USD_TO_EUR) if USD_TO_EUR else en_eur

    # Data-source label
    jp_source = "live" if row.get("jp_cm_live_trend") is not None else "reference"
    en_source = "live" if row.get("en_cm_live_trend") is not None else "reference"

    # Sanity filter: Spreads > 15× are almost always data artifacts.
    # JP marketplaces often don't list rare Western Prize Cards (V5+), so
    # our JP price is actually the *regular* card while EN is the Prize.
    # These would mislead users into "90× arbitrage" that doesn't exist.
    spread_sanity = en_eur / jp_eur if jp_eur > 0 else 0
    if spread_sanity > 15:
        return None
    if spread_sanity < 0.5:
        return None  # negative spread (JP > EN) = opposite direction, unusual

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
    # Use live Cardmarket URLs (scraped + verified) when available;
    # fall back to our URL-builder guess otherwise.
    links = {
        "tcgplayer": tcgplayer_url(row.get("en_tcgplayer_id")),
        "cardmarket_en": row.get("en_cm_live_url") or cardmarket_card_url(name, cid, sc, var, language="EN"),
        "cardmarket_jp": row.get("jp_cm_live_url") or cardmarket_card_url(name, cid, sc, var, language="JP"),
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
        "jp_source": jp_source,
        "en_source": en_source,
        "is_live": jp_source == "live" and en_source == "live",
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
async def market_summary(user: UserInfo = Depends(require_auth)):
    """Aggregate stats from cards_unified. Login-gated."""
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
    user: UserInfo = Depends(require_auth),
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
        current={
            "eu_cardmarket_7d_avg": card["eu_cardmarket_7d_avg"],
            "variant": card.get("variant"),
            "name": card.get("name"),
        },
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
    sort: str = Query("relevance", description="Sort column. 'relevance' = LIVE-first, then price"),
    order: str = Query("desc", description="asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    min_price_eur: Optional[float] = Query(None, ge=0, description="Hide cards cheaper than this"),
    max_price_eur: Optional[float] = Query(1000.0, ge=0, description="Hide cards pricier than this (default €1000 filters out serialized/prize)"),
    include_extreme: bool = Query(False, description="Include serialized/prize cards above €1000"),
    user: UserInfo = Depends(require_auth),
):
    """Browse all cards with EN + EU prices from both sources.

    By default, excludes serialized/prize cards above €1000 to keep the
    browser focused on typical tradable cards. Pass include_extreme=true
    to see the Kaido Serialized / Championship cards.
    """
    # Validate sort/order
    if sort not in CARD_SORT_COLUMNS:
        sort = "eu_cardmarket_7d_avg"
    order_sql = "DESC" if order.lower() != "asc" else "ASC"

    # LIVE FX rate (USD->EUR) — used in all SQL strings below for converting
    # pc_price_usd. Kept in a local to ensure consistency within a single request.
    from services.fx_rate import get_usd_to_eur
    FX = get_usd_to_eur()

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
                    "upgrade_url": "/?upgrade=pro",
                },
            )
        conditions.append(f"set_code = ${param_idx}")
        params.append(set_code.upper())
        param_idx += 1

    if search:
        # Match against name OR card_id (so radar/portfolio links by ID work too)
        conditions.append(f"(name ILIKE ${param_idx} OR card_id ILIKE ${param_idx})")
        params.append(f"%{search}%")
        param_idx += 1

    if rarity:
        conditions.append(f"rarity = ${param_idx}")
        params.append(rarity)
        param_idx += 1

    # Price range filters — these need to look at BOTH language prices
    # (EN and JP) so they're applied on the outer SELECT after the join.
    price_conditions: list[str] = []

    # Price range filters — hide serialized/prize cards by default
    if not include_extreme:
        # Exclude common prize/championship/manga/serialized variants
        # These are 1-of-N tournament prizes, not tradable regular cards.
        PRIZE_VARIANT_LIST = [
            "V5", "V6", "V7", "V8", "V9", "V10",
            "SP", "SP Gold", "SP Silver",
            "Manga", "Red Manga", "Magazine",
            "Championship 2023 Winner", "Championship 25-26",
            "Serial Prize", "Serialized",
            "Pre-Release", "2nd Anniversary", "Wanted",
            "Top Prize", "Top 16", "Top 64", "Flagship Battle", "Treasure Cup", "Treasure Cup 2025",
        ]
        placeholders = ",".join(f"${param_idx + i}" for i in range(len(PRIZE_VARIANT_LIST)))
        conditions.append(f"(variant NOT IN ({placeholders}))")
        params.extend(PRIZE_VARIANT_LIST)
        param_idx += len(PRIZE_VARIANT_LIST)

        if max_price_eur is not None:
            # Cap max of (EN-price-eur, JP-price-eur) — either side exceeding is dropped
            price_conditions.append(
                f"COALESCE(GREATEST(en.eu_cardmarket_7d_avg, jp.pc_price_usd * {FX}), "
                f"en.eu_cardmarket_7d_avg, jp.pc_price_usd * {FX}) < ${param_idx}"
            )
            params.append(max_price_eur)
            param_idx += 1
    if min_price_eur is not None and min_price_eur > 0:
        # At least one side needs to hit min_price
        price_conditions.append(
            f"COALESCE(en.eu_cardmarket_7d_avg, jp.pc_price_usd * {FX}, 0) >= ${param_idx}"
        )
        params.append(min_price_eur)
        param_idx += 1

    # Price-range filter — hide ultra-rare serialized/prize cards by default
    # unless user explicitly requests them via min_price / max_price query.
    # Default max €1000 keeps the browser usable for typical trading cards.
    # (apply after other conditions so set_code/search still work)
    # Note: conditions list is already built; we extend below.

    # Build a 'distinct (card_id, variant)' base set that includes rows from
    # EITHER language. This way a JP-only card (e.g. Zoro Flagship Battle) is
    # still visible in the Browser, not just EN-side cards.
    # Conditions apply to the card-metadata fields (card_id, name, rarity,
    # variant, set_code) — these are language-agnostic so we don't prefix.
    base_conditions = list(conditions)  # no language filter
    base_where = ("WHERE " + " AND ".join(base_conditions)) if base_conditions else ""

    # Outer WHERE — requires at least one price AND price-range bounds
    outer_conditions = ["(en.pc_price_usd IS NOT NULL OR jp.pc_price_usd IS NOT NULL)"]
    outer_conditions.extend(price_conditions)

    # TRUST GUARD: hide any card whose reference price ≥ €50 if we don't have
    # a live Cardmarket price to back it up. This includes Prize/Championship
    # variants — their PriceCharting prices (sometimes €1000+) are speculative
    # collector values, not actual Cardmarket liquidity, so they'd mislead
    # traders. Rule: high-price cards MUST have a live Cardmarket trend to be
    # shown. Sub-€50 cards: reference is usually close enough; show normally.
    outer_conditions.append(
        "("
        "   en.cm_live_trend IS NOT NULL"
        " OR jp.cm_live_trend IS NOT NULL"
        f" OR COALESCE(en.eu_cardmarket_7d_avg, jp.pc_price_usd * {FX}, 0) < 50"
        ")"
    )

    outer_where = "WHERE " + " AND ".join(outer_conditions)

    # Build sort expression.
    # 'relevance' is the new default — shows the most trustworthy + highest-
    # impact cards first. Composite score:
    #   +10000 if we have a LIVE Cardmarket price (scraped within 7 days)
    #   +  500 for alternate-art / parallel variants (more desirable)
    #   +    1 per € of effective price (so within each group, price decides)
    if sort == "relevance":
        effective_price_sql = (
            "COALESCE("
            "  en.cm_live_trend, jp.cm_live_trend, "
            f"  en.eu_cardmarket_7d_avg, jp.pc_price_usd * {FX}, 0)"
        )
        sort_expr = (
            "("
            "  CASE WHEN en.cm_live_trend IS NOT NULL OR jp.cm_live_trend IS NOT NULL THEN 10000 ELSE 0 END"
            " + CASE WHEN b.variant ILIKE 'Alternate Art%' OR b.variant ILIKE 'V%' THEN 500 ELSE 0 END"
            f" + LEAST({effective_price_sql}, 9999)"
            ")"
        )
    elif sort.startswith("eu_") or sort == "en_tcgplayer_market":
        sort_expr = f"COALESCE(en.{sort}, jp.pc_price_usd * {FX})"
    else:
        sort_expr = f"COALESCE(en.{sort}, jp.{sort})"

    count_query = f"""
        WITH base AS (
            SELECT DISTINCT card_id, variant
            FROM cards_unified
            {base_where}
        )
        SELECT COUNT(*)
        FROM base b
        LEFT JOIN cards_unified en
          ON en.card_id = b.card_id AND en.variant = b.variant AND en.language = 'EN'
        LEFT JOIN cards_unified jp
          ON jp.card_id = b.card_id AND jp.variant = b.variant AND jp.language = 'JP'
        {outer_where}
    """

    data_query = f"""
        WITH base AS (
            SELECT DISTINCT card_id, variant
            FROM cards_unified
            {base_where}
        )
        SELECT
            COALESCE(en.id, jp.id) AS id,
            b.card_id,
            b.variant,
            COALESCE(en.set_code, jp.set_code) AS set_code,
            COALESCE(en.set_name, jp.set_name) AS set_name,
            COALESCE(en.name, jp.name) AS name,
            COALESCE(en.rarity, jp.rarity) AS rarity,
            COALESCE(en.image_url, jp.image_url) AS image_url,
            en.en_tcgplayer_market,
            en.en_tcgplayer_low,
            en.eu_cardmarket_7d_avg,
            en.eu_cardmarket_30d_avg,
            en.eu_cardmarket_lowest,
            en.eu_source,
            en.eu_updated_at,
            en.pc_price_usd AS en_pc_price_usd,
            en.pricecharting_id AS en_pricecharting_id,
            jp.pc_price_usd AS jp_pc_price_usd,
            jp.pricecharting_id AS jp_pricecharting_id,
            jp.id AS jp_card_db_id,
            en.id AS en_card_db_id,
            -- LIVE Cardmarket scraped prices (EN row)
            en.cm_live_trend AS cm_live_trend,
            en.cm_live_30d_avg AS cm_live_30d_avg,
            en.cm_live_7d_avg AS cm_live_7d_avg,
            en.cm_live_lowest AS cm_live_lowest,
            en.cm_live_available AS cm_live_available,
            en.cm_live_url AS cm_live_url,
            en.cm_live_status AS cm_live_status,
            en.cm_live_updated_at AS cm_live_updated_at,
            -- JP-side live prices
            jp.cm_live_trend AS jp_cm_live_trend,
            jp.cm_live_30d_avg AS jp_cm_live_30d_avg,
            jp.cm_live_lowest AS jp_cm_live_lowest,
            jp.cm_live_available AS jp_cm_live_available,
            jp.cm_live_url AS jp_cm_live_url,
            jp.cm_live_status AS jp_cm_live_status,
            'EN' AS language  -- legacy field
        FROM base b
        LEFT JOIN cards_unified en
          ON en.card_id = b.card_id AND en.variant = b.variant AND en.language = 'EN'
        LEFT JOIN cards_unified jp
          ON jp.card_id = b.card_id AND jp.variant = b.variant AND jp.language = 'JP'
        {outer_where}
        ORDER BY {sort_expr} {order_sql} NULLS LAST
        LIMIT ${param_idx} OFFSET ${param_idx + 1}
    """

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
    language: Optional[str] = Query(None, description="EN or JP"),
    sort: str = Query("cm_live_trend", description="Sort column"),
    order: str = Query("desc", description="asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    live_only: bool = Query(True, description="Default true: only show items with cm_live_trend."),
    include: Optional[str] = Query(None, description="'all' = include reference-only items (Pro+)"),
    user: UserInfo = Depends(require_auth),
):
    """Browse sealed products. Defaults to live-only (cm_live_trend IS NOT NULL).

    Set live_only=false (Pro+ only) or include=all to show reference-only sealed.
    """
    if sort not in SEALED_SORT_COLUMNS:
        sort = "cm_live_trend"
    order_sql = "DESC" if order.lower() != "asc" else "ASC"

    # Honour both `live_only=false` and `include=all` for non-live items, but
    # gate that behind Pro+. Free + default = live-only, period.
    is_pro = user.can_access("pro")
    show_reference = (not live_only) or (include and include.lower() == "all")
    if show_reference and not is_pro:
        # Free users can't see reference-only sealed.
        show_reference = False

    allowed_sets: Optional[list[str]] = None
    if not is_pro:
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
                    "upgrade_url": "/?upgrade=pro",
                },
            )
        conditions.append(f"set_code = ${param_idx}")
        params.append(set_code.upper())
        param_idx += 1

    if product_type:
        conditions.append(f"product_type = ${param_idx}")
        params.append(product_type.lower().replace("_", " "))
        param_idx += 1
    else:
        # Default: nur Booster Boxes und Cases (Shop-relevant).
        # Blendet Booster Packs, Sleeved Booster, Tins, Displays, Double-Packs
        # aus — diese sind entweder nicht in unserem Shop-Scope oder existieren
        # gar nicht als eigenständige Cardmarket-Produkte.
        conditions.append("product_type IN ('booster box', 'case')")

    if language:
        lang_norm = language.strip().upper()
        if lang_norm in ("EN", "JP"):
            conditions.append(f"language = ${param_idx}")
            params.append(lang_norm)
            param_idx += 1

    # Live-only filter: by default, hide sealed without live Cardmarket data.
    # Pro+ may opt out via live_only=false (or include=all).
    if not show_reference:
        conditions.append("cm_live_trend IS NOT NULL")

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
        "is_pro": is_pro,
        "live_only": not show_reference,
        "products": products,
    }


@router.get("/sealed/ev/{set_code}")
async def sealed_ev_endpoint(
    set_code: str,
    language: str = Query("JP"),
    product_type: str = Query("booster box"),
    user: UserInfo = Depends(require_auth),
):
    """Compute Sealed EV on-demand for a given set/language/product_type.

    Returns full breakdown including per-rarity contributions. The persisted
    expected_value_eur in sealed_unified is updated nightly; this endpoint
    re-computes live and is mainly used by the Sealed-Tab EV detail modal.

    Pro+ gated.
    """
    if not user.can_access("pro"):
        raise HTTPException(
            403,
            detail={
                "error": "PRO_REQUIRED",
                "message": "Sealed EV is a Pro feature.",
                "upgrade_url": "/?upgrade=pro",
            },
        )

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT cm_live_trend, product_name, image_url, cm_live_url
            FROM sealed_unified
            WHERE set_code = $1 AND language = $2 AND product_type = $3
            LIMIT 1
            """,
            set_code.upper(), language.upper(), product_type.lower(),
        )

    box_price = float(row["cm_live_trend"]) if row and row["cm_live_trend"] else None
    from services.sealed_ev import compute_sealed_ev
    ev = await compute_sealed_ev(
        set_code=set_code,
        language=language,
        box_price_eur=box_price,
        product_type=product_type,
    )
    if row:
        ev["product_name"] = row["product_name"]
        ev["image_url"] = row["image_url"]
        ev["cardmarket_url"] = row["cm_live_url"]

    # Telemetry: sealed_ev_viewed
    try:
        from services.telemetry import emit
        await emit("sealed_ev_viewed", user_id=user.user_id, tier=user.tier,
                   properties={"set_code": set_code.upper(),
                               "language": language.upper(),
                               "product_type": product_type})
    except Exception:
        pass

    return ev


# ─── Arbitrage scanner ────────────────────────────────────────────────────────

@router.get("/arbitrage")
async def arbitrage_scanner(
    set_code: Optional[str] = Query(None, description="Filter by set code"),
    min_profit_pct: float = Query(5.0, ge=0.0, description="Minimum profit percentage"),
    live_only: bool = Query(False, description="Only show pairs where BOTH sides have cm_live_trend (verified live)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: UserInfo = Depends(require_auth),
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
                    "upgrade_url": "/?upgrade=pro",
                },
            )
        set_filter += f" AND jp.set_code = ${param_idx}"
        params.append(set_code.upper())
        param_idx += 1

    # When live_only=true, BOTH sides must have an actual cm_live_trend.
    # Without this guard, the result mixes verified live data with reference
    # values from PriceCharting * FX, and ranks them by phantom spreads.
    live_filter = ""
    if live_only:
        live_filter = (
            " AND jp.cm_live_trend IS NOT NULL AND jp.cm_live_trend > 0"
            " AND en.cm_live_trend IS NOT NULL AND en.cm_live_trend > 0"
        )

    query = f"""
        SELECT
            jp.card_id, jp.variant, jp.name, jp.set_code, jp.set_name,
            jp.rarity, jp.image_url,
            jp.pc_price_usd AS jp_price_usd,
            en.pc_price_usd AS en_price_usd,
            jp.cm_live_trend AS jp_cm_live_trend,
            en.cm_live_trend AS en_cm_live_trend,
            jp.cm_live_url   AS jp_cm_live_url,
            en.cm_live_url   AS en_cm_live_url,
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
          AND COALESCE(jp.cm_live_trend, jp.pc_price_usd * $LANG_USD_TO_EUR$) > 1.0
          AND COALESCE(en.cm_live_trend, en.pc_price_usd * $LANG_USD_TO_EUR$) > 1.0
          {live_filter}
          {set_filter}
        ORDER BY (
            COALESCE(en.cm_live_trend, en.pc_price_usd * $LANG_USD_TO_EUR$) -
            COALESCE(jp.cm_live_trend, jp.pc_price_usd * $LANG_USD_TO_EUR$)
        ) DESC
        LIMIT 1000
    """.replace("$LANG_USD_TO_EUR$", f"{float(USD_TO_EUR):.6f}")

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
        "live_only": live_only,
        "tier": user.tier,
        "fx_rate": {"usd_to_eur": float(USD_TO_EUR), "eur_to_usd": float(EUR_TO_USD)},
        "opportunities": page,
    }


# ─── Public FX rate endpoint ──────────────────────────────────────────────────
# Exposes the live USD→EUR rate so the frontend can convert reference USD prices
# without hardcoding 0.92 anymore. No auth: it's a public market data point and
# we already hit the FX provider on every backend startup.

_fx_router = APIRouter(prefix="/api/fx", tags=["fx"])


@_fx_router.get("/rate")
async def get_fx_rate():
    from services.fx_rate import get_usd_to_eur, _cache
    rate = get_usd_to_eur()
    return {
        "base": "USD",
        "quote": "EUR",
        "rate": float(rate),
        "inverse": round(1.0 / rate, 6) if rate > 0 else None,
        "fetched_at_unix": float(_cache[1]),
        "source": "Frankfurter.dev (ECB)",
    }


# ─── Telemetry endpoint ───────────────────────────────────────────────────────
# Frontend POSTs events here. Bound to the user's bearer token if present;
# anonymous events (e.g. before signup) get user_id=NULL.

_telemetry_router = APIRouter(prefix="/api/telemetry", tags=["telemetry"])


class _TelemetryEvent(BaseModel):
    event: str
    properties: dict = {}


@_telemetry_router.post("/event")
async def post_event(
    body: _TelemetryEvent,
    user: UserInfo = Depends(get_current_user),  # NOT require_auth — anonymous OK
):
    from services.telemetry import emit
    await emit(
        event_name=body.event,
        user_id=user.user_id if user.is_authenticated else None,
        tier=user.tier if user.is_authenticated else None,
        properties=body.properties or {},
    )
    return {"ok": True}


# ─── Market Radar (MVP) ──────────────────────────────────────────────────────
# GET /api/radar/today — Pro+ tier-gated personalized signals.
# Computed nightly; this is a pure read endpoint.

_radar_router = APIRouter(prefix="/api/radar", tags=["radar"])


@_radar_router.get("/today")
async def radar_today(user: UserInfo = Depends(require_auth)):
    """Return today's radar signals for the authenticated user.

    Pro+ tier required. Free users get an upgrade prompt.
    """
    if user.tier not in ("pro", "elite"):
        return {
            "tier": user.tier,
            "upgrade_required": True,
            "upgrade_url": "/?upgrade=pro",
            "signals": [],
            "message": "Market Radar is a Pro feature. Upgrade to see personalized daily signals.",
        }
    from services.radar import get_signals_for_user
    signals = await get_signals_for_user(user.user_id, limit=25)

    # Telemetry: radar_opened
    try:
        from services.telemetry import emit
        await emit("radar_opened", user_id=user.user_id, tier=user.tier,
                   properties={"signal_count": len(signals)})
    except Exception:
        pass

    return {
        "tier": user.tier,
        "upgrade_required": False,
        "signals": signals,
        "count": len(signals),
    }


# ─── Markets Endpoint (NEW — Investment-Tool pivot) ──────────────────────────
# Honest, live-only card data. Sources cards_investable materialized view
# which contains ~762 cards with verified Cardmarket live data, all with
# liquidity_score >= 60 (cm_live_status='ok', updated within 14d).

CARDS_MARKETS_SORT = {
    "liquidity_score": "liquidity_score",
    "cm_live_trend": "cm_live_trend",
    "cm_live_available": "cm_live_available",
    "name": "name",
    "set_code": "set_code",
    "spread_pct": "spread_pct",
}


@router.get("/markets")
async def markets_cards(
    set_code: Optional[str] = Query(None),
    search: Optional[str] = Query(None, min_length=2),
    rarity: Optional[str] = Query(None),
    language: Optional[str] = Query(None, description="EN or JP"),
    min_liquidity: int = Query(0, ge=0, le=100, description="0=all, 30=thin+, 60=liquid only"),
    sort: str = Query("liquidity_score"),
    order: str = Query("desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    min_price_eur: Optional[float] = Query(None, ge=0),
    max_price_eur: Optional[float] = Query(1000.0, ge=0),
    user: UserInfo = Depends(require_auth),
):
    """Investment-grade card list — Cardmarket live data only.

    Backed by cards_investable materialized view. No PriceCharting
    reference data here; if a card is missing, it doesn't appear.
    Always returns liquidity_score and spread_pct.
    """
    pool = await get_pool()
    sort_col = CARDS_MARKETS_SORT.get(sort, "liquidity_score")
    order_sql = "DESC" if order.lower() != "asc" else "ASC"

    where = ["1=1"]
    params: list = []
    pi = 1

    if set_code:
        where.append(f"set_code = ${pi}")
        params.append(set_code.upper()); pi += 1

    if search:
        where.append(f"(name ILIKE ${pi} OR card_id ILIKE ${pi})")
        params.append(f"%{search}%"); pi += 1

    if rarity and rarity.lower() not in ('all', ''):
        where.append(f"rarity = ${pi}")
        params.append(rarity); pi += 1

    if language and language.lower() not in ('all', ''):
        where.append(f"language = ${pi}")
        params.append(language.upper()); pi += 1

    if min_liquidity > 0:
        where.append(f"liquidity_score >= ${pi}")
        params.append(min_liquidity); pi += 1

    if min_price_eur is not None:
        where.append(f"cm_live_trend >= ${pi}")
        params.append(min_price_eur); pi += 1

    if max_price_eur is not None:
        where.append(f"cm_live_trend <= ${pi}")
        params.append(max_price_eur); pi += 1

    where_sql = " AND ".join(where)
    limit_param = pi
    offset_param = pi + 1
    params.extend([limit, offset])

    sql = f"""
        SELECT id, card_id, variant, language, set_code, set_name, name, rarity, image_url,
               cm_live_trend, cm_live_30d_avg, cm_live_7d_avg, cm_live_lowest,
               cm_live_available, cm_live_url, cm_live_updated_at,
               liquidity_score, spread_pct,
               COUNT(*) OVER() AS _total
        FROM cards_investable
        WHERE {where_sql}
        ORDER BY {sort_col} {order_sql} NULLS LAST, id ASC
        LIMIT ${limit_param} OFFSET ${offset_param}
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    total = rows[0]["_total"] if rows else 0
    items = []
    is_pro = user.can_access("pro")
    for r in rows:
        d = dict(r)
        d.pop("_total", None)
        # Free tier: hide actual prices, keep metadata + liquidity color (not number)
        if not is_pro:
            for k in ("cm_live_trend", "cm_live_30d_avg", "cm_live_7d_avg",
                     "cm_live_lowest", "cm_live_available", "spread_pct"):
                d[k] = None
            # Coarse liquidity bucket only (not exact score)
            score = d.get("liquidity_score") or 0
            d["liquidity_score"] = None
            d["liquidity_bucket"] = "liquid" if score >= 60 else "thin" if score >= 30 else "illiquid"
            d["upgrade_required"] = True
        else:
            score = d.get("liquidity_score") or 0
            d["liquidity_bucket"] = "liquid" if score >= 60 else "thin" if score >= 30 else "illiquid"
        # Datetime → ISO
        if d.get("cm_live_updated_at"):
            d["cm_live_updated_at"] = d["cm_live_updated_at"].isoformat()
        items.append(d)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
        "tier": user.tier,
        "is_pro": is_pro,
    }


@router.post("/markets/refresh-mview")
async def refresh_markets_mview(user: UserInfo = Depends(require_auth)):
    """Manually refresh cards_investable. Admin only.

    Normally runs nightly via cron after the Cardmarket scrape.
    """
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin role required")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY cards_investable")
        n = await conn.fetchval("SELECT COUNT(*) FROM cards_investable")
    return {"ok": True, "rows": n}

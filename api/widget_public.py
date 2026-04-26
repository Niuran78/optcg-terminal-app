"""Public widget API — no auth, no tier gating.

Endpoints intended for the embeddable product widget on Shopify.
Returns the same data as /api/cards/browse and /api/cards/sealed but
without authentication or free-tier set restrictions.

Safe because:
- Read-only (no writes)
- Rate-limited by CORS/Cloudflare at the edge
- Only public price data (same as Cardmarket public pages)
"""
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query, Response, HTTPException
from fastapi.responses import JSONResponse

from db.init import get_pool

router = APIRouter(prefix="/api/widget", tags=["widget"])


# Cache headers for the public sealed-widget endpoint. 15 min cache at
# any CDN, plus 60s stale-while-revalidate so users on holygrade.com always
# get a quick response. The data is at most ~24h old anyway (scraper cadence),
# so 15min is fine.
_PUBLIC_CACHE = "public, max-age=900, stale-while-revalidate=60"


@router.get("/set/{set_code}")
async def widget_set_data(
    set_code: str,
    limit_cards: int = Query(5, ge=1, le=20),
):
    """Return all data needed for the product widget in a single call.

    Returns:
      - set info (code, name)
      - sealed products grouped by product_type with JP and EN prices
      - top N cards by EU market price
    """
    set_code = set_code.upper()
    pool = await get_pool()

    async with pool.acquire() as conn:
        # Sealed products for this set — fetch all languages
        sealed_rows = await conn.fetch(
            """
            SELECT product_name, set_code, set_name, product_type, image_url,
                   eu_price, eu_30d_avg, eu_7d_avg, eu_trend, eu_source,
                   eu_updated_at, rapidapi_product_id,
                   COALESCE(language, 'JP') as language,
                   en_price_usd, pricecharting_id
            FROM sealed_unified
            WHERE set_code = $1
            ORDER BY product_type, language
            """,
            set_code,
        )

        # Top cards by EU price
        card_rows = await conn.fetch(
            """
            SELECT card_id, name, set_code, set_name, rarity, variant, image_url,
                   en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
                   eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest,
                   eu_source, eu_updated_at,
                   tcgplayer_id, cardmarket_id, pricecharting_id
            FROM cards_unified
            WHERE set_code = $1 AND eu_cardmarket_7d_avg IS NOT NULL
            ORDER BY eu_cardmarket_7d_avg DESC
            LIMIT $2
            """,
            set_code,
            limit_cards,
        )

        # Set name (from first card/product, or null)
        set_name = None
        if sealed_rows:
            set_name = sealed_rows[0]["set_name"]
        elif card_rows:
            set_name = card_rows[0]["set_name"]

    # Group sealed products by product_type with JP and EN sub-objects
    sealed_by_type: dict[str, dict] = {}
    for r in sealed_rows:
        pt = r["product_type"] or "other"
        lang = (r["language"] or "JP").upper()

        if pt not in sealed_by_type:
            sealed_by_type[pt] = {
                "product_type": pt,
                "product_name": r["product_name"],
                "image_url": r["image_url"],
                "jp": None,
                "en": None,
            }

        from services.marketplace_urls import (
            pricecharting_url, cardmarket_sealed_url,
        )
        price_obj = {
            "price_eur": r["eu_price"],
            "price_usd": r["en_price_usd"],
            "eu_7d_avg": r["eu_7d_avg"],
            "eu_30d_avg": r["eu_30d_avg"],
            "eu_trend": r["eu_trend"],
            "source": r["eu_source"] or "Cardmarket",
            "updated_at": str(r["eu_updated_at"]) if r["eu_updated_at"] else None,
            "links": {
                "pricecharting": pricecharting_url(r["pricecharting_id"]),
                "cardmarket": cardmarket_sealed_url(
                    r["product_type"], r["set_code"], r["set_name"], lang,
                ),
            },
        }

        if lang == "JP":
            sealed_by_type[pt]["jp"] = price_obj
            # Use JP product name as default
            sealed_by_type[pt]["product_name"] = r["product_name"]
        elif lang == "EN":
            sealed_by_type[pt]["en"] = price_obj

    sealed = list(sealed_by_type.values())

    # Flatten cards with buy links
    from services.marketplace_urls import build_card_links
    cards = [
        {
            "card_id": r["card_id"],
            "name": r["name"],
            "set_code": r["set_code"],
            "set_name": r["set_name"],
            "rarity": r["rarity"],
            "variant": r["variant"],
            "image_url": r["image_url"],
            "en_tcgplayer_market": r["en_tcgplayer_market"],
            "en_tcgplayer_low": r["en_tcgplayer_low"],
            "en_ebay_avg_7d": r["en_ebay_avg_7d"],
            "eu_cardmarket_7d_avg": r["eu_cardmarket_7d_avg"],
            "eu_cardmarket_30d_avg": r["eu_cardmarket_30d_avg"],
            "eu_cardmarket_lowest": r["eu_cardmarket_lowest"],
            "eu_source": r["eu_source"] or "Cardmarket",
            "eu_updated_at": str(r["eu_updated_at"]) if r["eu_updated_at"] else None,
            "links": build_card_links(dict(r)),
        }
        for r in card_rows
    ]

    return {
        "set_code": set_code,
        "set_name": set_name,
        "sealed": sealed,
        "cards": cards,
        "count": {"sealed": len(sealed), "cards": len(cards)},
    }


# ────────────────────────────────────────────────────────────────────
# Public Sealed Widget API (Shopify integration)
# ────────────────────────────────────────────────────────────────────
# This is the canonical endpoint embedded into holygrade.com Shopify
# product pages. It deliberately returns a small, stable JSON shape so the
# frontend widget can render fast and cache aggressively.
#
# - No auth, public.
# - Cache-Control: 15 minutes
# - 404 with structured error body if no live data exists for the
#   requested (set_code, language, product_type) tuple.
# - Telemetry: sealed_widget_view

_PRODUCT_TYPE_NORMALIZE = {
    "booster_box": "booster box",
    "booster":     "booster",
    "case":        "case",
    "display":     "display",
    "sleeved_booster": "sleeved booster",
}


@router.get("/sealed/{set_code}")
async def widget_sealed_one(
    set_code: str,
    response: Response,
    language: Optional[str] = Query(None, description="EN or JP"),
    type: Optional[str] = Query(
        "booster_box",
        description="booster_box | case | booster (default: booster_box)",
    ),
):
    """Single-product sealed-widget endpoint for Shopify embeds.

    Returns a small stable JSON object with current Cardmarket live data
    for one (set_code, language, product_type). 404 with a structured
    body if no live data exists.
    """
    set_code = set_code.upper().strip()
    pt_norm = _PRODUCT_TYPE_NORMALIZE.get(
        (type or "booster_box").lower().strip(),
        (type or "booster_box").lower().replace("_", " "),
    )
    lang_norm = language.upper().strip() if language else None

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Build a permissive query — if no language is given, prefer JP,
        # falling back to EN (or whatever exists).
        rows = await conn.fetch(
            """
            SELECT product_name, set_code, set_name, product_type, language,
                   image_url, cm_live_trend, cm_live_30d_avg, cm_live_7d_avg,
                   cm_live_lowest, cm_live_available, cm_live_url, cm_live_status,
                   cm_live_updated_at, expected_value_eur
            FROM sealed_unified
            WHERE set_code = $1
              AND product_type = $2
              AND ($3::text IS NULL OR language = $3)
              AND cm_live_trend IS NOT NULL
            ORDER BY
                CASE WHEN $3 IS NULL THEN
                    CASE language WHEN 'JP' THEN 0 WHEN 'EN' THEN 1 ELSE 2 END
                ELSE 0 END,
                cm_live_updated_at DESC NULLS LAST
            LIMIT 1
            """,
            set_code, pt_norm, lang_norm,
        )

    if not rows:
        # Telemetry: emit a miss so we can monitor for popular set codes
        # without live data.
        try:
            from services.telemetry import emit
            await emit(
                "sealed_widget_view",
                properties={
                    "set_code":     set_code,
                    "language":     lang_norm,
                    "product_type": pt_norm,
                    "hit":          False,
                },
            )
        except Exception:
            pass
        return JSONResponse(
            status_code=404,
            content={
                "error":   "no_live_data",
                "set_code": set_code,
                "language": lang_norm,
                "product_type": pt_norm,
                "message": (
                    "No live Cardmarket data for this product yet. The data "
                    "will appear automatically once our scraper has refreshed."
                ),
                "powered_by": {
                    "name":         "Holygrade Terminal",
                    "url":          "https://terminal.holygrade.com",
                    "attribution": "Live data via Cardmarket scrape",
                },
            },
            headers={"Cache-Control": "public, max-age=300"},
        )

    r = rows[0]
    trend = float(r["cm_live_trend"]) if r["cm_live_trend"] else None
    lowest = float(r["cm_live_lowest"]) if r["cm_live_lowest"] else None
    spread_pct = None
    if trend and lowest and trend > 0:
        spread_pct = round((trend - lowest) / trend * 100.0, 1)

    updated_at = r["cm_live_updated_at"]
    fresh = False
    if updated_at:
        age_h = (datetime.now(timezone.utc) - updated_at).total_seconds() / 3600.0
        fresh = age_h < 48.0

    ev = r["expected_value_eur"]
    ev_pct = None
    if ev is not None and trend and trend > 0:
        try:
            ev_pct = round((float(ev) - trend) / trend * 100.0, 1)
        except Exception:
            ev_pct = None

    body = {
        "set_code":      r["set_code"],
        "set_name":      r["set_name"],
        "language":      r["language"],
        "product_type":  r["product_type"],
        "product_name":  r["product_name"],
        "image_url":     r["image_url"],
        "price": {
            "trend_eur":           round(trend, 2) if trend is not None else None,
            "lowest_eur":          round(lowest, 2) if lowest is not None else None,
            "avg_7d_eur":          round(float(r["cm_live_7d_avg"]), 2) if r["cm_live_7d_avg"] else None,
            "avg_30d_eur":         round(float(r["cm_live_30d_avg"]), 2) if r["cm_live_30d_avg"] else None,
            "available_listings":  int(r["cm_live_available"]) if r["cm_live_available"] else 0,
        },
        "spread_pct":      spread_pct,
        "cardmarket_url":  r["cm_live_url"],
        "updated_at":      updated_at.isoformat() if updated_at else None,
        "data_freshness":  "fresh" if fresh else "stale",
        "ev": {
            "expected_value_eur": round(float(ev), 2) if ev is not None else None,
            "vs_box_pct":         ev_pct,
            "label":              "estimate",
        } if ev is not None else None,
        "powered_by": {
            "name":        "Holygrade Terminal",
            "url":         "https://terminal.holygrade.com",
            "attribution": "Live data via Cardmarket scrape",
        },
    }

    # Telemetry (best-effort, never blocks)
    try:
        from services.telemetry import emit
        await emit(
            "sealed_widget_view",
            properties={
                "set_code":     set_code,
                "language":     r["language"],
                "product_type": r["product_type"],
                "hit":          True,
            },
        )
    except Exception:
        pass

    response.headers["Cache-Control"] = _PUBLIC_CACHE
    response.headers["Vary"] = "Origin"
    return body


@router.post("/sealed/{set_code}/click")
async def widget_sealed_click(
    set_code: str,
    language: Optional[str] = Query(None),
    type: Optional[str] = Query("booster_box"),
):
    """Telemetry: user clicked the Cardmarket-link inside the Shopify widget."""
    try:
        from services.telemetry import emit
        await emit(
            "sealed_widget_click",
            properties={
                "set_code":     set_code.upper(),
                "language":     (language or "").upper() or None,
                "product_type": (type or "booster_box").lower(),
            },
        )
    except Exception:
        pass
    return {"ok": True}

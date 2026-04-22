"""Public widget API — no auth, no tier gating.

Endpoints intended for the embeddable product widget on Shopify.
Returns the same data as /api/cards/browse and /api/cards/sealed but
without authentication or free-tier set restrictions.

Safe because:
- Read-only (no writes)
- Rate-limited by CORS/Cloudflare at the edge
- Only public price data (same as Cardmarket public pages)
"""
from typing import Optional

from fastapi import APIRouter, Query

from db.init import get_pool

router = APIRouter(prefix="/api/widget", tags=["widget"])


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

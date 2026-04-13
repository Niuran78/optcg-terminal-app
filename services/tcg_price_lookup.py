"""TCG Price Lookup API adapter — EN price source (TCGPlayer + eBay)."""
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional

import httpx

from db.init import get_pool

logger = logging.getLogger(__name__)

TCG_API_KEY = os.getenv("TCG_PRICE_LOOKUP_KEY", "")
BASE_URL = "https://api.tcgpricelookup.com/v1"
GAME_SLUG = "onepiece"

# Cache durations
SETS_CACHE_HOURS = 24
CARDS_CACHE_HOURS = 6


def _headers() -> dict:
    return {"X-API-Key": TCG_API_KEY, "Accept": "application/json"}


def _sets_cache_threshold() -> datetime:
    return datetime.utcnow() - timedelta(hours=SETS_CACHE_HOURS)


def _cards_cache_threshold() -> datetime:
    return datetime.utcnow() - timedelta(hours=CARDS_CACHE_HOURS)


def _extract_en_prices(card: dict) -> dict:
    """Extract EN prices from a TCG Price Lookup card response.

    Price structure:
        prices.raw.near_mint.tcgplayer.market / low
        prices.raw.near_mint.ebay.avg_7d
    """
    prices = card.get("prices", {}) or {}
    raw = prices.get("raw", {}) or {}
    nm = raw.get("near_mint", {}) or {}

    tcgplayer = nm.get("tcgplayer", {}) or {}
    ebay = nm.get("ebay", {}) or {}

    def _safe_float(v) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    return {
        "tcgplayer_market": _safe_float(tcgplayer.get("market")),
        "tcgplayer_low": _safe_float(tcgplayer.get("low")),
        "ebay_avg_7d": _safe_float(ebay.get("avg_7d")),
    }


def _normalize_card(card: dict) -> dict:
    """Normalize a TCG Price Lookup card to a clean internal dict."""
    set_info = card.get("set", {}) or {}
    prices = _extract_en_prices(card)
    return {
        "tcg_price_lookup_id": card.get("id", ""),
        "tcgplayer_id": card.get("tcgplayer_id"),
        "name": card.get("name", ""),
        "card_id": card.get("number", ""),  # e.g. "OP01-001"
        "rarity": card.get("rarity", ""),
        "variant": card.get("variant", "Normal") or "Normal",
        "image_url": card.get("image_url", ""),
        "set_slug": set_info.get("slug", ""),
        "set_name": set_info.get("name", ""),
        "en_tcgplayer_market": prices["tcgplayer_market"],
        "en_tcgplayer_low": prices["tcgplayer_low"],
        "en_ebay_avg_7d": prices["ebay_avg_7d"],
    }


async def get_en_sets() -> list[dict]:
    """Fetch all One Piece EN sets from TCG Price Lookup, with DB caching."""
    threshold = _sets_cache_threshold()
    pool = await get_pool()

    async with pool.acquire() as conn:
        cached = await conn.fetchrow(
            "SELECT set_data_json, cached_at FROM tcg_sets_cache WHERE game_slug=$1 AND cached_at > $2",
            GAME_SLUG, threshold
        )
        if cached:
            sets = json.loads(cached["set_data_json"])
            logger.debug(f"TCG sets: returning {len(sets)} from cache")
            return sets

    # Fetch from API
    sets = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/sets",
                params={"game": GAME_SLUG},
                headers=_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            # Response may be a list or {"data": [...], ...}
            sets = data if isinstance(data, list) else data.get("data", data.get("sets", []))
            logger.info(f"TCG Price Lookup: fetched {len(sets)} EN sets")
    except Exception as e:
        logger.error(f"TCG Price Lookup: error fetching sets: {e}")
        # Return stale cache
        async with pool.acquire() as conn:
            stale = await conn.fetchrow(
                "SELECT set_data_json FROM tcg_sets_cache WHERE game_slug=$1",
                GAME_SLUG
            )
            if stale:
                return json.loads(stale["set_data_json"])
            return []

    # Store in cache (upsert the entire sets list as JSON)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tcg_sets_cache (game_slug, set_data_json, cached_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT(game_slug) DO UPDATE SET
                set_data_json=EXCLUDED.set_data_json,
                cached_at=NOW()
            """,
            GAME_SLUG, json.dumps(sets)
        )

    return sets


async def get_en_cards(set_slug: str) -> list[dict]:
    """Fetch all EN cards for a set from TCG Price Lookup with pagination.

    Paginates using limit=100 / offset until all cards are retrieved.
    Results are cached per set_slug.
    """
    threshold = _cards_cache_threshold()
    pool = await get_pool()

    async with pool.acquire() as conn:
        cached = await conn.fetch(
            "SELECT card_data_json FROM tcg_en_cards_cache WHERE set_slug=$1 AND cached_at > $2",
            set_slug, threshold
        )
        if cached:
            logger.debug(f"TCG EN cards: returning {len(cached)} from cache for {set_slug}")
            return [json.loads(row["card_data_json"]) for row in cached]

    # Fetch all pages from API
    all_cards = []
    limit = 100
    offset = 0
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                resp = await client.get(
                    f"{BASE_URL}/cards/search",
                    params={
                        "game": GAME_SLUG,
                        "set": set_slug,
                        "limit": limit,
                        "offset": offset,
                    },
                    headers=_headers(),
                )
                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, list):
                    page_cards = data
                    total = len(data) + offset  # No pagination info; stop after one page
                else:
                    page_cards = data.get("data", data.get("cards", []))
                    total = data.get("total", data.get("count", len(page_cards) + offset))

                all_cards.extend(page_cards)

                if len(page_cards) < limit or (offset + limit) >= total:
                    break
                offset += limit
                await asyncio.sleep(3.0)  # Rate limit for Free plan

        logger.info(f"TCG Price Lookup: fetched {len(all_cards)} EN cards for {set_slug}")
    except Exception as e:
        logger.error(f"TCG Price Lookup: error fetching cards for {set_slug}: {e}")
        # Return stale cache
        async with pool.acquire() as conn:
            stale = await conn.fetch(
                "SELECT card_data_json FROM tcg_en_cards_cache WHERE set_slug=$1",
                set_slug
            )
            return [json.loads(row["card_data_json"]) for row in stale]

    # Normalize and store in cache
    normalized = [_normalize_card(c) for c in all_cards]

    async with pool.acquire() as conn:
        for card in normalized:
            card_id = card.get("card_id", "")
            variant = card.get("variant", "Normal") or "Normal"
            if not card_id:
                continue
            await conn.execute(
                """
                INSERT INTO tcg_en_cards_cache (set_slug, card_data_json, card_id, variant, cached_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT(set_slug, card_id, variant) DO UPDATE SET
                    card_data_json=EXCLUDED.card_data_json,
                    cached_at=NOW()
                """,
                set_slug, json.dumps(card), card_id, variant
            )

    return normalized


async def search_en_cards(query: str) -> list[dict]:
    """Search EN cards by name from cache."""
    query_lower = query.lower()
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                "SELECT card_data_json FROM tcg_en_cards_cache WHERE card_data_json LIKE $1 LIMIT 100",
                f"%{query}%"
            )
            results = []
            for row in rows:
                card = json.loads(row["card_data_json"])
                if query_lower in (card.get("name") or "").lower():
                    results.append(card)
            return results
        except Exception as e:
            logger.error(f"TCG Price Lookup: search error for '{query}': {e}")
            return []

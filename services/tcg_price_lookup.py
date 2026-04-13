"""TCG Price Lookup API adapter — EN price source (TCGPlayer + eBay)."""
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional

import httpx
import aiosqlite

from db.init import DATABASE_PATH

logger = logging.getLogger(__name__)

TCG_API_KEY = os.getenv("TCG_PRICE_LOOKUP_KEY", "")
BASE_URL = "https://api.tcgpricelookup.com/v1"
GAME_SLUG = "onepiece"

# Cache durations
SETS_CACHE_HOURS = 24
CARDS_CACHE_HOURS = 6


def _headers() -> dict:
    return {"X-API-Key": TCG_API_KEY, "Accept": "application/json"}


def _sets_cache_threshold() -> str:
    return (datetime.utcnow() - timedelta(hours=SETS_CACHE_HOURS)).isoformat()


def _cards_cache_threshold() -> str:
    return (datetime.utcnow() - timedelta(hours=CARDS_CACHE_HOURS)).isoformat()


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


async def _ensure_tcg_sets_table(db: aiosqlite.Connection):
    """Ensure the tcg_sets cache table exists."""
    await db.execute("""
        CREATE TABLE IF NOT EXISTS tcg_sets_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            game_slug TEXT NOT NULL DEFAULT 'onepiece',
            cached_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    await db.commit()


async def _ensure_en_cards_table(db: aiosqlite.Connection):
    """Ensure the en_cards cache table exists."""
    await db.execute("""
        CREATE TABLE IF NOT EXISTS tcg_en_cards_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_slug TEXT NOT NULL,
            card_data_json TEXT NOT NULL,
            card_id TEXT NOT NULL,
            variant TEXT NOT NULL DEFAULT 'Normal',
            cached_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(set_slug, card_id, variant)
        )
    """)
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_en_cards_set ON tcg_en_cards_cache(set_slug)"
    )
    await db.commit()


async def get_en_sets() -> list[dict]:
    """Fetch all One Piece EN sets from TCG Price Lookup, with DB caching."""
    threshold = _sets_cache_threshold()

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_tcg_sets_table(db)
        cursor = await db.execute(
            "SELECT * FROM tcg_sets_cache WHERE game_slug=? AND cached_at > ? ORDER BY name",
            (GAME_SLUG, threshold),
        )
        cached = await cursor.fetchall()
        if cached:
            logger.debug(f"TCG sets: returning {len(cached)} from cache")
            return [dict(row) for row in cached]

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
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            await _ensure_tcg_sets_table(db)
            cursor = await db.execute(
                "SELECT * FROM tcg_sets_cache WHERE game_slug=? ORDER BY name",
                (GAME_SLUG,),
            )
            stale = await cursor.fetchall()
            return [dict(row) for row in stale]

    # Store in cache
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_tcg_sets_table(db)
        now = datetime.utcnow().isoformat()
        for s in sets:
            slug = s.get("slug", "")
            name = s.get("name", "")
            game_slug = s.get("game", {}).get("slug", GAME_SLUG) if isinstance(s.get("game"), dict) else GAME_SLUG
            if not slug:
                continue
            await db.execute(
                """
                INSERT INTO tcg_sets_cache (slug, name, game_slug, cached_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    name=excluded.name,
                    game_slug=excluded.game_slug,
                    cached_at=excluded.cached_at
                """,
                (slug, name, game_slug, now),
            )
        await db.commit()

        cursor = await db.execute(
            "SELECT * FROM tcg_sets_cache WHERE game_slug=? ORDER BY name",
            (GAME_SLUG,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_en_cards(set_slug: str) -> list[dict]:
    """Fetch all EN cards for a set from TCG Price Lookup with pagination.

    Paginates using limit=100 / offset until all cards are retrieved.
    Results are cached per set_slug.
    """
    threshold = _cards_cache_threshold()

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_en_cards_table(db)
        cursor = await db.execute(
            "SELECT card_data_json FROM tcg_en_cards_cache WHERE set_slug=? AND cached_at > ?",
            (set_slug, threshold),
        )
        cached = await cursor.fetchall()
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
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            await _ensure_en_cards_table(db)
            cursor = await db.execute(
                "SELECT card_data_json FROM tcg_en_cards_cache WHERE set_slug=?",
                (set_slug,),
            )
            stale = await cursor.fetchall()
            return [json.loads(row["card_data_json"]) for row in stale]

    # Normalize and store in cache
    normalized = [_normalize_card(c) for c in all_cards]

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await _ensure_en_cards_table(db)
        now = datetime.utcnow().isoformat()
        for card in normalized:
            card_id = card.get("card_id", "")
            variant = card.get("variant", "Normal") or "Normal"
            if not card_id:
                continue
            await db.execute(
                """
                INSERT INTO tcg_en_cards_cache (set_slug, card_data_json, card_id, variant, cached_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(set_slug, card_id, variant) DO UPDATE SET
                    card_data_json=excluded.card_data_json,
                    cached_at=excluded.cached_at
                """,
                (set_slug, json.dumps(card), card_id, variant, now),
            )
        await db.commit()

    return normalized


async def search_en_cards(query: str) -> list[dict]:
    """Search EN cards by name from cache."""
    query_lower = query.lower()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(
                "SELECT card_data_json FROM tcg_en_cards_cache WHERE card_data_json LIKE ? LIMIT 100",
                (f"%{query}%",),
            )
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                card = json.loads(row["card_data_json"])
                if query_lower in (card.get("name") or "").lower():
                    results.append(card)
            return results
        except Exception as e:
            logger.error(f"TCG Price Lookup: search error for '{query}': {e}")
            return []

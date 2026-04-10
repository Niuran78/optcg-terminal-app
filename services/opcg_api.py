"""One Piece Card Game API adapter via RapidAPI with SQLite caching."""
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx
import aiosqlite

from db.init import DATABASE_PATH

logger = logging.getLogger(__name__)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "one-piece-card-game1.p.rapidapi.com")
BASE_URL = f"https://{RAPIDAPI_HOST}"

# Cache durations
FREE_CACHE_HOURS = 24
PRO_CACHE_MINUTES = 15


def _cache_age_threshold(tier: str) -> datetime:
    """Return the oldest acceptable cache timestamp for a given tier."""
    now = datetime.utcnow()
    if tier == "free":
        return now - timedelta(hours=FREE_CACHE_HOURS)
    else:
        return now - timedelta(minutes=PRO_CACHE_MINUTES)


def _headers() -> dict:
    return {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
    }


def _extract_price(item: dict, source: str) -> Optional[float]:
    """Extract price from API response item for a given source.
    
    API response structure:
    "prices": {
        "cardmarket": {
            "currency": "EUR",
            "lowest_near_mint": 16500,
            "30d_average": 8011.11,
            "7d_average": 8585.71
        },
        "tcg_player": {
            "currency": "EUR",
            "market_price": 6892.48
        }
    }
    """
    prices = item.get("prices", {}) or {}
    if not isinstance(prices, dict):
        return None
    
    if source == "cardmarket":
        cm = prices.get("cardmarket", {})
        if isinstance(cm, dict):
            # Try lowest_near_mint first, then lowest, then 7d_average, then 30d_average
            for key in ["lowest_near_mint", "lowest", "7d_average", "30d_average"]:
                v = cm.get(key)
                if v is not None:
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        pass
        # If cm is a number directly
        if isinstance(cm, (int, float)):
            return float(cm)
    
    elif source == "tcgplayer":
        # API uses "tcg_player" (with underscore)
        for k in ["tcg_player", "tcgplayer", "tcgPlayer"]:
            tcg = prices.get(k, {})
            if isinstance(tcg, dict):
                v = tcg.get("market_price")
                if v is not None:
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        pass
            elif isinstance(tcg, (int, float)):
                return float(tcg)
    
    return None


async def get_sets(tier: str = "free") -> list[dict]:
    """Fetch all sets/episodes from API or cache."""
    threshold = _cache_age_threshold(tier)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM sets WHERE created_at > ? ORDER BY release_date DESC",
            (threshold.isoformat(),)
        )
        cached = await cursor.fetchall()
        if cached:
            return [dict(row) for row in cached]

    # Fetch from API (with pagination)
    episodes = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            page = 1
            while True:
                resp = await client.get(
                    f"{BASE_URL}/episodes",
                    params={"page": page},
                    headers=_headers()
                )
                resp.raise_for_status()
                data = resp.json()
                page_items = data if isinstance(data, list) else data.get("data", [])
                episodes.extend(page_items)
                # Check pagination
                paging = data.get("paging", {}) if isinstance(data, dict) else {}
                total_pages = paging.get("total", 1)
                if page >= total_pages:
                    break
                page += 1
    except Exception as e:
        logger.error(f"API error fetching sets: {e}")
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM sets ORDER BY release_date DESC")
            stale = await cursor.fetchall()
            return [dict(row) for row in stale]

    async with aiosqlite.connect(DATABASE_PATH) as db:
        now = datetime.utcnow().isoformat()
        for ep in episodes:
            api_id = str(ep.get("id", ep.get("_id", "")))
            name = ep.get("name", ep.get("title", "Unknown"))
            code = ep.get("code", ep.get("set_code", ""))
            release_date = ep.get("released_at", ep.get("release_date", ep.get("releaseDate", "")))
            card_count = ep.get("cards_total", ep.get("card_count", ep.get("cardCount", 0)))
            # Detect language from name/code — JP if name contains JP indicators
            lang = _detect_language(name, code, ep)

            await db.execute("""
                INSERT INTO sets (api_id, name, code, release_date, card_count, language, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(api_id) DO UPDATE SET
                    name=excluded.name, code=excluded.code,
                    release_date=excluded.release_date, card_count=excluded.card_count,
                    language=excluded.language, created_at=excluded.created_at
            """, (api_id, name, code, release_date, card_count, lang, now))
        await db.commit()

        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM sets ORDER BY release_date DESC")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


def _detect_language(name: str, code: str, ep: dict) -> str:
    """The API does not distinguish JP vs EN — all sets are mixed. Return ALL."""
    return "ALL"


def _classify_region(cm_price: Optional[float], tcp_price: Optional[float]) -> str:
    """Classify card region based on available marketplace price data.

    Cardmarket = primarily JP/EU market.
    TCGPlayer  = primarily US/EN market.

    Returns:
        "BOTH" — prices on both markets
        "JP"   — Cardmarket price only
        "EN"   — TCGPlayer price only
        "JP"   — no data (default fallback)
    """
    has_cm = cm_price is not None
    has_tcp = tcp_price is not None
    if has_cm and has_tcp:
        return "BOTH"
    if has_cm:
        return "JP"
    if has_tcp:
        return "EN"
    return "JP"


async def get_cards(set_id: str, tier: str = "free") -> list[dict]:
    """Fetch cards for a set from API or cache."""
    threshold = _cache_age_threshold(tier)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM cards_cache WHERE set_api_id=? AND last_updated>?",
            (set_id, threshold.isoformat())
        )
        cached = await cursor.fetchall()
        if cached:
            result = []
            for row in cached:
                item = json.loads(row["card_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                item["_region"] = _classify_region(row["cardmarket_price"], row["tcgplayer_price"])
                result.append(item)
            return result

    # Fetch from API
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/episodes/{set_id}/cards",
                params={"sort": "price_highest"},
                headers=_headers()
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error(f"API error fetching cards for set {set_id}: {e}")
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM cards_cache WHERE set_api_id=?", (set_id,)
            )
            stale = await cursor.fetchall()
            result = []
            for row in stale:
                item = json.loads(row["card_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                result.append(item)
            return result

    cards = data if isinstance(data, list) else data.get("data", data.get("cards", []))

    async with aiosqlite.connect(DATABASE_PATH) as db:
        now = datetime.utcnow().isoformat()
        for card in cards:
            card_api_id = str(card.get("id", card.get("_id", card.get("code", ""))))
            cm_price = _extract_price(card, "cardmarket")
            tcp_price = _extract_price(card, "tcgplayer")

            await db.execute("""
                INSERT INTO cards_cache (set_api_id, card_api_id, card_data_json, cardmarket_price, tcgplayer_price, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(set_api_id, card_api_id) DO UPDATE SET
                    card_data_json=excluded.card_data_json,
                    cardmarket_price=excluded.cardmarket_price,
                    tcgplayer_price=excluded.tcgplayer_price,
                    last_updated=excluded.last_updated
            """, (set_id, card_api_id, json.dumps(card), cm_price, tcp_price, now))

            # Record price history
            if cm_price is not None or tcp_price is not None:
                await db.execute("""
                    INSERT INTO price_history (item_type, item_api_id, cardmarket_price, tcgplayer_price, recorded_at)
                    VALUES ('card', ?, ?, ?, ?)
                """, (card_api_id, cm_price, tcp_price, now))

        await db.commit()

    # Return with prices attached
    result = []
    for card in cards:
        cm_price = _extract_price(card, "cardmarket")
        tcp_price = _extract_price(card, "tcgplayer")
        card["_cardmarket_price"] = cm_price
        card["_tcgplayer_price"] = tcp_price
        card["_region"] = _classify_region(cm_price, tcp_price)
        result.append(card)
    return result


async def get_products(set_id: str, tier: str = "free") -> list[dict]:
    """Fetch sealed products for a set from API or cache."""
    threshold = _cache_age_threshold(tier)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM products_cache WHERE set_api_id=? AND last_updated>?",
            (set_id, threshold.isoformat())
        )
        cached = await cursor.fetchall()
        if cached:
            result = []
            for row in cached:
                item = json.loads(row["product_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                result.append(item)
            return result

    # Fetch from API
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/episodes/{set_id}/products",
                params={"sort": "price_highest"},
                headers=_headers()
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error(f"API error fetching products for set {set_id}: {e}")
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM products_cache WHERE set_api_id=?", (set_id,)
            )
            stale = await cursor.fetchall()
            result = []
            for row in stale:
                item = json.loads(row["product_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                result.append(item)
            return result

    products = data if isinstance(data, list) else data.get("data", data.get("products", []))

    async with aiosqlite.connect(DATABASE_PATH) as db:
        now = datetime.utcnow().isoformat()
        for product in products:
            prod_api_id = str(product.get("id", product.get("_id", product.get("code", ""))))
            cm_price = _extract_price(product, "cardmarket")
            tcp_price = _extract_price(product, "tcgplayer")

            await db.execute("""
                INSERT INTO products_cache (set_api_id, product_api_id, product_data_json, cardmarket_price, tcgplayer_price, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(set_api_id, product_api_id) DO UPDATE SET
                    product_data_json=excluded.product_data_json,
                    cardmarket_price=excluded.cardmarket_price,
                    tcgplayer_price=excluded.tcgplayer_price,
                    last_updated=excluded.last_updated
            """, (set_id, prod_api_id, json.dumps(product), cm_price, tcp_price, now))

            if cm_price is not None or tcp_price is not None:
                await db.execute("""
                    INSERT INTO price_history (item_type, item_api_id, cardmarket_price, tcgplayer_price, recorded_at)
                    VALUES ('product', ?, ?, ?, ?)
                """, (prod_api_id, cm_price, tcp_price, now))

        await db.commit()

    result = []
    for product in products:
        cm_price = _extract_price(product, "cardmarket")
        tcp_price = _extract_price(product, "tcgplayer")
        product["_cardmarket_price"] = cm_price
        product["_tcgplayer_price"] = tcp_price
        result.append(product)
    return result


async def search_cards(query: str, tier: str = "free") -> list[dict]:
    """Search cards by name."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{BASE_URL}/cards",
                params={"search": query},
                headers=_headers()
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.error(f"API error searching cards '{query}': {e}")
        return []

    cards = data if isinstance(data, list) else data.get("data", data.get("cards", []))
    for card in cards:
        card["_cardmarket_price"] = _extract_price(card, "cardmarket")
        card["_tcgplayer_price"] = _extract_price(card, "tcgplayer")
    return cards


async def get_price_history(item_api_id: str, item_type: str = "card", days: int = 30) -> list[dict]:
    """Get price history for an item."""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT * FROM price_history
            WHERE item_api_id=? AND item_type=? AND recorded_at>?
            ORDER BY recorded_at ASC
        """, (item_api_id, item_type, since))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

"""One Piece Card Game API adapter via RapidAPI with PostgreSQL caching."""
import asyncio
import json
import os
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx

from db.init import get_pool


# ─── In-memory TTL Cache ────────────────────────────────────────────────────

class TTLCache:
    """Simple in-memory cache with per-key TTL. Thread-safe for async use."""

    def __init__(self, ttl_seconds: int = 86400):
        self._store: dict[str, tuple[float, Any]] = {}
        self._ttl = ttl_seconds

    def get(self, key: str) -> Optional[Any]:
        if key not in self._store:
            return None
        expires, value = self._store[key]
        if time.time() > expires:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any):
        self._store[key] = (time.time() + self._ttl, value)

    def clear(self):
        self._store.clear()

    def __len__(self):
        return len(self._store)


# Global in-memory caches (complement the DB cache)
_cards_memory_cache = TTLCache(ttl_seconds=3600)      # 1 hour
_products_memory_cache = TTLCache(ttl_seconds=86400)   # 24 hours (sealed rarely changes)
_sets_memory_cache = TTLCache(ttl_seconds=3600)        # 1 hour

logger = logging.getLogger(__name__)

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "one-piece-tcg-prices.p.rapidapi.com")
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


# RapidAPI set ID → set code mapping (complete, all 49 sets)
_RAPIDAPI_ID_TO_CODE: dict[str, str] = {
    # Booster sets
    "368": "OP01", "369": "OP02", "370": "OP03", "371": "OP04",
    "372": "OP05", "373": "OP06", "391": "OP07", "386": "OP08",
    "366": "OP09", "364": "OP10", "362": "OP11", "361": "OP12",
    "350": "OP13", "348": "OP14", "404": "OP15",
    # Extra / Premium
    "390": "EB01", "359": "EB02", "394": "EB03",
    "367": "PRB01", "351": "PRB02",
    # Starter decks
    "357": "ST01", "358": "ST02", "374": "ST03", "375": "ST04",
    "395": "ST05", "376": "ST06", "377": "ST07", "393": "ST08",
    "378": "ST09", "379": "ST10", "392": "ST11", "380": "ST12",
    "388": "ST13", "387": "ST14", "385": "ST15", "384": "ST16",
    "383": "ST17", "382": "ST18", "389": "ST19", "381": "ST20",
    "365": "ST21", "360": "ST22", "353": "ST23", "352": "ST24",
    "354": "ST25", "355": "ST26", "356": "ST27", "363": "ST28",
    "349": "ST29",
}


def _set_code_from_id(set_id: str) -> Optional[str]:
    """Resolve a RapidAPI set ID to a set code."""
    return _RAPIDAPI_ID_TO_CODE.get(str(set_id))


def _cents_to_eur(v, card_number: Optional[str] = None, set_code: Optional[str] = None) -> Optional[float]:
    """Normalize RapidAPI price to EUR.

    Values >= 100 are Eurocent (÷100), values < 100 are EUR.
    No OPTCG card averages > €100, so this heuristic is reliable.
    """
    if v is None:
        return None
    try:
        f = float(v)
        if f <= 0:
            return None
        if f >= 100:
            return round(f / 100.0, 2)
        return round(f, 2)
    except (ValueError, TypeError):
        return None


def _extract_price(item: dict, source: str, set_code: Optional[str] = None) -> Optional[float]:
    """Extract price from API response item for a given source.

    Uses card_number prefix to determine cents vs EUR conversion.
    """
    prices = item.get("prices", {}) or {}
    if not isinstance(prices, dict):
        return None

    card_num = str(item.get("card_number") or "").upper()

    if source == "cardmarket":
        cm = prices.get("cardmarket", {})
        if isinstance(cm, dict):
            for key in ["7d_average", "30d_average", "lowest_near_mint", "lowest"]:
                v = cm.get(key)
                if v is not None:
                    return _cents_to_eur(v, card_num, set_code)
        if isinstance(cm, (int, float)):
            return _cents_to_eur(cm, card_num, set_code)

    elif source == "tcgplayer":
        for k in ["tcg_player", "tcgplayer", "tcgPlayer"]:
            tcg = prices.get(k, {})
            if isinstance(tcg, dict):
                v = tcg.get("market_price")
                if v is not None:
                    return _cents_to_eur(v, card_num, set_code)
            elif isinstance(tcg, (int, float)):
                return _cents_to_eur(tcg, card_num, set_code)

    return None


def _classify_region(cm_price: Optional[float], tcp_price: Optional[float]) -> str:
    """Classify card region based on available marketplace price data.

    Cardmarket = primarily JP/EU market.
    TCGPlayer  = primarily US/EN market.

    Returns:
        "BOTH" — prices on both markets
        "JP"   — Cardmarket price only
        "EN"   — TCGPlayer price only
        "BOTH" — no data (default fallback)
    """
    has_cm = cm_price is not None and cm_price > 0
    has_tcp = tcp_price is not None and tcp_price > 0
    if has_cm and has_tcp:
        return "BOTH"
    if has_cm:
        return "JP"
    if has_tcp:
        return "EN"
    return "BOTH"  # Default


def _card_from_cache(row) -> dict:
    """Build a card dict from a cache row, attaching price and region fields."""
    item = json.loads(row["card_data_json"])
    item["_cardmarket_price"] = row["cardmarket_price"]
    item["_tcgplayer_price"] = row["tcgplayer_price"]
    item["_region"] = _classify_region(row["cardmarket_price"], row["tcgplayer_price"])
    return item


async def get_sets(tier: str = "free") -> list[dict]:
    """Fetch all sets/episodes from API or cache."""
    cache_key = f"sets:{tier}"
    mem = _sets_memory_cache.get(cache_key)
    if mem is not None:
        return mem

    threshold = _cache_age_threshold(tier)
    pool = await get_pool()
    async with pool.acquire() as conn:
        cached = await conn.fetch(
            "SELECT * FROM sets WHERE created_at > $1 ORDER BY release_date DESC",
            threshold
        )
        if cached:
            result = [dict(row) for row in cached]
            _sets_memory_cache.set(cache_key, result)
            return result

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
        async with pool.acquire() as conn:
            stale = await conn.fetch("SELECT * FROM sets ORDER BY release_date DESC")
            return [dict(row) for row in stale]

    async with pool.acquire() as conn:
        for ep in episodes:
            api_id = str(ep.get("id", ep.get("_id", "")))
            name = ep.get("name", ep.get("title", "Unknown"))
            code = ep.get("code", ep.get("set_code", ""))
            release_date = ep.get("released_at", ep.get("release_date", ep.get("releaseDate", "")))
            card_count = ep.get("cards_total", ep.get("card_count", ep.get("cardCount", 0)))
            # Detect language from name/code — JP if name contains JP indicators
            lang = _detect_language(name, code, ep)

            await conn.execute("""
                INSERT INTO sets (api_id, name, code, release_date, card_count, language, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT(api_id) DO UPDATE SET
                    name=EXCLUDED.name, code=EXCLUDED.code,
                    release_date=EXCLUDED.release_date, card_count=EXCLUDED.card_count,
                    language=EXCLUDED.language, created_at=NOW()
            """, api_id, name, code, release_date, card_count, lang)

        rows = await conn.fetch("SELECT * FROM sets ORDER BY release_date DESC")
        result = [dict(row) for row in rows]
        _sets_memory_cache.set(cache_key, result)
        return result


def _detect_language(name: str, code: str, ep: dict) -> str:
    """The API does not distinguish JP vs EN — all sets are mixed. Return ALL."""
    return "ALL"


async def get_cards(set_id: str, tier: str = "free") -> list[dict]:
    """Fetch ALL cards for a set from API or cache, with full pagination."""
    # Check in-memory cache first
    mem_key = f"cards:{set_id}:{tier}"
    mem = _cards_memory_cache.get(mem_key)
    if mem is not None:
        return mem

    set_code = _set_code_from_id(set_id)
    threshold = _cache_age_threshold(tier)
    pool = await get_pool()
    async with pool.acquire() as conn:
        cached = await conn.fetch(
            "SELECT * FROM cards_cache WHERE set_api_id=$1 AND last_updated>$2",
            set_id, threshold
        )
        if cached:
            result = []
            for row in cached:
                item = json.loads(row["card_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                item["_region"] = _classify_region(row["cardmarket_price"], row["tcgplayer_price"])
                result.append(item)
            _cards_memory_cache.set(mem_key, result)
            return result

    # Fetch ALL pages from API
    all_cards = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            page = 1
            while True:
                resp = await client.get(
                    f"{BASE_URL}/episodes/{set_id}/cards",
                    params={"page": page, "sort": "price_highest"},
                    headers=_headers()
                )
                resp.raise_for_status()
                data = resp.json()
                page_items = data.get("data", []) if isinstance(data, dict) else data
                all_cards.extend(page_items)

                # Check pagination
                paging = data.get("paging", {}) if isinstance(data, dict) else {}
                total_pages = paging.get("total", 1)
                if page >= total_pages:
                    break
                page += 1
                await asyncio.sleep(0.1)  # Small delay to be nice to API
    except Exception as e:
        logger.error(f"API error fetching cards for set {set_id}: {e}")
        # Return stale cache
        async with pool.acquire() as conn:
            stale = await conn.fetch("SELECT * FROM cards_cache WHERE set_api_id=$1", set_id)
            result = [_card_from_cache(row) for row in stale]
            if result:
                _cards_memory_cache.set(mem_key, result)
            return result

    # Store in cache
    async with pool.acquire() as conn:
        for card in all_cards:
            card_api_id = str(card.get("id", card.get("_id", card.get("code", ""))))
            cm_price = _extract_price(card, "cardmarket", set_code)
            tcp_price = _extract_price(card, "tcgplayer", set_code)

            await conn.execute("""
                INSERT INTO cards_cache (set_api_id, card_api_id, card_data_json, cardmarket_price, tcgplayer_price, last_updated)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT(set_api_id, card_api_id) DO UPDATE SET
                    card_data_json=EXCLUDED.card_data_json,
                    cardmarket_price=EXCLUDED.cardmarket_price,
                    tcgplayer_price=EXCLUDED.tcgplayer_price,
                    last_updated=NOW()
            """, set_id, card_api_id, json.dumps(card), cm_price, tcp_price)

            # Record price history
            if cm_price is not None or tcp_price is not None:
                await conn.execute("""
                    INSERT INTO price_history (item_type, item_api_id, cardmarket_price, tcgplayer_price, recorded_at)
                    VALUES ('card', $1, $2, $3, NOW())
                """, card_api_id, cm_price, tcp_price)

    # Return with extracted prices and region
    result = []
    for card in all_cards:
        cm_price = _extract_price(card, "cardmarket", set_code)
        tcp_price = _extract_price(card, "tcgplayer", set_code)
        card["_cardmarket_price"] = cm_price
        card["_tcgplayer_price"] = tcp_price
        card["_region"] = _classify_region(cm_price, tcp_price)
        result.append(card)
    _cards_memory_cache.set(mem_key, result)
    return result


async def get_products(set_id: str, tier: str = "free") -> list[dict]:
    """Fetch ALL sealed products for a set from API or cache, with full pagination."""
    # Check in-memory cache first
    mem_key = f"products:{set_id}:{tier}"
    mem = _products_memory_cache.get(mem_key)
    if mem is not None:
        return mem

    set_code = _set_code_from_id(set_id)
    threshold = _cache_age_threshold(tier)
    pool = await get_pool()
    async with pool.acquire() as conn:
        cached = await conn.fetch(
            "SELECT * FROM products_cache WHERE set_api_id=$1 AND last_updated>$2",
            set_id, threshold
        )
        if cached:
            result = []
            for row in cached:
                item = json.loads(row["product_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                result.append(item)
            _products_memory_cache.set(mem_key, result)
            return result

    # Fetch ALL pages from API
    all_products = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            page = 1
            while True:
                resp = await client.get(
                    f"{BASE_URL}/episodes/{set_id}/products",
                    params={"page": page, "sort": "price_highest"},
                    headers=_headers()
                )
                resp.raise_for_status()
                data = resp.json()
                page_items = data.get("data", []) if isinstance(data, dict) else data
                all_products.extend(page_items)

                # Check pagination
                paging = data.get("paging", {}) if isinstance(data, dict) else {}
                total_pages = paging.get("total", 1)
                if page >= total_pages:
                    break
                page += 1
                await asyncio.sleep(0.1)  # Small delay to be nice to API
    except Exception as e:
        logger.error(f"API error fetching products for set {set_id}: {e}")
        # Return stale cache
        async with pool.acquire() as conn:
            stale = await conn.fetch(
                "SELECT * FROM products_cache WHERE set_api_id=$1", set_id
            )
            result = []
            for row in stale:
                item = json.loads(row["product_data_json"])
                item["_cardmarket_price"] = row["cardmarket_price"]
                item["_tcgplayer_price"] = row["tcgplayer_price"]
                result.append(item)
            if result:
                _products_memory_cache.set(mem_key, result)
            return result

    # Store in cache
    async with pool.acquire() as conn:
        for product in all_products:
            prod_api_id = str(product.get("id", product.get("_id", product.get("code", ""))))
            cm_price = _extract_price(product, "cardmarket", set_code)
            tcp_price = _extract_price(product, "tcgplayer", set_code)

            await conn.execute("""
                INSERT INTO products_cache (set_api_id, product_api_id, product_data_json, cardmarket_price, tcgplayer_price, last_updated)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT(set_api_id, product_api_id) DO UPDATE SET
                    product_data_json=EXCLUDED.product_data_json,
                    cardmarket_price=EXCLUDED.cardmarket_price,
                    tcgplayer_price=EXCLUDED.tcgplayer_price,
                    last_updated=NOW()
            """, set_id, prod_api_id, json.dumps(product), cm_price, tcp_price)

            if cm_price is not None or tcp_price is not None:
                await conn.execute("""
                    INSERT INTO price_history (item_type, item_api_id, cardmarket_price, tcgplayer_price, recorded_at)
                    VALUES ('product', $1, $2, $3, NOW())
                """, prod_api_id, cm_price, tcp_price)

    result = []
    for product in all_products:
        cm_price = _extract_price(product, "cardmarket", set_code)
        tcp_price = _extract_price(product, "tcgplayer", set_code)
        product["_cardmarket_price"] = cm_price
        product["_tcgplayer_price"] = tcp_price
        result.append(product)
    _products_memory_cache.set(mem_key, result)
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
        card_num = str(card.get("card_number") or "")
        sc = card_num.split("-")[0].upper() if "-" in card_num else None
        card["_cardmarket_price"] = _extract_price(card, "cardmarket", sc)
        card["_tcgplayer_price"] = _extract_price(card, "tcgplayer", sc)
    return cards


async def get_price_history(item_api_id: str, item_type: str = "card", days: int = 30) -> list[dict]:
    """Get price history for an item."""
    since = datetime.utcnow() - timedelta(days=days)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM price_history
            WHERE item_api_id=$1 AND item_type=$2 AND recorded_at>$3
            ORDER BY recorded_at ASC
        """, item_api_id, item_type, since)
        return [dict(row) for row in rows]

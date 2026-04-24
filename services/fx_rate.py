"""Live USD/EUR FX rate service.

Fetches the daily closing rate from Frankfurter.dev (free, no API key,
ECB-sourced). Caches in memory with a 24h TTL. Falls back to a
conservative baseline if the upstream is unreachable.

Usage:
    from services.fx_rate import get_usd_to_eur, get_eur_to_usd
    eur_price = usd_price * get_usd_to_eur()

The module-level USD_TO_EUR constant in card_aggregator.py is kept as
a last-resort fallback but should NOT be used directly — use this
service instead so price conversions track actual market rates.
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Tuple

import httpx

logger = logging.getLogger(__name__)

# Fallback if upstream is unreachable (conservative: slightly below current
# rate so we under-state EUR values rather than over-state them).
_FALLBACK_USD_TO_EUR = 0.93

# Cache: (rate_usd_to_eur, fetched_at_unix_seconds)
_cache: Tuple[float, float] = (_FALLBACK_USD_TO_EUR, 0.0)
_TTL_SECONDS = 24 * 3600  # 24h
_lock = asyncio.Lock()


async def _fetch_rate() -> float:
    """Fetch latest USD->EUR rate from Frankfurter.dev (ECB-sourced)."""
    url = "https://api.frankfurter.dev/v1/latest?base=USD&symbols=EUR"
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        rate = float(data["rates"]["EUR"])
        if not 0.7 < rate < 1.3:
            # Sanity check — should always be roughly 0.85-0.95 lately
            raise ValueError(f"Implausible rate: {rate}")
        return rate


async def refresh() -> float:
    """Force-refresh the cache. Returns the new rate."""
    global _cache
    async with _lock:
        try:
            rate = await _fetch_rate()
            _cache = (rate, time.time())
            logger.info(f"FX rate refreshed: 1 USD = {rate:.4f} EUR")
            return rate
        except Exception as e:
            logger.warning(f"FX rate refresh failed: {e}. Keeping old value: {_cache[0]:.4f}")
            return _cache[0]


def _is_stale() -> bool:
    return (time.time() - _cache[1]) > _TTL_SECONDS


async def get_usd_to_eur_async() -> float:
    """Async version: refreshes cache if stale."""
    if _is_stale():
        await refresh()
    return _cache[0]


def get_usd_to_eur() -> float:
    """Sync accessor: returns cached rate (never triggers network call).

    Safe to call from anywhere, including SQL query string builders.
    The cache is populated on startup and refreshed by a background task.
    """
    return _cache[0]


def get_eur_to_usd() -> float:
    """Inverse rate for the rare case we need USD from EUR."""
    return 1.0 / _cache[0] if _cache[0] > 0 else 1.08

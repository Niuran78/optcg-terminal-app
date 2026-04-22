"""Seed `daily_price_snapshots` with synthetic 90-day history.

We have current snapshot + eu_cardmarket_30d_avg. We interpolate a plausible
price path backwards so indicators have data to work with TODAY. Once the
daily cron runs, real snapshots will gradually replace synthetic data.

This is a one-time operation. Safe to re-run (idempotent via UNIQUE constraint).
"""
from __future__ import annotations
import asyncio
import logging
import random
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


def _synth_path(
    start_price: float,
    end_price: float,
    days: int,
    volatility: float = 0.025,
    seed_key: str = "",
) -> list[float]:
    """Build a plausible price path using geometric random walk with drift.

    - Drift from start_price to end_price over `days` steps
    - Daily noise ≈ volatility (2.5% = typical card vol)
    - Deterministic per seed_key so same card always gets same curve
    """
    rng = random.Random(seed_key)
    path: list[float] = []
    price = start_price
    for i in range(days):
        t = i / max(days - 1, 1)
        # Log-linear drift
        if start_price > 0 and end_price > 0:
            target = start_price * (end_price / start_price) ** t
        else:
            target = end_price
        # Noise (mean-reverts softly toward target)
        noise = rng.gauss(0, volatility)
        price = target * (1 + noise)
        # Clamp to positive
        price = max(price, 0.01)
        path.append(round(price, 2))
    return path


async def seed_synthetic_history(days: int = 90, missing_only: bool = False) -> dict:
    """Create `days` days of synthetic snapshots per card via a single UNNEST bulk INSERT.

    Much faster than row-by-row: 2000 cards × 90 days in ~5s total.

    Args:
        days: how many days of history to seed per card
        missing_only: if True, only seed cards that have fewer than `days/2`
                      existing snapshot rows. Use this for incremental top-ups
                      after new cards are added by the CSV sync.
    """
    from db.init import get_pool

    pool = await get_pool()
    today = date.today()

    # Build flat arrays for one big INSERT
    card_ids: list[int] = []
    snap_dates: list[date] = []
    en_prices: list[Optional[float]] = []
    eu_prices: list[Optional[float]] = []
    eu_lowest: list[Optional[float]] = []

    async with pool.acquire() as conn:
        # Include JP-only cards too: if pc_price_usd is set but en/eu columns
        # are NULL, derive a JP-EUR baseline from pc_price_usd * 0.92.
        query = """
            SELECT cu.id, cu.card_id, cu.set_code, cu.language,
                   cu.eu_cardmarket_7d_avg, cu.eu_cardmarket_30d_avg, cu.eu_cardmarket_lowest,
                   cu.en_tcgplayer_market, cu.en_tcgplayer_low,
                   cu.pc_price_usd,
                   COALESCE(cu.eu_cardmarket_7d_avg, cu.pc_price_usd * 0.92) AS eu_current
            FROM cards_unified cu
            WHERE (cu.eu_cardmarket_7d_avg IS NOT NULL
                OR cu.en_tcgplayer_market IS NOT NULL
                OR cu.pc_price_usd IS NOT NULL)
        """
        if missing_only:
            query += f"""
              AND (
                SELECT COUNT(*) FROM daily_price_snapshots d
                WHERE d.card_unified_id = cu.id
              ) < {days // 2}
            """
        cards = await conn.fetch(query)

        for c in cards:
            cur = c["eu_current"]
            if cur is None or cur <= 0:
                continue
            avg30 = c["eu_cardmarket_30d_avg"] or cur
            low = c["eu_cardmarket_lowest"] or cur * 0.85
            drift_7_30 = (cur - avg30) / 30.0
            start_90 = max(avg30 - drift_7_30 * 60, low * 0.9, cur * 0.5)

            seed_key = f"{c['card_id']}-{c['set_code']}-{c['language'] or ''}"
            path = _synth_path(start_90, cur, days, volatility=0.028, seed_key=seed_key)

            en_cur = c["en_tcgplayer_market"]
            if en_cur:
                en_start = en_cur * (start_90 / cur) if cur else en_cur
                en_path = _synth_path(en_start, en_cur, days, volatility=0.03, seed_key=seed_key + "-en")
            else:
                en_path = [None] * days

            # For JP rows, store the JP-derived path under eu_cardmarket_7d_avg
            # so the chart modal (which reads that column) can render it.
            has_eu_real = c["eu_cardmarket_7d_avg"] is not None

            for i in range(days):
                snap_date = today - timedelta(days=days - 1 - i)
                card_ids.append(c["id"])
                snap_dates.append(snap_date)
                en_prices.append(en_path[i])
                eu_prices.append(path[i])
                eu_lowest.append(round(path[i] * 0.85, 2) if has_eu_real else None)

        total = len(card_ids)
        logger.info(f"Synthetic history: prepared {total} rows, bulk inserting...")

        # Single UNNEST-based INSERT with ON CONFLICT DO NOTHING
        inserted = await conn.fetchval(
            """
            WITH input AS (
                SELECT * FROM UNNEST($1::int[], $2::date[], $3::real[], $4::real[], $5::real[])
                AS t(card_unified_id, snap_date, en_tcgplayer_market, eu_cardmarket_7d_avg, eu_cardmarket_lowest)
            ),
            inserted AS (
                INSERT INTO daily_price_snapshots (
                    card_unified_id, snap_date,
                    en_tcgplayer_market, eu_cardmarket_7d_avg,
                    eu_cardmarket_30d_avg, eu_cardmarket_lowest
                )
                SELECT card_unified_id, snap_date, en_tcgplayer_market,
                       eu_cardmarket_7d_avg, eu_cardmarket_7d_avg, eu_cardmarket_lowest
                FROM input
                ON CONFLICT (card_unified_id, snap_date) DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*) FROM inserted
            """,
            card_ids, snap_dates, en_prices, eu_prices, eu_lowest,
        )

    logger.info(f"Synthetic history seed: {inserted} rows inserted, {total - (inserted or 0)} skipped (already present)")
    return {"inserted": inserted or 0, "skipped": total - (inserted or 0), "days": days}


async def daily_snapshot_from_current() -> dict:
    """Write one snapshot row per card from the CURRENT price state.

    Runs daily via cron. Idempotent on (card_unified_id, snap_date).

    Also handles JP-only cards: when language='JP' and eu_cardmarket_7d_avg
    is NULL, we derive the EUR baseline from pc_price_usd * 0.92 so JP charts
    have data.
    """
    from db.init import get_pool

    pool = await get_pool()
    today = date.today()

    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO daily_price_snapshots (
                card_unified_id, snap_date,
                en_tcgplayer_market, en_tcgplayer_low,
                eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest
            )
            SELECT id, $1,
                   en_tcgplayer_market, en_tcgplayer_low,
                   COALESCE(eu_cardmarket_7d_avg, pc_price_usd * 0.92),
                   COALESCE(eu_cardmarket_30d_avg, pc_price_usd * 0.92),
                   COALESCE(eu_cardmarket_lowest, pc_price_usd * 0.92 * 0.85)
            FROM cards_unified
            WHERE eu_cardmarket_7d_avg IS NOT NULL
               OR en_tcgplayer_market IS NOT NULL
               OR pc_price_usd IS NOT NULL
            ON CONFLICT (card_unified_id, snap_date) DO UPDATE SET
                en_tcgplayer_market = EXCLUDED.en_tcgplayer_market,
                en_tcgplayer_low = EXCLUDED.en_tcgplayer_low,
                eu_cardmarket_7d_avg = EXCLUDED.eu_cardmarket_7d_avg,
                eu_cardmarket_30d_avg = EXCLUDED.eu_cardmarket_30d_avg,
                eu_cardmarket_lowest = EXCLUDED.eu_cardmarket_lowest
            """,
            today,
        )

    logger.info(f"Daily snapshot written for {today}: {result}")
    return {"date": str(today), "result": result}

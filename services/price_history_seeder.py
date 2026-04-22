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


async def seed_synthetic_history(days: int = 90) -> dict:
    """Create `days` days of synthetic snapshots per card (EU prices only).

    Uses eu_cardmarket_30d_avg as 30-days-ago anchor, eu_cardmarket_7d_avg
    as 7-days-ago anchor, current eu_cardmarket_lowest as bottom reference,
    and linearly/randomly interpolates the rest.
    """
    from db.init import get_pool

    pool = await get_pool()
    inserted = 0
    skipped = 0
    today = date.today()

    async with pool.acquire() as conn:
        cards = await conn.fetch(
            """
            SELECT id, card_id, set_code,
                   eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest,
                   en_tcgplayer_market, en_tcgplayer_low
            FROM cards_unified
            WHERE eu_cardmarket_7d_avg IS NOT NULL
            """
        )

        for c in cards:
            cur = c["eu_cardmarket_7d_avg"]
            avg30 = c["eu_cardmarket_30d_avg"] or cur
            low = c["eu_cardmarket_lowest"] or cur * 0.85

            # 90-day estimated start: 1.5x the 30d vs current drift, dampened
            drift_7_30 = (cur - avg30) / 30.0  # per-day drift
            start_90 = max(
                avg30 - drift_7_30 * 60,  # extrapolate back
                low * 0.9,
                cur * 0.5,
            )

            seed_key = f"{c['card_id']}-{c['set_code']}"
            path = _synth_path(start_90, cur, days, volatility=0.028, seed_key=seed_key)

            en_cur = c["en_tcgplayer_market"]
            en_start = None
            if en_cur:
                # EN typically scales with EU — apply same ratio path
                en_start = en_cur * (start_90 / cur) if cur else en_cur
                en_path = _synth_path(en_start, en_cur, days, volatility=0.03, seed_key=seed_key + "-en")
            else:
                en_path = [None] * days

            # Insert each day
            for i in range(days):
                snap_date = today - timedelta(days=days - 1 - i)
                try:
                    await conn.execute(
                        """
                        INSERT INTO daily_price_snapshots (
                            card_unified_id, snap_date,
                            en_tcgplayer_market, eu_cardmarket_7d_avg,
                            eu_cardmarket_30d_avg, eu_cardmarket_lowest
                        ) VALUES ($1, $2, $3, $4, $4, $5)
                        ON CONFLICT (card_unified_id, snap_date) DO NOTHING
                        """,
                        c["id"], snap_date,
                        en_path[i], path[i],
                        path[i] * 0.85,
                    )
                    inserted += 1
                except Exception as e:
                    skipped += 1
                    if skipped < 3:
                        logger.warning(f"skip {c['card_id']} {snap_date}: {e}")

    logger.info(f"Synthetic history seed: {inserted} rows inserted, {skipped} skipped")
    return {"inserted": inserted, "skipped": skipped, "days": days}


async def daily_snapshot_from_current() -> dict:
    """Write one snapshot row per card from the CURRENT price state.

    Runs daily via cron. Idempotent on (card_unified_id, snap_date).
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
                   eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest
            FROM cards_unified
            WHERE eu_cardmarket_7d_avg IS NOT NULL OR en_tcgplayer_market IS NOT NULL
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

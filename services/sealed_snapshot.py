"""Sealed price snapshot writer.

Persists today's live Cardmarket data (already in sealed_unified.cm_live_*)
into the historical sealed_price_snapshots table so the public widget can
render a 30-day mini-sparkline.

Why a separate function?
  The Cardmarket scraper itself runs on a separate VPS that updates
  sealed_unified.cm_live_* directly via the database. We don't own that
  scraper here, so the cleanest hook is to copy the freshly-scraped values
  into a daily snapshot row from inside the Terminal's daily-cron loop.

Idempotent: the (sealed_id, snap_date) UNIQUE constraint means re-runs
during a single day are no-ops via ON CONFLICT DO UPDATE.
"""
import logging
from db.init import get_pool

logger = logging.getLogger(__name__)


async def backfill_sealed_snapshots_today() -> dict:
    """Write today's snapshot for every sealed_unified row that has live data.

    Uses (CURRENT_DATE) at the database to keep timezones consistent with
    the rest of the daily cron. Returns counts for telemetry.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO sealed_price_snapshots
                (sealed_id, snap_date, cm_live_trend, cm_live_lowest, cm_live_available)
            SELECT id, CURRENT_DATE, cm_live_trend, cm_live_lowest, cm_live_available
            FROM sealed_unified
            WHERE cm_live_trend IS NOT NULL
            ON CONFLICT (sealed_id, snap_date) DO UPDATE SET
                cm_live_trend     = EXCLUDED.cm_live_trend,
                cm_live_lowest    = EXCLUDED.cm_live_lowest,
                cm_live_available = EXCLUDED.cm_live_available
            """
        )

        # `INSERT ... 0 N` from asyncpg — pull the row count.
        try:
            written = int(result.split()[-1])
        except Exception:
            written = -1

        total = await conn.fetchval(
            "SELECT COUNT(*) FROM sealed_price_snapshots WHERE snap_date = CURRENT_DATE"
        )

    logger.info(
        f"[sealed_snapshot] backfill_today: wrote/updated {written} rows, "
        f"{total} total snapshots for today"
    )
    return {"written": written, "today_total": total}


async def get_history_for_widget(
    sealed_id: int,
    days: int = 30,
) -> list[dict]:
    """Return up to `days` recent snapshots for the public widget.

    Output order: oldest first (left-to-right rendering for the SVG sparkline).
    Returns empty list if no snapshots exist (caller should hide the chart).
    """
    if days <= 0:
        return []
    days = min(days, 90)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT snap_date, cm_live_trend, cm_live_lowest
            FROM sealed_price_snapshots
            WHERE sealed_id = $1
              AND snap_date >= CURRENT_DATE - ($2::int * INTERVAL '1 day')
            ORDER BY snap_date ASC
            """,
            sealed_id, days,
        )
    return [
        {
            "date":       r["snap_date"].isoformat(),
            "trend_eur":  float(r["cm_live_trend"]) if r["cm_live_trend"] is not None else None,
            "lowest_eur": float(r["cm_live_lowest"]) if r["cm_live_lowest"] is not None else None,
        }
        for r in rows
    ]

"""Holygrade Market Radar — daily signal computation (MVP).

Implements 3 signal types from market_radar_architecture.md §6:
  1. price_drop          — global, fanned to all Pro+ users
  2. fv_deviation        — global (uses fair_value_baselines)
  3. portfolio_pnl       — per-user, only if portfolio non-empty

Compute order (idempotent, all use ON CONFLICT):
  1. compute_fair_value_baselines()  — refresh fair_value_baselines table
  2. compute_radar_signals_for_today() — fan-out to per-user rows

Run via:
  await compute_all_radar()

Safe to call multiple times per day; UNIQUE constraint dedupes.
"""
from __future__ import annotations
import json
import logging
from datetime import date, timedelta
from typing import Optional

from db.init import get_pool

logger = logging.getLogger(__name__)


# ─── Tunables ────────────────────────────────────────────────────────────────
PRICE_DROP_THRESHOLD_PCT = 10.0      # % drop over lookback window to fire
PRICE_DROP_LOOKBACK_DAYS = 7
PRICE_DROP_MIN_EUR = 5.0             # ignore tiny cards
FV_DEVIATION_THRESHOLD_PCT = 15.0    # % below FV to fire 'undervalued' signal
FV_LOOKBACK_DAYS = 30
FV_MIN_SAMPLES = 20
PORTFOLIO_PNL_THRESHOLD_PCT = 3.0    # daily |%| change to fire
MAX_SIGNALS_PER_USER_PER_TYPE = 10   # spam guard


# ─── Fair Value baseline ─────────────────────────────────────────────────────
async def compute_fair_value_baselines() -> int:
    """Refresh the fair_value_baselines table.

    fv_eur = trimmed mean (10/90 percentile) of last 30 days of
    eu_cardmarket_7d_avg, with sample_size ≥ 20.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            f"""
            INSERT INTO fair_value_baselines (card_id, fv_eur, fv_method, sample_size, stddev_eur, computed_at)
            SELECT
                cu.card_id,
                ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY dps.eu_cardmarket_7d_avg)::numeric, 2) AS fv_eur,
                'median_30d' AS fv_method,
                COUNT(*) AS sample_size,
                ROUND(STDDEV_SAMP(dps.eu_cardmarket_7d_avg)::numeric, 2) AS stddev_eur,
                NOW()
            FROM cards_unified cu
            JOIN daily_price_snapshots dps ON dps.card_unified_id = cu.id
            WHERE dps.snap_date >= CURRENT_DATE - INTERVAL '{FV_LOOKBACK_DAYS} days'
              AND dps.eu_cardmarket_7d_avg IS NOT NULL
              AND dps.eu_cardmarket_7d_avg > 0
            GROUP BY cu.card_id
            HAVING COUNT(*) >= {FV_MIN_SAMPLES}
            ON CONFLICT (card_id) DO UPDATE SET
                fv_eur = EXCLUDED.fv_eur,
                fv_method = EXCLUDED.fv_method,
                sample_size = EXCLUDED.sample_size,
                stddev_eur = EXCLUDED.stddev_eur,
                computed_at = EXCLUDED.computed_at
            """
        )
        # result is e.g. "INSERT 0 1234"
        try:
            count = int(result.rsplit(" ", 1)[1])
        except Exception:
            count = 0
        logger.info(f"fair_value_baselines refreshed: {count} cards")
        return count


# ─── Signal: Price Drop (global) ─────────────────────────────────────────────
async def _compute_price_drops(today: date) -> list[dict]:
    """Find cards where price dropped >=THRESHOLD% over lookback days.

    Returns list of dicts ready for fan-out to all Pro+ users.
    Global, not per-user.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            WITH today_prices AS (
                SELECT cu.id, cu.card_id, cu.set_code, cu.name,
                       dps.eu_cardmarket_7d_avg AS now_price
                FROM cards_unified cu
                JOIN daily_price_snapshots dps ON dps.card_unified_id = cu.id
                WHERE dps.snap_date = $1
                  AND dps.eu_cardmarket_7d_avg IS NOT NULL
                  AND dps.eu_cardmarket_7d_avg >= {PRICE_DROP_MIN_EUR}
            ),
            past_prices AS (
                SELECT card_unified_id, eu_cardmarket_7d_avg AS past_price
                FROM daily_price_snapshots
                WHERE snap_date = $1 - INTERVAL '{PRICE_DROP_LOOKBACK_DAYS} days'
                  AND eu_cardmarket_7d_avg IS NOT NULL
                  AND eu_cardmarket_7d_avg > 0
            )
            SELECT t.card_id, t.set_code, t.name,
                   t.now_price::float, p.past_price::float,
                   ROUND(((t.now_price - p.past_price) / p.past_price * 100)::numeric, 1)::float AS pct_change
            FROM today_prices t
            JOIN past_prices p ON p.card_unified_id = t.id
            WHERE p.past_price > 0
              AND ((t.now_price - p.past_price) / p.past_price * 100) <= -{PRICE_DROP_THRESHOLD_PCT}
            ORDER BY pct_change ASC
            LIMIT 50
            """,
            today,
        )
    return [dict(r) for r in rows]


# ─── Signal: Fair Value Deviation (global, undervalued only for MVP) ─────────
async def _compute_fv_deviations(today: date) -> list[dict]:
    """Find cards trading >=THRESHOLD% below their 30d fair value."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT cu.card_id, cu.set_code, cu.name,
                   dps.eu_cardmarket_7d_avg::float AS now_price,
                   fvb.fv_eur::float AS fv_eur,
                   ROUND(((dps.eu_cardmarket_7d_avg - fvb.fv_eur) / fvb.fv_eur * 100)::numeric, 1)::float AS pct_dev
            FROM cards_unified cu
            JOIN daily_price_snapshots dps ON dps.card_unified_id = cu.id AND dps.snap_date = $1
            JOIN fair_value_baselines fvb ON fvb.card_id = cu.card_id
            WHERE dps.eu_cardmarket_7d_avg IS NOT NULL
              AND dps.eu_cardmarket_7d_avg > 0
              AND fvb.fv_eur > 0
              AND fvb.fv_eur >= {PRICE_DROP_MIN_EUR}
              AND ((dps.eu_cardmarket_7d_avg - fvb.fv_eur) / fvb.fv_eur * 100) <= -{FV_DEVIATION_THRESHOLD_PCT}
            ORDER BY pct_dev ASC
            LIMIT 50
            """,
            today,
        )
    return [dict(r) for r in rows]


# ─── Signal: Portfolio Daily P&L (per-user) ──────────────────────────────────
async def _compute_portfolio_pnl(today: date) -> list[dict]:
    """For each user with portfolio items, compute total value change today vs yesterday."""
    pool = await get_pool()
    yesterday = today - timedelta(days=1)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH user_today AS (
                SELECT p.user_id,
                       SUM(pi.quantity * COALESCE(dps_t.eu_cardmarket_7d_avg, 0))::float AS value_today,
                       SUM(pi.quantity * COALESCE(dps_y.eu_cardmarket_7d_avg, 0))::float AS value_yesterday,
                       COUNT(*) AS lot_count
                FROM portfolios p
                JOIN portfolio_items pi ON pi.portfolio_id = p.id
                LEFT JOIN daily_price_snapshots dps_t
                       ON dps_t.card_unified_id = pi.card_unified_id AND dps_t.snap_date = $1
                LEFT JOIN daily_price_snapshots dps_y
                       ON dps_y.card_unified_id = pi.card_unified_id AND dps_y.snap_date = $2
                GROUP BY p.user_id
            )
            SELECT user_id, value_today, value_yesterday, lot_count,
                   CASE WHEN value_yesterday > 0
                        THEN ROUND(((value_today - value_yesterday) / value_yesterday * 100)::numeric, 1)::float
                        ELSE 0 END AS pct_change
            FROM user_today
            WHERE value_today > 0 AND value_yesterday > 0
              AND ABS((value_today - value_yesterday) / value_yesterday * 100) >= $3
            """,
            today, yesterday, PORTFOLIO_PNL_THRESHOLD_PCT,
        )
    return [dict(r) for r in rows]


# ─── Fan-out: insert global signals for all eligible users ───────────────────
async def _eligible_user_ids() -> list[int]:
    """All users with tier in (pro, elite). Free tier doesn't get the radar."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM users WHERE tier IN ('pro','elite')")
    return [r["id"] for r in rows]


async def _insert_signal(
    conn,
    user_id: int,
    signal_type: str,
    entity_type: str,
    entity_id: str,
    severity: str,
    payload: dict,
    today: date,
):
    """Insert one radar signal. Idempotent via UNIQUE constraint."""
    await conn.execute(
        """
        INSERT INTO radar_signals
            (user_id, signal_type, entity_type, entity_id, severity, payload, computed_for)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        ON CONFLICT (user_id, signal_type, entity_type, entity_id, computed_for) DO UPDATE SET
            severity = EXCLUDED.severity,
            payload  = EXCLUDED.payload
        """,
        user_id, signal_type, entity_type, entity_id, severity,
        json.dumps(payload), today,
    )


async def compute_radar_signals_for_today() -> dict:
    """Top-level compute: refresh FV baselines, then build all signals."""
    today = date.today()

    fv_count = await compute_fair_value_baselines()

    user_ids = await _eligible_user_ids()
    if not user_ids:
        logger.info("radar: no Pro/Elite users — skipping signal generation")
        return {"users": 0, "fv_baselines": fv_count, "signals_created": 0}

    drops = await _compute_price_drops(today)
    fv_devs = await _compute_fv_deviations(today)
    pnl = await _compute_portfolio_pnl(today)

    pool = await get_pool()
    inserted = 0

    async with pool.acquire() as conn:
        # Cap the global lists so we don't fan out 50 signals × 1000 users.
        top_drops = drops[:MAX_SIGNALS_PER_USER_PER_TYPE]
        top_fv = fv_devs[:MAX_SIGNALS_PER_USER_PER_TYPE]

        for uid in user_ids:
            for d in top_drops:
                severity = "urgent" if d["pct_change"] <= -25 else "opportunity"
                payload = {
                    "card_id": d["card_id"],
                    "set_code": d["set_code"],
                    "card_name": d["name"],
                    "now_price": d["now_price"],
                    "past_price": d["past_price"],
                    "pct_change": d["pct_change"],
                    "lookback_days": PRICE_DROP_LOOKBACK_DAYS,
                    "wording": {
                        "en": f"{d['name']} ({d['card_id']}) is down {abs(d['pct_change']):.1f}% in {PRICE_DROP_LOOKBACK_DAYS}d — €{d['now_price']:.2f}",
                        "de": f"{d['name']} ({d['card_id']}) -{abs(d['pct_change']):.1f}% in {PRICE_DROP_LOOKBACK_DAYS}T — €{d['now_price']:.2f}",
                    },
                }
                await _insert_signal(conn, uid, "price_drop", "card", d["card_id"], severity, payload, today)
                inserted += 1

            for d in top_fv:
                payload = {
                    "card_id": d["card_id"],
                    "set_code": d["set_code"],
                    "card_name": d["name"],
                    "now_price": d["now_price"],
                    "fv_eur": d["fv_eur"],
                    "pct_dev": d["pct_dev"],
                    "wording": {
                        "en": f"{d['name']} trades {abs(d['pct_dev']):.1f}% below 30d fair value (€{d['fv_eur']:.2f})",
                        "de": f"{d['name']} {abs(d['pct_dev']):.1f}% unter 30T-Fair-Value (€{d['fv_eur']:.2f})",
                    },
                }
                await _insert_signal(conn, uid, "fv_deviation", "card", d["card_id"], "opportunity", payload, today)
                inserted += 1

        # Per-user portfolio P&L
        for p in pnl:
            severity = "urgent" if abs(p["pct_change"]) >= 8 else "info"
            direction = "up" if p["pct_change"] > 0 else "down"
            payload = {
                "value_today": p["value_today"],
                "value_yesterday": p["value_yesterday"],
                "pct_change": p["pct_change"],
                "lot_count": p["lot_count"],
                "wording": {
                    "en": f"Your portfolio is {direction} {abs(p['pct_change']):.1f}% today (€{p['value_today']:.2f})",
                    "de": f"Dein Portfolio ist heute {abs(p['pct_change']):.1f}% {'gestiegen' if direction=='up' else 'gefallen'} (€{p['value_today']:.2f})",
                },
            }
            await _insert_signal(conn, p["user_id"], "portfolio_pnl", "portfolio", "self", severity, payload, today)
            inserted += 1

    logger.info(
        f"radar compute: users={len(user_ids)}, drops={len(drops)}, fv={len(fv_devs)}, "
        f"pnl={len(pnl)}, signals_inserted={inserted}"
    )
    return {
        "users": len(user_ids),
        "fv_baselines": fv_count,
        "drops": len(drops),
        "fv_deviations": len(fv_devs),
        "portfolio_pnl": len(pnl),
        "signals_created": inserted,
    }


async def get_signals_for_user(user_id: int, limit: int = 25) -> list[dict]:
    """Fetch today's (and recent) signals for a user, severity-sorted."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, signal_type, entity_type, entity_id, severity,
                   payload, computed_for, created_at, dismissed_at, clicked_at
            FROM radar_signals
            WHERE user_id = $1
              AND dismissed_at IS NULL
              AND computed_for >= CURRENT_DATE - INTERVAL '3 days'
            ORDER BY
                CASE severity WHEN 'urgent' THEN 0 WHEN 'opportunity' THEN 1 ELSE 2 END,
                created_at DESC
            LIMIT $2
            """,
            user_id, limit,
        )
    out = []
    for r in rows:
        d = dict(r)
        # Parse JSONB to dict
        if isinstance(d.get("payload"), str):
            try:
                d["payload"] = json.loads(d["payload"])
            except Exception:
                d["payload"] = {}
        d["computed_for"] = d["computed_for"].isoformat() if d.get("computed_for") else None
        d["created_at"] = d["created_at"].isoformat() if d.get("created_at") else None
        d["dismissed_at"] = d["dismissed_at"].isoformat() if d.get("dismissed_at") else None
        d["clicked_at"] = d["clicked_at"].isoformat() if d.get("clicked_at") else None
        out.append(d)
    return out

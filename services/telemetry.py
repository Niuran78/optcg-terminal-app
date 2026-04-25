"""Lightweight telemetry / event log.

Append-only event store for product-analytics signals: signup,
login, upgrade-modal-open, checkout-start, checkout-success, plus
optional feature usage. No 3rd-party SDK, no network egress, no PII.

Querying patterns (run these in Supabase SQL editor):

  -- DAU last 14 days
  SELECT date_trunc('day', created_at) AS day, COUNT(DISTINCT user_id)
  FROM telemetry_events
  WHERE created_at > NOW() - INTERVAL '14 days'
  GROUP BY 1 ORDER BY 1 DESC;

  -- Conversion funnel
  SELECT event_name, COUNT(DISTINCT user_id) AS users
  FROM telemetry_events
  WHERE event_name IN ('signup','upgrade_modal_open','checkout_start','checkout_success')
    AND created_at > NOW() - INTERVAL '30 days'
  GROUP BY 1
  ORDER BY 2 DESC;
"""
from __future__ import annotations
import logging
from typing import Optional

from db.init import get_pool

logger = logging.getLogger(__name__)


# Whitelist: only these events make it to the DB. Prevents accidental
# event-name proliferation from frontend bugs.
ALLOWED_EVENTS = {
    "signup",
    "login",
    "upgrade_modal_open",
    "checkout_start",
    "checkout_success",
    "checkout_cancel",
    "alert_created",
    "portfolio_added",
    "arbitrage_viewed",
    "card_viewed",
    "welcome_banner_dismissed",
    "radar_opened",
    "radar_signal_clicked",
}


async def emit(
    event_name: str,
    user_id: Optional[int] = None,
    tier: Optional[str] = None,
    properties: Optional[dict] = None,
) -> bool:
    """Record one event. Never raises — telemetry must not break callers."""
    if event_name not in ALLOWED_EVENTS:
        logger.warning(f"telemetry.emit: unknown event '{event_name}' — dropped")
        return False
    try:
        import json
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO telemetry_events (user_id, event_name, tier, properties)
                   VALUES ($1, $2, $3, $4::jsonb)""",
                user_id, event_name, tier,
                json.dumps(properties or {}),
            )
        return True
    except Exception as e:
        logger.warning(f"telemetry.emit failed for {event_name}: {e}")
        return False

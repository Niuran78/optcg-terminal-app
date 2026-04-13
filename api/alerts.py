"""Price Alerts API — set alerts on card price thresholds.

Endpoints:
    GET    /api/alerts           — list user's active alerts with current prices
    POST   /api/alerts           — create a new price alert
    DELETE /api/alerts/{id}      — delete an alert
    GET    /api/alerts/check     — manually trigger alert evaluation (elite only)
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_auth, UserInfo
from services.card_aggregator import USD_TO_EUR

logger = logging.getLogger(__name__)

router = APIRouter(tags=["alerts"])


# ─── Schemas ─────────────────────────────────────────────────────────────────

class CreateAlert(BaseModel):
    card_id: str = Field(..., min_length=1, description="e.g. OP01-120")
    variant: str = Field("Normal")
    target_price: float = Field(..., gt=0)
    direction: str = Field(..., pattern="^(above|below)$")
    currency: str = Field("EUR", pattern="^(USD|EUR)$")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _current_price_eur(row: dict) -> Optional[float]:
    """Best-effort current price in EUR for a card."""
    eu = row.get("eu_cardmarket_7d_avg")
    if eu is not None:
        return float(eu)
    en = row.get("en_tcgplayer_market")
    if en is not None:
        return round(float(en) * USD_TO_EUR, 2)
    return None


# ─── Endpoints ───────────────────────────────────────────────────────────────

# 1. List user's alerts

@router.get("/api/alerts")
async def list_alerts(user: UserInfo = Depends(require_auth)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT pa.id, pa.direction, pa.target_price, pa.price_at_creation,
                      pa.is_active, pa.triggered_at, pa.triggered_price, pa.created_at,
                      c.card_id, c.name, c.set_code, c.rarity, c.variant, c.image_url,
                      c.eu_cardmarket_7d_avg, c.en_tcgplayer_market
               FROM price_alerts pa
               JOIN cards_unified c ON c.id = pa.card_unified_id
               WHERE pa.user_id = $1
               ORDER BY pa.is_active DESC, pa.created_at DESC""",
            user.user_id,
        )

    alerts = []
    for r in rows:
        current = _current_price_eur(dict(r))
        alerts.append({
            "id": r["id"],
            "card_id": r["card_id"],
            "name": r["name"],
            "set_code": r["set_code"],
            "rarity": r["rarity"],
            "variant": r["variant"],
            "image_url": r["image_url"],
            "direction": r["direction"],
            "target_price": r["target_price"],
            "price_at_creation": r["price_at_creation"],
            "current_price_eur": current,
            "is_active": r["is_active"],
            "triggered_at": str(r["triggered_at"]) if r["triggered_at"] else None,
            "triggered_price": r["triggered_price"],
            "created_at": str(r["created_at"]),
        })

    return {"alerts": alerts}


# 2. Create alert

@router.post("/api/alerts", status_code=201)
async def create_alert(body: CreateAlert, user: UserInfo = Depends(require_auth)):
    if not user.can_access("pro"):
        raise HTTPException(
            403,
            detail={
                "error": "PRO_REQUIRED",
                "message": "Price alerts require a Pro (CHF 19/mo) or Elite subscription.",
                "upgrade_url": "/login.html#upgrade",
            },
        )

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Validate card exists
        card = await conn.fetchrow(
            "SELECT id, eu_cardmarket_7d_avg, en_tcgplayer_market FROM cards_unified WHERE card_id = $1 AND variant = $2 LIMIT 1",
            body.card_id.upper(), body.variant,
        )
        if not card:
            card = await conn.fetchrow(
                "SELECT id, eu_cardmarket_7d_avg, en_tcgplayer_market FROM cards_unified WHERE card_id = $1 LIMIT 1",
                body.card_id.upper(),
            )
        if not card:
            raise HTTPException(404, f"Card {body.card_id} not found")

        card_unified_id = card["id"]
        current = _current_price_eur(dict(card))

        # Target price in EUR
        target_eur = body.target_price if body.currency == "EUR" else round(body.target_price * USD_TO_EUR, 2)

        # Pro: max 10 active alerts
        if user.tier == "pro":
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM price_alerts WHERE user_id = $1 AND is_active = TRUE",
                user.user_id,
            )
            if count >= 10:
                raise HTTPException(
                    403,
                    detail={
                        "error": "LIMIT_REACHED",
                        "message": "Pro tier allows 10 active alerts. Upgrade to Elite for unlimited.",
                        "upgrade_url": "/login.html#upgrade",
                    },
                )

        # Determine price field based on what's available
        price_field = "eu_cardmarket_7d_avg" if card["eu_cardmarket_7d_avg"] is not None else "en_tcgplayer_market"

        try:
            row = await conn.fetchrow(
                """INSERT INTO price_alerts
                       (user_id, card_unified_id, price_field, direction,
                        target_price, price_at_creation)
                   VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, created_at""",
                user.user_id, card_unified_id, price_field, body.direction,
                target_eur, current,
            )
        except Exception as e:
            if "unique" in str(e).lower():
                raise HTTPException(409, "You already have an identical alert for this card")
            raise

    return {
        "id": row["id"],
        "created_at": str(row["created_at"]),
        "current_price_eur": current,
    }


# 3. Delete alert

@router.delete("/api/alerts/{alert_id}")
async def delete_alert(alert_id: int, user: UserInfo = Depends(require_auth)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM price_alerts WHERE id = $1 AND user_id = $2",
            alert_id, user.user_id,
        )
        if result == "DELETE 0":
            raise HTTPException(404, "Alert not found")

    return {"ok": True}


# 4. Manual alert check (elite only — no admin role exists)

@router.get("/api/alerts/check")
async def check_alerts_manual(user: UserInfo = Depends(require_auth)):
    if not user.can_access("elite"):
        raise HTTPException(403, "Elite tier required to trigger manual alert check")

    count = await check_alerts_after_update()
    return {"triggered": count}


# ─── Shared check logic (called from seed_all_unified + manual) ─────────────

async def check_alerts_after_update() -> int:
    """Evaluate all active alerts against current card prices.

    For each active alert:
      - direction='below': triggers if current_price <= target_price
      - direction='above': triggers if current_price >= target_price

    Triggered alerts are deactivated (is_active=false) with triggered_at and
    triggered_price set.  Returns count of newly triggered alerts.
    """
    pool = await get_pool()
    triggered = 0

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT pa.id, pa.direction, pa.target_price, pa.price_field,
                      c.eu_cardmarket_7d_avg, c.en_tcgplayer_market,
                      c.card_id, c.name
               FROM price_alerts pa
               JOIN cards_unified c ON c.id = pa.card_unified_id
               WHERE pa.is_active = TRUE"""
        )

        for r in rows:
            current = _current_price_eur(dict(r))
            if current is None:
                continue

            hit = False
            if r["direction"] == "below" and current <= r["target_price"]:
                hit = True
            elif r["direction"] == "above" and current >= r["target_price"]:
                hit = True

            if hit:
                await conn.execute(
                    """UPDATE price_alerts
                       SET is_active = FALSE, triggered_at = NOW(), triggered_price = $1
                       WHERE id = $2""",
                    current, r["id"],
                )
                triggered += 1
                logger.info(
                    f"Alert triggered: {r['card_id']} ({r['name']}) "
                    f"{r['direction']} €{r['target_price']:.2f} — current €{current:.2f}"
                )

    if triggered:
        logger.info(f"check_alerts_after_update: {triggered} alert(s) triggered")
    return triggered

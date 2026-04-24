"""Portfolio Tracker API — manage card portfolios with live P&L.

Endpoints:
    GET    /api/portfolio                 — list user portfolios
    POST   /api/portfolio                 — create portfolio (Pro+)
    GET    /api/portfolio/{id}/items      — list items with live prices
    POST   /api/portfolio/{id}/items      — add card to portfolio
    PUT    /api/portfolio/{id}/items/{iid} — edit item
    DELETE /api/portfolio/{id}/items/{iid} — remove item
    GET    /api/portfolio/{id}/summary    — portfolio P&L summary
    GET    /api/portfolio/{id}/export     — CSV export (Elite)
    GET    /api/cards/search-autocomplete — card search for add-modal
"""
import csv
import io
import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_auth, UserInfo
from services.card_aggregator import USD_TO_EUR

logger = logging.getLogger(__name__)

router = APIRouter(tags=["portfolio"])


# ─── Schemas ─────────────────────────────────────────────────────────────────

class CreatePortfolio(BaseModel):
    name: str = Field("My Portfolio", min_length=1, max_length=100)


class AddItem(BaseModel):
    card_id: str = Field(..., min_length=1, description="e.g. OP01-120")
    variant: str = Field("Normal")
    quantity: int = Field(1, ge=1)
    buy_price: float = Field(..., ge=0)
    buy_currency: str = Field("EUR", pattern="^(USD|EUR)$")
    buy_date: Optional[date] = None
    notes: Optional[str] = Field(None, max_length=500)


class UpdateItem(BaseModel):
    quantity: Optional[int] = Field(None, ge=1)
    buy_price: Optional[float] = Field(None, ge=0)
    notes: Optional[str] = Field(None, max_length=500)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _current_value_eur(card_row: dict, quantity: int) -> Optional[float]:
    """Best-effort current value in EUR for a card.

    Priority order (most accurate first):
      1. cm_live_trend      — scraped LIVE Cardmarket EUR price
      2. eu_cardmarket_7d_avg — reference EU price (PriceCharting → EUR)
      3. en_tcgplayer_market — reference EN price (TCGPlayer USD → EUR)
    """
    live = card_row.get("cm_live_trend")
    if live is not None:
        return round(live * quantity, 2)
    eu = card_row.get("eu_cardmarket_7d_avg")
    if eu is not None:
        return round(eu * quantity, 2)
    en = card_row.get("en_tcgplayer_market")
    if en is not None:
        return round(en * USD_TO_EUR * quantity, 2)
    return None


def _price_source(card_row: dict) -> str:
    """Which price did we use? For transparency in the UI."""
    if card_row.get("cm_live_trend") is not None:
        return "live"
    if card_row.get("eu_cardmarket_7d_avg") is not None:
        return "reference_eu"
    if card_row.get("en_tcgplayer_market") is not None:
        return "reference_en"
    return "none"


def _cost_eur(buy_price: float, buy_currency: str, quantity: int) -> float:
    """Total cost in EUR."""
    unit = buy_price if buy_currency == "EUR" else buy_price * USD_TO_EUR
    return round(unit * quantity, 2)


async def _own_portfolio(conn, portfolio_id: int, user_id: int):
    """Return portfolio row or raise 404/403."""
    row = await conn.fetchrow("SELECT * FROM portfolios WHERE id = $1", portfolio_id)
    if not row:
        raise HTTPException(404, "Portfolio not found")
    if row["user_id"] != user_id:
        raise HTTPException(403, "Not your portfolio")
    return row


# ─── Endpoints ───────────────────────────────────────────────────────────────

# 1. List portfolios

@router.get("/api/portfolio")
async def list_portfolios(user: UserInfo = Depends(require_auth)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT p.*, COUNT(pi.id) AS item_count
               FROM portfolios p
               LEFT JOIN portfolio_items pi ON pi.portfolio_id = p.id
               WHERE p.user_id = $1
               GROUP BY p.id
               ORDER BY p.created_at DESC""",
            user.user_id,
        )
    return {
        "portfolios": [
            {
                "id": r["id"],
                "name": r["name"],
                "description": r["description"],
                "item_count": r["item_count"],
                "created_at": str(r["created_at"]),
            }
            for r in rows
        ]
    }


# 2. Create portfolio

@router.post("/api/portfolio", status_code=201)
async def create_portfolio(body: CreatePortfolio, user: UserInfo = Depends(require_auth)):
    if not user.can_access("pro"):
        raise HTTPException(
            403,
            detail={
                "error": "PRO_REQUIRED",
                "message": "Portfolio creation requires a Pro (CHF 19/mo) or Elite subscription.",
                "upgrade_url": "/?upgrade=pro",
            },
        )

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Pro: max 1 portfolio
        if user.tier == "pro":
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM portfolios WHERE user_id = $1", user.user_id
            )
            if count >= 1:
                raise HTTPException(
                    403,
                    detail={
                        "error": "LIMIT_REACHED",
                        "message": "Pro tier allows 1 portfolio. Upgrade to Elite for unlimited.",
                        "upgrade_url": "/?upgrade=pro",
                    },
                )

        try:
            row = await conn.fetchrow(
                "INSERT INTO portfolios (user_id, name) VALUES ($1, $2) RETURNING *",
                user.user_id, body.name.strip(),
            )
        except Exception as e:
            if "unique" in str(e).lower():
                raise HTTPException(409, "A portfolio with that name already exists")
            raise

    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "created_at": str(row["created_at"]),
    }


# 3. List portfolio items with live prices

@router.get("/api/portfolio/{portfolio_id}/items")
async def list_items(portfolio_id: int, user: UserInfo = Depends(require_auth)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _own_portfolio(conn, portfolio_id, user.user_id)

        rows = await conn.fetch(
            """SELECT pi.id, pi.quantity, pi.buy_price, pi.buy_currency,
                      pi.acquired_at, pi.notes, pi.created_at AS item_created,
                      c.card_id, c.name, c.set_code, c.set_name, c.rarity,
                      c.variant, c.image_url,
                      c.eu_cardmarket_7d_avg, c.en_tcgplayer_market,
                      c.cm_live_trend, c.cm_live_updated_at, c.cm_live_url,
                      c.cm_live_lowest, c.cm_live_available
               FROM portfolio_items pi
               JOIN cards_unified c ON c.id = pi.card_unified_id
               WHERE pi.portfolio_id = $1
               ORDER BY pi.created_at DESC""",
            portfolio_id,
        )

    items = []
    for r in rows:
        cost = _cost_eur(r["buy_price"], r["buy_currency"], r["quantity"])
        cur_val = _current_value_eur(dict(r), r["quantity"])
        pnl = round(cur_val - cost, 2) if cur_val is not None else None
        roi = round((pnl / cost) * 100, 2) if pnl is not None and cost > 0 else None

        items.append({
            "id": r["id"],
            "card_id": r["card_id"],
            "name": r["name"],
            "set_code": r["set_code"],
            "set_name": r["set_name"],
            "rarity": r["rarity"],
            "variant": r["variant"],
            "image_url": r["image_url"],
            "quantity": r["quantity"],
            "buy_price": r["buy_price"],
            "buy_currency": r["buy_currency"],
            "acquired_at": str(r["acquired_at"]) if r["acquired_at"] else None,
            "notes": r["notes"],
            "eu_cardmarket_7d_avg": r["eu_cardmarket_7d_avg"],
            "en_tcgplayer_market": r["en_tcgplayer_market"],
            "cm_live_trend":    r["cm_live_trend"],
            "cm_live_lowest":   r["cm_live_lowest"],
            "cm_live_available": r["cm_live_available"],
            "cm_live_url":      r["cm_live_url"],
            "price_source":     _price_source(dict(r)),
            "cost_eur": cost,
            "current_value_eur": cur_val,
            "pnl_eur": pnl,
            "roi_pct": roi,
        })

    # Sort by roi_pct desc (nulls last)
    items.sort(key=lambda x: x["roi_pct"] if x["roi_pct"] is not None else float("-inf"), reverse=True)

    # Pro: cap at 50 visible items
    limit_note = None
    if user.tier == "pro" and len(items) > 50:
        limit_note = "Pro tier shows 50 items. Upgrade to Elite for unlimited."
        items = items[:50]

    return {"items": items, "total": len(items), "limit_note": limit_note}


# 4. Add card to portfolio

@router.post("/api/portfolio/{portfolio_id}/items", status_code=201)
async def add_item(portfolio_id: int, body: AddItem, user: UserInfo = Depends(require_auth)):
    if not user.can_access("pro"):
        raise HTTPException(
            403,
            detail={
                "error": "PRO_REQUIRED",
                "message": "Adding cards to portfolios requires Pro or Elite.",
                "upgrade_url": "/?upgrade=pro",
            },
        )

    pool = await get_pool()
    async with pool.acquire() as conn:
        await _own_portfolio(conn, portfolio_id, user.user_id)

        # Validate card exists
        card = await conn.fetchrow(
            "SELECT id FROM cards_unified WHERE card_id = $1 AND variant = $2 LIMIT 1",
            body.card_id.upper(), body.variant,
        )
        if not card:
            # Fallback without variant
            card = await conn.fetchrow(
                "SELECT id FROM cards_unified WHERE card_id = $1 LIMIT 1",
                body.card_id.upper(),
            )
        if not card:
            raise HTTPException(404, f"Card {body.card_id} not found in database")

        card_unified_id = card["id"]

        # Pro: max 50 items
        if user.tier == "pro":
            item_count = await conn.fetchval(
                "SELECT COUNT(*) FROM portfolio_items WHERE portfolio_id = $1",
                portfolio_id,
            )
            if item_count >= 50:
                raise HTTPException(
                    403,
                    detail={
                        "error": "LIMIT_REACHED",
                        "message": "Pro tier allows 50 items per portfolio. Upgrade to Elite for unlimited.",
                        "upgrade_url": "/?upgrade=pro",
                    },
                )

        # UPSERT: if same card already in portfolio, update quantity and buy_price
        existing = await conn.fetchrow(
            "SELECT id, quantity FROM portfolio_items WHERE portfolio_id = $1 AND card_unified_id = $2",
            portfolio_id, card_unified_id,
        )

        acquired = body.buy_date or date.today()

        if existing:
            new_qty = existing["quantity"] + body.quantity
            row = await conn.fetchrow(
                """UPDATE portfolio_items
                   SET quantity = $1, buy_price = $2, buy_currency = $3,
                       acquired_at = $4, notes = COALESCE($5, notes)
                   WHERE id = $6 RETURNING id""",
                new_qty, body.buy_price, body.buy_currency,
                acquired, body.notes, existing["id"],
            )
            return {"id": row["id"], "action": "updated", "quantity": new_qty}
        else:
            row = await conn.fetchrow(
                """INSERT INTO portfolio_items
                       (portfolio_id, card_unified_id, quantity, buy_price,
                        buy_currency, acquired_at, notes)
                   VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id""",
                portfolio_id, card_unified_id, body.quantity, body.buy_price,
                body.buy_currency, acquired, body.notes,
            )
            return {"id": row["id"], "action": "created", "quantity": body.quantity}


# 5. Edit item

@router.put("/api/portfolio/{portfolio_id}/items/{item_id}")
async def edit_item(
    portfolio_id: int, item_id: int, body: UpdateItem,
    user: UserInfo = Depends(require_auth),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _own_portfolio(conn, portfolio_id, user.user_id)

        item = await conn.fetchrow(
            "SELECT id FROM portfolio_items WHERE id = $1 AND portfolio_id = $2",
            item_id, portfolio_id,
        )
        if not item:
            raise HTTPException(404, "Item not found")

        sets = []
        vals = []
        idx = 1
        if body.quantity is not None:
            sets.append(f"quantity = ${idx}")
            vals.append(body.quantity)
            idx += 1
        if body.buy_price is not None:
            sets.append(f"buy_price = ${idx}")
            vals.append(body.buy_price)
            idx += 1
        if body.notes is not None:
            sets.append(f"notes = ${idx}")
            vals.append(body.notes)
            idx += 1

        if not sets:
            raise HTTPException(400, "No fields to update")

        vals.append(item_id)
        await conn.execute(
            f"UPDATE portfolio_items SET {', '.join(sets)} WHERE id = ${idx}",
            *vals,
        )

    return {"ok": True}


# 6. Delete item

@router.delete("/api/portfolio/{portfolio_id}/items/{item_id}")
async def delete_item(
    portfolio_id: int, item_id: int,
    user: UserInfo = Depends(require_auth),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _own_portfolio(conn, portfolio_id, user.user_id)

        result = await conn.execute(
            "DELETE FROM portfolio_items WHERE id = $1 AND portfolio_id = $2",
            item_id, portfolio_id,
        )
        if result == "DELETE 0":
            raise HTTPException(404, "Item not found")

    return {"ok": True}


# 7. Portfolio summary

@router.get("/api/portfolio/{portfolio_id}/summary")
async def portfolio_summary(portfolio_id: int, user: UserInfo = Depends(require_auth)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        pf = await _own_portfolio(conn, portfolio_id, user.user_id)

        rows = await conn.fetch(
            """SELECT pi.quantity, pi.buy_price, pi.buy_currency,
                      c.card_id, c.name,
                      c.eu_cardmarket_7d_avg, c.en_tcgplayer_market,
                      c.cm_live_trend
               FROM portfolio_items pi
               JOIN cards_unified c ON c.id = pi.card_unified_id
               WHERE pi.portfolio_id = $1""",
            portfolio_id,
        )

    total_invested = 0.0
    total_current = 0.0
    best = None
    worst = None
    items_with_value = 0

    for r in rows:
        cost = _cost_eur(r["buy_price"], r["buy_currency"], r["quantity"])
        cur_val = _current_value_eur(dict(r), r["quantity"])
        total_invested += cost

        if cur_val is not None:
            total_current += cur_val
            items_with_value += 1
            pnl = cur_val - cost
            roi = (pnl / cost * 100) if cost > 0 else 0

            entry = {"card_id": r["card_id"], "name": r["name"], "roi_pct": round(roi, 2), "pnl_eur": round(pnl, 2)}
            if best is None or roi > best["roi_pct"]:
                best = entry
            if worst is None or roi < worst["roi_pct"]:
                worst = entry

    total_invested = round(total_invested, 2)
    total_current = round(total_current, 2)
    total_pnl = round(total_current - total_invested, 2)
    total_roi = round((total_pnl / total_invested) * 100, 2) if total_invested > 0 else 0

    return {
        "portfolio_id": portfolio_id,
        "name": pf["name"],
        "item_count": len(rows),
        "items_with_value": items_with_value,
        "total_invested_eur": total_invested,
        "current_value_eur": total_current,
        "total_pnl_eur": total_pnl,
        "total_roi_pct": total_roi,
        "best_performer": best,
        "worst_performer": worst,
    }


# 8. CSV export (Elite only)

@router.get("/api/portfolio/{portfolio_id}/export")
async def export_csv(portfolio_id: int, user: UserInfo = Depends(require_auth)):
    if not user.can_access("elite"):
        raise HTTPException(
            403,
            detail={
                "error": "ELITE_REQUIRED",
                "message": "CSV export requires an Elite subscription (CHF 69/mo).",
                "upgrade_url": "/?upgrade=pro",
            },
        )

    pool = await get_pool()
    async with pool.acquire() as conn:
        pf = await _own_portfolio(conn, portfolio_id, user.user_id)

        rows = await conn.fetch(
            """SELECT pi.quantity, pi.buy_price, pi.buy_currency,
                      pi.acquired_at, pi.notes,
                      c.card_id, c.name, c.set_code, c.rarity, c.variant,
                      c.eu_cardmarket_7d_avg, c.en_tcgplayer_market,
                      c.cm_live_trend
               FROM portfolio_items pi
               JOIN cards_unified c ON c.id = pi.card_unified_id
               WHERE pi.portfolio_id = $1
               ORDER BY c.set_code, c.card_id""",
            portfolio_id,
        )

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "card_id", "name", "set_code", "rarity", "variant",
        "quantity", "buy_price", "buy_currency", "acquired_at", "notes",
        "cm_live_trend", "eu_7d_avg", "en_market_usd", "price_source",
        "cost_eur", "current_value_eur", "pnl_eur", "roi_pct",
    ])

    for r in rows:
        cost = _cost_eur(r["buy_price"], r["buy_currency"], r["quantity"])
        cur_val = _current_value_eur(dict(r), r["quantity"])
        pnl = round(cur_val - cost, 2) if cur_val is not None else ""
        roi = round((pnl / cost) * 100, 2) if isinstance(pnl, (int, float)) and cost > 0 else ""

        writer.writerow([
            r["card_id"], r["name"], r["set_code"], r["rarity"], r["variant"],
            r["quantity"], r["buy_price"], r["buy_currency"],
            str(r["acquired_at"]) if r["acquired_at"] else "",
            r["notes"] or "",
            r["cm_live_trend"] or "",
            r["eu_cardmarket_7d_avg"] or "",
            r["en_tcgplayer_market"] or "",
            _price_source(dict(r)),
            cost, cur_val or "", pnl, roi,
        ])

    buf.seek(0)
    filename = f"portfolio_{pf['name'].replace(' ', '_')}_{date.today()}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# 9. Card search autocomplete

@router.get("/api/cards/search-autocomplete")
async def search_autocomplete(
    q: str = Query(..., min_length=1, max_length=100),
    user: UserInfo = Depends(require_auth),
):
    """Search cards_unified by name for add-to-portfolio modal. Login-gated.

    Applies the same trust guard as the browse view:
      - Cards ≥ €50 without a live Cardmarket trend are hidden
        (they'd show fantasy PriceCharting values).
      - Prefers cm_live_trend over the 7d reference when ranking results.
    """
    from services.fx_rate import get_usd_to_eur
    FX = get_usd_to_eur()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""SELECT card_id, name, set_code, rarity, variant, language, image_url,
                      eu_cardmarket_7d_avg, en_tcgplayer_market,
                      cm_live_trend,
                      COALESCE(cm_live_trend, eu_cardmarket_7d_avg,
                               pc_price_usd * {FX}) AS effective_price
               FROM cards_unified
               WHERE name ILIKE $1
                 AND (
                   cm_live_trend IS NOT NULL
                   OR COALESCE(eu_cardmarket_7d_avg, pc_price_usd * {FX}, 0) < 50
                 )
               ORDER BY
                 (cm_live_trend IS NOT NULL) DESC,
                 COALESCE(cm_live_trend, eu_cardmarket_7d_avg,
                          pc_price_usd * {FX}) DESC NULLS LAST
               LIMIT 10""",
            f"%{q}%",
        )

    return {
        "results": [
            {
                "card_id": r["card_id"],
                "name": r["name"],
                "set_code": r["set_code"],
                "rarity": r["rarity"],
                "variant": r["variant"],
                "language": r["language"],
                "image_url": r["image_url"],
                "eu_cardmarket_7d_avg": r["eu_cardmarket_7d_avg"],
                "en_tcgplayer_market": r["en_tcgplayer_market"],
                "cm_live_trend": r["cm_live_trend"],
                "effective_price": float(r["effective_price"]) if r["effective_price"] is not None else None,
                "has_live": r["cm_live_trend"] is not None,
            }
            for r in rows
        ]
    }

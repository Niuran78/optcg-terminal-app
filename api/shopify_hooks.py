"""Shopify webhook + claim endpoints (Phase C — Portfolio Hook).

Flow:
  1. Customer buys a sealed box on holygrade.com.
  2. Shopify fires the `orders/create` webhook to
        POST /api/shopify/purchase-webhook
     We persist one shopify_purchases row per line-item, generate a
     unique `claim_token`, and respond with the claim URLs.
  3. We email the customer a link like
        https://terminal.holygrade.com/claim/{token}
  4. Customer clicks → /claim.html (static) calls
        GET  /api/shopify/claim/{token}        (read details, public)
        POST /api/shopify/claim/{token}        (auth required → bind to user
                                                + insert sealed_portfolio row)
  5. After 30 days, services/email_followup.py mails the customer with
     the current Cardmarket value.

Auth:
  - Webhook: Shopify HMAC verification via SHOPIFY_WEBHOOK_SECRET env var.
    If the env is missing we accept the webhook unsigned (for initial
    setup) and log a warning. Once the secret is set we enforce it.
  - GET claim: public (no auth) — only returns the token-bound details.
  - POST claim: requires logged-in user.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Request, status
from pydantic import BaseModel, Field

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_auth, UserInfo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/shopify", tags=["shopify"])

SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "").strip()
TERMINAL_BASE = os.getenv("TERMINAL_BASE_URL", "https://terminal.holygrade.com").rstrip("/")


# ─── Schemas ─────────────────────────────────────────────────────────────────

class WebhookLineItem(BaseModel):
    set_code: Optional[str] = None
    language: Optional[str] = None
    product_type: Optional[str] = None
    quantity: int = Field(1, ge=1)
    price_eur: float = Field(0.0, ge=0)
    sealed_id: Optional[int] = None  # explicit override


class WebhookPayload(BaseModel):
    order_id: str = Field(..., min_length=1)
    email: Optional[str] = None
    first_name: Optional[str] = None
    line_items: list[WebhookLineItem] = Field(default_factory=list)
    currency: str = Field("EUR")


# ─── HMAC verification (Shopify standard) ────────────────────────────────────

def _verify_shopify_hmac(raw_body: bytes, hmac_header: Optional[str]) -> bool:
    """Verify the X-Shopify-Hmac-Sha256 header against the raw request body.

    Shopify computes HMAC-SHA256 of the raw body using the webhook secret as
    the key, then base64-encodes the digest. We do the same and compare.

    Returns True on match. If the secret is not configured we return True with
    a logged warning — letting the user complete initial setup before locking
    down the webhook.
    """
    if not SHOPIFY_WEBHOOK_SECRET:
        logger.warning(
            "SHOPIFY_WEBHOOK_SECRET not set — accepting webhook without "
            "signature verification. Set the env var in production."
        )
        return True
    if not hmac_header:
        return False
    try:
        digest = hmac.new(
            SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).digest()
        expected = base64.b64encode(digest).decode("utf-8")
        return hmac.compare_digest(expected, hmac_header)
    except Exception as e:
        logger.error(f"HMAC verification crash: {e}")
        return False


# ─── DB helpers ──────────────────────────────────────────────────────────────

_PRODUCT_TYPE_NORMALIZE = {
    "booster_box":     "booster box",
    "booster":         "booster",
    "case":            "case",
    "display":         "display",
    "sleeved_booster": "sleeved booster",
}


def _norm_pt(pt: Optional[str]) -> Optional[str]:
    if not pt:
        return None
    key = pt.lower().strip()
    return _PRODUCT_TYPE_NORMALIZE.get(key, key.replace("_", " "))


async def _resolve_sealed_id(conn, item: WebhookLineItem) -> Optional[int]:
    """Try to map a webhook line-item to a sealed_unified row.

    Strategy:
      1. Explicit sealed_id from the webhook payload (most reliable).
      2. Lookup by (set_code, language, product_type).
    Returns None if no match — we still persist the purchase with set_code
    so the email follow-up can attempt resolution later.
    """
    if item.sealed_id:
        row = await conn.fetchrow(
            "SELECT id FROM sealed_unified WHERE id = $1", item.sealed_id
        )
        if row:
            return int(row["id"])

    if not item.set_code:
        return None

    pt = _norm_pt(item.product_type) or "booster box"
    lang = (item.language or "").upper().strip() or None

    row = await conn.fetchrow(
        """
        SELECT id FROM sealed_unified
        WHERE set_code = $1
          AND product_type = $2
          AND ($3::text IS NULL OR language = $3)
        ORDER BY
            CASE WHEN $3 IS NULL THEN
                CASE language WHEN 'JP' THEN 0 WHEN 'EN' THEN 1 ELSE 2 END
            ELSE 0 END
        LIMIT 1
        """,
        item.set_code.upper().strip(), pt, lang,
    )
    return int(row["id"]) if row else None


def _new_token() -> str:
    """URL-safe random claim token, ~32 chars."""
    return secrets.token_urlsafe(24)


# ─── Endpoints ───────────────────────────────────────────────────────────────

@router.post("/purchase-webhook")
async def purchase_webhook(
    request: Request,
    x_shopify_hmac_sha256: Optional[str] = Header(None, alias="X-Shopify-Hmac-Sha256"),
):
    """Shopify orders/create webhook receiver.

    Idempotent via UNIQUE(order_id, line_index). Accepts either the
    Shopify native shape (line_items with title/sku) or our compact
    relay shape (line_items with set_code/language/product_type) — we
    only require `order_id` and a list of line-items.
    """
    # Read raw body for HMAC verification, then parse.
    raw = await request.body()
    if not _verify_shopify_hmac(raw, x_shopify_hmac_sha256):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "INVALID_HMAC", "message": "Shopify HMAC verification failed."},
        )

    try:
        import json
        data = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:
        raise HTTPException(400, "Invalid JSON body")

    # Normalize the payload — accept Shopify native too.
    order_id = str(data.get("order_id") or data.get("id") or "").strip()
    if not order_id:
        raise HTTPException(400, "missing order_id")

    customer = data.get("customer") or {}
    email = (data.get("email") or customer.get("email") or "").strip().lower() or None
    first = (
        data.get("first_name")
        or customer.get("first_name")
        or (data.get("billing_address") or {}).get("first_name")
        or ""
    ).strip() or None

    items_raw = data.get("line_items") or []
    items: list[WebhookLineItem] = []
    for li in items_raw:
        # Try compact shape first
        set_code = li.get("set_code")
        language = li.get("language")
        product_type = li.get("product_type")
        sealed_id = li.get("sealed_id")
        # Fall back to Shopify-native: parse from sku tag like "OP15-JP-BB" or properties
        if not set_code:
            sku = (li.get("sku") or "").upper()
            # Heuristic only — best to send the structured fields from Shopify Flow.
            parts = sku.split("-")
            if len(parts) >= 2 and parts[0].startswith(("OP", "EB", "ST", "PRB")):
                set_code = parts[0]
                if len(parts) >= 2 and parts[1] in ("JP", "EN"):
                    language = parts[1]

        try:
            qty = int(li.get("quantity") or 1)
        except Exception:
            qty = 1
        try:
            price = float(li.get("price_eur") or li.get("price") or 0.0)
        except Exception:
            price = 0.0

        items.append(WebhookLineItem(
            set_code=set_code,
            language=language,
            product_type=product_type or "booster_box",
            quantity=max(qty, 1),
            price_eur=max(price, 0.0),
            sealed_id=int(sealed_id) if sealed_id else None,
        ))

    if not items:
        # Empty order — log and no-op (still 200 so Shopify doesn't retry forever)
        logger.warning(f"purchase_webhook: order {order_id} had no line items")
        return {"ok": True, "tokens": [], "message": "no line items"}

    pool = await get_pool()
    tokens_out: list[dict] = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            for idx, item in enumerate(items):
                sealed_id = await _resolve_sealed_id(conn, item)
                token = _new_token()

                row = await conn.fetchrow(
                    """
                    INSERT INTO shopify_purchases
                      (order_id, line_index, customer_email, customer_first_name,
                       sealed_id, set_code, language, product_type,
                       quantity, unit_price_eur, currency, claim_token)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (order_id, line_index) DO UPDATE
                      SET customer_email      = COALESCE(EXCLUDED.customer_email, shopify_purchases.customer_email),
                          customer_first_name = COALESCE(EXCLUDED.customer_first_name, shopify_purchases.customer_first_name),
                          sealed_id           = COALESCE(EXCLUDED.sealed_id, shopify_purchases.sealed_id)
                    RETURNING id, claim_token, sealed_id, set_code, language, product_type, quantity, unit_price_eur
                    """,
                    order_id,
                    idx,
                    email,
                    first,
                    sealed_id,
                    (item.set_code or None) and item.set_code.upper(),
                    (item.language or None) and item.language.upper(),
                    _norm_pt(item.product_type) or "booster box",
                    item.quantity,
                    item.price_eur,
                    data.get("currency") or "EUR",
                    token,
                )

                tokens_out.append({
                    "purchase_id":  int(row["id"]),
                    "set_code":     row["set_code"],
                    "language":     row["language"],
                    "product_type": row["product_type"],
                    "quantity":     int(row["quantity"]),
                    "claim_token":  row["claim_token"],
                    "claim_url":    f"{TERMINAL_BASE}/claim.html?t={row['claim_token']}",
                    "matched":      row["sealed_id"] is not None,
                })

    # Telemetry (best-effort)
    try:
        from services.telemetry import emit
        await emit(
            "shopify_purchase_webhook",
            properties={"order_id": order_id, "items": len(tokens_out)},
        )
    except Exception:
        pass

    return {
        "ok":     True,
        "order_id": order_id,
        "tokens": tokens_out,
        # First token's URL as a convenience for single-item orders:
        "claim_url": tokens_out[0]["claim_url"] if tokens_out else None,
    }


@router.get("/claim/{token}")
async def claim_get(token: str):
    """Read claim details by token — public, no auth.

    Returns the data needed to render claim.html (greeting, product, current
    market price). Does NOT bind the claim — that's the POST.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT p.id, p.order_id, p.customer_email, p.customer_first_name,
                   p.sealed_id, p.set_code, p.language, p.product_type,
                   p.quantity, p.unit_price_eur, p.purchased_at,
                   p.user_id, p.claimed_at,
                   s.product_name, s.set_name, s.image_url, s.cm_live_trend, s.cm_live_url
            FROM shopify_purchases p
            LEFT JOIN sealed_unified s ON s.id = p.sealed_id
            WHERE p.claim_token = $1
            """,
            token,
        )
        if not row:
            raise HTTPException(404, "Token not found")

    cm_trend = float(row["cm_live_trend"]) if row["cm_live_trend"] is not None else None
    paid = float(row["unit_price_eur"]) * int(row["quantity"])
    pl = round(cm_trend * int(row["quantity"]) - paid, 2) if cm_trend is not None else None

    return {
        "ok":           True,
        "claimed":      row["claimed_at"] is not None,
        "user_id":      row["user_id"],
        "customer": {
            "email":      row["customer_email"],
            "first_name": row["customer_first_name"],
        },
        "product": {
            "sealed_id":    row["sealed_id"],
            "name":         row["product_name"],
            "set_code":     row["set_code"],
            "set_name":     row["set_name"],
            "language":     row["language"],
            "product_type": row["product_type"],
            "image_url":    row["image_url"],
            "quantity":     int(row["quantity"]),
            "unit_price_eur": float(row["unit_price_eur"]),
            "total_paid_eur": round(paid, 2),
        },
        "market": {
            "cm_trend_eur": cm_trend,
            "cm_url":       row["cm_live_url"],
            "pl_eur":       pl,
        },
        "purchased_at": row["purchased_at"].isoformat() if row["purchased_at"] else None,
    }


@router.post("/claim/{token}")
async def claim_post(token: str, user: UserInfo = Depends(require_auth)):
    """Bind a Shopify purchase to the logged-in user and create a free
    sealed_portfolio row. Idempotent.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT id, sealed_id, quantity, unit_price_eur, purchased_at,
                       user_id, claimed_at
                FROM shopify_purchases
                WHERE claim_token = $1
                FOR UPDATE
                """,
                token,
            )
            if not row:
                raise HTTPException(404, "Token not found")

            if row["user_id"] is not None and row["user_id"] != user.user_id:
                raise HTTPException(
                    409,
                    {"error": "ALREADY_CLAIMED",
                     "message": "This purchase is already linked to another account."},
                )

            # Set user_id + claimed_at if not already done.
            if row["user_id"] is None:
                await conn.execute(
                    """
                    UPDATE shopify_purchases
                       SET user_id = $1, claimed_at = NOW()
                     WHERE id = $2
                    """,
                    user.user_id, int(row["id"]),
                )

            # Create the free sealed_portfolio row, but only if a sealed_id
            # was matched. If not, we still bind the purchase to the user
            # (so the follow-up email goes to them) but don't insert into
            # the portfolio yet — let support resolve manually.
            portfolio_inserted = False
            if row["sealed_id"] is not None:
                # Idempotent: don't create a second portfolio row for the
                # same purchase if the user re-clicks the email link.
                exists = await conn.fetchval(
                    """
                    SELECT id FROM sealed_portfolio
                    WHERE user_id = $1 AND purchase_id = $2
                    LIMIT 1
                    """,
                    user.user_id, int(row["id"]),
                )
                if not exists:
                    await conn.execute(
                        """
                        INSERT INTO sealed_portfolio
                          (user_id, sealed_id, purchase_id,
                           quantity, purchase_price_eur, purchased_at, source)
                        VALUES ($1, $2, $3, $4, $5, $6, 'shopify')
                        """,
                        user.user_id,
                        int(row["sealed_id"]),
                        int(row["id"]),
                        int(row["quantity"]),
                        float(row["unit_price_eur"]),
                        row["purchased_at"],
                    )
                    portfolio_inserted = True

    # Telemetry (best-effort)
    try:
        from services.telemetry import emit
        await emit(
            "shopify_claim_bound",
            user_id=user.user_id,
            properties={"purchase_id": int(row["id"]), "portfolio_inserted": portfolio_inserted},
        )
    except Exception:
        pass

    return {
        "ok":                 True,
        "purchase_id":        int(row["id"]),
        "portfolio_inserted": portfolio_inserted,
        "redirect":           "/?tab=portfolio",
    }

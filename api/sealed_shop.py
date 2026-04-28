"""Public Holygrade-Shop inventory endpoint for the Sealed-Tracker.

Phase 2 — exposes `/api/sealed/shop-stock`, a small JSON map of
`(set_code, language, product_type)` keys to Shopify variant info. The
frontend uses this to show "IN WARENKORB · CHF X · Y auf Lager" buttons
on each Sealed-Card tile (replacing the old Cardmarket-CTA).

Design notes:
  - Auth-free. The same data is already public on holygrade.com.
  - 5-minute in-memory cache (single asyncio.Lock + TTL).
  - Reuses `services.pricing_report.parse_sku` so the SKU-Mapping logic
    stays in one place (same parser the pricing-mailer uses).
  - Bypasses the "JP-only mailer" filter: we expose EN too if it ever
    appears in the shop.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Response

from services.pricing_report import parse_sku

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sealed", tags=["sealed-shop"])

# ── Config ──────────────────────────────────────────────────────────────────
SHOPIFY_DOMAIN = os.getenv("SHOPIFY_DOMAIN", "holygrade.myshopify.com")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "")
SHOPIFY_STOREFRONT_BASE = os.getenv(
    "SHOPIFY_STOREFRONT_BASE", "https://holygrade.com"
).rstrip("/")
CACHE_TTL_SECONDS = 300  # 5 Minuten

# ── In-memory cache ─────────────────────────────────────────────────────────
_cache_lock = asyncio.Lock()
_cache: dict[str, Any] = {
    "data": None,           # dict[str, dict] (the response body)
    "fetched_at": 0.0,      # epoch seconds
}


def _make_key(set_code: str, language: str, product_type: str) -> str:
    """Build the dict key the frontend looks up.

    Format chosen to be readable in JSON inspector: `OP15:JP:booster box`.
    """
    return f"{set_code.upper()}:{language.upper()}:{product_type.lower()}"


async def _fetch_shopify_products() -> list[dict]:
    """Pull the Shopify product list (read-only). Raises on transport errors."""
    if not SHOPIFY_TOKEN:
        raise RuntimeError("SHOPIFY_ADMIN_TOKEN not configured on the server")
    url = (
        f"https://{SHOPIFY_DOMAIN}/admin/api/2024-10/products.json"
        "?limit=250&fields=id,handle,title,variants"
    )
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(
            url,
            headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN},
        )
        resp.raise_for_status()
        body = resp.json()
    return body.get("products", []) or []


def _build_stock_map(products: list[dict]) -> dict[str, dict]:
    """Iterate variants, parse SKUs, build the stock-map keyed by triple."""
    out: dict[str, dict] = {}
    for p in products:
        handle = p.get("handle") or ""
        title = p.get("title") or ""
        for v in p.get("variants", []) or []:
            sku = (v.get("sku") or "").strip()
            parsed = parse_sku(sku)
            if not parsed:
                continue
            key = _make_key(
                parsed["set_code"],
                parsed["language"],
                parsed["product_type"],
            )
            try:
                price = float(v.get("price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            try:
                qty = int(v.get("inventory_quantity") or 0)
            except (TypeError, ValueError):
                qty = 0
            variant_id = v.get("id")
            cart_url = (
                f"{SHOPIFY_STOREFRONT_BASE}/cart/add?id={variant_id}"
                if variant_id
                else None
            )
            product_url = (
                f"{SHOPIFY_STOREFRONT_BASE}/products/{handle}"
                if handle
                else None
            )
            entry = {
                "in_stock": qty > 0,
                "qty": qty,
                "price_chf": f"{price:.2f}",
                "variant_id": variant_id,
                "handle": handle,
                "product_title": title,
                "variant_title": v.get("title") or "",
                "cart_url": cart_url,
                "product_url": product_url,
                "sku": sku,
            }
            # If a triple already exists (rare — duplicate SKUs), prefer the
            # one with stock so a buyable variant always wins.
            existing = out.get(key)
            if existing is None or (entry["in_stock"] and not existing["in_stock"]):
                out[key] = entry
    return out


async def _get_or_refresh_cache(force: bool = False) -> dict[str, dict]:
    """Return the cached stock-map or refresh it if expired."""
    now = time.time()
    if (
        not force
        and _cache["data"] is not None
        and (now - _cache["fetched_at"]) < CACHE_TTL_SECONDS
    ):
        return _cache["data"]

    async with _cache_lock:
        # Re-check inside the lock (another caller may have refreshed).
        now = time.time()
        if (
            not force
            and _cache["data"] is not None
            and (now - _cache["fetched_at"]) < CACHE_TTL_SECONDS
        ):
            return _cache["data"]

        try:
            products = await _fetch_shopify_products()
            data = _build_stock_map(products)
            _cache["data"] = data
            _cache["fetched_at"] = time.time()
            logger.info(
                "sealed_shop: cache refreshed (%d products → %d keys)",
                len(products),
                len(data),
            )
            return data
        except Exception as e:
            # Serve stale data if we have it; else re-raise.
            logger.warning("sealed_shop: refresh failed: %s", e)
            if _cache["data"] is not None:
                return _cache["data"]
            raise


# ── Routes ──────────────────────────────────────────────────────────────────

@router.get("/shop-stock")
async def sealed_shop_stock(response: Response):
    """Return Holygrade-Shop inventory keyed by `set_code:language:product_type`.

    Public endpoint (no auth). Used by the Sealed-Tracker frontend to render
    the "IN WARENKORB · CHF X · Y auf Lager" CTA per card.

    Response shape:
      {
        "OP15:JP:booster box": {
          "in_stock": true,
          "qty": 240,
          "price_chf": "95.90",
          "variant_id": 56586800791900,
          "handle": "one-piece-op-15-jp-sealed",
          "product_title": "...",
          "variant_title": "Booster Display (24 packs)",
          "cart_url": "https://holygrade.com/cart/add?id=...",
          "product_url": "https://holygrade.com/products/...",
          "sku": "OP-15-DISP-JP"
        },
        ...
      }
    """
    try:
        data = await _get_or_refresh_cache()
    except Exception as e:
        logger.error("sealed_shop: hard failure: %s", e)
        raise HTTPException(status_code=503, detail="Shopify inventory unavailable")

    response.headers["Cache-Control"] = f"public, max-age={CACHE_TTL_SECONDS}"
    return {
        "items": data,
        "count": len(data),
        "ttl_seconds": CACHE_TTL_SECONDS,
        "fetched_at": _cache["fetched_at"],
    }


@router.post("/shop-stock/refresh")
async def sealed_shop_stock_refresh():
    """Force-refresh the cache (admin-only would be nice; for now just open).

    Mainly here to support manual cache-busting after Shopify-side changes.
    Returns the new count.
    """
    try:
        data = await _get_or_refresh_cache(force=True)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Refresh failed: {e}")
    return {"ok": True, "count": len(data), "fetched_at": _cache["fetched_at"]}

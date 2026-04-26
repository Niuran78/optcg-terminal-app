"""Image proxy \u2014 fetch external card images server-side and serve them from
our own origin so the browser doesn't get blocked by Cross-Origin-Resource-
Policy (CORP) headers on the upstream CDN (Bandai, TCGPriceLookup, TCGGO).

The upstream servers use `Cross-Origin-Resource-Policy: same-site` which
causes Chrome/Safari to drop the image response when embedded cross-origin.

Caching: we set a 7-day s-max-age so Render's CDN (and the browser) cache
aggressively \u2014 after the first hit, the image is free to serve.
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/image", tags=["image-proxy"])

# Only allow images from these hosts \u2014 prevents open-redirect / abuse
ALLOWED_HOSTS = {
    "en.onepiece-cardgame.com",
    "asia-en.onepiece-cardgame.com",
    "onepiece-cardgame.com",
    "www.onepiece-cardgame.com",
    "images.tcggo.com",
    "cdn.tcgpricelookup.com",
    "storage.googleapis.com",  # PriceCharting image CDN
    "images.pricecharting.com",
    "product-images.s3.cardmarket.com",  # Cardmarket sealed product photos
    "static.cardmarket.com",
}

# Allow-listed image content-types
ALLOWED_TYPES = {
    "image/png", "image/jpeg", "image/jpg",
    "image/webp", "image/gif", "image/avif",
}


@router.get("/proxy")
async def proxy_image(url: str = Query(..., description="Absolute image URL")):
    """GET /api/image/proxy?url=https://en.onepiece-cardgame.com/images/..."""
    try:
        parsed = urlparse(url)
    except Exception:
        raise HTTPException(400, "invalid url")

    if parsed.scheme not in ("http", "https"):
        raise HTTPException(400, "invalid scheme")
    if parsed.hostname not in ALLOWED_HOSTS:
        raise HTTPException(403, f"host not allowed: {parsed.hostname}")

    # Cardmarket S3 blocks generic UAs — mimic a normal browser.
    ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    )
    try:
        async with httpx.AsyncClient(
            headers={
                "User-Agent": ua,
                "Accept": "image/avif,image/webp,image/png,image/jpeg,*/*",
                "Referer": "https://www.cardmarket.com/",
            },
            timeout=15,
            follow_redirects=True,
        ) as client:
            r = await client.get(url)
    except httpx.RequestError as e:
        logger.warning(f"image proxy fetch failed: {url}  err={e}")
        raise HTTPException(502, "upstream fetch failed")

    if r.status_code != 200:
        raise HTTPException(r.status_code, "upstream returned non-200")

    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
    # Cardmarket S3 sometimes returns 'multerS3.AUTO_CONTENT_TYPE' — a bug on
    # their side. Fall back to extension-based content-type detection.
    if ctype not in ALLOWED_TYPES:
        ext_map = {
            ".png": "image/png",
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".webp": "image/webp", ".gif": "image/gif", ".avif": "image/avif",
        }
        path_lower = parsed.path.lower()
        guessed = next((m for ext, m in ext_map.items() if path_lower.endswith(ext)), None)
        if guessed:
            ctype = guessed
        else:
            raise HTTPException(415, f"unsupported content-type: {ctype}")

    # Serve with long cache + CORS-safe headers
    return Response(
        content=r.content,
        media_type=ctype,
        headers={
            # 7 days browser + shared cache
            "Cache-Control": "public, max-age=604800, s-maxage=604800, immutable",
            # Explicitly allow cross-origin embedding for our images
            "Cross-Origin-Resource-Policy": "cross-origin",
            "Access-Control-Allow-Origin": "*",
        },
    )

"""Cardmarket official CSV price guide integration.

Downloads the daily price guide CSV from Cardmarket's public Data page.
This replaces the unreliable RapidAPI scraper for EU price data.

Cardmarket CSV columns (semicolon-separated):
  idProduct, Name, Category ID, Category, Expansion ID, Metacard ID,
  Date Updated, Avg Sell Price, Low Price, Trend Price, German Pro Low,
  Suggested Price, Foil Sell, Foil Low, Foil Trend, AVG1, AVG7, AVG30,
  Foil AVG1, Foil AVG7, Foil AVG30

Matching strategy:
  1. If card has `cardmarket_id` in cards_unified → direct ID match
  2. Else: fuzzy match on card name + set_code from Cardmarket expansion name

The CSV is gzip-compressed. Updated daily by Cardmarket.

Usage:
  result = await refresh_from_cardmarket()          # auto-download
  result = await refresh_from_cardmarket(csv_bytes)  # manual upload
"""
import csv
import gzip
import io
import logging
import re
from typing import Optional

import httpx

from db.init import get_pool

logger = logging.getLogger(__name__)

# Cardmarket Data page URLs — One Piece game.
# The download page is at cardmarket.com/en/OnePiece/Data/Price-Guide
# The actual CSV download link requires a browser session (Cloudflare challenge).
# We try multiple known URL patterns.
PRICE_GUIDE_URLS = [
    "https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_OnePiece.csv.gz",
    "https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_6.csv.gz",
    "https://www.cardmarket.com/en/OnePiece/Data/Price-Guide/Download",
]

# Cardmarket expansion names → set_code mapping (partial, covers major sets)
_CM_EXPANSION_TO_CODE: dict[str, str] = {
    "Romance Dawn": "OP01",
    "Paramount War": "OP02",
    "Pillars of Strength": "OP03",
    "Kingdoms of Intrigue": "OP04",
    "Awakening of the New Era": "OP05",
    "Wings of the Captain": "OP06",
    "500 Years in the Future": "OP07",
    "Two Legends": "OP08",
    "The Four Emperors": "OP09",
    "Royal Blood": "OP10",
    "Fist of Divine Speed": "OP11",
    "Heavenly Demon": "OP12",
    "Carrying on His Will": "OP13",
    "The Strongest Battlefield": "OP14",
    "A Spark of Rebellion": "OP15",
    "Memorial Collection": "EB01",
    "Extra Booster 02": "EB02",
    "Extreme Bond": "EB03",
    "Egghead Crisis": "EB04",
    "Premium Booster": "PRB01",
    "Premium Booster 02": "PRB02",
}

# Known set code patterns in Cardmarket card names (e.g., "OP01-001")
_SET_CODE_PATTERN = re.compile(r'\b(OP\d{2}|EB\d{2}|PRB\d{2}|ST\d{2})-\d{3}\b', re.IGNORECASE)


async def download_cardmarket_csv() -> bytes:
    """Download the One Piece price guide CSV from Cardmarket.

    Tries multiple URL patterns. The file is typically gzip-compressed.
    Returns raw CSV bytes (decompressed).
    Raises RuntimeError if all download attempts fail.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/csv,application/gzip,*/*",
        "Accept-Encoding": "gzip",
    }

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for url in PRICE_GUIDE_URLS:
            try:
                logger.info(f"Trying Cardmarket CSV from: {url}")
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    content = resp.content
                    # Decompress if gzipped
                    try:
                        content = gzip.decompress(content)
                    except gzip.BadGzipFile:
                        pass  # Already decompressed
                    # Sanity check: should contain CSV-like content
                    sample = content[:500].decode("utf-8", errors="replace")
                    if "idProduct" in sample or "Avg" in sample or ";" in sample:
                        logger.info(f"Downloaded Cardmarket CSV ({len(content)} bytes) from {url}")
                        return content
                    else:
                        logger.warning(f"URL {url} returned non-CSV content")
                else:
                    logger.warning(f"URL {url} returned HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"Failed to download from {url}: {e}")

    raise RuntimeError(
        "Could not download Cardmarket CSV. "
        "The Cardmarket Data page may be behind Cloudflare challenge. "
        "Use the manual upload endpoint: POST /admin/refresh-cardmarket with CSV body."
    )


def _parse_csv(csv_bytes: bytes) -> list[dict]:
    """Parse Cardmarket price guide CSV into a list of dicts.

    Handles both semicolon-separated (Cardmarket standard) and
    comma-separated formats. Returns list of row dicts.
    """
    text = csv_bytes.decode("utf-8-sig", errors="replace")

    # Detect delimiter
    first_line = text.split("\n")[0]
    delimiter = ";" if ";" in first_line else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = []
    for row in reader:
        rows.append(row)
    return rows


def _safe_float(val: Optional[str]) -> Optional[float]:
    """Convert string price to float, handling commas and empty strings."""
    if not val or val.strip() in ("", "N/A", "-"):
        return None
    try:
        # Cardmarket may use comma as decimal separator
        return float(val.strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def _extract_set_code_from_name(name: str) -> Optional[str]:
    """Try to extract set code from a Cardmarket product name like 'Monkey D. Luffy (OP01-001)'."""
    m = _SET_CODE_PATTERN.search(name)
    if m:
        code = m.group(1).upper()
        return code
    return None


async def parse_and_update_prices(csv_bytes: bytes) -> dict:
    """Parse Cardmarket CSV and update cards_unified EU prices.

    Matching strategy (in order):
      1. Match by cardmarket_id (idProduct from CSV → cardmarket_id in DB)
      2. Match by card_id extracted from CSV product name (e.g., 'OP01-001')

    Updates: eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest,
             eu_source, eu_updated_at, and cardmarket_id (if newly matched).

    Returns: {"total_rows": N, "matched": N, "updated": N, "skipped": N}
    """
    rows = _parse_csv(csv_bytes)
    if not rows:
        return {"total_rows": 0, "matched": 0, "updated": 0, "skipped": 0}

    pool = await get_pool()
    matched = 0
    updated = 0
    skipped = 0

    async with pool.acquire() as conn:
        # Build cardmarket_id → db_id lookup
        existing_cm = await conn.fetch(
            "SELECT id, cardmarket_id FROM cards_unified WHERE cardmarket_id IS NOT NULL"
        )
        cm_id_map: dict[int, int] = {r["cardmarket_id"]: r["id"] for r in existing_cm}

        # Build card_id → db_id lookup (for fallback matching)
        existing_cards = await conn.fetch(
            "SELECT id, card_id, variant FROM cards_unified"
        )
        card_id_map: dict[str, int] = {}
        for r in existing_cards:
            key = r["card_id"].upper()
            # Prefer Normal variant
            if key not in card_id_map or r["variant"] == "Normal":
                card_id_map[key] = r["id"]

        for row in rows:
            # Extract Cardmarket product ID
            id_product_str = row.get("idProduct", "").strip()
            if not id_product_str:
                skipped += 1
                continue

            try:
                id_product = int(id_product_str)
            except ValueError:
                skipped += 1
                continue

            # Extract prices
            name = row.get("Name", "")
            avg7 = _safe_float(row.get("AVG7"))
            avg30 = _safe_float(row.get("AVG30"))
            low_price = _safe_float(row.get("Low Price"))
            avg_sell = _safe_float(row.get("Avg Sell Price"))
            trend = _safe_float(row.get("Trend Price"))

            # Use best available price for 7d avg
            eu_7d = avg7 if avg7 is not None else avg_sell
            eu_30d = avg30
            eu_low = low_price

            # Skip rows with no useful price data
            if eu_7d is None and eu_30d is None and eu_low is None:
                skipped += 1
                continue

            # Try to find matching card in DB
            db_id = None

            # Strategy 1: Match by cardmarket_id
            if id_product in cm_id_map:
                db_id = cm_id_map[id_product]

            # Strategy 2: Match by card_id extracted from name
            if db_id is None:
                card_code = _extract_set_code_from_name(name)
                if card_code:
                    # Extract full card_id (e.g., "OP01-001") from name
                    m = re.search(r'\b((?:OP|EB|PRB|ST)\d{2}-\d{3}[a-zA-Z]?)\b', name, re.IGNORECASE)
                    if m:
                        full_card_id = m.group(1).upper()
                        if full_card_id in card_id_map:
                            db_id = card_id_map[full_card_id]

            if db_id is None:
                skipped += 1
                continue

            matched += 1

            # Update the card
            await conn.execute(
                """UPDATE cards_unified
                   SET eu_cardmarket_7d_avg = COALESCE($1, eu_cardmarket_7d_avg),
                       eu_cardmarket_30d_avg = COALESCE($2, eu_cardmarket_30d_avg),
                       eu_cardmarket_lowest = COALESCE($3, eu_cardmarket_lowest),
                       eu_source = 'Cardmarket CSV',
                       eu_updated_at = NOW(),
                       cardmarket_id = COALESCE(cardmarket_id, $4)
                   WHERE id = $5""",
                eu_7d, eu_30d, eu_low, id_product, db_id,
            )
            updated += 1

    result = {
        "total_rows": len(rows),
        "matched": matched,
        "updated": updated,
        "skipped": skipped,
    }
    logger.info(f"Cardmarket CSV update: {result}")
    return result


async def refresh_from_cardmarket(csv_bytes: Optional[bytes] = None) -> dict:
    """Main entry point — downloads CSV (or uses provided bytes) and updates DB.

    Args:
        csv_bytes: If provided, skip download and use these bytes directly.
                   Useful for manual upload via admin endpoint.

    Returns: dict with update stats.
    """
    if csv_bytes is None:
        csv_bytes = await download_cardmarket_csv()

    return await parse_and_update_prices(csv_bytes)

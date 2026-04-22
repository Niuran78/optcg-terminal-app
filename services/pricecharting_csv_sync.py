"""PriceCharting CSV Sync — the single source of truth for OPTCG prices.

Downloads the full One Piece Cards CSV from PriceCharting (~11k products in
one request) and upserts both sealed products and single cards into our DB.

This replaces the per-product scraper and the RapidAPI / TCG Price Lookup
services for price data.

Rate limit: CSV downloads are capped at 1 per 10 minutes by PriceCharting.
The CSV is regenerated once per 24h upstream, so daily sync is sufficient.
"""
import asyncio
import csv
import io
import logging
import os
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

PRICECHARTING_CSV_URL = (
    "https://www.pricecharting.com/price-guide/download-custom"
    "?category=one-piece-cards"
)

# USD → EUR rate (updated via env var in production)
USD_TO_EUR = 0.92

# ─────────────────────────────────────────────────────────────────────────────
# PriceCharting "console-name" → our internal set_code + language
# English sets: "One Piece <Set Name>"
# Japanese sets: "One Piece Japanese <Set Name>"
# ─────────────────────────────────────────────────────────────────────────────
PC_CONSOLE_MAP: dict[str, tuple[str, str]] = {
    # ─── Main Booster Sets (OP01–OP15) ───
    "One Piece Romance Dawn":                 ("OP01", "EN"),
    "One Piece Japanese Romance Dawn":        ("OP01", "JP"),
    "One Piece Paramount War":                ("OP02", "EN"),
    "One Piece Japanese Paramount War":       ("OP02", "JP"),
    "One Piece Pillars of Strength":          ("OP03", "EN"),
    "One Piece Japanese Pillars of Strength": ("OP03", "JP"),
    "One Piece Kingdoms of Intrigue":         ("OP04", "EN"),
    "One Piece Japanese Kingdoms of Intrigue":("OP04", "JP"),
    "One Piece Awakening of the New Era":     ("OP05", "EN"),
    "One Piece Japanese Awakening of the New Era": ("OP05", "JP"),
    "One Piece Wings of the Captain":         ("OP06", "EN"),
    "One Piece Japanese Wings of the Captain":("OP06", "JP"),
    "One Piece 500 Years in the Future":      ("OP07", "EN"),
    "One Piece Japanese 500 Years in the Future": ("OP07", "JP"),
    "One Piece Two Legends":                  ("OP08", "EN"),
    "One Piece Japanese Two Legends":         ("OP08", "JP"),
    "One Piece Emperors in the New World":    ("OP09", "EN"),
    "One Piece Japanese Emperors in the New World": ("OP09", "JP"),
    "One Piece Royal Blood":                  ("OP10", "EN"),
    "One Piece Japanese Royal Blood":         ("OP10", "JP"),
    "One Piece Fist of Divine Speed":         ("OP11", "EN"),
    "One Piece Japanese Fist of Divine Speed":("OP11", "JP"),
    "One Piece Legacy of the Master":         ("OP12", "EN"),
    "One Piece Japanese Legacy of the Master":("OP12", "JP"),
    "One Piece Carrying on His Will":         ("OP13", "EN"),
    "One Piece Japanese Carrying on His Will":("OP13", "JP"),
    "One Piece Azure Sea's Seven":            ("OP14", "EN"),
    "One Piece Japanese Azure Sea's Seven":   ("OP14", "JP"),
    "One Piece Adventure on Kami's Island":   ("OP15", "EN"),
    "One Piece Japanese Adventure on Kami's Island": ("OP15", "JP"),

    # ─── Extra Boosters (EB01–EB04) ───
    "One Piece Extra Booster Memorial Collection":          ("EB01", "EN"),
    "One Piece Japanese Extra Booster Memorial Collection": ("EB01", "JP"),
    "One Piece Extra Booster Anime 25th Collection":        ("EB02", "EN"),
    "One Piece Japanese Extra Booster Anime 25th Collection": ("EB02", "JP"),
    "One Piece Extra Booster Heroines Edition":             ("EB03", "EN"),
    "One Piece Japanese Extra Booster Heroines Edition":    ("EB03", "JP"),
    "One Piece Extra Booster EB04":                         ("EB04", "EN"),
    "One Piece Japanese Extra Booster Egghead Crisis":      ("EB04", "JP"),

    # ─── Premium Boosters (PRB01–PRB02) ───
    "One Piece Premium Booster":            ("PRB01", "EN"),
    "One Piece Japanese Premium Booster":   ("PRB01", "JP"),
    "One Piece Premium Booster 2":          ("PRB02", "EN"),
    "One Piece Japanese Premium Booster 2": ("PRB02", "JP"),

    # ─── Starter Decks (ST01–ST30) ───
    "One Piece Starter Deck 1: Straw Hat Crew":          ("ST01", "EN"),
    "One Piece Japanese Starter Deck 1: Straw Hat Crew": ("ST01", "JP"),
    "One Piece Starter Deck 2: Worst Generation":        ("ST02", "EN"),
    "One Piece Japanese Starter Deck 2: Worst Generation": ("ST02", "JP"),
    "One Piece Starter Deck 3: The Seven Warlords of the Sea": ("ST03", "EN"),
    "One Piece Japanese Starter Deck 3: The Seven Warlords of the Sea": ("ST03", "JP"),
    "One Piece Starter Deck 4: Animal Kingdom Pirates":  ("ST04", "EN"),
    "One Piece Japanese Starter Deck 4: Animal Kingdom Pirates": ("ST04", "JP"),
    "One Piece Starter Deck 5: Film Edition":            ("ST05", "EN"),
    "One Piece Japanese Starter Deck 5: Film Edition":   ("ST05", "JP"),
    "One Piece Starter Deck 6: Absolute Justice":        ("ST06", "EN"),
    "One Piece Japanese Starter Deck 6: Absolute Justice": ("ST06", "JP"),
    "One Piece Starter Deck 7: Big Mom Pirates":         ("ST07", "EN"),
    "One Piece Japanese Starter Deck 7: Big Mom Pirates": ("ST07", "JP"),
    "One Piece Starter Deck 8: Monkey.D.Luffy":          ("ST08", "EN"),
    "One Piece Japanese Starter Deck 8: Monkey.D.Luffy": ("ST08", "JP"),
    "One Piece Starter Deck 9: Yamato":                  ("ST09", "EN"),
    "One Piece Japanese Starter Deck 9: Yamato":         ("ST09", "JP"),
    "One Piece Ultra Deck: The Three Captains":          ("ST10", "EN"),
    "One Piece Japanese Ultra Deck: The Three Captains": ("ST10", "JP"),
    "One Piece Starter Deck 11: Uta":                    ("ST11", "EN"),
    "One Piece Japanese Starter Deck 11: Uta":           ("ST11", "JP"),
    "One Piece Starter Deck 12":                         ("ST12", "EN"),
    "One Piece Japanese Starter Deck 12":                ("ST12", "JP"),
    "One Piece Ultra Deck: The Three Brothers":          ("ST13", "EN"),
    "One Piece Japanese Ultra Deck: The Three Brothers": ("ST13", "JP"),
    "One Piece Starter Deck 14: 3D2Y":                   ("ST14", "EN"),
    "One Piece Japanese Starter Deck 14: 3D2Y":          ("ST14", "JP"),
    "One Piece Starter Deck 15: Edward Newgate":         ("ST15", "EN"),
    "One Piece Japanese Starter Deck 15: Edward Newgate": ("ST15", "JP"),
    "One Piece Starter Deck 16: Uta":                    ("ST16", "EN"),
    "One Piece Japanese Starter Deck 16: Uta":           ("ST16", "JP"),
    "One Piece Starter Deck 17: Donquixote Donflamingo": ("ST17", "EN"),
    "One Piece Japanese Starter Deck 17: Donquixote Donflamingo": ("ST17", "JP"),
    "One Piece Starter Deck 18: Monkey.D.Luffy":         ("ST18", "EN"),
    "One Piece Japanese Starter Deck 18: Monkey.D.Luffy": ("ST18", "JP"),
    "One Piece Starter Deck 19: Smoker":                 ("ST19", "EN"),
    "One Piece Japanese Starter Deck 19: Smoker":        ("ST19", "JP"),
    "One Piece Starter Deck 20: Charlotte Katakuri":     ("ST20", "EN"),
    "One Piece Japanese Starter Deck 20: Charlotte Katakuri": ("ST20", "JP"),
    "One Piece Starter Deck 21: Gear5":                  ("ST21", "EN"),
    "One Piece Japanese Starter Deck 21: Gear5":         ("ST21", "JP"),
    "One Piece Starter Deck 22: Ace & Newgate":          ("ST22", "EN"),
    "One Piece Japanese Starter Deck 22: Ace & Newgate": ("ST22", "JP"),
    "One Piece Starter Deck 23: Red Shanks":             ("ST23", "EN"),
    "One Piece Japanese Starter Deck 23: Red Shanks":    ("ST23", "JP"),
    "One Piece Starter Deck 24: Green Jewelry Bonney":   ("ST24", "EN"),
    "One Piece Japanese Starter Deck 24: Green Jewelry Bonney": ("ST24", "JP"),
    "One Piece Starter Deck 25: Blue Buggy":             ("ST25", "EN"),
    "One Piece Japanese Starter Deck 25: Blue Buggy":    ("ST25", "JP"),
    "One Piece Starter Deck 26: Purple Monkey.D.Luffy":  ("ST26", "EN"),
    "One Piece Japanese Starter Deck 26: Purple Monkey.D.Luffy": ("ST26", "JP"),
    "One Piece Starter Deck 27: Black Marshall.D.Teach": ("ST27", "EN"),
    "One Piece Japanese Starter Deck 27: Black Marshall.D.Teach": ("ST27", "JP"),
    "One Piece Starter Deck 28: Yellow Yamato":          ("ST28", "EN"),
    "One Piece Japanese Starter Deck 28: Yellow Yamato": ("ST28", "JP"),
    "One Piece Starter Deck 29: Egghead":                ("ST29", "EN"),
    "One Piece Japanese Starter Deck 29: Egghead Arc":   ("ST29", "JP"),
    "One Piece Starter Deck 30: Luffy & Ace":            ("ST30", "EN"),
    "One Piece Japanese Starter Deck 30: Luffy & Ace":   ("ST30", "JP"),

    # ─── Promo Sets ───
    "One Piece Promo":                      ("PROMO", "EN"),
    "One Piece Japanese Promo":             ("PROMO", "JP"),

    # ─── Collector / Bundle Products ───
    "One Piece The Quest Begins":           ("QB01", "EN"),
    "One Piece Passage to the Grand Line":  ("PGL01", "EN"),
    "One Piece Seven Warlords of the Sea Binder Set": ("BIND01", "EN"),
    "One Piece Tin Pack Set Vol 1":         ("TIN01", "EN"),
    "One Piece Learn Together Deck Set":    ("LEARN01", "EN"),
    "One Piece Carddass Hyper Battle First Stage": ("CARD01", "JP"),
}

# Product-name keywords → sealed product type
SEALED_KEYWORDS = [
    ("Case", "case"),
    ("Sealed Booster Box", "booster box"),
    ("Booster Box", "booster box"),
    ("Sleeved Booster Pack", "sleeved booster"),
    ("Booster Pack", "booster"),
    ("Starter Deck Sealed", "starter deck"),
    ("Deck Sealed", "starter deck"),
    ("Display", "display"),
    ("Gift Box", "gift box"),
    ("Tin", "tin"),
]

# Card ID pattern in product-name: "Luffy OP13-001" / "Ace ST17-008" / etc.
CARD_ID_RE = re.compile(r'\b([A-Z]+\d+)-(\d+)\b')


def _price_to_cents(price_str: str) -> Optional[int]:
    """'$12.45' → 1245 cents. Returns None if empty/unparseable."""
    if not price_str:
        return None
    cleaned = price_str.replace('$', '').replace(',', '').strip()
    if not cleaned:
        return None
    try:
        return int(float(cleaned) * 100)
    except ValueError:
        return None


def _classify_row(product_name: str) -> Optional[str]:
    """Return 'sealed_<type>' or 'card' or None (unknown)."""
    pn = product_name.strip()
    for kw, stype in SEALED_KEYWORDS:
        if kw in pn:
            return f"sealed:{stype}"
    # Card: must have Set-ID pattern like OP13-001
    if CARD_ID_RE.search(pn):
        return "card"
    return None


async def download_csv() -> str:
    """Download the One Piece Cards CSV from PriceCharting."""
    token = os.getenv("PRICECHARTING_API_TOKEN")
    if not token:
        raise RuntimeError("PRICECHARTING_API_TOKEN env var not set")

    url = f"{PRICECHARTING_CSV_URL}&t={token}"
    logger.info(f"pricecharting_csv_sync: downloading {url[:60]}...")

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text


def parse_csv(csv_text: str) -> dict:
    """Parse the CSV into sealed + cards lists, grouped by set_code.

    Returns:
        {
            'sealed': [dict(set_code, language, product_type, product_name,
                          price_usd_cents, pc_id, console_name)],
            'cards':  [dict(set_code, language, card_id, variant, name,
                          price_usd_cents, pc_id)],
            'unmapped_consoles': [str]  # consoles in CSV not in our map
        }
    """
    reader = csv.DictReader(io.StringIO(csv_text))

    sealed: list[dict] = []
    cards: list[dict] = []
    unmapped: set[str] = set()

    for row in reader:
        console = row.get('console-name', '').strip()
        product = row.get('product-name', '').strip()
        price   = _price_to_cents(row.get('loose-price', ''))
        pc_id   = row.get('id', '').strip()

        if not console or not product:
            continue

        mapping = PC_CONSOLE_MAP.get(console)
        if not mapping:
            unmapped.add(console)
            continue

        set_code, language = mapping
        classification = _classify_row(product)

        if classification and classification.startswith('sealed:'):
            product_type = classification.split(':', 1)[1]
            sealed.append({
                "set_code": set_code,
                "language": language,
                "product_type": product_type,
                "product_name": product.strip(),
                "price_usd_cents": price,
                "pc_id": pc_id,
                "console_name": console,
            })
        elif classification == 'card':
            # Parse card ID from product name
            m = CARD_ID_RE.search(product)
            if not m:
                continue
            card_id = f"{m.group(1)}-{m.group(2)}"
            # Name = product-name with card ID stripped
            name = CARD_ID_RE.sub('', product).strip()
            cards.append({
                "set_code": set_code,
                "language": language,
                "card_id": card_id,
                "name": name or card_id,
                "price_usd_cents": price,
                "pc_id": pc_id,
            })
        # else: unknown row type — silently skip

    return {
        "sealed": sealed,
        "cards": cards,
        "unmapped_consoles": sorted(unmapped),
    }


async def sync_from_csv() -> dict:
    """Full sync: download CSV, parse, upsert sealed + cards into DB."""
    from db.init import get_pool

    csv_text = await download_csv()
    parsed = parse_csv(csv_text)

    sealed = parsed["sealed"]
    cards = parsed["cards"]
    unmapped = parsed["unmapped_consoles"]

    logger.info(
        f"pricecharting_csv_sync: parsed {len(sealed)} sealed, {len(cards)} cards, "
        f"{len(unmapped)} unmapped consoles"
    )

    pool = await get_pool()

    # ─── Ensure schema ───
    async with pool.acquire() as conn:
        # Sealed schema
        await conn.execute(
            "ALTER TABLE sealed_unified ADD COLUMN IF NOT EXISTS language VARCHAR(10) DEFAULT 'JP'"
        )
        await conn.execute(
            "ALTER TABLE sealed_unified ADD COLUMN IF NOT EXISTS en_price_usd REAL"
        )
        await conn.execute(
            "ALTER TABLE sealed_unified ADD COLUMN IF NOT EXISTS pricecharting_id TEXT"
        )
        # Cards schema — add optional PC id + language if not there
        await conn.execute(
            "ALTER TABLE cards_unified ADD COLUMN IF NOT EXISTS pricecharting_id TEXT"
        )
        await conn.execute(
            "ALTER TABLE cards_unified ADD COLUMN IF NOT EXISTS language VARCHAR(10)"
        )
        await conn.execute(
            "ALTER TABLE cards_unified ADD COLUMN IF NOT EXISTS pc_price_usd REAL"
        )
        await conn.execute(
            "ALTER TABLE cards_unified ADD COLUMN IF NOT EXISTS pc_updated_at TIMESTAMPTZ"
        )

    # ─── Upsert sealed ───
    sealed_inserted = 0
    sealed_updated = 0

    # Group by (set_code, product_type, language) — keep the item with the
    # highest price (usually the main booster box, not variants)
    sealed_best: dict[tuple, dict] = {}
    for s in sealed:
        key = (s["set_code"], s["product_type"], s["language"])
        prev = sealed_best.get(key)
        if prev is None or (s["price_usd_cents"] or 0) > (prev["price_usd_cents"] or 0):
            sealed_best[key] = s

    async with pool.acquire() as conn:
        for s in sealed_best.values():
            if s["price_usd_cents"] is None:
                continue
            usd = s["price_usd_cents"] / 100.0
            eur = usd * USD_TO_EUR
            set_code = s["set_code"]
            lang = s["language"]
            product_type = s["product_type"]
            set_name = s["console_name"].replace("One Piece Japanese ", "").replace("One Piece ", "")

            product_name_fmt = (
                f"{set_name} {s['product_name'].strip()}"
                f" ({'JP' if lang == 'JP' else 'EN'})"
            )

            status = await conn.execute("""
                INSERT INTO sealed_unified (
                    product_name, set_code, set_name, product_type,
                    eu_price, eu_7d_avg, en_price_usd, pricecharting_id,
                    eu_source, eu_updated_at, language, created_at
                ) VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, NOW(), $9, NOW())
                ON CONFLICT (set_code, product_type, language) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    set_name = EXCLUDED.set_name,
                    eu_price = EXCLUDED.eu_price,
                    eu_7d_avg = EXCLUDED.eu_7d_avg,
                    en_price_usd = EXCLUDED.en_price_usd,
                    pricecharting_id = EXCLUDED.pricecharting_id,
                    eu_source = EXCLUDED.eu_source,
                    eu_updated_at = EXCLUDED.eu_updated_at
            """,
                product_name_fmt, set_code, set_name, product_type,
                eur, usd, s["pc_id"],
                f"PriceCharting {lang}", lang,
            )
            if "INSERT" in status:
                sealed_inserted += 1
            else:
                sealed_updated += 1

    # ─── Upsert cards (batched via UNNEST for speed) ───
    cards_updated = 0
    cards_missing: list[str] = []

    priced_cards = [c for c in cards if c["price_usd_cents"] is not None]
    set_codes = [c["set_code"] for c in priced_cards]
    card_ids = [c["card_id"] for c in priced_cards]
    prices   = [c["price_usd_cents"] / 100.0 for c in priced_cards]
    pc_ids   = [c["pc_id"] for c in priced_cards]

    async with pool.acquire() as conn:
        # Single UPDATE using UNNEST to stream all rows in one query
        upd_count = await conn.fetchval("""
            WITH input AS (
                SELECT * FROM UNNEST($1::text[], $2::text[], $3::real[], $4::text[])
                AS t(set_code, card_id, pc_price_usd, pricecharting_id)
            ),
            updated AS (
                UPDATE cards_unified c
                SET pc_price_usd = i.pc_price_usd,
                    pricecharting_id = i.pricecharting_id,
                    pc_updated_at = NOW()
                FROM input i
                WHERE c.set_code = i.set_code AND c.card_id = i.card_id
                RETURNING 1
            )
            SELECT COUNT(*) FROM updated
        """, set_codes, card_ids, prices, pc_ids)
        cards_updated = upd_count or 0

        # Find CSV cards not in DB — use UNNEST again for fast LEFT JOIN
        miss_rows = await conn.fetch("""
            WITH input AS (
                SELECT * FROM UNNEST($1::text[], $2::text[])
                AS t(set_code, card_id)
            )
            SELECT i.set_code, i.card_id
            FROM input i
            LEFT JOIN cards_unified c
              ON c.set_code = i.set_code AND c.card_id = i.card_id
            WHERE c.id IS NULL
            LIMIT 100
        """, set_codes, card_ids)
        cards_missing = [f"{r['set_code']}-{r['card_id']}" for r in miss_rows]

    logger.info(
        f"pricecharting_csv_sync complete: sealed={sealed_inserted}+{sealed_updated}, "
        f"cards_updated={cards_updated}, cards_missing_in_db={len(cards_missing)}"
    )

    return {
        "sealed_inserted": sealed_inserted,
        "sealed_updated": sealed_updated,
        "sealed_total": sealed_inserted + sealed_updated,
        "cards_updated": cards_updated,
        "cards_missing_in_db": len(cards_missing),
        "cards_missing_sample": cards_missing[:20],
        "unmapped_consoles": unmapped,
        "parsed_rows": {
            "sealed_in_csv": len(sealed),
            "cards_in_csv": len(cards),
        },
    }

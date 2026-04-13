"""Card data aggregator — merges EN and EU price sources into unified model.

EN source: TCG Price Lookup (tcg_price_lookup.py)
EU source: RapidAPI / Cardmarket (opcg_api.py)

The aggregator matches cards by card_id (e.g. "OP01-001") + variant,
then writes merged records into the cards_unified and sealed_unified tables.
"""
import asyncio
import logging
import json
from datetime import datetime
from typing import Optional

from db.init import get_pool
from services import opcg_api
from services import tcg_price_lookup

logger = logging.getLogger(__name__)

# FX conversion constants (approximate; update periodically)
USD_TO_EUR: float = 0.92
EUR_TO_USD: float = 1.08

# ─── Set mapping: EN slug ↔ RapidAPI set ID ─────────────────────────────────
# Keys are the set codes that appear in card numbers (e.g. "OP01").
# en_slug: slug used by TCG Price Lookup API.
# rapidapi_id: numeric set ID used by RapidAPI.
# name: canonical English name for fuzzy matching.
SET_MAPPING: dict[str, dict] = {
    "OP01": {"en_slug": "onepiece--romance-dawn", "rapidapi_id": "368", "name": "Romance Dawn"},
    "OP02": {"en_slug": "onepiece--paramount-war", "rapidapi_id": "369", "name": "Paramount War"},
    "OP03": {"en_slug": "onepiece--pillars-of-strength", "rapidapi_id": "370", "name": "Pillars of Strength"},
    "OP04": {"en_slug": "onepiece--kingdoms-of-intrigue", "rapidapi_id": "371", "name": "Kingdoms of Intrigue"},
    "OP05": {"en_slug": "onepiece--awakening-of-the-new-era", "rapidapi_id": "372", "name": "Awakening of the New Era"},
    "OP06": {"en_slug": "onepiece--wings-of-the-captain", "rapidapi_id": "373", "name": "Wings of the Captain"},
    "OP07": {"en_slug": "onepiece--500-years-in-the-future", "rapidapi_id": "374", "name": "500 Years in the Future"},
    "OP08": {"en_slug": "onepiece--two-legends", "rapidapi_id": "375", "name": "Two Legends"},
    "OP09": {"en_slug": "onepiece--emperors-in-the-new-world", "rapidapi_id": "376", "name": "Emperors in the New World"},
    "ST01": {"en_slug": "onepiece--starter-deck-straw-hat-crew", "rapidapi_id": "377", "name": "Starter Deck: Straw Hat Crew"},
    "ST02": {"en_slug": "onepiece--starter-deck-worst-generation", "rapidapi_id": "378", "name": "Starter Deck: Worst Generation"},
    "ST03": {"en_slug": "onepiece--starter-deck-the-seven-warlords-of-the-sea", "rapidapi_id": "379", "name": "Starter Deck: The Seven Warlords of the Sea"},
    "ST04": {"en_slug": "onepiece--starter-deck-animal-kingdom-pirates", "rapidapi_id": "380", "name": "Starter Deck: Animal Kingdom Pirates"},
    "ST05": {"en_slug": "onepiece--starter-deck-film-edition", "rapidapi_id": "381", "name": "Starter Deck: Film Edition"},
    "ST06": {"en_slug": "onepiece--starter-deck-absolute-justice", "rapidapi_id": "382", "name": "Starter Deck: Absolute Justice"},
    "ST07": {"en_slug": "onepiece--starter-deck-big-mom-pirates", "rapidapi_id": "383", "name": "Starter Deck: Big Mom Pirates"},
    "ST08": {"en_slug": "onepiece--starter-deck-monkey-d-luffy", "rapidapi_id": "384", "name": "Starter Deck: Monkey D. Luffy"},
    "ST09": {"en_slug": "onepiece--starter-deck-yamato", "rapidapi_id": "385", "name": "Starter Deck: Yamato"},
    "ST10": {"en_slug": "onepiece--starter-deck-uta", "rapidapi_id": "386", "name": "Starter Deck: UTA"},
    "ST11": {"en_slug": "onepiece--starter-deck-uta-2", "rapidapi_id": "387", "name": "Starter Deck: UTA 2"},
    "ST12": {"en_slug": "onepiece--starter-deck-zoro-sanji", "rapidapi_id": "388", "name": "Starter Deck: Zoro & Sanji"},
    "ST13": {"en_slug": "onepiece--starter-deck-navy", "rapidapi_id": "389", "name": "Starter Deck: Navy"},
    "ST14": {"en_slug": "onepiece--three-captains", "rapidapi_id": "390", "name": "Three Captains"},
    "ST15": {"en_slug": "onepiece--red-purple-law", "rapidapi_id": "391", "name": "Red-Purple Law"},
    "ST16": {"en_slug": "onepiece--green-black-sakazuki", "rapidapi_id": "392", "name": "Green-Black Sakazuki"},
    "ST17": {"en_slug": "onepiece--blue-purple-crocodile", "rapidapi_id": "393", "name": "Blue-Purple Crocodile"},
    "ST18": {"en_slug": "onepiece--black-yellow-big-mom", "rapidapi_id": "394", "name": "Black-Yellow Big Mom"},
    "ST19": {"en_slug": "onepiece--red-blue-garp", "rapidapi_id": "395", "name": "Red-Blue Garp"},
    "ST20": {"en_slug": "onepiece--red-green-shanks", "rapidapi_id": "396", "name": "Red-Green Shanks"},
    "EB01": {"en_slug": "onepiece--extra-booster-memorial-collection", "rapidapi_id": "397", "name": "Extra Booster: Memorial Collection"},
    "PRB01": {"en_slug": "onepiece--premium-booster-the-best", "rapidapi_id": "398", "name": "Premium Booster: The Best"},
}

# Reverse lookup: EN slug → set code
_SLUG_TO_CODE: dict[str, str] = {v["en_slug"]: k for k, v in SET_MAPPING.items()}


def _extract_set_code_from_card_id(card_id: str) -> Optional[str]:
    """Extract set code from a card ID like 'OP01-001' → 'OP01'."""
    if not card_id or "-" not in card_id:
        return None
    return card_id.split("-")[0].upper()


def _normalize_variant(variant: str) -> str:
    """Normalize variant strings for matching."""
    if not variant:
        return "Normal"
    v = variant.strip().lower()
    if v in ("normal", "standard", "regular", ""):
        return "Normal"
    if "foil" in v or "holo" in v:
        return "Foil"
    if "alternate" in v or "alt art" in v or "alternate art" in v:
        return "Alternate Art"
    if "leader" in v:
        return "Leader"
    return variant.strip().title()


def _extract_eu_card_prices(card: dict) -> dict:
    """Extract EU (Cardmarket) prices from a RapidAPI card dict."""
    prices = card.get("prices", {}) or {}
    cm = prices.get("cardmarket", {}) or {}

    def _safe_float(v) -> Optional[float]:
        if v is None:
            return None
        try:
            f = float(v)
            # RapidAPI returns prices in cents for some fields — detect by magnitude
            # Typical card prices: 0.01 to 500 EUR. If > 1000, assume cents.
            if f > 1000:
                return round(f / 100.0, 2)
            return f
        except (ValueError, TypeError):
            return None

    return {
        "eu_cardmarket_7d_avg": _safe_float(cm.get("7d_average")),
        "eu_cardmarket_30d_avg": _safe_float(cm.get("30d_average")),
        "eu_cardmarket_lowest": _safe_float(cm.get("lowest") or cm.get("lowest_near_mint")),
    }


def _extract_eu_product_prices(product: dict) -> dict:
    """Extract EU product prices from a RapidAPI product dict."""
    prices = product.get("prices", {}) or {}
    cm = prices.get("cardmarket", {}) or {}

    def _safe_float(v) -> Optional[float]:
        if v is None:
            return None
        try:
            f = float(v)
            if f > 10000:
                return round(f / 100.0, 2)
            return f
        except (ValueError, TypeError):
            return None

    lowest = _safe_float(cm.get("lowest") or cm.get("lowest_near_mint"))
    avg_30 = _safe_float(cm.get("30d_average"))
    avg_7 = _safe_float(cm.get("7d_average"))

    # Determine trend
    trend = "stable"
    if avg_7 and avg_30:
        if avg_7 > avg_30 * 1.05:
            trend = "up"
        elif avg_7 < avg_30 * 0.95:
            trend = "down"

    return {
        "eu_price": lowest or avg_30 or avg_7,
        "eu_30d_avg": avg_30,
        "eu_7d_avg": avg_7,
        "eu_trend": trend,
    }


async def aggregate_set(set_code: str, set_name: str) -> int:
    """Merge EN and EU card data for a given set into cards_unified.

    Returns the number of records upserted.
    """
    mapping = SET_MAPPING.get(set_code.upper(), {})
    en_slug = mapping.get("en_slug", "")
    rapidapi_id = mapping.get("rapidapi_id", "")

    upserted = 0

    # ── Fetch EN cards ──────────────────────────────────────────────────────
    en_cards: list[dict] = []
    if en_slug:
        try:
            en_cards = await tcg_price_lookup.get_en_cards(en_slug)
            logger.info(f"Aggregator [{set_code}]: {len(en_cards)} EN cards from TCG Price Lookup")
        except Exception as e:
            logger.warning(f"Aggregator [{set_code}]: EN fetch failed: {e}")

    # ── Fetch EU cards ──────────────────────────────────────────────────────
    eu_cards: list[dict] = []
    if rapidapi_id:
        try:
            eu_cards = await opcg_api.get_cards(rapidapi_id, tier="elite")
            logger.info(f"Aggregator [{set_code}]: {len(eu_cards)} EU cards from RapidAPI")
        except Exception as e:
            logger.warning(f"Aggregator [{set_code}]: EU fetch failed: {e}")

    # ── Build EU lookup: card_id + normalized_variant → card dict ───────────
    eu_lookup: dict[tuple, dict] = {}
    for card in eu_cards:
        # RapidAPI uses various field names for card number
        card_num = (
            card.get("card_number")
            or card.get("number")
            or card.get("code")
            or card.get("id", "")
        )
        card_num = str(card_num).upper()
        variant = _normalize_variant(card.get("variant", "") or card.get("type", "") or "Normal")
        if card_num:
            eu_lookup[(card_num, variant)] = card
            # Also index with "Normal" fallback for unvariant EU cards
            if variant != "Normal":
                eu_lookup.setdefault((card_num, "Normal"), card)

    # ── Merge and upsert into cards_unified ────────────────────────────────
    pool = await get_pool()
    async with pool.acquire() as conn:
        for en_card in en_cards:
            card_id = (en_card.get("card_id") or "").upper()
            if not card_id:
                continue

            variant = _normalize_variant(en_card.get("variant", "Normal"))
            name = en_card.get("name", "")

            # Look up matching EU card
            eu_card = eu_lookup.get((card_id, variant)) or eu_lookup.get((card_id, "Normal"))
            eu_prices = _extract_eu_card_prices(eu_card) if eu_card else {
                "eu_cardmarket_7d_avg": None,
                "eu_cardmarket_30d_avg": None,
                "eu_cardmarket_lowest": None,
            }

            rapidapi_card_id = None
            cardmarket_id = None
            if eu_card:
                rapidapi_card_id = str(eu_card.get("id") or eu_card.get("_id") or "")
                cardmarket_id = eu_card.get("cardmarket_id") or eu_card.get("cardmarketId")

            await conn.execute(
                """
                INSERT INTO cards_unified (
                    card_id, name, set_code, set_name, rarity, variant, image_url,
                    en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
                    en_source, en_updated_at,
                    eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest,
                    eu_source, eu_updated_at,
                    tcg_price_lookup_id, rapidapi_card_id,
                    tcgplayer_id, cardmarket_id,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10,
                    'TCG Price Lookup', NOW(),
                    $11, $12, $13,
                    'Cardmarket', $14,
                    $15, $16,
                    $17, $18,
                    NOW()
                )
                ON CONFLICT(card_id, variant) DO UPDATE SET
                    name=EXCLUDED.name,
                    set_code=EXCLUDED.set_code,
                    set_name=EXCLUDED.set_name,
                    rarity=EXCLUDED.rarity,
                    image_url=EXCLUDED.image_url,
                    en_tcgplayer_market=EXCLUDED.en_tcgplayer_market,
                    en_tcgplayer_low=EXCLUDED.en_tcgplayer_low,
                    en_ebay_avg_7d=EXCLUDED.en_ebay_avg_7d,
                    en_source=EXCLUDED.en_source,
                    en_updated_at=EXCLUDED.en_updated_at,
                    eu_cardmarket_7d_avg=EXCLUDED.eu_cardmarket_7d_avg,
                    eu_cardmarket_30d_avg=EXCLUDED.eu_cardmarket_30d_avg,
                    eu_cardmarket_lowest=EXCLUDED.eu_cardmarket_lowest,
                    eu_source=EXCLUDED.eu_source,
                    eu_updated_at=EXCLUDED.eu_updated_at,
                    tcg_price_lookup_id=EXCLUDED.tcg_price_lookup_id,
                    rapidapi_card_id=EXCLUDED.rapidapi_card_id,
                    tcgplayer_id=EXCLUDED.tcgplayer_id,
                    cardmarket_id=EXCLUDED.cardmarket_id
                """,
                card_id, name, set_code, set_name,
                en_card.get("rarity", ""),
                variant,
                en_card.get("image_url", ""),
                en_card.get("en_tcgplayer_market"),
                en_card.get("en_tcgplayer_low"),
                en_card.get("en_ebay_avg_7d"),
                eu_prices["eu_cardmarket_7d_avg"],
                eu_prices["eu_cardmarket_30d_avg"],
                eu_prices["eu_cardmarket_lowest"],
                datetime.utcnow() if eu_card else None,
                en_card.get("tcg_price_lookup_id", ""),
                rapidapi_card_id,
                en_card.get("tcgplayer_id"),
                cardmarket_id,
            )
            upserted += 1

        # Also upsert EU-only cards (no EN counterpart found)
        en_known: set[tuple] = {
            (_normalize_card_id(c.get("card_id", "")), _normalize_variant(c.get("variant", "Normal")))
            for c in en_cards
        }
        for eu_card in eu_cards:
            card_num = (
                str(eu_card.get("card_number") or eu_card.get("number") or eu_card.get("code") or "")
                .upper()
            )
            if not card_num:
                continue
            variant = _normalize_variant(eu_card.get("variant", "") or "Normal")
            if (card_num, variant) in en_known:
                continue  # Already handled above

            eu_prices = _extract_eu_card_prices(eu_card)
            name = eu_card.get("name") or eu_card.get("card_name") or ""
            image_url = eu_card.get("image_url") or eu_card.get("image") or ""
            rarity = eu_card.get("rarity") or ""
            rapidapi_card_id = str(eu_card.get("id") or eu_card.get("_id") or "")

            await conn.execute(
                """
                INSERT INTO cards_unified (
                    card_id, name, set_code, set_name, rarity, variant, image_url,
                    en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
                    en_source, en_updated_at,
                    eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest,
                    eu_source, eu_updated_at,
                    tcg_price_lookup_id, rapidapi_card_id,
                    tcgplayer_id, cardmarket_id,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    NULL, NULL, NULL,
                    'TCG Price Lookup', NULL,
                    $8, $9, $10,
                    'Cardmarket', NOW(),
                    NULL, $11,
                    NULL, $12,
                    NOW()
                )
                ON CONFLICT(card_id, variant) DO UPDATE SET
                    eu_cardmarket_7d_avg=EXCLUDED.eu_cardmarket_7d_avg,
                    eu_cardmarket_30d_avg=EXCLUDED.eu_cardmarket_30d_avg,
                    eu_cardmarket_lowest=EXCLUDED.eu_cardmarket_lowest,
                    eu_updated_at=EXCLUDED.eu_updated_at,
                    rapidapi_card_id=EXCLUDED.rapidapi_card_id,
                    cardmarket_id=EXCLUDED.cardmarket_id
                """,
                card_num, name, set_code, set_name, rarity, variant, image_url,
                eu_prices["eu_cardmarket_7d_avg"],
                eu_prices["eu_cardmarket_30d_avg"],
                eu_prices["eu_cardmarket_lowest"],
                rapidapi_card_id,
                eu_card.get("cardmarket_id") or eu_card.get("cardmarketId"),
            )
            upserted += 1

    logger.info(f"Aggregator [{set_code}]: upserted {upserted} cards into cards_unified")
    return upserted


def _normalize_card_id(card_id: str) -> str:
    return (card_id or "").upper()


async def aggregate_sealed(set_code: str, set_name: str) -> int:
    """Fetch sealed products from RapidAPI and store in sealed_unified.

    Returns the number of records upserted.
    """
    mapping = SET_MAPPING.get(set_code.upper(), {})
    rapidapi_id = mapping.get("rapidapi_id", "")
    if not rapidapi_id:
        logger.debug(f"Aggregator sealed [{set_code}]: no RapidAPI ID in mapping, skipping")
        return 0

    upserted = 0

    try:
        products = await opcg_api.get_products(rapidapi_id, tier="elite")
        logger.info(f"Aggregator sealed [{set_code}]: {len(products)} products from RapidAPI")
    except Exception as e:
        logger.warning(f"Aggregator sealed [{set_code}]: fetch failed: {e}")
        return 0

    def _classify_product_type(name: str) -> str:
        name_lower = (name or "").lower()
        if "case" in name_lower:
            return "case"
        if "booster box" in name_lower or "display" in name_lower:
            return "booster_box"
        if "booster" in name_lower or "pack" in name_lower:
            return "booster"
        return "other"

    pool = await get_pool()
    async with pool.acquire() as conn:
        for product in products:
            name = (
                product.get("name")
                or product.get("product_name")
                or product.get("title")
                or ""
            )
            if not name:
                continue

            product_type = _classify_product_type(name)
            image_url = product.get("image_url") or product.get("image") or ""
            rapidapi_product_id = str(product.get("id") or product.get("_id") or "")
            prices = _extract_eu_product_prices(product)

            await conn.execute(
                """
                INSERT INTO sealed_unified (
                    product_name, set_code, set_name, product_type, image_url,
                    eu_price, eu_30d_avg, eu_7d_avg, eu_trend,
                    eu_source, eu_updated_at,
                    rapidapi_product_id,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'Cardmarket', NOW(), $10, NOW())
                ON CONFLICT(product_name, set_code) DO UPDATE SET
                    product_type=EXCLUDED.product_type,
                    image_url=EXCLUDED.image_url,
                    eu_price=EXCLUDED.eu_price,
                    eu_30d_avg=EXCLUDED.eu_30d_avg,
                    eu_7d_avg=EXCLUDED.eu_7d_avg,
                    eu_trend=EXCLUDED.eu_trend,
                    eu_updated_at=EXCLUDED.eu_updated_at,
                    rapidapi_product_id=EXCLUDED.rapidapi_product_id
                """,
                name, set_code, set_name, product_type, image_url,
                prices["eu_price"], prices["eu_30d_avg"],
                prices["eu_7d_avg"], prices["eu_trend"],
                rapidapi_product_id,
            )
            upserted += 1

    logger.info(f"Aggregator sealed [{set_code}]: upserted {upserted} products")
    return upserted


async def seed_all_unified():
    """Background task: aggregate all sets from both sources.

    1. Fetch EN sets from TCG Price Lookup.
    2. Augment with EU sets from RapidAPI.
    3. For each known set, run aggregate_set() + aggregate_sealed().
    4. Rate limit: 0.5s between sets.
    """
    logger.info("seed_all_unified: starting multi-source data seed...")

    # Collect sets to process — start from mapping, augment with live EN set list
    sets_to_process: dict[str, str] = {}  # code → name

    # Seed from SET_MAPPING as baseline
    for code, info in SET_MAPPING.items():
        sets_to_process[code] = info.get("name", code)

    # Try to fetch live EN set list and add any unknown sets
    try:
        en_sets = await tcg_price_lookup.get_en_sets()
        for s in en_sets:
            slug = s.get("slug", "")
            name = s.get("name", "")
            # Try to infer set code from slug if not in mapping
            code = _SLUG_TO_CODE.get(slug)
            if code and code not in sets_to_process:
                sets_to_process[code] = name
    except Exception as e:
        logger.warning(f"seed_all_unified: could not load EN sets: {e}")

    # Try to fetch EU sets and reconcile
    try:
        eu_sets = await opcg_api.get_sets(tier="elite")
        for s in eu_sets:
            code = s.get("code", "")
            name = s.get("name", "")
            if code and code not in sets_to_process:
                sets_to_process[code] = name
    except Exception as e:
        logger.warning(f"seed_all_unified: could not load EU sets: {e}")

    total_cards = 0
    total_sealed = 0
    processed = 0

    for code, name in sets_to_process.items():
        try:
            cards_count = await aggregate_set(code, name)
            total_cards += cards_count
            sealed_count = await aggregate_sealed(code, name)
            total_sealed += sealed_count
            processed += 1
            logger.info(
                f"seed_all_unified: [{processed}/{len(sets_to_process)}] {code} "
                f"— {cards_count} cards, {sealed_count} sealed"
            )
        except Exception as e:
            logger.error(f"seed_all_unified: error for {code}: {e}")

        await asyncio.sleep(3.5)  # Rate limit: TCG Price Lookup Free = 200 req/day = ~1 req/3s safe

    logger.info(
        f"seed_all_unified complete: {total_cards} cards, {total_sealed} sealed "
        f"across {processed}/{len(sets_to_process)} sets"
    )

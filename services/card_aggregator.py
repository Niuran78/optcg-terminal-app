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
from services.price_snapshots import take_daily_snapshots

logger = logging.getLogger(__name__)

# FX conversion constants (approximate; update periodically)
# FX-Kurse werden ab jetzt LIVE über services/fx_rate.py geholt.
# Die folgenden Konstanten sind nur ein absoluter Notfall-Fallback wenn
# der FX-Service nie initialisiert wurde (sollte nie passieren).
# Neuen Code NIEMALS gegen diese Konstanten schreiben — stattdessen:
#   from services.fx_rate import get_usd_to_eur
#   rate = get_usd_to_eur()
from services.fx_rate import get_usd_to_eur as _get_usd_to_eur, get_eur_to_usd as _get_eur_to_usd

# Backwards-kompatible "Konstanten" als Properties — lazy-lookup des Live-Kurses.
# Das Objekt verhält sich in allen Standardoperationen wie ein float.
class _LiveRate:
    def __init__(self, getter):
        self._getter = getter
    def __float__(self):      return self._getter()
    def __int__(self):        return int(self._getter())
    def __bool__(self):       return bool(self._getter())
    def __format__(self, s):  return format(self._getter(), s)
    def __str__(self):        return str(self._getter())
    def __repr__(self):       return f"<LiveRate {self._getter():.4f}>"
    # Arithmetik mit Skalaren:
    def __mul__(self, o):     return self._getter() * o
    def __rmul__(self, o):    return o * self._getter()
    def __truediv__(self, o): return self._getter() / o
    def __rtruediv__(self, o):return o / self._getter()
    def __add__(self, o):     return self._getter() + o
    def __radd__(self, o):    return o + self._getter()
    def __sub__(self, o):     return self._getter() - o
    def __rsub__(self, o):    return o - self._getter()
    def __neg__(self):        return -self._getter()
    # Vergleiche:
    def __lt__(self, o):      return self._getter() < o
    def __le__(self, o):      return self._getter() <= o
    def __gt__(self, o):      return self._getter() > o
    def __ge__(self, o):      return self._getter() >= o
    def __eq__(self, o):      return self._getter() == o
    def __ne__(self, o):      return self._getter() != o
    def __hash__(self):       return hash(self._getter())

USD_TO_EUR = _LiveRate(_get_usd_to_eur)
EUR_TO_USD = _LiveRate(_get_eur_to_usd)

# ─── Set mapping: EN slug ↔ RapidAPI set ID ─────────────────────────────────
# Keys are the set codes that appear in card numbers (e.g. "OP01").
# en_slug: slug used by TCG Price Lookup API.
# rapidapi_id: numeric set ID used by RapidAPI.
# name: canonical English name for fuzzy matching.
SET_MAPPING: dict[str, dict] = {
    # ── Booster Sets ──────────────────────────────────────────────────────
    "OP01": {"en_slug": "onepiece--romance-dawn", "rapidapi_id": "368", "name": "Romance Dawn"},
    "OP02": {"en_slug": "onepiece--paramount-war", "rapidapi_id": "369", "name": "Paramount War"},
    "OP03": {"en_slug": "onepiece--pillars-of-strength", "rapidapi_id": "370", "name": "Pillars of Strength"},
    "OP04": {"en_slug": "onepiece--kingdoms-of-intrigue", "rapidapi_id": "371", "name": "Kingdoms of Intrigue"},
    "OP05": {"en_slug": "onepiece--awakening-of-the-new-era", "rapidapi_id": "372", "name": "Awakening of the New Era"},
    "OP06": {"en_slug": "onepiece--wings-of-the-captain", "rapidapi_id": "373", "name": "Wings of the Captain"},
    "OP07": {"en_slug": "onepiece--500-years-in-the-future", "rapidapi_id": "391", "name": "500 Years in the Future"},
    "OP08": {"en_slug": "onepiece--two-legends", "rapidapi_id": "386", "name": "Two Legends"},
    "OP09": {"en_slug": "onepiece--emperors-in-the-new-world", "rapidapi_id": "366", "name": "Emperors in the New World"},
    "OP10": {"en_slug": "", "rapidapi_id": "364", "name": "Royal Blood"},
    "OP11": {"en_slug": "", "rapidapi_id": "362", "name": "A Fist of Divine Speed"},
    "OP12": {"en_slug": "", "rapidapi_id": "361", "name": "Legacy of the Master"},
    "OP13": {"en_slug": "", "rapidapi_id": "350", "name": "Carrying on His Will"},
    "OP14": {"en_slug": "", "rapidapi_id": "348", "name": "The Azure Sea's Seven"},
    "OP15": {"en_slug": "", "rapidapi_id": "404", "name": "Adventure on Kami's Island"},
    # ── Extra / Premium Boosters ──────────────────────────────────────────
    "EB01": {"en_slug": "onepiece--extra-booster-memorial-collection", "rapidapi_id": "390", "name": "Memorial Collection"},
    "EB02": {"en_slug": "", "rapidapi_id": "359", "name": "Anime 25th Collection"},
    "EB03": {"en_slug": "", "rapidapi_id": "394", "name": "Heroines Edition"},
    "PRB01": {"en_slug": "onepiece--premium-booster-the-best", "rapidapi_id": "367", "name": "Premium Booster: The Best"},
    "PRB02": {"en_slug": "", "rapidapi_id": "351", "name": "The Best Vol.2"},
    # ── Starter Decks ─────────────────────────────────────────────────────
    "ST01": {"en_slug": "onepiece--starter-deck-straw-hat-crew", "rapidapi_id": "357", "name": "Straw Hat Crew"},
    "ST02": {"en_slug": "onepiece--starter-deck-worst-generation", "rapidapi_id": "358", "name": "Worst Generation"},
    "ST03": {"en_slug": "", "rapidapi_id": "374", "name": "The Seven Warlords of the Sea"},
    "ST04": {"en_slug": "", "rapidapi_id": "375", "name": "Animal Kingdom Pirates"},
    "ST05": {"en_slug": "", "rapidapi_id": "395", "name": "ONE PIECE FILM edition"},
    "ST06": {"en_slug": "", "rapidapi_id": "376", "name": "Absolute Justice"},
    "ST07": {"en_slug": "", "rapidapi_id": "377", "name": "Big Mom Pirates"},
    "ST08": {"en_slug": "", "rapidapi_id": "393", "name": "Monkey D. Luffy"},
    "ST09": {"en_slug": "", "rapidapi_id": "378", "name": "Yamato"},
    "ST10": {"en_slug": "", "rapidapi_id": "379", "name": "Ultra Deck: The Three Captains"},
    "ST11": {"en_slug": "", "rapidapi_id": "392", "name": "Uta"},
    "ST12": {"en_slug": "", "rapidapi_id": "380", "name": "Zoro & Sanji"},
    "ST13": {"en_slug": "", "rapidapi_id": "388", "name": "Ultra Deck: The Three Brothers"},
    "ST14": {"en_slug": "", "rapidapi_id": "387", "name": "3D2Y"},
    "ST15": {"en_slug": "", "rapidapi_id": "385", "name": "RED Edward.Newgate"},
    "ST16": {"en_slug": "", "rapidapi_id": "384", "name": "GREEN Uta"},
    "ST17": {"en_slug": "", "rapidapi_id": "383", "name": "BLUE Donquixote Doflamingo"},
    "ST18": {"en_slug": "", "rapidapi_id": "382", "name": "PURPLE Monkey.D.Luffy"},
    "ST19": {"en_slug": "", "rapidapi_id": "389", "name": "BLACK Smoker"},
    "ST20": {"en_slug": "", "rapidapi_id": "381", "name": "YELLOW Charlotte Katakuri"},
    "ST21": {"en_slug": "", "rapidapi_id": "365", "name": "EX Gear 5"},
    "ST22": {"en_slug": "", "rapidapi_id": "360", "name": "Ace & Newgate"},
    "ST23": {"en_slug": "", "rapidapi_id": "353", "name": "RED Shanks"},
    "ST24": {"en_slug": "", "rapidapi_id": "352", "name": "GREEN Jewelry Bonney"},
    "ST25": {"en_slug": "", "rapidapi_id": "354", "name": "BLUE Buggy"},
    "ST26": {"en_slug": "", "rapidapi_id": "355", "name": "PURPLE/BLACK Monkey.D.Luffy"},
    "ST27": {"en_slug": "", "rapidapi_id": "356", "name": "BLACK Marshall.D.Teach"},
    "ST28": {"en_slug": "", "rapidapi_id": "363", "name": "GREEN/YELLOW Yamato"},
    "ST29": {"en_slug": "", "rapidapi_id": "349", "name": "Egghead"},
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
    if v in ("normal", "standard", "regular", "singles", ""):
        return "Normal"
    if "foil" in v or "holo" in v:
        return "Foil"
    if "alternate" in v or "alt art" in v or "alternate art" in v:
        return "Alternate Art"
    if "leader" in v:
        return "Leader"
    return variant.strip().title()


def _variant_from_rapidapi(card: dict) -> str:
    """Derive variant from RapidAPI card data.

    RapidAPI uses 'version' (v1 = Normal, v2+ = Alt Art), not 'variant'.
    The 'type' field is always 'singles' and MUST NOT be used as variant.
    """
    version = (card.get("version") or "").strip().lower()
    if version in ("", "v1"):
        return "Normal"
    if version == "v2":
        return "Alternate Art"
    if version == "v3":
        return "Alternate Art 2"
    return _normalize_variant(version)


def _safe_int(v) -> Optional[int]:
    """Convert a value to int, returning None if not possible."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _cents_to_eur(v, card_number: Optional[str] = None, set_code: Optional[str] = None) -> Optional[float]:
    """Normalize RapidAPI price to EUR.

    RapidAPI is inconsistent — some prices are in Eurocent, some in EUR,
    even within the same set type. The only reliable signal is the VALUE:
    - Values >= 100 are Eurocent (no OPTCG card averages > €100)
    - Values < 100 are EUR

    card_number and set_code params kept for future use but not used
    in the heuristic (API is too inconsistent for set-based detection).
    """
    if v is None:
        return None
    try:
        f = float(v)
        if f <= 0:
            return None
        if f >= 100:
            return round(f / 100.0, 2)  # Eurocent → EUR
        return round(f, 2)               # Already EUR
    except (ValueError, TypeError):
        return None


def _extract_eu_card_prices(card: dict, set_code: Optional[str] = None) -> dict:
    """Extract EU (Cardmarket) prices from a RapidAPI card dict."""
    prices = card.get("prices", {}) or {}
    cm = prices.get("cardmarket", {}) or {}
    # Use card_number prefix for cent detection (more reliable than set_code)
    card_num = str(card.get("card_number") or "").upper()

    return {
        "eu_cardmarket_7d_avg": _cents_to_eur(cm.get("7d_average"), card_num, set_code),
        "eu_cardmarket_30d_avg": _cents_to_eur(cm.get("30d_average"), card_num, set_code),
        "eu_cardmarket_lowest": _cents_to_eur(cm.get("lowest_near_mint") or cm.get("lowest"), card_num, set_code),
    }


def _extract_eu_product_prices(product: dict, set_code: Optional[str] = None) -> dict:
    """Extract EU product prices from a RapidAPI product dict.
    
    Sealed products (booster boxes, cases) are ALWAYS in EUR from RapidAPI.
    No cent conversion needed — a Booster Box at 330.27 means €330.27.
    """
    prices = product.get("prices", {}) or {}
    cm = prices.get("cardmarket", {}) or {}

    def _safe_eur(v) -> Optional[float]:
        if v is None:
            return None
        try:
            f = float(v)
            return round(f, 2) if f > 0 else None
        except (ValueError, TypeError):
            return None

    lowest = _safe_eur(cm.get("lowest_near_mint") or cm.get("lowest"))
    avg_30 = _safe_eur(cm.get("30d_average"))
    avg_7 = _safe_eur(cm.get("7d_average"))

    # Fallback: use the pre-extracted _cardmarket_price from opcg_api cache
    if lowest is None and avg_30 is None and avg_7 is None:
        fallback_price = product.get("_cardmarket_price")
        if fallback_price is not None:
            lowest = _safe_eur(fallback_price)

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


async def aggregate_set(set_code: str, set_name: str, skip_en: bool = False) -> int:
    """Merge EN and EU card data for a given set into cards_unified.

    Returns the number of records upserted.
    If skip_en is True, only EU data is fetched (used when EN rate limit is hit).
    """
    mapping = SET_MAPPING.get(set_code.upper(), {})
    en_slug = mapping.get("en_slug", "")
    rapidapi_id = mapping.get("rapidapi_id", "")

    upserted = 0

    # ── Fetch EN cards ──────────────────────────────────────────────────────
    en_cards: list[dict] = []
    if en_slug and not skip_en:
        try:
            en_cards = await tcg_price_lookup.get_en_cards(en_slug)
            logger.info(f"Aggregator [{set_code}]: {len(en_cards)} EN cards from TCG Price Lookup")
        except Exception as e:
            logger.warning(f"Aggregator [{set_code}]: EN fetch failed: {e}")
    elif skip_en:
        logger.info(f"Aggregator [{set_code}]: skipping EN fetch (rate limit reached)")

    # ── Fetch EU cards ──────────────────────────────────────────────────────
    eu_cards: list[dict] = []
    if rapidapi_id:
        try:
            eu_cards = await opcg_api.get_cards(rapidapi_id, tier="elite")
            # Debug: check first EU card's price structure
            if eu_cards:
                sample = eu_cards[0]
                sample_prices = sample.get("prices", {})
                sample_cm = (sample_prices or {}).get("cardmarket", {})
                logger.info(
                    f"Aggregator [{set_code}]: {len(eu_cards)} EU cards from RapidAPI. "
                    f"Sample card_number={sample.get('card_number')}, "
                    f"7d_avg={sample_cm.get('7d_average') if sample_cm else 'N/A'}, "
                    f"version={sample.get('version')}"
                )
            else:
                logger.info(f"Aggregator [{set_code}]: 0 EU cards from RapidAPI")
        except Exception as e:
            logger.warning(f"Aggregator [{set_code}]: EU fetch failed: {e}")

    # ── Build EU lookup: card_id + normalized_variant → card dict ───────────
    eu_lookup: dict[tuple, dict] = {}
    for card in eu_cards:
        card_num = str(card.get("card_number") or "").upper()
        if not card_num:
            continue
        variant = _variant_from_rapidapi(card)
        eu_lookup[(card_num, variant)] = card
        # Also index with "Normal" fallback so EN cards always find a match
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
            eu_prices = _extract_eu_card_prices(eu_card, set_code) if eu_card else {
                "eu_cardmarket_7d_avg": None,
                "eu_cardmarket_30d_avg": None,
                "eu_cardmarket_lowest": None,
            }

            rapidapi_card_id = None
            cardmarket_id = None
            if eu_card:
                rapidapi_card_id = str(eu_card.get("id") or eu_card.get("_id") or "")
                cm_id_raw = eu_card.get("cardmarket_id") or eu_card.get("cardmarketId")
                try:
                    cardmarket_id = int(cm_id_raw) if cm_id_raw is not None else None
                except (ValueError, TypeError):
                    cardmarket_id = None

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
                _safe_int(en_card.get("tcgplayer_id")),
                cardmarket_id,
            )
            upserted += 1

        # Also upsert EU-only cards (no EN counterpart found)
        en_known: set[tuple] = {
            (_normalize_card_id(c.get("card_id", "")), _normalize_variant(c.get("variant", "Normal")))
            for c in en_cards
        }
        for eu_card in eu_cards:
            card_num = str(eu_card.get("card_number") or "").upper()
            if not card_num:
                continue
            variant = _variant_from_rapidapi(eu_card)
            if (card_num, variant) in en_known:
                continue  # Already handled above

            eu_prices = _extract_eu_card_prices(eu_card, set_code)
            name = eu_card.get("name") or eu_card.get("card_name") or ""
            image_url = eu_card.get("image") or eu_card.get("image_url") or ""
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
                    name=COALESCE(NULLIF(EXCLUDED.name,''), cards_unified.name),
                    image_url=COALESCE(NULLIF(EXCLUDED.image_url,''), cards_unified.image_url),
                    rarity=COALESCE(NULLIF(EXCLUDED.rarity,''), cards_unified.rarity),
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
                _safe_int(eu_card.get("cardmarket_id") or eu_card.get("cardmarketId")),
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
            return "booster box"
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
            prices = _extract_eu_product_prices(product, set_code)

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

    1. Flush stale caches so we fetch fresh API data.
    2. Fetch EN sets from TCG Price Lookup.
    3. Augment with EU sets from RapidAPI.
    4. For each known set, run aggregate_set() + aggregate_sealed().
    5. Rate limit: 3.5s between sets.
    """
    logger.info("seed_all_unified: starting multi-source data seed...")

    # Flush stale caches to force fresh API fetches
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM cards_cache")
            await conn.execute("DELETE FROM products_cache")
            logger.info("seed_all_unified: flushed cards_cache + products_cache")
    except Exception as e:
        logger.warning(f"seed_all_unified: could not flush caches: {e}")

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
    skipped = 0
    errors = []
    en_requests_made = 0
    EN_REQUEST_LIMIT = 190  # Stay below TCG Price Lookup free plan (200 req/day)

    # Pre-check existing card counts per set for skip logic
    existing_counts: dict[str, int] = {}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT set_code, COUNT(*) as cnt FROM cards_unified GROUP BY set_code"
            )
            existing_counts = {r["set_code"]: r["cnt"] for r in rows}
    except Exception as e:
        logger.warning(f"seed_all_unified: could not check existing counts: {e}")

    for code, name in sets_to_process.items():
        # Skip sets that already have ≥10 cards (fast re-seed)
        if existing_counts.get(code, 0) >= 10:
            skipped += 1
            logger.info(
                f"seed_all_unified: [{processed + skipped}/{len(sets_to_process)}] "
                f"SKIP {code} ({existing_counts[code]} cards already in DB)"
            )
            continue

        logger.info(
            f"seed_all_unified: [{processed + skipped + 1}/{len(sets_to_process)}] "
            f"Processing {code} ({name})..."
        )

        skip_en = en_requests_made >= EN_REQUEST_LIMIT
        try:
            cards_count = await aggregate_set(code, name, skip_en=skip_en)
            if not skip_en:
                en_requests_made += 1  # Each set = ~1 API call (paginated internally)
            total_cards += cards_count
        except Exception as e:
            logger.error(f"seed_all_unified: card error for {code}: {e}")
            errors.append(f"{code}/cards: {e}")
            cards_count = 0

        try:
            sealed_count = await aggregate_sealed(code, name)
            total_sealed += sealed_count
        except Exception as e:
            logger.error(f"seed_all_unified: sealed error for {code}: {e}")
            errors.append(f"{code}/sealed: {e}")
            sealed_count = 0

        processed += 1
        logger.info(
            f"seed_all_unified: [{processed + skipped}/{len(sets_to_process)}] {code} "
            f"— {cards_count} cards, {sealed_count} sealed "
            f"(EN reqs: {en_requests_made}/{EN_REQUEST_LIMIT})"
        )

        await asyncio.sleep(3.5)  # Rate limit between sets

    # Log final summary
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM cards_unified")
        with_eu = await conn.fetchval("SELECT COUNT(*) FROM cards_unified WHERE eu_cardmarket_7d_avg IS NOT NULL")
        with_en = await conn.fetchval("SELECT COUNT(*) FROM cards_unified WHERE en_tcgplayer_market IS NOT NULL")

    logger.info(
        f"seed_all_unified complete: {total_cards} upserts across {processed}/{len(sets_to_process)} sets "
        f"({skipped} skipped). "
        f"DB totals: {total} cards, {with_eu} with EU prices, {with_en} with EN prices. "
        f"Errors: {len(errors)}"
    )
    if errors:
        for err in errors[:10]:
            logger.warning(f"  - {err}")

    # Take daily price snapshots after seed completes
    try:
        snapshot_count = await take_daily_snapshots()
        logger.info(f"seed_all_unified: captured {snapshot_count} daily price snapshots")
    except Exception as e:
        logger.warning(f"seed_all_unified: daily snapshot failed: {e}")

    # Check price alerts against updated prices
    try:
        from api.alerts import check_alerts_after_update
        triggered = await check_alerts_after_update()
        logger.info(f"seed_all_unified: {triggered} price alert(s) triggered")
    except Exception as e:
        logger.warning(f"seed_all_unified: alert check failed: {e}")

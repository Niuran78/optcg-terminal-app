"""Generate marketplace buy-links from product IDs + slug-generation.

TCGPlayer uses numeric product IDs directly in the URL.
Cardmarket uses slug-based URLs (set-slug / card-slug). No numeric ID lookup.
PriceCharting uses numeric game IDs.
"""
import re
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# TCGPlayer — numeric ID works as URL segment
# ─────────────────────────────────────────────────────────────────────────────
def tcgplayer_url(tcgplayer_id: Optional[int]) -> Optional[str]:
    """TCGPlayer product URL from numeric ID."""
    if not tcgplayer_id:
        return None
    return f"https://www.tcgplayer.com/product/{int(tcgplayer_id)}?Language=English"


def tcgplayer_search_url(card_name: str, card_id: Optional[str] = None) -> Optional[str]:
    """Fallback: TCGPlayer search URL when no numeric ID known."""
    if not card_name:
        return None
    q = f"{card_name} {card_id}" if card_id else card_name
    q = re.sub(r"[^\w\s-]", " ", q).strip()
    q = re.sub(r"\s+", "+", q)
    return f"https://www.tcgplayer.com/search/one-piece-card-game/product?q={q}"


# ─────────────────────────────────────────────────────────────────────────────
# Cardmarket — slug-based URLs (numeric IDs don't work)
# Pattern: /en/OnePiece/Products/Singles/{Set-Slug}/{Card-Name-Slug-CardId-Variant}
# ─────────────────────────────────────────────────────────────────────────────

# Hand-curated map from our set_code to Cardmarket set-slug.
# Based on cardmarket.com/en/OnePiece/Expansions observation.
# Cardmarket JP-suffix override: most JP sets end with '-Japanese', but a few
# older Starter Decks use '-Non-English'. Keyed by the EN set slug.
_JP_SET_SUFFIX_OVERRIDES: dict[str, str] = {
    "Starter-Deck-The-Seven-Warlords-of-the-Sea": "-Non-English",
    # (add more exceptions here as we discover them)
    "Unnumbered-Promos": "-Japanese",  # confirmed Apr 2026
}

CM_SET_SLUGS: dict[str, str] = {
    "OP01": "Romance-Dawn",
    "OP02": "Paramount-War",
    "OP03": "Pillars-of-Strength",
    "OP04": "Kingdoms-of-Intrigue",
    "OP05": "Awakening-of-the-New-Era",
    "OP06": "Wings-of-the-Captain",
    "OP07": "500-Years-into-the-Future",  # verified: 'into', not 'in'
    "OP08": "Two-Legends",
    "OP09": "Emperors-in-the-New-World",
    "OP10": "Royal-Blood",
    "OP11": "A-Fist-of-Divine-Speed",
    "OP12": "Legacy-of-the-Master",
    "OP13": "Carrying-on-his-Will",
    "OP14": "The-Azure-Seas-Seven",  # verified
    "OP15": "Adventure-on-Kamis-Island",  # verified
    "EB01": "Extra-Booster-Memorial-Collection",
    "EB02": "Extra-Booster-Anime-25th-Collection",
    "EB03": "Extra-Booster-Heroines-Edition",
    "EB04": "Egghead-Crisis-Asia-Region-Legal",  # verified: no EN version, only JP Asia-Legal
    "PRB01": "Premium-Booster-The-Best",
    "PRB02": "The-Best-Vol2",  # verified: short name, not 'Premium-Booster-'
    "ST01": "Starter-Deck-Straw-Hat-Crew",
    "ST02": "Starter-Deck-Worst-Generation",
    "ST03": "Starter-Deck-The-Seven-Warlords-of-the-Sea",
    "ST04": "Starter-Deck-Animal-Kingdom-Pirates",
    "ST05": "Starter-Deck-ONE-PIECE-FILM-edition",
    "ST06": "Starter-Deck-Absolute-Justice",
    "ST07": "Starter-Deck-Big-Mom-Pirates",
    "ST08": "Starter-Deck-Monkey-D-Luffy",
    "ST09": "Starter-Deck-Yamato",
    "ST10": "Ultra-Deck-The-Three-Captains",
    "ST11": "Starter-Deck-Uta",
    "ST12": "Starter-Deck-ST12",
    "ST13": "Ultra-Deck-The-Three-Brothers",
    "ST14": "Starter-Deck-3D2Y",
    "ST15": "Starter-Deck-RED-Edward-Newgate",
    "ST16": "Starter-Deck-GREEN-Uta",
    "ST17": "Starter-Deck-BLUE-Donquixote-Doflamingo",
    "ST18": "Starter-Deck-PURPLE-Monkey-D-Luffy",
    "ST19": "Starter-Deck-Smoker",
    "ST20": "Starter-Deck-Charlotte-Katakuri",
    "ST21": "Starter-Deck-Gear5",
    "ST22": "Starter-Deck-Ace-and-Newgate",
    "ST23": "Starter-Deck-Red-Shanks",
    "ST24": "Starter-Deck-Green-Jewelry-Bonney",
    "ST25": "Starter-Deck-Blue-Buggy",
    "ST26": "Starter-Deck-Purple-Monkey-D-Luffy",
    "ST27": "Starter-Deck-Black-Marshall-D-Teach",
    "ST28": "Starter-Deck-Yellow-Yamato",
    "ST29": "Starter-Deck-Egghead",
    "ST30": "Starter-Deck-Luffy-and-Ace",
}


def _slugify_card(name: str, card_id: str, variant: Optional[str] = None) -> str:
    """Convert 'Monkey.D.Luffy' + 'OP01-024' + 'Normal' → 'MonkeyDLuffy-OP01-024-V1'

    Based on observed Cardmarket pattern:
      - Strip dots, hyphens (except in card_id), spaces
      - Append 'V1' for Normal variants (Cardmarket default)
      - 'Alternate Art' → 'V2', 'V3' ...  (approximation, may need tuning)
    """
    if not name:
        return ""
    # Remove parenthetical variant suffix from name: "Luffy (Alternate Art)" → "Luffy"
    name = re.sub(r"\s*\(.*?\)\s*", "", name)
    # Replace special chars, keep alphanum only
    name_slug = re.sub(r"[^\w]", "", name)

    # Variant suffix — Cardmarket uses V1, V2, V3, V4…
    # Rule: Normal=V1, Alternate Art=V2, Alternate Art 2=V3, Alternate Art 3=V4,
    # Alternate Art 4=V5, and so on. Explicit Vn labels (e.g. 'V4') pass through.
    variant_suffix = "V1"
    if variant:
        v = variant.lower().strip()
        m_v = re.match(r"^v(\d+)$", v)
        m_alt = re.match(r"^(?:alternate art|alt art|parallel)(?:\s+(\d+))?$", v)
        if m_v:
            variant_suffix = f"V{m_v.group(1)}"
        elif m_alt:
            n = int(m_alt.group(1) or 1)  # 'Alternate Art' alone = V2, +1 per counter
            variant_suffix = f"V{n + 1}"
        elif "foil" in v and "normal" not in v:
            # Foil reprints typically live on the last V slot for that card
            variant_suffix = "V4"
    return f"{name_slug}-{card_id}-{variant_suffix}"


def _canonical_set_from_card_id(card_id: str) -> Optional[str]:
    """Derive the original set_code from a card_id like 'OP13-120' → 'OP13'.

    Handles prefixes OP/EB/PRB/ST/P.
    """
    if not card_id:
        return None
    m = re.match(r"^([A-Z]+\d+)-\d+", card_id.upper())
    return m.group(1) if m else None


# Variants that are Prize / Promo cards — listed on Cardmarket under
# /Unnumbered-Promos/ instead of the normal set slug.
# V5+ are almost always tournament-prize or anniversary promos.
PROMO_VARIANTS = {"V5", "V6", "V7", "V8", "V9", "V10", "SP", "SP Gold", "SP Silver", "Top Prize",
                  "Manga", "Red Manga", "Pre-Release", "Magazine", "2nd Anniversary", "Wanted"}


def _is_promo_variant(variant: Optional[str]) -> bool:
    if not variant:
        return False
    return variant.strip() in PROMO_VARIANTS


def cardmarket_card_url(
    name: Optional[str],
    card_id: Optional[str],
    set_code: Optional[str],
    variant: Optional[str] = None,
    language: Optional[str] = None,
) -> Optional[str]:
    """Build Cardmarket single-card URL from our fields.

    Cardmarket uses two separate product pages per card:
      EN regular: /Singles/{Set-Slug}/{card-slug}
      JP regular: /Singles/{Set-Slug}-Non-English/{card-slug}
      EN promos:  /Singles/Unnumbered-Promos/{card-slug}
      JP promos:  /Singles/Unnumbered-Promos-Non-English/{card-slug}

    Uses the card_id prefix (e.g. 'OP13' in OP13-120) as the canonical set.
    Reprints in other sets share the original card ID's base slug.
    Prize/Promo variants (V5+) route to /Unnumbered-Promos/.
    """
    if not name or not card_id or not set_code:
        return None

    # Promo/prize variants live on a different Cardmarket page
    if _is_promo_variant(variant):
        base_slug = "Unnumbered-Promos"
    else:
        canonical = _canonical_set_from_card_id(card_id) or set_code.upper()
        base_slug = CM_SET_SLUGS.get(canonical)
        if not base_slug:
            base_slug = CM_SET_SLUGS.get(set_code.upper())
        if not base_slug:
            return cardmarket_search_url(f"{name} {card_id}")

    # Append JP suffix.
    # Cardmarket is INCONSISTENT about this: most JP sets use '-Japanese', but
    # a handful of older Starter Decks (notably ST03) use '-Non-English' in the
    # URL. _JP_SET_SUFFIX_OVERRIDES captures these exceptions.
    if language and language.upper() == "JP":
        base_slug = f"{base_slug}{_JP_SET_SUFFIX_OVERRIDES.get(base_slug, '-Japanese')}"
    card_slug = _slugify_card(name, card_id, variant)
    if not card_slug:
        return None
    return f"https://www.cardmarket.com/en/OnePiece/Products/Singles/{base_slug}/{card_slug}"


def cardmarket_search_url(query: str) -> Optional[str]:
    """Fallback: Cardmarket search URL when we can't build the exact slug."""
    if not query:
        return None
    q = re.sub(r"[^\w\s-]", " ", query).strip()
    q = re.sub(r"\s+", "+", q)
    return f"https://www.cardmarket.com/en/OnePiece/Products/Search?searchString={q}"


def cardmarket_sealed_url(
    product_type: Optional[str],
    set_code: Optional[str],
    set_name: Optional[str] = None,
    language: Optional[str] = None,
) -> Optional[str]:
    """Build Cardmarket sealed-product URL.

    Cardmarket uses separate product pages per language:
      EN: /Booster-Boxes/{set-slug}-Booster-Box
      JP: /Booster-Boxes/{set-slug}-Booster-Box-Non-English
    """
    if not set_code:
        return None
    set_slug = CM_SET_SLUGS.get(set_code.upper())
    if not set_slug:
        return None

    # JP suffix for sealed products mirrors the singles logic (see above).
    if language and language.upper() == "JP":
        lang_suffix = _JP_SET_SUFFIX_OVERRIDES.get(set_slug, "-Japanese")
    else:
        lang_suffix = ""
    ptype = (product_type or "").lower()
    if "booster box" in ptype:
        return f"https://www.cardmarket.com/en/OnePiece/Products/Booster-Boxes/{set_slug}-Booster-Box{lang_suffix}"
    if "booster" in ptype or "pack" in ptype:
        return f"https://www.cardmarket.com/en/OnePiece/Products/Booster/{set_slug}-Booster-Pack{lang_suffix}"
    if "case" in ptype:
        # Cases often sold as bundles; fall back to booster-box page
        return f"https://www.cardmarket.com/en/OnePiece/Products/Booster-Boxes/{set_slug}-Booster-Box{lang_suffix}"
    if "starter" in ptype or "deck" in ptype:
        return f"https://www.cardmarket.com/en/OnePiece/Products/Starter-Decks/{set_slug}{lang_suffix}"
    # Generic search fallback
    if set_name:
        return cardmarket_search_url(f"{set_name} {product_type or ''}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PriceCharting — numeric ID in URL works
# ─────────────────────────────────────────────────────────────────────────────
def pricecharting_url(pricecharting_id: Optional[str]) -> Optional[str]:
    """PriceCharting product URL from game ID."""
    if not pricecharting_id:
        return None
    return f"https://www.pricecharting.com/game/{pricecharting_id}"


# ─────────────────────────────────────────────────────────────────────────────
# Combined builders
# ─────────────────────────────────────────────────────────────────────────────

def build_card_links(row: dict) -> dict:
    """Build ALL marketplace links for a card row.

    Returns a dict with TCGPlayer + PriceCharting URLs (language-agnostic)
    PLUS BOTH Cardmarket URLs (en + jp) so the UI can offer both.
    """
    tcg = tcgplayer_url(row.get("tcgplayer_id"))
    if not tcg:
        tcg = tcgplayer_search_url(row.get("name", ""), row.get("card_id"))

    name = row.get("name")
    cid = row.get("card_id")
    sc = row.get("set_code")
    var = row.get("variant")

    cm_en = cardmarket_card_url(name, cid, sc, var, language="EN")
    cm_jp = cardmarket_card_url(name, cid, sc, var, language="JP")

    pc = pricecharting_url(row.get("pricecharting_id"))

    # Default 'cardmarket' = EN (most common on the Browser tab). JP exposed too.
    return {
        "tcgplayer": tcg,
        "cardmarket": cm_en,
        "cardmarket_en": cm_en,
        "cardmarket_jp": cm_jp,
        "pricecharting": pc,
    }


def build_sealed_links(row: dict) -> dict:
    """Build all marketplace links for a sealed product row."""
    cm = cardmarket_sealed_url(
        row.get("product_type"),
        row.get("set_code"),
        row.get("set_name"),
        row.get("language"),
    )
    pc = pricecharting_url(row.get("pricecharting_id"))

    return {
        "cardmarket": cm,
        "pricecharting": pc,
    }

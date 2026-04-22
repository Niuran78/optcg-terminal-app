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
CM_SET_SLUGS: dict[str, str] = {
    "OP01": "Romance-Dawn",
    "OP02": "Paramount-War",
    "OP03": "Pillars-of-Strength",
    "OP04": "Kingdoms-of-Intrigue",
    "OP05": "Awakening-of-the-New-Era",
    "OP06": "Wings-of-the-Captain",
    "OP07": "500-Years-in-the-Future",
    "OP08": "Two-Legends",
    "OP09": "Emperors-in-the-New-World",
    "OP10": "Royal-Blood",
    "OP11": "A-Fist-of-Divine-Speed",
    "OP12": "Legacy-of-the-Master",
    "OP13": "Carrying-on-his-Will",
    "OP14": "The-Azure-Sea-s-Seven",
    "OP15": "Adventure-on-Kami-s-Island",
    "EB01": "Extra-Booster-Memorial-Collection",
    "EB02": "Extra-Booster-Anime-25th-Collection",
    "EB03": "Extra-Booster-Heroines-Edition",
    "EB04": "Extra-Booster-Egghead-Crisis",
    "PRB01": "Premium-Booster-The-Best",
    "PRB02": "Premium-Booster-The-Best-Vol2",
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
    # Rule of thumb: Normal=V1, first alt=V2, etc.
    variant_suffix = "V1"
    if variant:
        v = variant.lower().strip()
        # Explicit Vn labels (e.g. 'V4', 'V2') pass through
        m = re.match(r"^v(\d+)$", v)
        if m:
            variant_suffix = f"V{m.group(1)}"
        elif "alternate art 2" in v or "alt art 2" in v:
            variant_suffix = "V3"
        elif "alternate art" in v or "alt art" in v or "parallel" in v:
            variant_suffix = "V2"
        elif "foil" in v and "normal" not in v:
            variant_suffix = "V4"
    return f"{name_slug}-{card_id}-{variant_suffix}"


def cardmarket_card_url(
    name: Optional[str],
    card_id: Optional[str],
    set_code: Optional[str],
    variant: Optional[str] = None,
) -> Optional[str]:
    """Build Cardmarket single-card URL from our fields."""
    if not name or not card_id or not set_code:
        return None
    set_slug = CM_SET_SLUGS.get(set_code.upper())
    if not set_slug:
        # Unknown set — fall back to search page
        return cardmarket_search_url(f"{name} {card_id}")
    card_slug = _slugify_card(name, card_id, variant)
    if not card_slug:
        return None
    return f"https://www.cardmarket.com/en/OnePiece/Products/Singles/{set_slug}/{card_slug}"


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
) -> Optional[str]:
    """Build Cardmarket sealed-product URL.

    Booster Boxes: /en/OnePiece/Products/Booster-Boxes/{set-slug}-Booster-Box
    """
    if not set_code:
        return None
    set_slug = CM_SET_SLUGS.get(set_code.upper())
    if not set_slug:
        return None

    ptype = (product_type or "").lower()
    if "booster box" in ptype:
        return f"https://www.cardmarket.com/en/OnePiece/Products/Booster-Boxes/{set_slug}-Booster-Box"
    if "booster" in ptype or "pack" in ptype:
        return f"https://www.cardmarket.com/en/OnePiece/Products/Booster/{set_slug}-Booster-Pack"
    if "case" in ptype:
        # Cases often sold as bundles; fall back to booster-box page
        return f"https://www.cardmarket.com/en/OnePiece/Products/Booster-Boxes/{set_slug}-Booster-Box"
    if "starter" in ptype or "deck" in ptype:
        return f"https://www.cardmarket.com/en/OnePiece/Products/Starter-Decks/{set_slug}"
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

    Prioritizes exact URLs (via ID or slug) over search fallbacks.
    Returns dict with keys: tcgplayer, cardmarket, pricecharting (any may be None).
    """
    tcg = tcgplayer_url(row.get("tcgplayer_id"))
    if not tcg:
        tcg = tcgplayer_search_url(row.get("name", ""), row.get("card_id"))

    cm = cardmarket_card_url(
        row.get("name"),
        row.get("card_id"),
        row.get("set_code"),
        row.get("variant"),
    )

    pc = pricecharting_url(row.get("pricecharting_id"))

    return {
        "tcgplayer": tcg,
        "cardmarket": cm,
        "pricecharting": pc,
    }


def build_sealed_links(row: dict) -> dict:
    """Build all marketplace links for a sealed product row."""
    cm = cardmarket_sealed_url(
        row.get("product_type"),
        row.get("set_code"),
        row.get("set_name"),
    )
    pc = pricecharting_url(row.get("pricecharting_id"))

    return {
        "cardmarket": cm,
        "pricecharting": pc,
    }

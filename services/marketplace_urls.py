"""Generate marketplace buy-links from product IDs.

Each card/sealed product can have up to 3 marketplace links:
  - TCGPlayer (US, English)  — uses tcgplayer_id
  - Cardmarket (EU)          — uses cardmarket_id
  - PriceCharting (price ref) — uses pricecharting_id
"""
from typing import Optional


def tcgplayer_url(tcgplayer_id: Optional[int]) -> Optional[str]:
    """TCGPlayer product URL from numeric ID."""
    if not tcgplayer_id:
        return None
    return f"https://www.tcgplayer.com/product/{int(tcgplayer_id)}"


def cardmarket_url(cardmarket_id: Optional[int]) -> Optional[str]:
    """Cardmarket product URL from numeric product ID.

    Format: https://www.cardmarket.com/en/OnePiece/Products/Singles/{id}
    Cardmarket accepts ID-based lookups and redirects to the canonical slug URL.
    """
    if not cardmarket_id:
        return None
    return f"https://www.cardmarket.com/en/OnePiece/Products/Singles/{int(cardmarket_id)}"


def cardmarket_sealed_url(cardmarket_id: Optional[int]) -> Optional[str]:
    """Cardmarket sealed product URL."""
    if not cardmarket_id:
        return None
    return f"https://www.cardmarket.com/en/OnePiece/Products/Boosters/{int(cardmarket_id)}"


def pricecharting_url(pricecharting_id: Optional[str]) -> Optional[str]:
    """PriceCharting product URL from game ID.

    Format: https://www.pricecharting.com/game/{id}
    PriceCharting accepts numeric IDs in the URL path and redirects.
    """
    if not pricecharting_id:
        return None
    return f"https://www.pricecharting.com/game/{pricecharting_id}"


def build_card_links(row: dict) -> dict:
    """Build all available marketplace links for a card row."""
    return {
        "tcgplayer":     tcgplayer_url(row.get("tcgplayer_id")),
        "cardmarket":    cardmarket_url(row.get("cardmarket_id")),
        "pricecharting": pricecharting_url(row.get("pricecharting_id")),
    }


def build_sealed_links(row: dict) -> dict:
    """Build all available marketplace links for a sealed product row."""
    return {
        "cardmarket":    cardmarket_sealed_url(row.get("cardmarket_id") or row.get("rapidapi_product_id")),
        "pricecharting": pricecharting_url(row.get("pricecharting_id")),
    }

"""Expected Value (EV) calculation engine for OPTCG booster boxes.

Pull rates are community-sourced static data.
Supports both Japanese (10 packs/box) and English (24 packs/box).
"""
from dataclasses import dataclass
from typing import Optional


# ─── Pull Rate Config ──────────────────────────────────────────────────────────
# Community data — not from API.
# Rates represent expected number of pulls per box.

JP_BOX_PACKS = 10
EN_BOX_PACKS = 24

PULL_RATES = {
    "JP": {
        "packs_per_box": JP_BOX_PACKS,
        "SR": 3.5,           # Super Rare: ~3.5 per box
        "SEC": 0.5,          # Secret Rare: ~0.5 per box (1 in 2 boxes)
        "L_AA": 1 / 3,       # Leader Alternate Art: ~1 in 3 boxes
        "MANGA": 1 / 54,     # Manga Alternate Art: ~1 in 48-60 boxes (avg ~54)
        "R": 8.0,            # Rare: ~8 per box (1 per pack)
        "UC": 20.0,          # Uncommon: ~20 per box (2 per pack)
        "C": 50.0,           # Common: ~50 per box (5 per pack)
        # Special rarities
        "DON": 0,            # DON!! cards (not pulled)
        "PROMO": 0,
    },
    "EN": {
        "packs_per_box": EN_BOX_PACKS,
        "SR": 7.0,           # Super Rare: ~7 per box
        "SEC": 0.67,         # Secret Rare: ~2 in 3 boxes
        "L_AA": 2.0,         # Leader Alternate Art: ~2 per box
        "MANGA": 1 / 54,     # Manga Alternate Art: ~1 in 48-60 boxes
        "R": 24.0,           # Rare: ~24 per box (1 per pack)
        "UC": 48.0,          # Uncommon
        "C": 96.0,           # Common
        "DON": 0,
        "PROMO": 0,
    }
}

# Rarity mapping from API rarity strings to our pull rate keys
RARITY_MAP = {
    # Standard mappings
    "SR": "SR", "Super Rare": "SR", "super rare": "SR", "super_rare": "SR",
    "SEC": "SEC", "Secret Rare": "SEC", "secret rare": "SEC", "secret_rare": "SEC",
    "Secret": "SEC",
    "R": "R", "Rare": "R", "rare": "R",
    "UC": "UC", "Uncommon": "UC", "uncommon": "UC",
    "C": "C", "Common": "C", "common": "C",
    "L": "R",   # Leader cards treated like rare for EV
    "DON": "DON",
    "PROMO": "PROMO",
    # Leader AA / Manga AA are handled separately if we can detect them
    # Typically denoted as "L" + "AA" or "Manga" in name
}


def _map_rarity(rarity_str: str, card_name: str = "") -> str:
    """Map API rarity string to our internal rarity key."""
    if not rarity_str:
        return "C"

    # Check for Alternate Art/Manga variants
    name_lower = card_name.lower()
    rarity_lower = rarity_str.lower()

    if "manga" in name_lower or "manga" in rarity_lower:
        return "MANGA"
    if ("alternate art" in name_lower or "alt art" in name_lower or
            "aa" in rarity_lower or "alternate" in rarity_lower):
        if "leader" in name_lower or rarity_lower.startswith("l"):
            return "L_AA"
        return "L_AA"

    return RARITY_MAP.get(rarity_str, RARITY_MAP.get(rarity_str.upper(), "C"))


@dataclass
class EVResult:
    set_id: str
    set_name: str
    language: str
    box_cost: float               # Current market price of a sealed box
    calculated_ev: float          # EV per box in EUR
    ev_minus_box: float           # EV - box cost (profit/loss per box opened)
    ev_ratio: float               # EV / box_cost ratio
    verdict: str                  # "OPEN", "HOLD_SEALED", "BORDERLINE"
    verdict_color: str            # "positive", "negative", "warning"
    packs_per_box: int
    breakdown: list[dict]         # Per-rarity EV breakdown
    card_sample_size: int         # Number of cards used in calculation

    def to_dict(self) -> dict:
        return {
            "set_id": self.set_id,
            "set_name": self.set_name,
            "language": self.language,
            "box_cost": round(self.box_cost, 2),
            "calculated_ev": round(self.calculated_ev, 2),
            "ev_minus_box": round(self.ev_minus_box, 2),
            "ev_ratio": round(self.ev_ratio, 2),
            "verdict": self.verdict,
            "verdict_color": self.verdict_color,
            "packs_per_box": self.packs_per_box,
            "breakdown": self.breakdown,
            "card_sample_size": self.card_sample_size,
        }


def calculate_ev(
    set_id: str,
    set_name: str,
    language: str,
    cards: list[dict],
    box_cost: float,
) -> Optional[EVResult]:
    """
    Calculate Expected Value per box for a given set.

    Args:
        set_id: API set ID
        set_name: Display name of the set
        language: "JP" or "EN"
        cards: List of card dicts with _cardmarket_price and rarity fields
        box_cost: Current market price of a sealed booster box in EUR

    Returns:
        EVResult or None if insufficient data
    """
    if not cards or box_cost <= 0:
        return None

    lang = language.upper() if language else "JP"
    if lang not in PULL_RATES:
        lang = "JP"

    rates = PULL_RATES[lang]

    # Group cards by rarity and get average prices
    rarity_groups: dict[str, list[float]] = {}
    for card in cards:
        rarity_raw = card.get("rarity", card.get("card_rarity", ""))
        card_name = (card.get("name", "") or card.get("card_name", ""))
        rarity_key = _map_rarity(str(rarity_raw), str(card_name))

        price = card.get("_cardmarket_price")
        if price is None or price <= 0:
            continue

        if rarity_key not in rarity_groups:
            rarity_groups[rarity_key] = []
        rarity_groups[rarity_key].append(price)

    if not rarity_groups:
        return None

    # Calculate EV contribution per rarity
    breakdown = []
    total_ev = 0.0

    for rarity_key, prices in rarity_groups.items():
        rate = rates.get(rarity_key, 0)
        if rate <= 0:
            continue

        avg_price = sum(prices) / len(prices)
        ev_contribution = avg_price * rate

        breakdown.append({
            "rarity": rarity_key,
            "rate_per_box": round(rate, 3),
            "avg_price": round(avg_price, 2),
            "card_count": len(prices),
            "ev_contribution": round(ev_contribution, 2),
        })
        total_ev += ev_contribution

    # Sort breakdown by EV contribution desc
    breakdown.sort(key=lambda x: x["ev_contribution"], reverse=True)

    ev_minus_box = total_ev - box_cost
    ev_ratio = total_ev / box_cost if box_cost > 0 else 0

    # Verdict thresholds
    if ev_ratio >= 1.15:
        verdict = "OPEN"
        verdict_color = "positive"
    elif ev_ratio >= 0.90:
        verdict = "BORDERLINE"
        verdict_color = "warning"
    else:
        verdict = "HOLD_SEALED"
        verdict_color = "negative"

    card_sample_size = sum(len(v) for v in rarity_groups.values())

    return EVResult(
        set_id=set_id,
        set_name=set_name,
        language=lang,
        box_cost=box_cost,
        calculated_ev=total_ev,
        ev_minus_box=ev_minus_box,
        ev_ratio=ev_ratio,
        verdict=verdict,
        verdict_color=verdict_color,
        packs_per_box=rates["packs_per_box"],
        breakdown=breakdown,
        card_sample_size=card_sample_size,
    )


def calculate_custom_ev(
    cards: list[dict],
    language: str,
    box_cost: float,
    custom_pull_rates: Optional[dict] = None,
) -> Optional[dict]:
    """
    Calculate EV with custom box cost and optional custom pull rates.
    Useful for user-provided overrides.
    """
    lang = language.upper() if language else "JP"
    if lang not in PULL_RATES:
        lang = "JP"

    rates = dict(PULL_RATES[lang])
    if custom_pull_rates:
        rates.update(custom_pull_rates)

    # Temporarily create a fake set for calculation
    result = calculate_ev("custom", "Custom Calculation", lang, cards, box_cost)
    if result:
        return result.to_dict()
    return None

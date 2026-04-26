"""Pull-rate estimates for One Piece TCG sealed products.

These numbers are ESTIMATES, not officially published rates. Bandai does not
publish per-rarity pull rates for OPTCG. The values below are reverse-engineered
from community box-opening averages (~50+ box openings reported on
Reddit r/OnePieceTCG and Bandai-TCG community forums) and the known set
composition (commons + uncommons + rares per pack).

Always label any UI value derived from this table as "Schätzung" / "estimate".
Never present EV numbers as truth — they're a planning aid, not a guarantee.

Reference rules (consistent across most main sets, EN and JP):

    Pack contents (12-card pack):
        4 Commons
        3 Uncommons
        3 Rares (or 2 R + 1 SR / 2 R + 1 L)
        1 R/SR/SEC slot   <-- the "hit slot"
        1 Leader (every 12 packs ~= ~1 per box)

    Box: 24 packs (main sets, OP01-OP15)
    Extra Booster (EB): 14 packs
    Premium Booster (PRB): smaller boxes ~10-14 packs

Probability values are PER PACK (not per box).
"""

from __future__ import annotations
from typing import Optional

# ───────────────────────────────────────────────────────────────────────────
# Pull rates per pack (per main set, EN/JP equivalent)
# ───────────────────────────────────────────────────────────────────────────
PULL_RATES_PER_PACK = {
    "default": {
        # Bulk fillers (always present in every pack)
        "Common":         4.00,   # 4 commons per pack
        "Uncommon":       3.00,   # 3 uncommons per pack
        "Rare":           1.80,   # ~1.8 rares per pack on average
        # The "hit slot" — distributes across these rarities each pack
        "Super Rare":     0.18,   # ~18% chance per pack -> ~4-5 SRs per box
        "Secret Rare":    0.040,  # ~4% per pack -> ~1 in 24 packs
        "Leader":         0.083,  # 1 in 12 packs (~2 per box)
        # Alternate Art / Manga Rare = parallel bonus pull, slot variant
        "Alternate Art":  0.050,  # ~1 AA per box
        "Manga Rare":     0.012,  # rare parallel
        "Special Rare":   0.020,  # SP CARD parallel (foil-bordered)
    },
    # Extra Booster sets (smaller, denser hits) — empirically richer per pack
    "EB01": {
        "Common":         4.00,
        "Uncommon":       3.00,
        "Rare":           1.80,
        "Super Rare":     0.22,
        "Secret Rare":    0.060,
        "Leader":         0.083,
        "Alternate Art":  0.090,  # EB sets famously AA-dense
        "Manga Rare":     0.020,
    },
    "EB02": {
        "Common":         4.00,
        "Uncommon":       3.00,
        "Rare":           1.80,
        "Super Rare":     0.22,
        "Secret Rare":    0.055,
        "Leader":         0.083,
        "Alternate Art":  0.085,
        "Manga Rare":     0.020,
    },
    "EB03": {
        "Common":         4.00,
        "Uncommon":       3.00,
        "Rare":           1.80,
        "Super Rare":     0.22,
        "Secret Rare":    0.060,
        "Leader":         0.083,
        "Alternate Art":  0.090,
        "Manga Rare":     0.020,
    },
    # Premium Booster sets (PRB01, PRB02): premium pull rates, AA-heavy
    "PRB01": {
        "Common":         3.00,    # smaller pack composition
        "Uncommon":       2.00,
        "Rare":           1.50,
        "Super Rare":     0.30,    # premium = higher SR rate
        "Secret Rare":    0.080,
        "Leader":         0.100,
        "Alternate Art":  0.150,   # PRB is AA-focused
        "Manga Rare":     0.030,
    },
    "PRB02": {
        "Common":         3.00,
        "Uncommon":       2.00,
        "Rare":           1.50,
        "Super Rare":     0.30,
        "Secret Rare":    0.080,
        "Leader":         0.100,
        "Alternate Art":  0.150,
        "Manga Rare":     0.030,
    },
}

# ───────────────────────────────────────────────────────────────────────────
# Packs per Box (sealed-product specific)
# ───────────────────────────────────────────────────────────────────────────
PACKS_PER_BOX = {
    "default":    24,   # main OP sets (OP01-OP15), JP and EN
    "EB":         14,   # Extra Booster sets typically 14 packs/box
    "PRB":        14,   # Premium Booster ~14 (verify with bandai-tcg sheet)
}

# Cases (per-set, packs in case = boxes_per_case * packs_per_box)
BOXES_PER_CASE = {
    "default":    12,    # standard OPTCG case = 12 booster boxes
}


# ───────────────────────────────────────────────────────────────────────────
# Lookup helpers
# ───────────────────────────────────────────────────────────────────────────
def get_pull_rates(set_code: Optional[str]) -> dict:
    """Return per-pack pull rates for a set. Falls back to 'default'."""
    if not set_code:
        return PULL_RATES_PER_PACK["default"]
    sc = set_code.upper().strip()
    if sc in PULL_RATES_PER_PACK:
        return PULL_RATES_PER_PACK[sc]
    return PULL_RATES_PER_PACK["default"]


def get_packs_per_box(set_code: Optional[str]) -> int:
    """Return packs-per-box for a set. Heuristic by prefix."""
    if not set_code:
        return PACKS_PER_BOX["default"]
    sc = set_code.upper().strip()
    if sc.startswith("EB"):
        return PACKS_PER_BOX["EB"]
    if sc.startswith("PRB"):
        return PACKS_PER_BOX["PRB"]
    return PACKS_PER_BOX["default"]


def get_boxes_per_case(set_code: Optional[str]) -> int:
    """Return boxes-per-case for a set."""
    return BOXES_PER_CASE.get((set_code or "").upper(), BOXES_PER_CASE["default"])


# ───────────────────────────────────────────────────────────────────────────
# Rarity-name normalization
# ───────────────────────────────────────────────────────────────────────────
# DB rarity strings vary: 'SR', 'Super Rare', 'Secret Rare', 'L', 'AA',
# 'Alternate Art', 'Manga Rare'. We need to map them to the keys in
# PULL_RATES_PER_PACK so the EV-engine SQL can group consistently.
RARITY_ALIASES = {
    "common":         "Common",
    "c":              "Common",
    "uncommon":       "Uncommon",
    "uc":             "Uncommon",
    "rare":           "Rare",
    "r":              "Rare",
    "super rare":     "Super Rare",
    "sr":             "Super Rare",
    "secret rare":    "Secret Rare",
    "sec":            "Secret Rare",
    "treasure rare":  "Secret Rare",   # SEC tier; we group to keep model simple
    "tr":             "Secret Rare",
    "leader":         "Leader",
    "l":              "Leader",
    "alternate art":  "Alternate Art",
    "alt art":        "Alternate Art",
    "aa":             "Alternate Art",
    "manga rare":     "Manga Rare",
    "manga":          "Manga Rare",
    "special card":   "Special Rare",
    "sp":             "Special Rare",
    "special rare":   "Special Rare",
    "promo":          None,             # promos shouldn't contribute to box EV
    "p":              None,
}


def normalize_rarity(raw: Optional[str]) -> Optional[str]:
    """Normalize a DB rarity string to a pull-rate-table key."""
    if not raw:
        return None
    return RARITY_ALIASES.get(raw.strip().lower())

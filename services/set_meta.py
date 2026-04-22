"""Set metadata: release dates, tier/category.

Used by indicators for reprint-risk calculation (age-based) and other
set-level signals.
"""
from datetime import date

# Release dates — official English/EN release dates (BanDai TCG)
# Source: official OPTCG release schedule
SET_RELEASE_DATES: dict[str, date] = {
    # Main sets
    "OP01": date(2022, 12,  2),  # Romance Dawn
    "OP02": date(2023,  3, 10),  # Paramount War
    "OP03": date(2023,  6, 30),  # Pillars of Strength
    "OP04": date(2023,  9, 22),  # Kingdoms of Intrigue
    "OP05": date(2023, 12, 22),  # Awakening of the New Era
    "OP06": date(2024,  3, 15),  # Wings of the Captain
    "OP07": date(2024,  6, 28),  # 500 Years into the Future
    "OP08": date(2024,  9, 13),  # Two Legends
    "OP09": date(2024, 12,  6),  # Emperors in the New World
    "OP10": date(2025,  3, 14),  # Royal Blood
    "OP11": date(2025,  6, 27),  # A Fist of Divine Speed
    "OP12": date(2025,  9, 12),  # Legacy of the Master
    "OP13": date(2025, 12,  5),  # Carrying on His Will
    "OP14": date(2026,  3, 13),  # The Azure Sea's Seven
    "OP15": date(2026,  3, 13),  # Adventure on Kami's Island (approx)

    # Extra Boosters
    "EB01": date(2023, 11, 25),  # Memorial Collection
    "EB02": date(2024, 10, 25),  # Anime 25th Collection
    "EB03": date(2025,  4, 25),  # Heroines Edition
    "EB04": date(2025, 10, 25),  # Egghead Crisis

    # Premium Boosters
    "PRB01": date(2024,  5, 31),  # The Best
    "PRB02": date(2025, 11, 28),  # The Best Vol 2

    # Starter Decks (approx release dates; some undocumented)
    "ST01": date(2022, 12,  2),
    "ST02": date(2022, 12,  2),
    "ST03": date(2022, 12,  2),
    "ST04": date(2023,  3, 10),
    "ST05": date(2023,  6, 30),
    "ST06": date(2023,  9, 22),
    "ST07": date(2023,  9, 22),
    "ST08": date(2023, 12, 22),
    "ST09": date(2023, 12, 22),
    "ST10": date(2024,  3, 15),
    "ST11": date(2024,  5, 31),
    "ST12": date(2024,  7, 26),
    "ST13": date(2024,  9, 13),
    "ST14": date(2024, 12,  6),
    "ST15": date(2025,  1, 31),
    "ST16": date(2025,  1, 31),
    "ST17": date(2025,  3, 14),
    "ST18": date(2025,  3, 14),
    "ST19": date(2025,  6, 27),
    "ST20": date(2025,  6, 27),
    "ST21": date(2025,  8, 29),
    "ST22": date(2025,  9, 12),
    "ST23": date(2025, 11, 28),
    "ST24": date(2025, 11, 28),
    "ST25": date(2026,  1, 30),
    "ST26": date(2026,  1, 30),
    "ST27": date(2026,  3, 13),
    "ST28": date(2026,  3, 13),
    "ST29": date(2026,  6, 12),  # approx
    "ST30": date(2026,  6, 12),  # approx
}

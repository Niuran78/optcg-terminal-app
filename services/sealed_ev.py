"""Sealed Expected-Value (EV) computation.

For a given Booster Box (set_code + language), estimate the expected EUR
value of *opening* the box, by combining:

  1. Per-pack pull-rate estimates (services/sealed_pull_rates.py)
  2. Median Cardmarket live trend price per rarity, scoped to the same
     set_code + language, sourced from cards_investable.

The result is *per-pack EV * packs/box*, which gives you an "if you opened
this box and sold every card at trend" estimate. Real-world realisation is
lower (you can't actually sell every common at trend), but this is the
industry-standard way OP-TCG investors evaluate sealed.

Output is always labelled "estimate" so the UI never mistakes it for truth.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional

from db.init import get_pool
from services.sealed_pull_rates import (
    get_pull_rates, get_packs_per_box, get_boxes_per_case, RARITY_ALIASES,
)

logger = logging.getLogger(__name__)


def _build_rarity_case_clause() -> str:
    """Build a SQL CASE expression that maps cards_investable.rarity onto
    canonical pull-rate keys (Common, Rare, Super Rare, ...).
    Returns the CASE expression as a SQL string fragment."""
    # Group aliases by canonical name so we collapse e.g. 'SR' / 'Super Rare'.
    by_canonical: dict[str, list[str]] = {}
    for alias, canon in RARITY_ALIASES.items():
        if canon is None:
            continue
        by_canonical.setdefault(canon, []).append(alias.lower())

    parts = []
    for canon, aliases in by_canonical.items():
        in_clause = ",".join(f"'{a}'" for a in aliases)
        # Escape single quotes in canon (none of ours have any, but be safe)
        canon_lit = canon.replace("'", "''")
        parts.append(f"WHEN LOWER(rarity) IN ({in_clause}) THEN '{canon_lit}'")
    return "CASE " + " ".join(parts) + " ELSE NULL END"


_RARITY_CASE_SQL = _build_rarity_case_clause()


async def _fetch_median_prices_by_rarity(
    set_code: str, language: str
) -> dict[str, float]:
    """Return {canonical_rarity: median_eur_trend} for cards in this set+lang.

    Uses cards_investable (live cm_live_trend only). Median is computed in SQL
    via PERCENTILE_CONT(0.5).
    """
    pool = await get_pool()
    sql = f"""
        SELECT
            ({_RARITY_CASE_SQL}) AS canon_rarity,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cm_live_trend) AS median_eur,
            COUNT(*) AS sample_size
        FROM cards_investable
        WHERE set_code = $1
          AND language = $2
          AND cm_live_trend > 0
        GROUP BY canon_rarity
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, set_code.upper(), language.upper())

    result: dict[str, float] = {}
    sample_sizes: dict[str, int] = {}
    for r in rows:
        canon = r["canon_rarity"]
        if not canon:
            continue
        median = r["median_eur"]
        if median is None or median <= 0:
            continue
        result[canon] = float(median)
        sample_sizes[canon] = int(r["sample_size"] or 0)
    # Stash sample sizes on the dict so callers can decide if EV is meaningful
    result["_sample_sizes"] = sample_sizes  # type: ignore[assignment]
    return result


async def compute_sealed_ev(
    set_code: str,
    language: str,
    box_price_eur: Optional[float] = None,
    product_type: str = "booster box",
) -> dict:
    """Compute Expected Value of opening a sealed product.

    Args:
        set_code:    Set code (e.g. 'OP15', 'EB03')
        language:    'EN' or 'JP'
        box_price_eur:  Current Cardmarket trend for the box (used to compute
                        EV minus box price). If None, only EV is returned.
        product_type:   'booster box' (default) or 'case' — multiplies by
                        boxes_per_case for cases.

    Returns dict with ev_per_pack_eur, ev_per_box_eur, breakdown, etc.
    Always includes 'label': 'estimate'.
    """
    set_code = set_code.upper()
    language = language.upper()

    pull_rates = get_pull_rates(set_code)
    packs_per_box = get_packs_per_box(set_code)
    boxes_per_case = get_boxes_per_case(set_code)

    medians = await _fetch_median_prices_by_rarity(set_code, language)
    sample_sizes = medians.pop("_sample_sizes", {})  # type: ignore[arg-type]

    breakdown = []
    ev_per_pack = 0.0
    # Sanity guards: a rarity bucket only counts toward EV if we have at least
    # MIN_SAMPLES live cards for it (otherwise the median is just that one
    # outlier card's price). Without this, sets with one €1234 alt-art and
    # nothing else end up with absurd 5-figure EVs.
    MIN_SAMPLES_FOR_EV = 3

    for rarity, rate in pull_rates.items():
        median_eur = medians.get(rarity)
        sample_n = sample_sizes.get(rarity, 0)

        if median_eur is None or median_eur <= 0:
            breakdown.append({
                "rarity": rarity,
                "pull_rate": round(rate, 4),
                "median_eur": None,
                "sample_size": sample_n,
                "ev_contribution": 0.0,
                "note": "no live data",
            })
            continue

        if sample_n < MIN_SAMPLES_FOR_EV:
            # Too few samples — median would just be one outlier price.
            breakdown.append({
                "rarity": rarity,
                "pull_rate": round(rate, 4),
                "median_eur": round(median_eur, 2),
                "sample_size": sample_n,
                "ev_contribution": 0.0,
                "note": f"sample too thin (<{MIN_SAMPLES_FOR_EV} cards)",
            })
            continue

        contribution = median_eur * rate
        ev_per_pack += contribution
        breakdown.append({
            "rarity": rarity,
            "pull_rate": round(rate, 4),
            "median_eur": round(median_eur, 2),
            "sample_size": sample_n,
            "ev_contribution": round(contribution, 4),
        })

    # Sort breakdown by contribution descending — most-impactful rarities first
    breakdown.sort(key=lambda x: x["ev_contribution"] or 0, reverse=True)

    multiplier = packs_per_box
    label_unit = "box"
    if product_type and product_type.lower() == "case":
        multiplier = packs_per_box * boxes_per_case
        label_unit = "case"

    ev_per_box = ev_per_pack * multiplier

    out = {
        "set_code":            set_code,
        "language":            language,
        "product_type":        product_type,
        "packs_per_unit":      multiplier,   # packs in box (or case)
        "ev_per_pack_eur":     round(ev_per_pack, 2),
        "ev_per_box_eur":      round(ev_per_box, 2),
        f"ev_per_{label_unit}_eur": round(ev_per_box, 2),  # for clarity
        "rarities_breakdown":  breakdown,
        "label":               "estimate",
        "computed_at":         datetime.now(timezone.utc).isoformat(),
        "method": (
            "median trend price per rarity * pull-rate per pack * packs/box. "
            "Pull rates are community estimates; treat as planning aid."
        ),
    }

    if box_price_eur is not None and box_price_eur > 0:
        diff = ev_per_box - box_price_eur
        out["box_price_eur"]   = round(float(box_price_eur), 2)
        out["ev_minus_box"]    = round(diff, 2)
        out["ev_pct"]          = round(diff / box_price_eur * 100.0, 1)

        # Plausibility checks: drop EVs that are clearly nonsense.
        # 1) EV > 5× box price: would have been arbitraged away if real
        # 2) EV < 30% of box price: thin samples, missing rarities, or wrong pull rate
        unreliable_reason = None
        if ev_per_box > 5.0 * float(box_price_eur):
            unreliable_reason = (
                f"computed EV (€{round(ev_per_box,2)}) exceeded 5× box price "
                f"(€{round(float(box_price_eur),2)}) — likely thin-sample bias"
            )
        elif ev_per_box < 0.3 * float(box_price_eur):
            unreliable_reason = (
                f"computed EV (€{round(ev_per_box,2)}) below 30% of box price "
                f"(€{round(float(box_price_eur),2)}) — likely missing rarities in sample"
            )
        if unreliable_reason:
            out["ev_per_box_eur"] = None
            out["ev_per_pack_eur"] = None
            out["ev_minus_box"] = None
            out["ev_pct"] = None
            out["unreliable"] = True
            out["unreliable_reason"] = unreliable_reason

    return out


async def compute_and_persist_all_ev() -> dict:
    """Compute EV for every sealed_unified row that has a live cm_live_trend
    AND is a booster box or case, then persist into expected_value_eur and
    ev_computed_at. Returns summary stats."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, set_code, language, product_type, cm_live_trend
            FROM sealed_unified
            WHERE cm_live_trend IS NOT NULL
              AND cm_live_trend > 0
              AND product_type IN ('booster box', 'case')
              AND set_code IS NOT NULL
            """
        )

    updated = 0
    skipped = 0
    failed = 0
    for r in rows:
        try:
            res = await compute_sealed_ev(
                set_code=r["set_code"],
                language=r["language"] or "JP",
                box_price_eur=float(r["cm_live_trend"]),
                product_type=r["product_type"] or "booster box",
            )
            ev = res.get("ev_per_box_eur")
            if ev is None or ev <= 0 or res.get("unreliable"):
                skipped += 1
                continue
            async with pool.acquire() as conn2:
                await conn2.execute(
                    """
                    UPDATE sealed_unified
                       SET expected_value_eur = $1,
                           ev_computed_at = NOW()
                     WHERE id = $2
                    """,
                    round(ev, 2), r["id"],
                )
            updated += 1
        except Exception as e:
            failed += 1
            logger.warning(
                f"[sealed_ev] failed for id={r['id']} "
                f"({r['set_code']}/{r['language']}): {e}"
            )

    return {
        "total":   len(rows),
        "updated": updated,
        "skipped": skipped,
        "failed":  failed,
    }

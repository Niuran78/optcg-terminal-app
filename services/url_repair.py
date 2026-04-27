"""URL Repair: deterministic mapping for reprint sets.

The original `cardmarket_card_url` builder derives the set-slug from the
card_id prefix (e.g. card_id='OP07-038' → /500-Years-into-the-Future/...).
That breaks for REPRINT SETS where the same card_id is reprinted in a
different set (EB02, EB01, EB03, PRB01, PRB02 etc).

This module supplies the correct slug derived from `set_code` for those
reprint sets, plus a ground-truth URL lookup table built from a one-time
Cardmarket crawl. When the DB has a wrong cm_live_url for a card whose
set_code is one of the known reprint sets, we replace the URL with the
correct one (or `None` if we can't find a match — never overwrite with
another wrong URL).

Run independently:
    python -m services.url_repair --report      # show planned changes
    python -m services.url_repair --apply       # write to DB

Or import and use `repair_urls(conn)` from another script.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

import asyncpg

log = logging.getLogger("url_repair")

# ─────────────────────────────────────────────────────────────────────────────
# Reprint sets — set_code → Cardmarket Singles URL slug.
# These slugs are SHORTER than the names we used in marketplace_urls.py and
# replace those for reprint sets where the bug occurred.
# ─────────────────────────────────────────────────────────────────────────────
REPRINT_SET_SLUGS: dict[str, str] = {
    "EB01":  "Memorial-Collection",
    "EB02":  "Anime-25th-Collection",
    "EB03":  "Heroines-Edition",
    "EB04":  "Egghead-Crisis-Asia-Region-Legal",
    "PRB01": "The-Best",
    "PRB02": "The-Best-Vol2",
}

# Japanese-language counterparts. Cardmarket uses different slugs for the JP
# editions of these reprint sets (mostly '-Japanese' suffix, but EB03/EB04 use
# '-Asia-Region-Legal' because there's no English-language print at all, and
# PRB01/PRB02 use '-Non-English').
REPRINT_SET_SLUGS_JP: dict[str, str] = {
    "EB01":  "Memorial-Collection-Japanese",
    "EB02":  "Anime-25th-Collection-Japanese",
    "EB03":  "Heroines-Edition-Asia-Region-Legal",
    "EB04":  "Egghead-Crisis-Asia-Region-Legal",
    "PRB01": "The-Best-Non-English",
    "PRB02": "The-Best-Vol-2-Non-English",
}

# Path to the ground-truth URL table (built by probe_all_reprint_sets.py)
DEFAULT_URL_TABLE = Path(__file__).resolve().parent.parent.parent / "all_reprint_set_urls.json"
# Allow override via env (the repo lives outside `optcg-terminal-app/` in dev)
URL_TABLE_PATH = Path(os.environ.get("REPRINT_URL_TABLE", DEFAULT_URL_TABLE))


def _slugify_name(name: str) -> str:
    """Cardmarket name → URL slug.

    Examples:
      'Boa Hancock'           → 'Boa-Hancock'
      'Tony Tony.Chopper'     → 'Tony-TonyChopper'
      'Monkey.D.Luffy'        → 'MonkeyDLuffy'
      'Gum-Gum Giant Pistol'  → 'Gum-Gum-Giant-Pistol'
      "BRAND NEW WORLD"       → 'BRAND-NEW-WORLD'
      "Don't" / quotes        → strip
    """
    if not name:
        return ""
    name = re.sub(r"\s*\(.*?\)\s*", "", name)              # remove parentheticals
    # Strip dots (Cardmarket convention: 'Tony Tony.Chopper' → 'Tony-TonyChopper')
    name = name.replace(".", "")
    # Split on whitespace, then within each word strip non-alphanum/hyphen
    words = re.split(r"\s+", name.strip())
    cleaned = []
    for w in words:
        w = re.sub(r"[^\w\-]", "", w)
        if w:
            cleaned.append(w)
    return "-".join(cleaned)


def _variant_suffix(variant: Optional[str]) -> list[str]:
    """Cardmarket variant suffix candidates, ordered by likelihood.

    Returns possible suffixes including the empty string. Card pages on
    Cardmarket use these suffixes when there are multiple variants of the
    same card_id; when there's only one variant, the suffix is omitted.
    """
    if not variant:
        return ["-V1", ""]
    v = variant.strip().lower()
    m_v = re.match(r"^v(\d+)$", v)
    m_alt = re.match(r"^(?:alternate art|alt art|parallel)(?:\s+(\d+))?$", v)
    if m_v:
        return [f"-V{int(m_v.group(1))}"]
    if m_alt:
        n = int(m_alt.group(1) or 1)
        return [f"-V{n + 1}", "-V2"]
    if v in ("normal", "foil"):
        return ["-V1", ""]   # try both — singletons have no suffix
    if "manga" in v or "dodgers" in v:
        return ["-V1", "-V2", ""]   # variant lives on the same canonical page
    # Fallback
    return ["-V1", ""]


def build_repair_url(
    *,
    name: str,
    card_id: str,
    set_code: str,
    variant: Optional[str],
    language: Optional[str],
    real_urls: dict[str, set[str]],
) -> Optional[str]:
    """Build the correct Cardmarket URL for a card in a reprint set.

    Strategy:
      1. Look up the reprint set slug.
      2. Generate candidate slugs (name + card_id + variant suffix).
      3. Match against the ground-truth URL set crawled from Cardmarket.
         If unique match → return it.
      4. If no exact match, return None (don't overwrite with a guess).
    """
    sc = (set_code or "").upper()
    is_jp = (language or "").upper() == "JP"
    if is_jp:
        full_slug = REPRINT_SET_SLUGS_JP.get(sc)
        table_key = f"{sc}-JP"
    else:
        full_slug = REPRINT_SET_SLUGS.get(sc)
        table_key = sc
    if not full_slug:
        return None

    name_slug = _slugify_name(name)
    if not name_slug or not card_id:
        return None

    real = real_urls.get(table_key)
    if not real:
        return None

    # Try variant suffix candidates
    for suffix in _variant_suffix(variant):
        path = f"/en/OnePiece/Products/Singles/{full_slug}/{name_slug}-{card_id}{suffix}"
        if path in real:
            return f"https://www.cardmarket.com{path}"

    # Looser fallback — any URL that contains card_id and a variant of name
    # that matches case-insensitively (handles minor name slug differences).
    cid_match = re.compile(rf"/{re.escape(full_slug)}/.*-{re.escape(card_id)}(-V\d+)?$")
    candidates = [u for u in real if cid_match.search(u)]
    if not candidates:
        return None
    if len(candidates) == 1:
        return f"https://www.cardmarket.com{candidates[0]}"

    # Multiple variants exist → pick by variant suffix
    if variant:
        wanted_suffixes = _variant_suffix(variant)
        for suf in wanted_suffixes:
            for c in candidates:
                if c.endswith(suf if suf else card_id):
                    return f"https://www.cardmarket.com{c}"
    # Default: -V1 if available, else first
    for c in candidates:
        if c.endswith("-V1"):
            return f"https://www.cardmarket.com{c}"
    return f"https://www.cardmarket.com{candidates[0]}"


def load_url_table(path: Path = URL_TABLE_PATH) -> dict[str, set[str]]:
    """Load the ground-truth URL table {set_code: set(url-paths)}."""
    if not path.exists():
        log.warning(f"Ground-truth URL table not found: {path}")
        return {}
    with open(path) as f:
        data = json.load(f)
    out: dict[str, set[str]] = {}
    for sc, info in data.items():
        out[sc] = set(info["links"])
    return out


async def collect_repair_plan(conn: asyncpg.Connection,
                              real_urls: dict[str, set[str]]
                              ) -> list[dict]:
    """Build a list of {id, set_code, card_id, name, language, variant,
    old_url, new_url, action} entries for ALL cards in reprint sets.

    action:
      'fix'         — old URL is wrong, new URL is the correct match
      'set_unverified' — old URL is wrong, no match found in ground-truth
      'unchanged'   — old URL already matches ground-truth (no change)
      'no_url_yet'  — no old URL but we found a new one
      'still_missing'  — no old URL and no new URL found
    """
    plan = []
    set_codes = sorted(set(REPRINT_SET_SLUGS.keys()) | set(REPRINT_SET_SLUGS_JP.keys()))
    rows = await conn.fetch(
        """
        SELECT id, set_code, card_id, name, language, variant,
               cm_live_url, cm_live_status, cm_live_trend,
               eu_cardmarket_7d_avg
        FROM cards_unified
        WHERE set_code = ANY($1::text[])
        """,
        set_codes,
    )
    for r in rows:
        new_url = build_repair_url(
            name=r["name"], card_id=r["card_id"], set_code=r["set_code"],
            variant=r["variant"], language=r["language"], real_urls=real_urls,
        )
        old_url = r["cm_live_url"]

        if old_url and new_url and old_url == new_url:
            action = "unchanged"
        elif old_url and new_url and old_url != new_url:
            action = "fix"
        elif old_url and not new_url:
            action = "set_unverified"
        elif not old_url and new_url:
            action = "no_url_yet"
        else:
            action = "still_missing"

        plan.append({
            "id": r["id"],
            "set_code": r["set_code"],
            "card_id": r["card_id"],
            "name": r["name"],
            "language": r["language"],
            "variant": r["variant"],
            "old_url": old_url,
            "new_url": new_url,
            "old_trend": float(r["cm_live_trend"]) if r["cm_live_trend"] is not None else None,
            "ref_price": float(r["eu_cardmarket_7d_avg"]) if r["eu_cardmarket_7d_avg"] is not None else None,
            "action": action,
        })
    return plan


async def apply_repair(conn: asyncpg.Connection, plan: list[dict]) -> dict:
    """Apply fixes to the DB. Returns counts by action."""
    counts = {"fix": 0, "set_unverified": 0, "no_url_yet": 0,
              "unchanged": 0, "still_missing": 0}
    for item in plan:
        a = item["action"]
        counts[a] = counts.get(a, 0) + 1
        if a == "fix":
            # Replace URL — clear old prices to force re-scrape, mark as
            # "needs_rescrape" so the next cron-run picks it up.
            await conn.execute(
                """
                UPDATE cards_unified
                SET cm_live_url = $2,
                    cm_live_trend = NULL,
                    cm_live_30d_avg = NULL,
                    cm_live_7d_avg = NULL,
                    cm_live_lowest = NULL,
                    cm_live_available = NULL,
                    cm_live_status = 'needs_rescrape',
                    cm_live_updated_at = NULL
                WHERE id = $1
                """,
                item["id"], item["new_url"],
            )
        elif a == "no_url_yet":
            # We have a confirmed URL where there was none — set it, mark
            # for scrape on the next cron run.
            await conn.execute(
                """
                UPDATE cards_unified
                SET cm_live_url = $2,
                    cm_live_status = 'needs_rescrape',
                    cm_live_updated_at = NULL
                WHERE id = $1
                """,
                item["id"], item["new_url"],
            )
        elif a == "set_unverified":
            # Old URL is wrong, no match in our table → null out old data
            # so the terminal doesn't display misleading prices.
            await conn.execute(
                """
                UPDATE cards_unified
                SET cm_live_status = 'unverified',
                    cm_live_trend = NULL,
                    cm_live_30d_avg = NULL,
                    cm_live_7d_avg = NULL,
                    cm_live_lowest = NULL,
                    cm_live_available = NULL
                WHERE id = $1
                """,
                item["id"],
            )
    return counts


async def repair_urls(dsn: str, *, apply: bool = False, report_path: Optional[Path] = None) -> dict:
    """Top-level entry point. Loads ground-truth table, builds plan, optionally applies."""
    real_urls = load_url_table()
    if not real_urls:
        log.error("Ground-truth URL table is empty. Aborting.")
        return {"error": "no_url_table"}

    conn = await asyncpg.connect(dsn)
    try:
        plan = await collect_repair_plan(conn, real_urls)
        counts = {"fix": 0, "set_unverified": 0, "no_url_yet": 0,
                  "unchanged": 0, "still_missing": 0}
        for item in plan:
            counts[item["action"]] = counts.get(item["action"], 0) + 1

        if report_path:
            report_path.write_text(
                json.dumps(plan, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )

        if apply:
            applied = await apply_repair(conn, plan)
            log.info(f"Applied: {applied}")
            return {"plan_counts": counts, "applied_counts": applied, "plan_size": len(plan)}
        return {"plan_counts": counts, "plan_size": len(plan)}
    finally:
        await conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Write changes to DB")
    ap.add_argument("--report", type=str, default="/tmp/url_repair_plan.json",
                    help="Path to write the JSON plan")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL env var required")

    out = asyncio.run(repair_urls(dsn, apply=args.apply, report_path=Path(args.report)))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

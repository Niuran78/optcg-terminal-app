"""Admin API — manual triggers for data operations.

Endpoints:
    POST /admin/refresh-cardmarket — trigger Cardmarket CSV price update
    POST /admin/backfill-en-prices — backfill EN prices from TCG Price Lookup
    POST /admin/seed-missing-sets  — seed sets with < 10 cards
    GET  /admin/status             — DB stats dashboard
"""
import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request

from db.init import get_pool
from middleware.tier_gate import get_current_user, UserInfo
from services.cardmarket_csv import refresh_from_cardmarket

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


# ── Cardmarket CSV refresh ────────────────────────────────────────────────────

@router.post("/admin/refresh-cardmarket")
async def admin_refresh_cardmarket(request: Request, user: UserInfo = Depends(get_current_user)):
    """Trigger Cardmarket CSV price update.

    - Elite tier: auto-download CSV from Cardmarket
    - Upload mode: POST raw CSV bytes in the request body (for manual upload
      when Cloudflare blocks the auto-download)
    """
    if user.tier != "elite":
        raise HTTPException(403, "Elite tier required")

    # Check if CSV was uploaded in the body
    body = await request.body()
    csv_bytes = body if len(body) > 100 else None

    try:
        result = await refresh_from_cardmarket(csv_bytes=csv_bytes)
        return result
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    except Exception as e:
        logger.error(f"Cardmarket refresh failed: {e}")
        raise HTTPException(500, f"Refresh failed: {e}")


# ── Backfill EN prices ────────────────────────────────────────────────────────

@router.post("/admin/backfill-en-prices")
async def admin_backfill_en_prices(user: UserInfo = Depends(get_current_user)):
    """Backfill EN prices for all cards missing them.

    Iterates sets that have cards with en_tcgplayer_market IS NULL,
    fetches EN prices from TCG Price Lookup, and updates cards_unified.
    Idempotent — safe to call multiple times.
    """
    if user.tier != "elite":
        raise HTTPException(403, "Elite tier required")

    from services import tcg_price_lookup
    from services.card_aggregator import SET_MAPPING

    pool = await get_pool()

    # Find sets that have cards missing EN prices
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT set_code, COUNT(*) as missing
            FROM cards_unified
            WHERE en_tcgplayer_market IS NULL AND set_code IS NOT NULL
            GROUP BY set_code
            ORDER BY set_code
        """)

    sets_to_backfill = [(r["set_code"], r["missing"]) for r in rows]
    logger.info(f"backfill-en-prices: {len(sets_to_backfill)} sets need EN prices")

    # Fetch live EN sets list to build slug lookup (covers sets not in SET_MAPPING)
    slug_lookup: dict[str, str] = {}
    for code, info in SET_MAPPING.items():
        if info.get("en_slug"):
            slug_lookup[code] = info["en_slug"]

    try:
        en_sets = await tcg_price_lookup.get_en_sets()
        for s in en_sets:
            slug = s.get("slug", "")
            name = (s.get("name") or "").lower()
            # Try to match slug to set code via SET_MAPPING names
            for code, info in SET_MAPPING.items():
                if code in slug_lookup:
                    continue  # Already have slug
                if info.get("name", "").lower() in name or name in info.get("name", "").lower():
                    slug_lookup[code] = slug
                    logger.info(f"backfill-en-prices: discovered slug '{slug}' for {code}")
    except Exception as e:
        logger.warning(f"backfill-en-prices: could not fetch EN sets list: {e}")

    total_updated = 0
    sets_processed = 0
    errors = []

    for set_code, missing_count in sets_to_backfill:
        en_slug = slug_lookup.get(set_code, "")
        if not en_slug:
            logger.info(f"backfill-en-prices: SKIP {set_code} — no EN slug available")
            continue

        logger.info(f"backfill-en-prices: [{sets_processed + 1}/{len(sets_to_backfill)}] "
                     f"Processing {set_code} ({missing_count} cards missing EN prices)...")

        try:
            en_cards = await tcg_price_lookup.get_en_cards(en_slug)
            if not en_cards:
                logger.info(f"backfill-en-prices: {set_code} — 0 EN cards returned")
                sets_processed += 1
                await asyncio.sleep(0.5)
                continue

            # Build lookup: card_id (upper) → EN price data
            en_lookup: dict[str, dict] = {}
            for card in en_cards:
                card_id = (card.get("card_id") or card.get("number") or "").upper()
                if card_id:
                    # Prefer the first occurrence (usually Normal variant)
                    en_lookup.setdefault(card_id, card)

            # Update cards_unified WHERE en_tcgplayer_market IS NULL for this set
            updated_in_set = 0
            async with pool.acquire() as conn:
                null_rows = await conn.fetch("""
                    SELECT id, card_id FROM cards_unified
                    WHERE set_code = $1 AND en_tcgplayer_market IS NULL
                """, set_code)

                for row in null_rows:
                    card_id = (row["card_id"] or "").upper()
                    en_card = en_lookup.get(card_id)
                    if not en_card:
                        continue

                    market = en_card.get("en_tcgplayer_market")
                    low = en_card.get("en_tcgplayer_low")
                    ebay = en_card.get("en_ebay_avg_7d")

                    if market is None and low is None:
                        continue

                    await conn.execute("""
                        UPDATE cards_unified
                        SET en_tcgplayer_market = $1,
                            en_tcgplayer_low = $2,
                            en_ebay_avg_7d = $3,
                            en_source = 'TCG Price Lookup',
                            en_updated_at = NOW()
                        WHERE id = $4
                    """, market, low, ebay, row["id"])
                    updated_in_set += 1

            total_updated += updated_in_set
            sets_processed += 1
            logger.info(f"backfill-en-prices: {set_code} — updated {updated_in_set}/{len(null_rows)} cards")

        except Exception as e:
            logger.error(f"backfill-en-prices: error for {set_code}: {e}")
            errors.append(f"{set_code}: {e}")
            sets_processed += 1

        await asyncio.sleep(0.5)  # Rate limit between sets

    logger.info(f"backfill-en-prices complete: {total_updated} cards updated across {sets_processed} sets")

    return {
        "updated": total_updated,
        "sets_processed": sets_processed,
        "sets_skipped_no_slug": len(sets_to_backfill) - sets_processed,
        "errors": errors[:20],
    }


# ── Seed missing sets ─────────────────────────────────────────────────────────

@router.post("/admin/seed-missing-sets")
async def admin_seed_missing_sets(user: UserInfo = Depends(get_current_user)):
    """Seed sets that have < 10 cards in cards_unified.

    Runs aggregate_set + aggregate_sealed for each under-seeded set.
    """
    if user.tier != "elite":
        raise HTTPException(403, "Elite tier required")

    from services.card_aggregator import SET_MAPPING, aggregate_set, aggregate_sealed

    pool = await get_pool()

    # Get current card counts per set
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT set_code, COUNT(*) as cnt FROM cards_unified GROUP BY set_code"
        )
    existing_counts = {r["set_code"]: r["cnt"] for r in rows}

    # Find sets needing seeding
    sets_to_seed = []
    for code, info in SET_MAPPING.items():
        if existing_counts.get(code, 0) < 10:
            sets_to_seed.append((code, info.get("name", code)))

    logger.info(f"seed-missing-sets: {len(sets_to_seed)} sets need seeding "
                f"(out of {len(SET_MAPPING)} in SET_MAPPING)")

    total_cards = 0
    total_sealed = 0
    processed = 0
    errors = []

    for code, name in sets_to_seed:
        logger.info(f"seed-missing-sets: [{processed + 1}/{len(sets_to_seed)}] "
                     f"Seeding {code} ({name})...")

        cards_count = 0
        try:
            cards_count = await aggregate_set(code, name)
            total_cards += cards_count
        except Exception as e:
            logger.error(f"seed-missing-sets: card error for {code}: {e}")
            errors.append(f"{code}/cards: {e}")

        sealed_count = 0
        try:
            sealed_count = await aggregate_sealed(code, name)
            total_sealed += sealed_count
        except Exception as e:
            logger.error(f"seed-missing-sets: sealed error for {code}: {e}")
            errors.append(f"{code}/sealed: {e}")

        processed += 1
        logger.info(f"seed-missing-sets: {code} — {cards_count} cards, {sealed_count} sealed")

        await asyncio.sleep(3.5)  # Rate limit between sets

    logger.info(f"seed-missing-sets complete: {total_cards} cards, {total_sealed} sealed "
                f"across {processed} sets")

    return {
        "sets_seeded": processed,
        "total_cards": total_cards,
        "total_sealed": total_sealed,
        "errors": errors[:20],
    }


# ── Status dashboard ──────────────────────────────────────────────────────────

@router.get("/admin/status")
async def admin_status():
    """Return current DB stats for monitoring.

    Public endpoint — no tier check needed.
    """
    from services.card_aggregator import SET_MAPPING

    pool = await get_pool()
    async with pool.acquire() as conn:
        total_cards = await conn.fetchval("SELECT COUNT(*) FROM cards_unified") or 0
        with_en = await conn.fetchval(
            "SELECT COUNT(*) FROM cards_unified WHERE en_tcgplayer_market IS NOT NULL"
        ) or 0
        with_eu = await conn.fetchval(
            "SELECT COUNT(*) FROM cards_unified WHERE eu_cardmarket_7d_avg IS NOT NULL"
        ) or 0

        # Sets present in DB
        db_sets = await conn.fetch(
            "SELECT set_code, COUNT(*) as cnt FROM cards_unified "
            "WHERE set_code IS NOT NULL GROUP BY set_code ORDER BY set_code"
        )
        sets_in_db = {r["set_code"]: r["cnt"] for r in db_sets}

        # Timestamp stats
        oldest_en = await conn.fetchval(
            "SELECT MIN(en_updated_at) FROM cards_unified WHERE en_updated_at IS NOT NULL"
        )
        newest_en = await conn.fetchval(
            "SELECT MAX(en_updated_at) FROM cards_unified WHERE en_updated_at IS NOT NULL"
        )
        oldest_eu = await conn.fetchval(
            "SELECT MIN(eu_updated_at) FROM cards_unified WHERE eu_updated_at IS NOT NULL"
        )
        newest_eu = await conn.fetchval(
            "SELECT MAX(eu_updated_at) FROM cards_unified WHERE eu_updated_at IS NOT NULL"
        )

        # Sealed stats
        total_sealed = await conn.fetchval("SELECT COUNT(*) FROM sealed_unified") or 0

    # Compare SET_MAPPING vs DB
    all_set_codes = sorted(SET_MAPPING.keys())
    missing_sets = [c for c in all_set_codes if c not in sets_in_db]
    present_sets = [c for c in all_set_codes if c in sets_in_db]

    return {
        "cards": {
            "total": total_cards,
            "with_en_prices": with_en,
            "with_eu_prices": with_eu,
            "missing_en": total_cards - with_en,
            "missing_eu": total_cards - with_eu,
        },
        "sealed": {
            "total": total_sealed,
        },
        "sets": {
            "in_mapping": len(all_set_codes),
            "in_db": len(present_sets),
            "missing": missing_sets,
            "present": {code: sets_in_db[code] for code in present_sets},
        },
        "timestamps": {
            "en_oldest": oldest_en.isoformat() if oldest_en else None,
            "en_newest": newest_en.isoformat() if newest_en else None,
            "eu_oldest": oldest_eu.isoformat() if oldest_eu else None,
            "eu_newest": newest_eu.isoformat() if newest_eu else None,
        },
    }

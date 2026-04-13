"""Daily price snapshot job — captures closing prices for all cards."""
import logging

from db.init import get_pool

logger = logging.getLogger(__name__)


async def take_daily_snapshots() -> int:
    """Snapshot current prices from cards_unified into daily_price_snapshots.

    Reads all cards with at least one non-null price and inserts a row per card
    for today's date. Uses UPSERT so running twice on the same day is idempotent.

    Returns the number of snapshots upserted.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            INSERT INTO daily_price_snapshots
                   (card_unified_id, snap_date,
                    en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
                    eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest)
            SELECT id, CURRENT_DATE,
                   en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
                   eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest
              FROM cards_unified
             WHERE en_tcgplayer_market IS NOT NULL
                OR en_tcgplayer_low IS NOT NULL
                OR en_ebay_avg_7d IS NOT NULL
                OR eu_cardmarket_7d_avg IS NOT NULL
                OR eu_cardmarket_30d_avg IS NOT NULL
                OR eu_cardmarket_lowest IS NOT NULL
            ON CONFLICT (card_unified_id, snap_date) DO UPDATE SET
                   en_tcgplayer_market   = EXCLUDED.en_tcgplayer_market,
                   en_tcgplayer_low      = EXCLUDED.en_tcgplayer_low,
                   en_ebay_avg_7d        = EXCLUDED.en_ebay_avg_7d,
                   eu_cardmarket_7d_avg  = EXCLUDED.eu_cardmarket_7d_avg,
                   eu_cardmarket_30d_avg = EXCLUDED.eu_cardmarket_30d_avg,
                   eu_cardmarket_lowest  = EXCLUDED.eu_cardmarket_lowest
        """)

        # asyncpg returns "INSERT 0 N" — extract N
        count = int(result.split()[-1]) if result else 0

    logger.info(f"Daily price snapshots: upserted {count} snapshots for today")
    return count

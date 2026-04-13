"""PostgreSQL database initialization and connection pool."""
import os
import ssl
import logging
import asyncpg

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Connection pool (initialized on startup)
_pool: asyncpg.Pool = None


def _make_ssl_context():
    """Create an SSL context for Supabase (requires SSL, self-signed cert)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        try:
            _pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=10, ssl=_make_ssl_context()
            )
        except Exception as e:
            logger.warning(f"SSL pool creation failed ({e}), retrying without SSL...")
            _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def get_db() -> asyncpg.Connection:
    pool = await get_pool()
    return await pool.acquire()


async def release_db(conn: asyncpg.Connection):
    pool = await get_pool()
    await pool.release(conn)


async def init_db():
    """Create all tables if they don't exist."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT 'free',
                stripe_customer_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                stripe_subscription_id TEXT UNIQUE NOT NULL,
                tier TEXT NOT NULL,
                status TEXT NOT NULL,
                current_period_end TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sets (
                id SERIAL PRIMARY KEY,
                api_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                code TEXT,
                release_date TEXT,
                card_count INTEGER,
                language TEXT NOT NULL DEFAULT 'EN',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS cards_cache (
                id SERIAL PRIMARY KEY,
                set_api_id TEXT NOT NULL,
                card_api_id TEXT NOT NULL,
                card_data_json TEXT NOT NULL,
                cardmarket_price REAL,
                tcgplayer_price REAL,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(set_api_id, card_api_id)
            );

            CREATE TABLE IF NOT EXISTS products_cache (
                id SERIAL PRIMARY KEY,
                set_api_id TEXT NOT NULL,
                product_api_id TEXT NOT NULL,
                product_data_json TEXT NOT NULL,
                cardmarket_price REAL,
                tcgplayer_price REAL,
                last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(set_api_id, product_api_id)
            );

            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                item_type TEXT NOT NULL,
                item_api_id TEXT NOT NULL,
                cardmarket_price REAL,
                tcgplayer_price REAL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS cards_unified (
                id SERIAL PRIMARY KEY,
                card_id TEXT NOT NULL,
                name TEXT NOT NULL,
                set_code TEXT,
                set_name TEXT,
                rarity TEXT,
                variant TEXT DEFAULT 'Normal',
                image_url TEXT,
                en_tcgplayer_market REAL,
                en_tcgplayer_low REAL,
                en_ebay_avg_7d REAL,
                en_source TEXT DEFAULT 'TCG Price Lookup',
                en_updated_at TIMESTAMPTZ,
                eu_cardmarket_7d_avg REAL,
                eu_cardmarket_30d_avg REAL,
                eu_cardmarket_lowest REAL,
                eu_source TEXT DEFAULT 'Cardmarket',
                eu_updated_at TIMESTAMPTZ,
                tcg_price_lookup_id TEXT,
                rapidapi_card_id TEXT,
                tcgplayer_id INTEGER,
                cardmarket_id INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(card_id, variant)
            );

            CREATE TABLE IF NOT EXISTS sealed_unified (
                id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL,
                set_code TEXT,
                set_name TEXT,
                product_type TEXT,
                image_url TEXT,
                eu_price REAL,
                eu_30d_avg REAL,
                eu_7d_avg REAL,
                eu_trend TEXT,
                eu_source TEXT DEFAULT 'Cardmarket',
                eu_updated_at TIMESTAMPTZ,
                rapidapi_product_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(product_name, set_code)
            );

            CREATE TABLE IF NOT EXISTS tcg_sets_cache (
                id SERIAL PRIMARY KEY,
                game_slug TEXT NOT NULL,
                set_data_json TEXT NOT NULL,
                cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(game_slug)
            );

            CREATE TABLE IF NOT EXISTS tcg_en_cards_cache (
                id SERIAL PRIMARY KEY,
                set_slug TEXT NOT NULL,
                card_data_json TEXT NOT NULL,
                card_id TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT 'Normal',
                cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(set_slug, card_id, variant)
            );

            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_cards_cache_set ON cards_cache(set_api_id);
            CREATE INDEX IF NOT EXISTS idx_products_cache_set ON products_cache(set_api_id);
            CREATE INDEX IF NOT EXISTS idx_price_history_item ON price_history(item_api_id, recorded_at);
            CREATE INDEX IF NOT EXISTS idx_sets_language ON sets(language);
            CREATE INDEX IF NOT EXISTS idx_cards_unified_set ON cards_unified(set_code);
            CREATE INDEX IF NOT EXISTS idx_cards_unified_card_id ON cards_unified(card_id);
            CREATE INDEX IF NOT EXISTS idx_sealed_unified_set ON sealed_unified(set_code);
            CREATE INDEX IF NOT EXISTS idx_tcg_en_cards_set ON tcg_en_cards_cache(set_slug);
        """)
    logger.info(f"[DB] PostgreSQL database initialized")


async def close_db():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

"""PostgreSQL database initialization and connection pool."""
import os
import logging
import asyncpg

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Connection pool (initialized on startup)
_pool: asyncpg.Pool = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = DATABASE_URL
        if 'pooler.supabase.com' in dsn:
            # Supabase Pooler: parse URL manually because asyncpg misparses
            # the dotted username (postgres.project_ref).
            from urllib.parse import urlparse
            p = urlparse(dsn)
            _pool = await asyncpg.create_pool(
                host=p.hostname,
                port=p.port or 5432,
                user=p.username,        # postgres.gwddra...
                password=p.password,
                database=p.path.lstrip('/') or 'postgres',
                min_size=2,
                max_size=10,
                ssl='require',
            )
        else:
            _pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        logger.info("PostgreSQL pool created.")
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
                language VARCHAR(10) DEFAULT 'JP',
                en_price_usd REAL,
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

            -- Indexes (Phase 1)
            CREATE INDEX IF NOT EXISTS idx_cards_cache_set ON cards_cache(set_api_id);
            CREATE INDEX IF NOT EXISTS idx_products_cache_set ON products_cache(set_api_id);
            CREATE INDEX IF NOT EXISTS idx_price_history_item ON price_history(item_api_id, recorded_at);
            CREATE INDEX IF NOT EXISTS idx_sets_language ON sets(language);
            CREATE INDEX IF NOT EXISTS idx_cards_unified_set ON cards_unified(set_code);
            CREATE INDEX IF NOT EXISTS idx_cards_unified_card_id ON cards_unified(card_id);
            CREATE INDEX IF NOT EXISTS idx_sealed_unified_set ON sealed_unified(set_code);

            -- Sealed language columns (added for JP/EN price separation)
            ALTER TABLE sealed_unified ADD COLUMN IF NOT EXISTS language VARCHAR(10) DEFAULT 'JP';
            ALTER TABLE sealed_unified ADD COLUMN IF NOT EXISTS en_price_usd REAL;

            CREATE UNIQUE INDEX IF NOT EXISTS sealed_unified_set_type_lang
                ON sealed_unified(set_code, product_type, language);

            -- Role column for admin separation (tier = monetization, role = permission).
            -- Elite != Admin. Admin role must be granted explicitly, never via subscription.
            ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user';
            CREATE INDEX IF NOT EXISTS idx_tcg_en_cards_set ON tcg_en_cards_cache(set_slug);

            -- ═══ Phase 2 Tables ═══

            CREATE TABLE IF NOT EXISTS daily_price_snapshots (
                id                    SERIAL PRIMARY KEY,
                card_unified_id       INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,
                snap_date             DATE    NOT NULL,
                en_tcgplayer_market   REAL,
                en_tcgplayer_low      REAL,
                en_ebay_avg_7d        REAL,
                eu_cardmarket_7d_avg  REAL,
                eu_cardmarket_30d_avg REAL,
                eu_cardmarket_lowest  REAL,
                created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (card_unified_id, snap_date)
            );

            CREATE TABLE IF NOT EXISTS price_alerts (
                id                SERIAL PRIMARY KEY,
                user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                card_unified_id   INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,
                price_field       TEXT    NOT NULL DEFAULT 'en_tcgplayer_market',
                direction         TEXT    NOT NULL CHECK (direction IN ('above', 'below')),
                target_price      REAL    NOT NULL CHECK (target_price >= 0),
                price_at_creation REAL,
                is_active         BOOLEAN     NOT NULL DEFAULT TRUE,
                triggered_at      TIMESTAMPTZ,
                triggered_price   REAL,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, card_unified_id, price_field, direction, target_price)
            );

            CREATE TABLE IF NOT EXISTS portfolios (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name        TEXT    NOT NULL DEFAULT 'My Portfolio',
                description TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, name)
            );

            CREATE TABLE IF NOT EXISTS portfolio_items (
                id              SERIAL PRIMARY KEY,
                portfolio_id    INTEGER NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
                card_unified_id INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,
                quantity        INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
                buy_price       REAL    NOT NULL CHECK (buy_price >= 0),
                buy_currency    TEXT    NOT NULL DEFAULT 'USD' CHECK (buy_currency IN ('USD', 'EUR')),
                acquired_at     DATE    NOT NULL DEFAULT CURRENT_DATE,
                notes           TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                id                SERIAL PRIMARY KEY,
                user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                card_unified_id   INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,
                price_when_added  REAL,
                price_field       TEXT NOT NULL DEFAULT 'en_tcgplayer_market',
                created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, card_unified_id)
            );

            CREATE TABLE IF NOT EXISTS market_indices (
                id              SERIAL PRIMARY KEY,
                snap_date       DATE NOT NULL,
                index_name      TEXT NOT NULL,
                index_value     REAL,
                pct_change_1d   REAL,
                detail_json     TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (snap_date, index_name)
            );

            -- Indexes (Phase 2)
            CREATE INDEX IF NOT EXISTS idx_dps_card_date ON daily_price_snapshots(card_unified_id, snap_date DESC);
            CREATE INDEX IF NOT EXISTS idx_dps_snap_date ON daily_price_snapshots(snap_date);
            CREATE INDEX IF NOT EXISTS idx_alerts_active ON price_alerts(is_active) WHERE is_active = TRUE;
            CREATE INDEX IF NOT EXISTS idx_alerts_user ON price_alerts(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_alerts_card ON price_alerts(card_unified_id) WHERE is_active = TRUE;
            CREATE INDEX IF NOT EXISTS idx_portfolios_user ON portfolios(user_id);
            CREATE INDEX IF NOT EXISTS idx_pitems_portfolio ON portfolio_items(portfolio_id);
            CREATE INDEX IF NOT EXISTS idx_pitems_card ON portfolio_items(card_unified_id);
            CREATE INDEX IF NOT EXISTS idx_watchlist_user ON watchlist(user_id);
            CREATE INDEX IF NOT EXISTS idx_watchlist_card ON watchlist(card_unified_id);
            CREATE INDEX IF NOT EXISTS idx_mindices_date ON market_indices(snap_date DESC);
            CREATE INDEX IF NOT EXISTS idx_mindices_name_date ON market_indices(index_name, snap_date DESC);
        """)
    logger.info(f"[DB] PostgreSQL database initialized")


async def close_db():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

"""SQLite schema initialization and database utilities."""
import aiosqlite
import os
from datetime import datetime

DATABASE_PATH = os.getenv("DATABASE_PATH", "optcg.db")


async def get_db() -> aiosqlite.Connection:
    """Get a database connection."""
    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    """Initialize the database schema."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT 'free',
                stripe_customer_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                stripe_subscription_id TEXT UNIQUE NOT NULL,
                tier TEXT NOT NULL,
                status TEXT NOT NULL,
                current_period_end TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                code TEXT,
                release_date TEXT,
                card_count INTEGER,
                language TEXT NOT NULL DEFAULT 'EN',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cards_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_api_id TEXT NOT NULL,
                card_api_id TEXT NOT NULL,
                card_data_json TEXT NOT NULL,
                cardmarket_price REAL,
                tcgplayer_price REAL,
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(set_api_id, card_api_id)
            );

            CREATE TABLE IF NOT EXISTS products_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_api_id TEXT NOT NULL,
                product_api_id TEXT NOT NULL,
                product_data_json TEXT NOT NULL,
                cardmarket_price REAL,
                tcgplayer_price REAL,
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(set_api_id, product_api_id)
            );

            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_type TEXT NOT NULL,
                item_api_id TEXT NOT NULL,
                cardmarket_price REAL,
                tcgplayer_price REAL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_cards_set ON cards_cache(set_api_id);
            CREATE INDEX IF NOT EXISTS idx_products_set ON products_cache(set_api_id);
            CREATE INDEX IF NOT EXISTS idx_price_history_item ON price_history(item_api_id, recorded_at);
            CREATE INDEX IF NOT EXISTS idx_sets_language ON sets(language);

            -- ── Unified card model (multi-source) ──────────────────────────────
            CREATE TABLE IF NOT EXISTS cards_unified (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_id TEXT NOT NULL,
                name TEXT NOT NULL,
                set_code TEXT,
                set_name TEXT,
                rarity TEXT,
                variant TEXT,
                image_url TEXT,

                -- EN prices (TCG Price Lookup, USD)
                en_tcgplayer_market REAL,
                en_tcgplayer_low REAL,
                en_ebay_avg_7d REAL,
                en_source TEXT DEFAULT 'TCG Price Lookup',
                en_updated_at TEXT,

                -- EU prices (RapidAPI / Cardmarket, EUR)
                eu_cardmarket_7d_avg REAL,
                eu_cardmarket_30d_avg REAL,
                eu_cardmarket_lowest REAL,
                eu_source TEXT DEFAULT 'Cardmarket',
                eu_updated_at TEXT,

                -- Linking IDs
                tcg_price_lookup_id TEXT,
                rapidapi_card_id TEXT,
                tcgplayer_id INTEGER,
                cardmarket_id INTEGER,

                -- Metadata
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(card_id, variant)
            );

            CREATE TABLE IF NOT EXISTS sealed_unified (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                eu_updated_at TEXT,

                rapidapi_product_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(product_name, set_code)
            );

            CREATE INDEX IF NOT EXISTS idx_cards_unified_set ON cards_unified(set_code);
            CREATE INDEX IF NOT EXISTS idx_cards_unified_card_id ON cards_unified(card_id);
            CREATE INDEX IF NOT EXISTS idx_sealed_unified_set ON sealed_unified(set_code);
        """)
        await db.commit()
        print(f"[DB] Database initialized at {DATABASE_PATH}")

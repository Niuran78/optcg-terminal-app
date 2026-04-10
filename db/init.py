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
        """)
        await db.commit()
        print(f"[DB] Database initialized at {DATABASE_PATH}")

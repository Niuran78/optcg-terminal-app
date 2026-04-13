-- =============================================================================
-- OPTCG Terminal — Phase 1.5 + Phase 2 Schema Additions
-- =============================================================================
-- Compatible with: asyncpg / PostgreSQL 14+
-- Depends on existing tables: users, cards_unified, sealed_unified
-- Tier limits (Pro: capped, Elite: unlimited) enforced at API level, NOT DB.
-- =============================================================================


-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PHASE 1.5 — Price History + Alerts
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- -----------------------------------------------------------------------------
-- 1. daily_price_snapshots
-- One row per card per day. Captures closing prices from both EN and EU markets.
-- Used to render 7d / 30d / 90d sparklines and price-history charts.
-- Populated by a daily cron job after the price-update pipeline completes.
-- NOTE: The existing `price_history` table stores raw per-fetch records for
--       both cards and sealed products. This new table is purpose-built for
--       daily card snapshots with a unique constraint on (card_unified_id, snap_date).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_price_snapshots (
    id                    SERIAL PRIMARY KEY,
    card_unified_id       INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,
    snap_date             DATE    NOT NULL,

    -- EN market prices (mirrored from cards_unified at end of day)
    en_tcgplayer_market   REAL,
    en_tcgplayer_low      REAL,
    en_ebay_avg_7d        REAL,

    -- EU market prices
    eu_cardmarket_7d_avg  REAL,
    eu_cardmarket_30d_avg REAL,
    eu_cardmarket_lowest  REAL,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (card_unified_id, snap_date)
);

-- Fast lookup: "give me the last 90 days of prices for card X"
CREATE INDEX IF NOT EXISTS idx_dps_card_date
    ON daily_price_snapshots (card_unified_id, snap_date DESC);

-- Fast cleanup: "delete snapshots older than 1 year"
CREATE INDEX IF NOT EXISTS idx_dps_snap_date
    ON daily_price_snapshots (snap_date);


-- -----------------------------------------------------------------------------
-- 2. price_alerts
-- Users set a target price and direction for a specific card.
-- Checked once daily after the price-update job. When triggered, the system
-- sends an email and marks the alert as triggered.
-- Tier limits — Pro: 10 active alerts, Elite: unlimited — enforced at API level.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_alerts (
    id                SERIAL PRIMARY KEY,
    user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    card_unified_id   INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,

    -- Which price field to watch (e.g. 'en_tcgplayer_market', 'eu_cardmarket_7d_avg')
    price_field       TEXT    NOT NULL DEFAULT 'en_tcgplayer_market',

    -- 'above' = notify when price >= target; 'below' = notify when price <= target
    direction         TEXT    NOT NULL CHECK (direction IN ('above', 'below')),
    target_price      REAL    NOT NULL CHECK (target_price >= 0),

    -- Price at the moment the alert was created (for context in notifications)
    price_at_creation REAL,

    -- Alert lifecycle
    is_active         BOOLEAN     NOT NULL DEFAULT TRUE,
    triggered_at      TIMESTAMPTZ,
    triggered_price   REAL,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Prevent exact-duplicate alerts
    UNIQUE (user_id, card_unified_id, price_field, direction, target_price)
);

-- "Get all active alerts for today's check run"
CREATE INDEX IF NOT EXISTS idx_alerts_active
    ON price_alerts (is_active) WHERE is_active = TRUE;

-- "Get all alerts for a specific user"
CREATE INDEX IF NOT EXISTS idx_alerts_user
    ON price_alerts (user_id, is_active);

-- "Get all alerts watching a specific card" (used during price-check sweep)
CREATE INDEX IF NOT EXISTS idx_alerts_card
    ON price_alerts (card_unified_id) WHERE is_active = TRUE;


-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- PHASE 2 — Portfolio, Watchlist, Market Indices
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- -----------------------------------------------------------------------------
-- 3a. portfolios
-- Container for a user's collection of cards. Each user may own multiple
-- portfolios (e.g. "Main Binder", "Investments", "Trade Stock").
-- Tier limits — Pro: 1 portfolio / 50 items, Elite: unlimited — API-level.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolios (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT    NOT NULL DEFAULT 'My Portfolio',
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A user cannot have two portfolios with the same name
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_portfolios_user
    ON portfolios (user_id);


-- -----------------------------------------------------------------------------
-- 3b. portfolio_items
-- Individual card holdings within a portfolio. Tracks buy price, quantity,
-- and the date acquired so the API can compute current value, P&L, and ROI.
-- A user may hold the same card in multiple rows (different buy prices / dates).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio_items (
    id              SERIAL PRIMARY KEY,
    portfolio_id    INTEGER NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
    card_unified_id INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,

    quantity        INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    buy_price       REAL    NOT NULL CHECK (buy_price >= 0),

    -- Optional: track which market the buy price refers to
    buy_currency    TEXT    NOT NULL DEFAULT 'USD' CHECK (buy_currency IN ('USD', 'EUR')),

    -- When the user acquired the card (defaults to now, but can be back-dated)
    acquired_at     DATE    NOT NULL DEFAULT CURRENT_DATE,

    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "Show me everything in portfolio X"
CREATE INDEX IF NOT EXISTS idx_pitems_portfolio
    ON portfolio_items (portfolio_id);

-- "What portfolios hold card Y?" (useful for batch P&L recalc)
CREATE INDEX IF NOT EXISTS idx_pitems_card
    ON portfolio_items (card_unified_id);


-- -----------------------------------------------------------------------------
-- 4. watchlist
-- Simple bookmarking: user marks a card to watch. The API returns price change
-- since the card was added (by comparing current price vs. price_when_added).
-- No tier gating by default; can be added later.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS watchlist (
    id                SERIAL PRIMARY KEY,
    user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    card_unified_id   INTEGER NOT NULL REFERENCES cards_unified(id) ON DELETE CASCADE,

    -- Snapshot of the card's price at the time it was added
    price_when_added  REAL,
    price_field       TEXT NOT NULL DEFAULT 'en_tcgplayer_market',

    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A user can only bookmark a card once
    UNIQUE (user_id, card_unified_id)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_user
    ON watchlist (user_id);

CREATE INDEX IF NOT EXISTS idx_watchlist_card
    ON watchlist (card_unified_id);


-- -----------------------------------------------------------------------------
-- 5. market_indices
-- Daily aggregated market-level metrics computed by a cron job.
-- Powers the Market Overview Dashboard: total market cap, segment indices,
-- top movers, volume indicators.
-- One row per index_name per day.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_indices (
    id              SERIAL PRIMARY KEY,
    snap_date       DATE NOT NULL,

    -- Examples: 'total_market', 'singles_index', 'sealed_index',
    --           'top_gainers', 'top_losers'
    index_name      TEXT NOT NULL,

    -- Scalar value for simple indices (e.g. total market cap in USD)
    index_value     REAL,

    -- Percentage change from previous day
    pct_change_1d   REAL,

    -- JSON blob for composite data (e.g. top-10 movers list, breakdown by set)
    -- Stored as TEXT for asyncpg simplicity; parsed as JSON in application code.
    detail_json     TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (snap_date, index_name)
);

-- "Get latest value for all indices"
CREATE INDEX IF NOT EXISTS idx_mindices_date
    ON market_indices (snap_date DESC);

-- "Get 90-day history for a specific index"
CREATE INDEX IF NOT EXISTS idx_mindices_name_date
    ON market_indices (index_name, snap_date DESC);


-- =============================================================================
-- EXAMPLE QUERIES (for developer reference)
-- =============================================================================

-- ---- Daily Price Snapshots -------------------------------------------------

-- Get 30-day price history for a card (by cards_unified.id = 42)
-- SELECT snap_date, en_tcgplayer_market, eu_cardmarket_7d_avg
--   FROM daily_price_snapshots
--  WHERE card_unified_id = 42
--    AND snap_date >= CURRENT_DATE - INTERVAL '30 days'
--  ORDER BY snap_date ASC;

-- Insert today's snapshot (idempotent via ON CONFLICT)
-- INSERT INTO daily_price_snapshots
--        (card_unified_id, snap_date,
--         en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
--         eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest)
-- SELECT id, CURRENT_DATE,
--        en_tcgplayer_market, en_tcgplayer_low, en_ebay_avg_7d,
--        eu_cardmarket_7d_avg, eu_cardmarket_30d_avg, eu_cardmarket_lowest
--   FROM cards_unified
-- ON CONFLICT (card_unified_id, snap_date) DO UPDATE SET
--        en_tcgplayer_market   = EXCLUDED.en_tcgplayer_market,
--        en_tcgplayer_low      = EXCLUDED.en_tcgplayer_low,
--        en_ebay_avg_7d        = EXCLUDED.en_ebay_avg_7d,
--        eu_cardmarket_7d_avg  = EXCLUDED.eu_cardmarket_7d_avg,
--        eu_cardmarket_30d_avg = EXCLUDED.eu_cardmarket_30d_avg,
--        eu_cardmarket_lowest  = EXCLUDED.eu_cardmarket_lowest;

-- ---- Price Alerts ----------------------------------------------------------

-- Create an alert: notify when Luffy-OP01 drops below $5
-- INSERT INTO price_alerts
--        (user_id, card_unified_id, price_field, direction, target_price, price_at_creation)
-- VALUES (1, 42, 'en_tcgplayer_market', 'below', 5.00, 7.50);

-- Daily alert check: find all triggered alerts after price update
-- SELECT pa.id, pa.user_id, pa.card_unified_id, pa.direction, pa.target_price,
--        cu.en_tcgplayer_market AS current_price, cu.name AS card_name, u.email
--   FROM price_alerts pa
--   JOIN cards_unified cu ON cu.id = pa.card_unified_id
--   JOIN users u ON u.id = pa.user_id
--  WHERE pa.is_active = TRUE
--    AND (
--          (pa.direction = 'below' AND cu.en_tcgplayer_market <= pa.target_price)
--       OR (pa.direction = 'above' AND cu.en_tcgplayer_market >= pa.target_price)
--        );

-- Mark alert as triggered
-- UPDATE price_alerts
--    SET is_active = FALSE, triggered_at = NOW(), triggered_price = 4.80
--  WHERE id = 123;

-- Count active alerts for tier enforcement (API-level)
-- SELECT COUNT(*) FROM price_alerts WHERE user_id = 1 AND is_active = TRUE;

-- ---- Portfolio -------------------------------------------------------------

-- Create a portfolio
-- INSERT INTO portfolios (user_id, name, description)
-- VALUES (1, 'Main Collection', 'Cards I own for playing and investing');

-- Add a card to a portfolio
-- INSERT INTO portfolio_items (portfolio_id, card_unified_id, quantity, buy_price, buy_currency)
-- VALUES (1, 42, 3, 6.50, 'USD');

-- Portfolio summary: total cost, current value, P&L per item
-- SELECT pi.id, cu.name, cu.card_id, pi.quantity, pi.buy_price, pi.buy_currency,
--        cu.en_tcgplayer_market AS current_price,
--        (cu.en_tcgplayer_market * pi.quantity) AS current_value,
--        (cu.en_tcgplayer_market - pi.buy_price) * pi.quantity AS pnl,
--        CASE WHEN pi.buy_price > 0
--             THEN ROUND(((cu.en_tcgplayer_market - pi.buy_price) / pi.buy_price * 100)::numeric, 2)
--             ELSE 0 END AS roi_pct
--   FROM portfolio_items pi
--   JOIN cards_unified cu ON cu.id = pi.card_unified_id
--  WHERE pi.portfolio_id = 1
--  ORDER BY current_value DESC;

-- Aggregate portfolio value
-- SELECT SUM(cu.en_tcgplayer_market * pi.quantity) AS total_current_value,
--        SUM(pi.buy_price * pi.quantity) AS total_cost,
--        SUM((cu.en_tcgplayer_market - pi.buy_price) * pi.quantity) AS total_pnl
--   FROM portfolio_items pi
--   JOIN cards_unified cu ON cu.id = pi.card_unified_id
--  WHERE pi.portfolio_id = 1;

-- Count items for tier enforcement (API-level)
-- SELECT COUNT(*) FROM portfolio_items WHERE portfolio_id = 1;
-- SELECT COUNT(*) FROM portfolios WHERE user_id = 1;

-- ---- Watchlist --------------------------------------------------------------

-- Add a card to watchlist
-- INSERT INTO watchlist (user_id, card_unified_id, price_when_added, price_field)
-- VALUES (1, 42, 7.50, 'en_tcgplayer_market')
-- ON CONFLICT (user_id, card_unified_id) DO NOTHING;

-- Get watchlist with current price delta
-- SELECT w.id, cu.name, cu.card_id, cu.image_url,
--        w.price_when_added,
--        cu.en_tcgplayer_market AS current_price,
--        (cu.en_tcgplayer_market - w.price_when_added) AS price_change,
--        CASE WHEN w.price_when_added > 0
--             THEN ROUND(((cu.en_tcgplayer_market - w.price_when_added) / w.price_when_added * 100)::numeric, 2)
--             ELSE 0 END AS pct_change,
--        w.created_at AS added_at
--   FROM watchlist w
--   JOIN cards_unified cu ON cu.id = w.card_unified_id
--  WHERE w.user_id = 1
--  ORDER BY w.created_at DESC;

-- ---- Market Indices --------------------------------------------------------

-- Insert / update today's total market cap
-- INSERT INTO market_indices (snap_date, index_name, index_value, pct_change_1d, detail_json)
-- VALUES (CURRENT_DATE, 'total_market', 12500000.00, 1.25, NULL)
-- ON CONFLICT (snap_date, index_name) DO UPDATE SET
--     index_value   = EXCLUDED.index_value,
--     pct_change_1d = EXCLUDED.pct_change_1d,
--     detail_json   = EXCLUDED.detail_json;

-- Get latest snapshot of all indices
-- SELECT index_name, index_value, pct_change_1d, detail_json
--   FROM market_indices
--  WHERE snap_date = CURRENT_DATE;

-- Get 90-day trend for the singles index
-- SELECT snap_date, index_value, pct_change_1d
--   FROM market_indices
--  WHERE index_name = 'singles_index'
--    AND snap_date >= CURRENT_DATE - INTERVAL '90 days'
--  ORDER BY snap_date ASC;

-- Get top movers detail for today
-- SELECT detail_json
--   FROM market_indices
--  WHERE index_name = 'top_gainers' AND snap_date = CURRENT_DATE;

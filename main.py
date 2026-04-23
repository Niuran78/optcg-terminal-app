"""Holygrade Terminal — FastAPI Application Entry Point."""
import asyncio
import os
import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

# Validate critical env vars early (non-fatal — just warn)
_TCG_KEY = os.getenv("TCG_PRICE_LOOKUP_KEY", "")
if not _TCG_KEY:
    logging.warning("TCG_PRICE_LOOKUP_KEY not set — EN price source will be disabled")

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from middleware.tier_gate import require_auth

from db.init import init_db, get_pool
from api.auth import router as auth_router
from api.cards import router as cards_router
from api.sets import router as sets_router
from api.arbitrage import router as arbitrage_router
from api.sealed import router as sealed_router
from api.ev import router as ev_router
from api.stripe_billing import router as billing_router
from api.scraper import router as scraper_router
from api.unified import router as unified_router
from api.portfolio import router as portfolio_router
from api.alerts import router as alerts_router
from api.widget_public import router as widget_router
from api.admin import router as admin_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("optcg")


async def seed_all_data():
    """Background task to seed all card and product data (legacy — kept for compat)."""
    from services import opcg_api
    logger.info("Starting legacy data seed (RapidAPI only)...")
    try:
        sets = await opcg_api.get_sets(tier="elite")
    except Exception as e:
        logger.error(f"Data seed: could not load sets: {e}")
        return
    total_cards = 0
    total_products = 0
    for s in sets:
        try:
            cards = await opcg_api.get_cards(s["api_id"], tier="elite")
            total_cards += len(cards)
            products = await opcg_api.get_products(s["api_id"], tier="elite")
            total_products += len(products)
            logger.info(f"  {s.get('code', s['api_id'])}: {len(cards)} cards, {len(products)} products")
            await asyncio.sleep(0.5)  # Pace the requests
        except Exception as e:
            logger.error(f"  {s.get('code', s['api_id'])}: seed error: {e}")
    logger.info(
        f"Legacy seed complete: {total_cards} cards, {total_products} products "
        f"across {len(sets)} sets"
    )


async def _daily_pricecharting_sync_loop():
    """Run PriceCharting CSV sync once per day + write a daily price snapshot.

    Order:
      1. Sleep 60s (let the app come up).
      2. Sync prices from PriceCharting CSV.
      3. Write today's snapshot row from current prices (for chart history).
      4. Sleep 24h, loop.
    """
    from services.pricecharting_csv_sync import sync_from_csv
    from services.price_history_seeder import daily_snapshot_from_current

    await asyncio.sleep(60)  # Let the app finish startup first
    while True:
        try:
            logger.info("[daily sync] starting PriceCharting CSV sync...")
            result = await sync_from_csv()
            logger.info(
                f"[daily sync] complete: sealed={result.get('sealed_total')}, "
                f"cards={result.get('cards_updated')}"
            )
        except Exception as e:
            logger.error(f"[daily sync] failed: {e}")

        try:
            logger.info("[daily sync] writing daily price snapshot...")
            snap = await daily_snapshot_from_current()
            logger.info(f"[daily sync] snapshot written: {snap}")
        except Exception as e:
            logger.error(f"[daily sync] snapshot failed: {e}")

        # Sleep 24h before next sync
        await asyncio.sleep(24 * 60 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("Starting Holygrade Terminal...")
    await init_db()

    # Seed sets from API on startup
    try:
        from services import opcg_api
        sets = await opcg_api.get_sets(tier="free")
        logger.info(f"Loaded {len(sets)} sets from API/cache.")
    except Exception as e:
        logger.warning(f"Could not seed sets on startup: {e}")

    # Ensure admin account exists
    pool = await get_pool()
    async with pool.acquire() as conn:
        admin = await conn.fetchrow(
            "SELECT id FROM users WHERE email=$1",
            "mail@blockreaction-investments.ch",
        )
        if not admin:
            import bcrypt
            hashed = bcrypt.hashpw(b"Holygrade2026!", bcrypt.gensalt()).decode()
            await conn.execute(
                "INSERT INTO users (email, password_hash, tier) VALUES ($1, $2, 'elite')",
                "mail@blockreaction-investments.ch",
                hashed,
            )
            logger.info("Admin account created: mail@blockreaction-investments.ch (elite)")
        else:
            logger.info(f"Admin account exists: id={admin['id']}")

    # Check if cards_unified has data — only seed if empty
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM cards_unified")
        priced = await conn.fetchval(
            "SELECT COUNT(*) FROM cards_unified WHERE en_tcgplayer_market IS NOT NULL OR eu_cardmarket_7d_avg IS NOT NULL"
        )

    from services.card_aggregator import seed_all_unified
    if count == 0:
        logger.info("cards_unified is empty — starting full data seed...")
        asyncio.create_task(seed_all_unified())
    elif priced == 0:
        logger.info(f"cards_unified has {count} records but 0 with prices — re-seeding...")
        asyncio.create_task(seed_all_unified())
    else:
        logger.info(f"cards_unified: {count} records, {priced} with prices — skipping seed")

    # Start daily PriceCharting CSV sync loop (runs in background forever)
    import os
    if os.getenv("PRICECHARTING_API_TOKEN"):
        asyncio.create_task(_daily_pricecharting_sync_loop())
        logger.info("Daily PriceCharting CSV sync loop started (runs every 24h)")
    else:
        logger.warning("PRICECHARTING_API_TOKEN not set — skipping daily CSV sync")

    yield

    from db.init import close_db
    await close_db()
    logger.info("Holygrade Terminal shutting down.")


app = FastAPI(
    title="Holygrade Terminal",
    description="Holygrade Terminal — market intelligence for One Piece TCG. Arbitrage scanner, sealed product tracking, portfolio analytics.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routers
app.include_router(auth_router)
app.include_router(unified_router)  # unified multi-source endpoints (new)
from api.image_proxy import router as image_proxy_router
app.include_router(image_proxy_router)  # proxy external card images (CORP workaround)
app.include_router(widget_router)  # public widget endpoints (no auth)
app.include_router(cards_router)    # legacy single-source endpoints (kept for compat)
app.include_router(sets_router)
app.include_router(arbitrage_router)
app.include_router(sealed_router)
app.include_router(ev_router)
app.include_router(billing_router)
app.include_router(scraper_router)
app.include_router(portfolio_router)
app.include_router(alerts_router)
app.include_router(admin_router)


# Health check
@app.get("/api/health")
async def health():
    return {"status": "ok", "app": "Holygrade Terminal", "version": "1.0.0"}


# Market overview endpoint
@app.get("/api/market/overview")
async def market_overview(user=Depends(require_auth)):
    """Market overview powered entirely by live scraped Cardmarket data.

    Returns the data that actually helps an OP TCG trader:
      - Top 5 LIVE-priced Alt-Art cards right now (most valuable liquid cards)
      - Top 5 JP-EN arbitrage opportunities (live-live cross-market spreads)
      - Market-wide stats
      - Recent scraper activity (transparency)
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Big picture stats
        stats = await conn.fetchrow("""
            SELECT
                (SELECT COUNT(DISTINCT set_code) FROM cards_unified) AS total_sets,
                (SELECT COUNT(*) FROM cards_unified) AS total_cards,
                (SELECT COUNT(*) FROM cards_unified WHERE cm_live_trend IS NOT NULL) AS cards_with_live,
                (SELECT MAX(cm_live_updated_at) FROM cards_unified) AS last_scrape,
                (SELECT COUNT(*) FROM cards_unified WHERE cm_live_status='ok'
                                                        AND cm_live_updated_at > NOW() - INTERVAL '48 hours') AS fresh_prices
        """)

        # TOP 5 MOST VALUABLE LIVE-PRICED ALT-ARTS
        top_valuable = await conn.fetch("""
            SELECT card_id, variant, language, name, set_code, image_url,
                   cm_live_trend, cm_live_lowest, cm_live_available, cm_live_url
            FROM cards_unified
            WHERE cm_live_trend IS NOT NULL
              AND (variant ILIKE 'Alternate Art%' OR variant ILIKE 'V%')
              AND cm_live_trend >= 100
            ORDER BY cm_live_trend DESC
            LIMIT 5
        """)

        # TOP JP-EN ARBITRAGE: same card, big price gap between JP and EN live prices
        arbitrage = await conn.fetch("""
            SELECT en.card_id, en.variant, en.name, en.set_code, en.image_url,
                   en.cm_live_trend AS en_price, en.cm_live_url AS en_url,
                   jp.cm_live_trend AS jp_price, jp.cm_live_url AS jp_url,
                   (en.cm_live_trend - jp.cm_live_trend) AS spread_eur,
                   (en.cm_live_trend / NULLIF(jp.cm_live_trend, 0)) AS ratio
            FROM cards_unified en
            INNER JOIN cards_unified jp
              ON jp.card_id = en.card_id AND jp.variant = en.variant AND jp.language = 'JP'
            WHERE en.language = 'EN'
              AND en.cm_live_trend IS NOT NULL
              AND jp.cm_live_trend IS NOT NULL
              AND jp.cm_live_trend >= 5       -- skip penny-cards (noise)
              AND en.cm_live_trend > jp.cm_live_trend * 1.5  -- at least 50% spread
            ORDER BY (en.cm_live_trend - jp.cm_live_trend) DESC
            LIMIT 5
        """)

        # RECENT SETS
        recent_sets = await conn.fetch("""
            SELECT set_code,
                   COUNT(*) AS card_count,
                   COUNT(*) FILTER (WHERE cm_live_trend IS NOT NULL) AS live_count,
                   ROUND(AVG(cm_live_trend)::numeric, 2) AS avg_price
            FROM cards_unified
            WHERE set_code IS NOT NULL
            GROUP BY set_code
            ORDER BY
                -- OP18, OP17, OP16... come first; ST/EB sets after
                CASE WHEN set_code LIKE 'OP%' THEN 0 ELSE 1 END,
                set_code DESC
            LIMIT 8
        """)

    return {
        "stats": {
            "total_sets":       stats["total_sets"],
            "total_cards":      stats["total_cards"],
            "cards_with_live":  stats["cards_with_live"],
            "fresh_prices":     stats["fresh_prices"],
            "last_scrape":      stats["last_scrape"].isoformat() if stats["last_scrape"] else None,
        },
        "top_valuable": [dict(r) for r in top_valuable],
        "arbitrage":    [dict(r) for r in arbitrage],
        "recent_sets":  [dict(r) for r in recent_sets],
    }


# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/login.html")
    async def serve_login():
        return FileResponse(os.path.join(static_dir, "login.html"))

    @app.get("/widget", include_in_schema=False)
    async def serve_widget():
        """Product-aware widget for Shopify embed via iframe.
        Usage: /widget?set=OP13 or /widget?sku=OP13-BOX-JP"""
        return FileResponse(os.path.join(static_dir, "product-widget.html"))

    @app.get("/teaser", include_in_schema=False)
    async def serve_teaser():
        return FileResponse(os.path.join(static_dir, "teaser.html"))

    @app.get("/", include_in_schema=False)
    async def serve_index():
        return FileResponse(os.path.join(static_dir, "index.html"))

    @app.get("/{path:path}", include_in_schema=False)
    async def serve_static(path: str):
        file_path = os.path.join(static_dir, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

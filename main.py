"""OPTCG Market Terminal — FastAPI Application Entry Point."""
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

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("Starting OPTCG Market Terminal...")
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

    yield

    from db.init import close_db
    await close_db()
    logger.info("OPTCG Market Terminal shutting down.")


app = FastAPI(
    title="OPTCG Market Terminal",
    description="Bloomberg Terminal for One Piece TCG — arbitrage, EV, sealed product tracking.",
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
    return {"status": "ok", "app": "OPTCG Market Terminal", "version": "1.0.0"}


# Market overview endpoint
@app.get("/api/market/overview")
async def market_overview():
    """Quick market overview — top movers and summary stats."""
    from services import opcg_api
    from services.arbitrage_engine import analyze_items

    # Fetch ALL sets for total count
    all_sets = await opcg_api.get_sets(tier="elite")
    total_sets = len(all_sets)

    pool = await get_pool()
    async with pool.acquire() as conn:
        recent_sets = await conn.fetch(
            "SELECT * FROM sets ORDER BY release_date DESC LIMIT 10"
        )

    recent_sets = [dict(row) for row in recent_sets]

    top_movers = []
    for s in recent_sets[:2]:  # Limit API calls on overview
        try:
            products = await opcg_api.get_products(s["api_id"], tier="free")
            analyzed = analyze_items(products, "product")
            for item in analyzed[:3]:
                item["set_name"] = s.get("name", "")
                item["set_language"] = s.get("language", "EN")
                top_movers.append(item)
        except Exception:
            continue

    top_movers.sort(key=lambda x: abs(x.get("profit_eur") or 0), reverse=True)

    return {
        "recent_sets": recent_sets,
        "top_movers": top_movers[:10],
        "stats": {
            "sets_tracked": total_sets,
            "total_sets": total_sets,
            "top_signal": top_movers[0].get("signal") if top_movers else "NEUTRAL",
        }
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

"""OPTCG Market Terminal — FastAPI Application Entry Point."""
import os
import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from db.init import init_db
from api.auth import router as auth_router
from api.cards import router as cards_router
from api.sets import router as sets_router
from api.arbitrage import router as arbitrage_router
from api.sealed import router as sealed_router
from api.ev import router as ev_router
from api.stripe_billing import router as billing_router
from api.scraper import router as scraper_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("optcg")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("Starting OPTCG Market Terminal...")
    await init_db()

    # Seed sets from API on startup (non-blocking)
    try:
        from services import opcg_api
        sets = await opcg_api.get_sets(tier="free")
        logger.info(f"Loaded {len(sets)} sets from API/cache.")
    except Exception as e:
        logger.warning(f"Could not seed sets on startup: {e}")

    yield
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
app.include_router(cards_router)
app.include_router(sets_router)
app.include_router(arbitrage_router)
app.include_router(sealed_router)
app.include_router(ev_router)
app.include_router(billing_router)
app.include_router(scraper_router)


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
    import aiosqlite
    from db.init import DATABASE_PATH

    # Fetch ALL sets for total count
    all_sets = await opcg_api.get_sets(tier="elite")
    total_sets = len(all_sets)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM sets ORDER BY release_date DESC LIMIT 10"
        )
        recent_sets = await cursor.fetchall()

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

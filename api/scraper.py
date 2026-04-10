"""Cardmarket scraper API endpoints."""
from fastapi import APIRouter, Depends, Query
from middleware.tier_gate import get_current_user, UserInfo, require_tier
from services.cardmarket_scraper import scrape_cardmarket_search, scrape_sealed_prices

router = APIRouter(prefix="/api/scraper", tags=["scraper"])


@router.get("/search")
async def search_cardmarket(
    q: str = Query(..., min_length=2),
    user: UserInfo = Depends(require_tier("pro"))
):
    """Search Cardmarket directly (Pro+ only)."""
    results = await scrape_cardmarket_search(q)
    return {"results": results, "query": q, "source": "cardmarket_scraper"}


@router.get("/sealed/{set_code}")
async def sealed_prices(
    set_code: str,
    user: UserInfo = Depends(require_tier("pro"))
):
    """Get scraped sealed prices for a set (Pro+ only)."""
    results = await scrape_sealed_prices(set_code)
    return {"results": results, "set_code": set_code, "source": "cardmarket_scraper"}

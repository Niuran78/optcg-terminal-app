"""News API — public feed + admin CRUD for the /news page.

Endpoints:
    GET  /api/news              — public paginated feed (is_published=TRUE only)
    GET  /api/news/{id}         — single news item
    POST /api/news              — admin: create news item
    PATCH /api/news/{id}        — admin: toggle publish / edit
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from db.init import get_pool
from middleware.tier_gate import get_current_user, require_admin, UserInfo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/news", tags=["news"])


# ── Helpers ──────────────────────────────────────────────────────────────────

def _compute_featured_score(source: str, category: str, published_at: datetime,
                            related_set: Optional[str]) -> int:
    """Compute featured_score (0–100) per the formula in news_db_schema.sql."""
    now = datetime.now(timezone.utc)
    score = 0
    if source in ("bandai", "twitter"):
        score += 40
    diff = now - published_at
    hours = diff.total_seconds() / 3600
    if hours <= 6:
        score += 20
    elif hours <= 24:
        score += 10
    if category == "set_release":
        score += 15
    elif category == "tournament":
        score += 10
    if related_set:
        score += 5
    if diff.days > 14:
        score = 0
    return max(0, min(100, score))


def _row_to_dict(row) -> dict:
    d = dict(row)
    for k in ("published_at", "ingested_at", "updated_at"):
        if k in d and d[k] is not None:
            d[k] = d[k].isoformat()
    # Convert enum values to strings
    for k in ("source", "category", "language"):
        if k in d and d[k] is not None:
            d[k] = str(d[k])
    return d


# ── Public endpoints ─────────────────────────────────────────────────────────

@router.get("")
async def list_news(
    cat: Optional[str] = Query(None, description="Filter by category"),
    source: Optional[str] = Query(None, description="Filter by source type"),
    featured: bool = Query(False, description="Return only top-3 featured items"),
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
):
    """Public news feed — only is_published=TRUE items."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if featured:
            rows = await conn.fetch(
                "SELECT * FROM news_items "
                "WHERE is_published = TRUE AND featured_score > 0 "
                "ORDER BY featured_score DESC, published_at DESC "
                "LIMIT 3"
            )
            return {"total": len(rows), "items": [_row_to_dict(r) for r in rows]}

        # Build query with optional filters
        conditions = ["is_published = TRUE"]
        params = []
        idx = 1

        if cat and cat != "all":
            conditions.append(f"category = ${idx}::news_category")
            params.append(cat)
            idx += 1

        if source:
            conditions.append(f"source = ${idx}::news_source_type")
            params.append(source)
            idx += 1

        where = " AND ".join(conditions)

        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM news_items WHERE {where}", *params
        )

        params_with_paging = params + [limit, offset]
        rows = await conn.fetch(
            f"SELECT * FROM news_items WHERE {where} "
            f"ORDER BY published_at DESC LIMIT ${idx} OFFSET ${idx+1}",
            *params_with_paging,
        )

        # KPI stats for the header
        today_count = await conn.fetchval(
            "SELECT COUNT(*) FROM news_items "
            "WHERE is_published = TRUE AND ingested_at >= CURRENT_DATE"
        )
        week_count = await conn.fetchval(
            "SELECT COUNT(*) FROM news_items "
            "WHERE is_published = TRUE AND ingested_at >= CURRENT_DATE - INTERVAL '7 days'"
        )
        active_sources = await conn.fetchval(
            "SELECT COUNT(DISTINCT source) FROM news_items "
            "WHERE is_published = TRUE AND published_at >= NOW() - INTERVAL '30 days'"
        )

        return {
            "total": total,
            "items": [_row_to_dict(r) for r in rows],
            "kpi": {
                "today": today_count,
                "week": week_count,
                "active_sources": active_sources,
            },
        }


@router.get("/{item_id}")
async def get_news_item(item_id: int):
    """Single news item by ID — only if published."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM news_items WHERE id = $1 AND is_published = TRUE",
            item_id,
        )
    if not row:
        raise HTTPException(404, "News item not found")
    return _row_to_dict(row)


# ── Admin endpoints ──────────────────────────────────────────────────────────

class NewsCreate(BaseModel):
    source: str = Field("holygrade", description="Source type")
    source_url: str = Field(..., description="Canonical URL (unique)")
    title_de: str = Field(..., max_length=120)
    teaser_de: Optional[str] = Field(None, max_length=240)
    category: str = Field("shop", description="Category")
    related_set: Optional[str] = None
    image_url: Optional[str] = None
    published_at: Optional[datetime] = None
    is_published: bool = True


@router.post("", status_code=201)
async def create_news(body: NewsCreate, user: UserInfo = Depends(require_admin)):
    """Admin: manually add a news item."""
    pub_at = body.published_at or datetime.now(timezone.utc)
    score = _compute_featured_score(body.source, body.category, pub_at, body.related_set)

    # Map source to source_key
    source_key_map = {
        "holygrade": "holygrade_internal",
        "bandai": "bandai_op_official",
        "market": "market_signals",
        "community": "limitless_tcg",
    }
    source_key = source_key_map.get(body.source, "holygrade_internal")

    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO news_items "
                "(source, source_key, source_url, title_de, teaser_de, "
                " category, related_set, image_url, featured_score, "
                " is_published, published_at, language) "
                "VALUES ($1::news_source_type, $2, $3, $4, $5, "
                "        $6::news_category, $7, $8, $9, $10, $11, 'de'::news_language) "
                "RETURNING *",
                body.source, source_key, body.source_url, body.title_de,
                body.teaser_de, body.category, body.related_set, body.image_url,
                score, body.is_published, pub_at,
            )
        except Exception as e:
            if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                raise HTTPException(409, "source_url already exists")
            raise HTTPException(500, str(e))

    return _row_to_dict(row)


class NewsPatch(BaseModel):
    is_published: Optional[bool] = None
    featured_score: Optional[int] = Field(None, ge=0, le=100)
    title_de: Optional[str] = Field(None, max_length=120)
    teaser_de: Optional[str] = Field(None, max_length=240)
    category: Optional[str] = None


@router.patch("/{item_id}")
async def patch_news(item_id: int, body: NewsPatch, user: UserInfo = Depends(require_admin)):
    """Admin: toggle publish, edit fields."""
    updates = []
    params = []
    idx = 1

    if body.is_published is not None:
        updates.append(f"is_published = ${idx}")
        params.append(body.is_published)
        idx += 1
    if body.featured_score is not None:
        updates.append(f"featured_score = ${idx}")
        params.append(body.featured_score)
        idx += 1
    if body.title_de is not None:
        updates.append(f"title_de = ${idx}")
        params.append(body.title_de)
        idx += 1
    if body.teaser_de is not None:
        updates.append(f"teaser_de = ${idx}")
        params.append(body.teaser_de)
        idx += 1
    if body.category is not None:
        updates.append(f"category = ${idx}::news_category")
        params.append(body.category)
        idx += 1

    if not updates:
        raise HTTPException(400, "No fields to update")

    updates.append(f"updated_at = NOW()")
    set_clause = ", ".join(updates)
    params.append(item_id)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE news_items SET {set_clause} WHERE id = ${idx} RETURNING *",
            *params,
        )
    if not row:
        raise HTTPException(404, "News item not found")
    return _row_to_dict(row)


# ── Ingest trigger (admin only) ──────────────────────────────────

@router.post("/ingest")
async def trigger_ingest(user: UserInfo = Depends(require_admin)):
    """Admin: manually trigger the news ingest pipeline."""
    from scripts.news_ingest import run_full_ingest
    try:
        results = await run_full_ingest()
        return {"status": "ok", "results": results}
    except Exception as e:
        logger.error(f"Ingest trigger failed: {e}")
        raise HTTPException(500, f"Ingest failed: {e}")

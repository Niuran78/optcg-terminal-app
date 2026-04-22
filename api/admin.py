"""Admin API — manual triggers for data operations.

Endpoints:
    POST /admin/refresh-cardmarket — trigger Cardmarket CSV price update
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from middleware.tier_gate import get_current_user, UserInfo
from services.cardmarket_csv import refresh_from_cardmarket

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


@router.post("/admin/refresh-cardmarket")
async def admin_refresh_cardmarket(request: Request, user: UserInfo = Depends(get_current_user)):
    """Trigger Cardmarket CSV price update.

    - Elite tier: auto-download CSV from Cardmarket
    - Upload mode: POST raw CSV bytes in the request body (for manual upload
      when Cloudflare blocks the auto-download)
    """
    if user.tier != "elite":
        raise HTTPException(403, "Elite tier required")

    # Check if CSV was uploaded in the body
    body = await request.body()
    csv_bytes = body if len(body) > 100 else None

    try:
        result = await refresh_from_cardmarket(csv_bytes=csv_bytes)
        return result
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    except Exception as e:
        logger.error(f"Cardmarket refresh failed: {e}")
        raise HTTPException(500, f"Refresh failed: {e}")

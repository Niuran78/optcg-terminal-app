"""Admin endpoint for the weekly pricing recommendation report.

POST /api/admin/pricing-report
    Headers: X-Admin-Secret: <ADMIN_SECRET>
    Query:   ?send=1            → also email the report (else: preview-only)
             ?to=<email>         → override recipient (default: ADMIN_EMAIL)
             ?dry=1              → render HTML, return JSON with stats only

Returns JSON:
    {
      "ok": true,
      "subject": "...",
      "stats": {...},
      "preview_path": "/home/user/workspace/pricing_report_PREVIEW.html",
      "mail": {"sent": false, "info": "..."}
    }
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query

from services.pricing_report import (
    build_recommendations_async,
    render_html,
    write_preview,
    _subject,
    PREVIEW_PATH,
    ADMIN_EMAIL,
)
from services.pricing_mailer import send_report, _backend as mail_backend

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin-pricing"])

ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")


def _check_secret(secret: Optional[str]) -> None:
    if not secret or secret != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="invalid admin secret")


@router.post("/api/admin/pricing-report")
async def trigger_pricing_report(
    send: int = Query(0, description="1 = also send email"),
    dry: int = Query(0, description="1 = render only, no preview write"),
    to: Optional[str] = Query(None, description="recipient override"),
    x_admin_secret: Optional[str] = Header(None, alias="X-Admin-Secret"),
):
    _check_secret(x_admin_secret)

    try:
        report = await build_recommendations_async()
        html = render_html(report)
        subject = _subject(report["stats"])
    except Exception as e:
        logger.exception("pricing report build failed")
        raise HTTPException(500, f"build failed: {e}")

    preview_path: Optional[str] = None
    if not dry:
        try:
            preview_path = write_preview(html)
        except Exception as e:
            logger.warning(f"preview write failed: {e}")

    mail_info = {"sent": False, "info": "skipped"}
    if send:
        recipient = to or ADMIN_EMAIL
        try:
            ok, info = await send_report(recipient, subject, html)
            mail_info = {"sent": ok, "info": info, "to": recipient,
                         "backend": mail_backend()}
        except Exception as e:
            mail_info = {"sent": False, "info": f"error: {e}",
                         "backend": mail_backend()}

    stats = report["stats"]
    # generated_at is a datetime — make JSON-serializable
    stats_out = {
        "n_total": stats["n_total"],
        "n_with_data": stats["n_with_data"],
        "n_recommendations": stats["n_recommendations"],
        "avg_market_position_pct": round(stats["avg_market_position_pct"], 2),
        "n_warnings": stats["n_warnings"],
        "fx_eur_chf": round(stats["fx_eur_chf"], 4),
        "generated_at": stats["generated_at"].isoformat(),
    }

    return {
        "ok": True,
        "subject": subject,
        "stats": stats_out,
        "preview_path": preview_path,
        "mail": mail_info,
    }


@router.get("/api/admin/pricing-report/preview")
async def get_pricing_preview(
    x_admin_secret: Optional[str] = Header(None, alias="X-Admin-Secret"),
):
    """Return the latest preview HTML (if it exists)."""
    _check_secret(x_admin_secret)
    if not os.path.exists(PREVIEW_PATH):
        raise HTTPException(404, "no preview yet — POST to /api/admin/pricing-report first")
    from fastapi.responses import FileResponse
    return FileResponse(PREVIEW_PATH, media_type="text/html")

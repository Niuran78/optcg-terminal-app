"""Standalone weekly pricing-report runner.

Designed to be invoked from a Render Cron Service:
    Cron schedule: 0 5 * * 1   (Monday 05:00 UTC = 07:00 CEST / 06:00 CET)
    Command:        python scripts/run_weekly_pricing_report.py

Reads ENV:
    SUPABASE_DSN, SHOPIFY_ADMIN_TOKEN, RESEND_API_KEY (or SMTP_*),
    ADMIN_EMAIL.

Exits 0 on success, 1 on hard error. A "no email backend configured"
condition is NOT a hard error — it logs a warning and writes the
preview file so the owner can inspect what *would* have been sent.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

# Allow `python scripts/run_weekly_pricing_report.py` from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.pricing_report import (
    ADMIN_EMAIL,
    build_recommendations_async,
    render_html,
    write_preview,
    _subject,
)
from services.pricing_mailer import send_report, _backend


async def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("pricing-cron")

    log.info("Starting weekly pricing report …")
    try:
        report = await build_recommendations_async()
    except Exception:
        log.exception("Failed to build recommendations")
        return 1

    html = render_html(report)
    subject = _subject(report["stats"])

    try:
        path = write_preview(html)
        log.info(f"Preview written: {path}")
    except Exception as e:
        log.warning(f"Could not write preview: {e}")

    backend = _backend()
    if backend == "none":
        log.warning(
            "No email backend configured — set RESEND_API_KEY or "
            "SMTP_HOST+SMTP_USER+SMTP_PASSWORD in env. Report not sent."
        )
        return 0  # Soft success

    recipient = os.getenv("PRICING_REPORT_TO", ADMIN_EMAIL)
    ok, info = await send_report(recipient, subject, html)
    if ok:
        log.info(f"Email sent to {recipient} via {backend}: {info}")
        return 0
    else:
        log.error(f"Email send failed via {backend}: {info}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

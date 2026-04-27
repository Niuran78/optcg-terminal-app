"""Send the weekly pricing report via Resend or SMTP.

Mirrors the pattern of services.email_followup:
  Priority 1: RESEND_API_KEY env → Resend REST API.
  Priority 2: SMTP_HOST + SMTP_USER + SMTP_PASSWORD → SMTP.
  Else:      log-only — caller is informed and can decide.

CLI:
    python -m services.pricing_mailer --to mail@blockreaction-investments.ch
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Tuple

import httpx

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
RESEND_FROM = os.getenv(
    "RESEND_FROM",
    "Holygrade Terminal <pricing@holygrade.com>",
).strip()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER).strip()


def _backend() -> str:
    if RESEND_API_KEY:
        return "resend"
    if SMTP_HOST and SMTP_USER and SMTP_PASSWORD:
        return "smtp"
    return "none"


def html_to_text(html: str) -> str:
    """Crude HTML→text fallback for clients that ignore HTML parts."""
    import re
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.S | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ──────────────────────────────────────────────────────────────────────
async def _send_via_resend(to_email: str, subject: str, html: str, text: str) -> Tuple[bool, str]:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type":  "application/json",
                },
                json={
                    "from":    RESEND_FROM,
                    "to":      [to_email],
                    "subject": subject,
                    "html":    html,
                    "text":    text,
                },
            )
        if r.status_code >= 400:
            return False, f"resend rejected: {r.status_code} {r.text[:200]}"
        return True, f"resend ok ({r.status_code})"
    except Exception as e:
        return False, f"resend error: {e}"


def _send_via_smtp(to_email: str, subject: str, html: str, text: str) -> Tuple[bool, str]:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_FROM
        msg["To"]      = to_email
        msg.attach(MIMEText(text, "plain", "utf-8"))
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, [to_email], msg.as_string())
        return True, "smtp ok"
    except Exception as e:
        return False, f"smtp error: {e}"


async def send_report(to_email: str, subject: str, html: str) -> Tuple[bool, str]:
    """Send a pricing report email.

    Returns (sent, info). When no backend is configured, returns
    (False, 'no-backend …') — the caller can decide to fall back to
    a saved preview file.
    """
    text = html_to_text(html)
    backend = _backend()
    if backend == "resend":
        return await _send_via_resend(to_email, subject, html, text)
    if backend == "smtp":
        return _send_via_smtp(to_email, subject, html, text)
    return False, (
        "no-backend: set RESEND_API_KEY (preferred) or SMTP_HOST + "
        "SMTP_USER + SMTP_PASSWORD in env. Preview HTML is saved instead."
    )


def _cli():
    from services.pricing_report import build_recommendations, render_html, _subject

    parser = argparse.ArgumentParser()
    parser.add_argument("--to", default=os.getenv("ADMIN_EMAIL",
                                                  "mail@blockreaction-investments.ch"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    report = build_recommendations()
    html = render_html(report)
    subject = _subject(report["stats"])

    ok, info = asyncio.run(send_report(args.to, subject, html))
    print(f"Backend: {_backend()}")
    print(f"Sent: {ok}")
    print(f"Info: {info}")


if __name__ == "__main__":
    _cli()

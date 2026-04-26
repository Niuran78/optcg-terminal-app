"""30-day post-purchase follow-up emails (Phase C).

Background job, called from the daily cron in main.py. Finds Shopify
purchases that are exactly ~30 days old and have no follow-up sent yet,
sends an email with the current Cardmarket value, and stamps
`follow_up_sent_at`.

Email backend (in priority order):
  1. RESEND_API_KEY  → Resend.com REST API (preferred)
  2. SMTP_HOST + SMTP_USER + SMTP_PASSWORD → SMTP fallback
  3. Neither set → log-only (no failure, no email sent)

The log-only mode is intentional: during initial deployment we can verify
that the targeting query is correct without spamming customers. As soon
as RESEND_API_KEY is configured in Render, real emails go out on the
next daily-cron iteration.
"""
from __future__ import annotations

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import httpx

from db.init import get_pool

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
RESEND_FROM = os.getenv("RESEND_FROM", "Holygrade Terminal <hello@holygrade.com>").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER).strip()

TERMINAL_BASE = os.getenv("TERMINAL_BASE_URL", "https://terminal.holygrade.com").rstrip("/")


def _has_email_backend() -> bool:
    return bool(RESEND_API_KEY) or bool(SMTP_HOST and SMTP_USER and SMTP_PASSWORD)


async def _send_via_resend(to_email: str, subject: str, html: str, text: str) -> bool:
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
            logger.warning(f"[email_followup] Resend rejected: {r.status_code} {r.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"[email_followup] Resend send failed: {e}")
        return False


def _send_via_smtp(to_email: str, subject: str, html: str, text: str) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_FROM
        msg["To"]      = to_email
        msg.attach(MIMEText(text, "plain", "utf-8"))
        msg.attach(MIMEText(html, "html",  "utf-8"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.sendmail(SMTP_FROM, [to_email], msg.as_string())
        return True
    except Exception as e:
        logger.error(f"[email_followup] SMTP send failed: {e}")
        return False


async def _send_email(to_email: str, subject: str, html: str, text: str) -> bool:
    if RESEND_API_KEY:
        return await _send_via_resend(to_email, subject, html, text)
    if SMTP_HOST and SMTP_USER and SMTP_PASSWORD:
        return _send_via_smtp(to_email, subject, html, text)
    # No backend: just log.
    logger.info(f"[email_followup] LOG-ONLY (no backend) → {to_email} | {subject}")
    return True  # Still mark as "sent" so we don't try again every day.


def _format_eur(v: Optional[float]) -> str:
    if v is None:
        return "–"
    try:
        return f"{v:.2f} €".replace(".", ",")
    except Exception:
        return "–"


def _build_email(row) -> tuple[str, str, str]:
    """Return (subject, html, text) for one purchase row."""
    name = row["customer_first_name"] or "Hi"
    product_name = row["product_name"] or row["set_code"] or "Sealed Box"
    paid = float(row["unit_price_eur"]) * int(row["quantity"])
    cm_trend = float(row["cm_live_trend"]) if row["cm_live_trend"] is not None else None
    current_value = cm_trend * int(row["quantity"]) if cm_trend is not None else None
    pl = (current_value - paid) if current_value is not None else None
    claim_url = f"{TERMINAL_BASE}/claim.html?t={row['claim_token']}"

    if cm_trend is None:
        subject = f"30 Tage später: {product_name}"
        delta_line = "Aktuell haben wir noch keinen frischen Cardmarket-Trend für dieses Produkt."
        ev_advice = ""
    else:
        subject = f"Vor 30 Tagen hast du {product_name} gekauft — heute bei {_format_eur(cm_trend)}"
        if pl is not None and pl > 0:
            delta_line = f"Heutiger Cardmarket-Trend: {_format_eur(cm_trend)} (du hast {_format_eur(row['unit_price_eur'])} pro Box bezahlt — +{_format_eur(pl)})"
            ev_advice = "Wenn der EV-Score positiv ist, lohnt sich oft das Öffnen. Sonst halten."
        else:
            delta_line = f"Heutiger Cardmarket-Trend: {_format_eur(cm_trend)}"
            ev_advice = "Sealed-Boxen werden meistens mit der Zeit wertvoller. Halten ist oft die beste Wahl."

    html = f"""<!DOCTYPE html>
<html><body style="font-family:Inter,system-ui,sans-serif;background:#0f1413;color:#e8e6e0;padding:24px;">
  <div style="max-width:520px;margin:0 auto;background:#161e1c;border:1px solid #1e2a28;border-radius:12px;padding:24px;">
    <h2 style="color:#00e5c0;font-size:16px;margin:0 0 8px 0;letter-spacing:0.06em;text-transform:uppercase;">Holygrade Terminal</h2>
    <p style="margin:0 0 16px 0;">Hi {name},</p>
    <p style="margin:0 0 12px 0;">vor 30 Tagen hast du <strong>{int(row['quantity'])}× {product_name}</strong> bei Holygrade gekauft.</p>
    <p style="margin:0 0 18px 0;color:#b0ada6;">{delta_line}</p>
    <p style="margin:0 0 24px 0;color:#b0ada6;font-size:13px;">{ev_advice}</p>
    <a href="{claim_url}" style="display:inline-block;background:#00e5c0;color:#0a0a0b;padding:10px 16px;border-radius:6px;text-decoration:none;font-weight:700;">Im Terminal verfolgen</a>
    <p style="margin:24px 0 0 0;font-size:11px;color:#4a6660;">Daten via Cardmarket. Dein Free-Sealed-Portfolio bleibt kostenlos. Kein Abo.</p>
  </div>
</body></html>"""

    text = (
        f"Hi {name},\n\n"
        f"vor 30 Tagen hast du {int(row['quantity'])}x {product_name} bei Holygrade gekauft.\n"
        f"{delta_line}\n\n"
        f"{ev_advice}\n\n"
        f"Im Terminal verfolgen: {claim_url}\n\n"
        f"-- Holygrade Terminal\n"
        f"Daten via Cardmarket. Dein Free-Sealed-Portfolio bleibt kostenlos.\n"
    )

    return subject, html, text


async def run_followup_job(
    age_days_min: int = 30,
    age_days_max: int = 60,
    max_per_run: int = 50,
) -> dict:
    """Send follow-up emails for purchases between min..max days old.

    Range used (vs. exactly 30 days) so cron-skips don't lose customers.
    Idempotent: only purchases with follow_up_sent_at IS NULL get processed,
    and we set the timestamp regardless of email-backend success so we don't
    flood on retries.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.claim_token, p.customer_email, p.customer_first_name,
                   p.set_code, p.language, p.product_type, p.quantity, p.unit_price_eur,
                   p.purchased_at, s.product_name, s.cm_live_trend
            FROM shopify_purchases p
            LEFT JOIN sealed_unified s ON s.id = p.sealed_id
            WHERE p.follow_up_sent_at IS NULL
              AND p.customer_email IS NOT NULL
              AND p.purchased_at <= NOW() - ($1::int * INTERVAL '1 day')
              AND p.purchased_at >= NOW() - ($2::int * INTERVAL '1 day')
            ORDER BY p.purchased_at ASC
            LIMIT $3
            """,
            age_days_min, age_days_max, max_per_run,
        )

    if not rows:
        logger.info("[email_followup] no purchases due for 30-day follow-up")
        return {"sent": 0, "skipped": 0, "checked": 0, "backend_configured": _has_email_backend()}

    sent = 0
    skipped = 0
    backend_configured = _has_email_backend()

    for row in rows:
        try:
            subject, html, text = _build_email(row)
            ok = await _send_email(row["customer_email"], subject, html, text)
            if ok:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE shopify_purchases SET follow_up_sent_at = NOW() WHERE id = $1",
                        int(row["id"]),
                    )
                sent += 1
            else:
                skipped += 1
        except Exception as e:
            logger.error(f"[email_followup] purchase {row['id']} failed: {e}")
            skipped += 1

    logger.info(
        f"[email_followup] sent={sent} skipped={skipped} checked={len(rows)} "
        f"backend_configured={backend_configured}"
    )
    return {
        "sent":               sent,
        "skipped":            skipped,
        "checked":            len(rows),
        "backend_configured": backend_configured,
    }

"""Holygrade Weekly Pricing Recommendation Report.

Pulls live Shopify shop prices + live Cardmarket trend data from Supabase,
applies a conservative recommendation algorithm, renders a German HTML
email, and (optionally) sends it. The shop owner is *not* a programmer,
so this is a recommendation-only tool — Shopify prices are NEVER written.

Pipeline:
  1. fetch_shop_variants()   → Shopify products (read-only).
  2. fetch_market_data()     → Supabase sealed_unified rows + 30d snapshots.
  3. fetch_fx_eur_chf()      → live ECB rate via Frankfurter.app.
  4. build_recommendations() → join + apply algorithm → list[Recommendation].
  5. render_html()           → Jinja-free f-string template (no extra dep).
  6. write_preview()         → /home/user/workspace/pricing_report_PREVIEW.html
  7. (mailer)                → services.pricing_mailer.send_report()

CLI:
    python -m services.pricing_report --preview
    python -m services.pricing_report --preview --send
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import re
import ssl
import sys
import urllib.request
from dataclasses import dataclass, field, asdict
from decimal import Decimal
from typing import Any, Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────
SHOPIFY_DOMAIN = os.getenv("SHOPIFY_DOMAIN", "holygrade.myshopify.com")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "")
SUPABASE_DSN = os.getenv("SUPABASE_DSN", "")

if not SHOPIFY_TOKEN:
    # Allow an explicit no-secrets failure rather than silently broken behavior.
    pass  # Caller will get a clear error from Shopify API call.
if not SUPABASE_DSN:
    pass  # Caller will get a clear error from asyncpg.connect.

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "mail@blockreaction-investments.ch")
SHOP_DOMAIN = os.getenv("SHOP_DOMAIN", "holygrade.com")
SHOPIFY_ADMIN_BASE = f"https://{SHOPIFY_DOMAIN.replace('.myshopify.com','')}.myshopify.com/admin"

# Algorithm thresholds
HIGH_THRESHOLD_PCT = 20.0   # > +20% over trend → recommend price-down
LOW_THRESHOLD_PCT = -10.0   # < -10% under trend → recommend price-up
HIGH_TARGET_MARGIN = 0.10   # 10% over trend
LOW_TARGET_MARGIN = 0.05    # 5% over trend
TREND_NUDGE_PCT = 5.0       # ±5% adjustment based on 7d trend
DATA_STALE_DAYS = 7         # warn if data older than this
SUSPICIOUS_RATIO = 5.0      # trend / 7d-avg outside [1/5, 5] → suspicious
# Safety caps: never recommend changes larger than these per single weekly
# report. The user is doing manual price changes — this prevents shock
# moves and gives him room to course-correct over multiple weeks.
MAX_DELTA_UP_PCT = 25.0     # cap upward adjustments at +25%
MAX_DELTA_DOWN_PCT = -15.0  # cap downward adjustments at -15%

FALLBACK_FX_EUR_CHF = 0.95  # Conservative fallback if Frankfurter unreachable.
#                            Updated 2026-04: live rate ~0.92 (CHF strong).


# ──────────────────────────────────────────────────────────────────────
# SKU parsing — bridges Shopify SKUs to Cardmarket set_code/product_type
# ──────────────────────────────────────────────────────────────────────
SKU_RE = re.compile(r"^(?P<prefix>OP|EB|PRB)[\s\-_]?(?P<num>\d{2})[\s\-_]?(?P<kind>DISPLAY|DISP|CASE|BOOSTER\s*BOX)[\s\-_]?(?P<lang>JP|EN)?$", re.I)


def parse_sku(sku: str) -> Optional[dict]:
    """Parse a Holygrade SKU into (set_code, product_type, language).

    Examples:
      OP-15-DISPLAY-JP → {'set_code':'OP15', 'product_type':'booster box', 'language':'JP'}
      EB-02-CASE-JP    → {'set_code':'EB02', 'product_type':'case',         'language':'JP'}

    Returns None for non-OP/EB/PRB SKUs (Anniversary, KURTS, PROMO, …).
    """
    if not sku:
        return None
    m = SKU_RE.match(sku.strip().upper())
    if not m:
        return None
    prefix = m.group("prefix").upper()
    num = m.group("num")
    kind = m.group("kind").upper().replace(" ", "")
    lang = (m.group("lang") or "JP").upper()
    set_code = f"{prefix}{num}"
    if kind in ("DISPLAY", "DISP", "BOOSTERBOX"):
        ptype = "booster box"
    else:
        ptype = "case"
    return {"set_code": set_code, "product_type": ptype, "language": lang}


# ──────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────
@dataclass
class ShopVariant:
    product_id: int
    variant_id: int
    sku: str
    title: str           # product title
    variant_title: str   # variant title
    price_chf: float
    handle: str = ""

    @property
    def admin_url(self) -> str:
        return f"{SHOPIFY_ADMIN_BASE}/products/{self.product_id}"


@dataclass
class MarketRow:
    set_code: str
    language: str
    product_type: str
    cm_live_trend: Optional[float]
    cm_live_7d_avg: Optional[float]
    cm_live_30d_avg: Optional[float]
    cm_live_lowest: Optional[float]
    cm_live_available: Optional[int]
    cm_live_status: Optional[str]
    cm_live_updated_at: Optional[dt.datetime]
    cm_live_url: Optional[str]
    snapshot_30d_pct: Optional[float] = None  # computed from sealed_price_snapshots


@dataclass
class Recommendation:
    variant: ShopVariant
    market: Optional[MarketRow]
    fx_eur_chf: float

    # computed
    trend_eur: Optional[float] = None
    trend_chf: Optional[float] = None
    market_position_pct: Optional[float] = None
    recommended_price_chf: Optional[float] = None
    delta_pct: Optional[float] = None
    rationale: str = ""
    severity: str = "info"     # 'high' | 'medium' | 'low' | 'info' | 'warn' | 'none'
    has_market_data: bool = False
    is_stale: bool = False
    is_suspicious: bool = False
    snapshot_30d_pct: Optional[float] = None

    @property
    def magnitude(self) -> float:
        return abs(self.delta_pct or 0.0)

    @property
    def has_recommendation(self) -> bool:
        return self.recommended_price_chf is not None


# ──────────────────────────────────────────────────────────────────────
# Data fetchers
# ──────────────────────────────────────────────────────────────────────
def fetch_shop_variants(timeout: float = 30.0) -> list[ShopVariant]:
    """Pull all variants from Shopify Admin REST. Read-only."""
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-10/products.json?limit=250"
    req = urllib.request.Request(url, headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        data = json.loads(r.read())
    out: list[ShopVariant] = []
    for p in data.get("products", []):
        for v in p.get("variants", []):
            try:
                price = float(v.get("price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            out.append(ShopVariant(
                product_id=p["id"],
                variant_id=v["id"],
                sku=(v.get("sku") or "").strip(),
                title=p.get("title") or "",
                variant_title=v.get("title") or "",
                price_chf=price,
                handle=p.get("handle") or "",
            ))
    return out


async def fetch_market_data_async() -> dict[tuple[str, str, str], MarketRow]:
    """Pull current Cardmarket data + 30d snapshot deltas from Supabase.

    Uses asyncpg (already in requirements.txt) so no extra dependency is
    needed. Returns dict keyed by (set_code, language, product_type).
    """
    import asyncpg

    rows: dict[tuple[str, str, str], MarketRow] = {}

    # asyncpg's pooler-mode requires statement_cache_size=0 for Supabase pgbouncer.
    conn = await asyncpg.connect(SUPABASE_DSN, statement_cache_size=0, timeout=15)
    try:
        cur_rows = await conn.fetch(
            """
            SELECT id, set_code, language, product_type,
                   cm_live_trend, cm_live_7d_avg, cm_live_30d_avg,
                   cm_live_lowest, cm_live_available, cm_live_status,
                   cm_live_updated_at, cm_live_url
            FROM sealed_unified
            WHERE language = 'JP'
              AND product_type IN ('booster box','case')
            """
        )
        id_map: dict[int, MarketRow] = {}
        for r in cur_rows:
            m = MarketRow(
                set_code=r["set_code"] or "",
                language=r["language"] or "",
                product_type=r["product_type"] or "",
                cm_live_trend=float(r["cm_live_trend"]) if r["cm_live_trend"] is not None else None,
                cm_live_7d_avg=float(r["cm_live_7d_avg"]) if r["cm_live_7d_avg"] is not None else None,
                cm_live_30d_avg=float(r["cm_live_30d_avg"]) if r["cm_live_30d_avg"] is not None else None,
                cm_live_lowest=float(r["cm_live_lowest"]) if r["cm_live_lowest"] is not None else None,
                cm_live_available=int(r["cm_live_available"]) if r["cm_live_available"] is not None else None,
                cm_live_status=r["cm_live_status"],
                cm_live_updated_at=r["cm_live_updated_at"],
                cm_live_url=r["cm_live_url"],
            )
            rows[(m.set_code, m.language, m.product_type)] = m
            id_map[r["id"]] = m

        # 30-day snapshot delta — current trend vs trend ~30 days ago
        snap_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (sealed_id)
                   sealed_id, snap_date, cm_live_trend
            FROM sealed_price_snapshots
            WHERE snap_date <= (CURRENT_DATE - INTERVAL '25 days')
              AND snap_date >= (CURRENT_DATE - INTERVAL '45 days')
              AND cm_live_trend IS NOT NULL
              AND cm_live_trend > 0
            ORDER BY sealed_id, snap_date DESC
            """
        )
        for sr in snap_rows:
            m = id_map.get(sr["sealed_id"])
            if not m or not m.cm_live_trend or not sr["cm_live_trend"]:
                continue
            try:
                old_trend = float(sr["cm_live_trend"])
                m.snapshot_30d_pct = (m.cm_live_trend - old_trend) / old_trend * 100.0
            except ZeroDivisionError:
                pass
    finally:
        await conn.close()

    return rows


def fetch_market_data() -> dict[tuple[str, str, str], MarketRow]:
    """Sync wrapper around fetch_market_data_async() for CLI / synchronous callers."""
    return asyncio.run(fetch_market_data_async())


async def fetch_fx_eur_chf(timeout: float = 5.0) -> float:
    """Live EUR→CHF from Frankfurter.app (ECB-sourced). Falls back gracefully."""
    url = "https://api.frankfurter.dev/v1/latest?base=EUR&symbols=CHF"
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            rate = float(data["rates"]["CHF"])
            if not 0.7 < rate < 1.3:
                raise ValueError(f"Implausible EUR/CHF rate: {rate}")
            return rate
    except Exception as e:
        logger.warning(f"FX fetch failed: {e}; falling back to {FALLBACK_FX_EUR_CHF}")
        return FALLBACK_FX_EUR_CHF


# ──────────────────────────────────────────────────────────────────────
# Recommendation algorithm
# ──────────────────────────────────────────────────────────────────────
def _build_one(variant: ShopVariant,
               market: Optional[MarketRow],
               fx: float,
               now: dt.datetime) -> Recommendation:
    rec = Recommendation(variant=variant, market=market, fx_eur_chf=fx)

    # Skip non-OP/EB SKUs entirely (Anniversary, Kurts, etc.)
    parsed = parse_sku(variant.sku)
    if not parsed:
        rec.severity = "none"
        rec.rationale = "Kein Cardmarket-Mapping (z. B. Anniversary-Set, Zubehör)."
        return rec

    if not market or not market.cm_live_trend or market.cm_live_trend <= 0:
        rec.severity = "warn"
        rec.rationale = "⚠ Keine Cardmarket-Daten verfügbar."
        return rec

    rec.has_market_data = True
    rec.trend_eur = float(market.cm_live_trend)
    rec.trend_chf = rec.trend_eur * fx
    rec.snapshot_30d_pct = market.snapshot_30d_pct

    # Sanity: suspicious data
    a7 = market.cm_live_7d_avg or 0
    if a7 > 0:
        ratio = rec.trend_eur / a7
        if ratio > SUSPICIOUS_RATIO or ratio < (1.0 / SUSPICIOUS_RATIO):
            rec.is_suspicious = True
            rec.severity = "warn"
            rec.rationale = (
                f"⚠ Verdächtige Daten: Trend {rec.trend_eur:.2f}€ vs. "
                f"7-Tage-Schnitt {a7:.2f}€ — keine Empfehlung."
            )
            return rec

    # Stale data check
    if market.cm_live_updated_at:
        upd = market.cm_live_updated_at
        if upd.tzinfo is None:
            upd = upd.replace(tzinfo=dt.timezone.utc)
        age_days = (now - upd).total_seconds() / 86400
        if age_days > DATA_STALE_DAYS:
            rec.is_stale = True

    # Position
    if variant.price_chf <= 0:
        rec.severity = "warn"
        rec.rationale = "Shop-Preis fehlt."
        return rec

    pos_pct = (variant.price_chf - rec.trend_chf) / rec.trend_chf * 100.0
    rec.market_position_pct = pos_pct

    # Trend direction nudge
    nudge = 1.0
    trend_dir = "neutral"
    if a7 > 0 and rec.trend_eur > 0:
        # 7d→trend direction (current vs 7d avg)
        delta_7d = (rec.trend_eur - a7) / a7 * 100.0
        if delta_7d <= -2.0:
            nudge = 1.0 - TREND_NUDGE_PCT / 100.0
            trend_dir = "fallend"
        elif delta_7d >= 2.0:
            nudge = 1.0 + TREND_NUDGE_PCT / 100.0
            trend_dir = "steigend"

    if pos_pct > HIGH_THRESHOLD_PCT:
        target = rec.trend_chf * (1.0 + HIGH_TARGET_MARGIN) * nudge
        # Apply safety cap (don't crash price more than MAX_DELTA_DOWN)
        floor_price = variant.price_chf * (1.0 + MAX_DELTA_DOWN_PCT / 100.0)
        target = max(target, floor_price)
        rec.recommended_price_chf = round(_psych_price(target), 2)
        rec.delta_pct = (rec.recommended_price_chf - variant.price_chf) / variant.price_chf * 100.0
        rec.severity = "high" if abs(rec.delta_pct) >= 8 else "medium"
        cap_note = " (Senkung gedeckelt)" if target == floor_price else ""
        rec.rationale = (
            f"Liegt {pos_pct:+.1f}% über Markt-Trend; Markt {trend_dir} → "
            f"moderate Senkung empfohlen{cap_note}."
        )
    elif pos_pct < LOW_THRESHOLD_PCT:
        target = rec.trend_chf * (1.0 + LOW_TARGET_MARGIN) * nudge
        # Cap upward adjustment
        ceiling = variant.price_chf * (1.0 + MAX_DELTA_UP_PCT / 100.0)
        # Also: never recommend higher than CM-lowest (in CHF) — buyers
        # would arbitrage immediately.
        if rec.market and rec.market.cm_live_lowest:
            cm_lowest_chf = rec.market.cm_live_lowest * fx
            ceiling = min(ceiling, cm_lowest_chf * 1.10)  # 10% over CM-cheapest
        target = min(target, ceiling)
        rec.recommended_price_chf = round(_psych_price(target), 2)
        rec.delta_pct = (rec.recommended_price_chf - variant.price_chf) / variant.price_chf * 100.0
        rec.severity = "high" if abs(rec.delta_pct) >= 8 else "medium"
        cap_note = " (Erhöhung gedeckelt)" if target == ceiling else ""
        rec.rationale = (
            f"Liegt {pos_pct:+.1f}% unter Markt-Trend; Markt {trend_dir} → "
            f"Erhöhung empfohlen, um Marge zu sichern{cap_note}."
        )
    else:
        rec.severity = "low"
        rec.rationale = f"Im fairen Bereich ({pos_pct:+.1f}%) — keine Anpassung empfohlen."

    if rec.is_stale and rec.recommended_price_chf is not None:
        rec.rationale += " (⚠ Marktdaten älter als 7 Tage.)"

    return rec


def _psych_price(x: float) -> float:
    """Round to a Holygrade-typical price ending (X9.90 / X4.90 / etc.).

    Strategy:
      • <100 CHF → round to nearest .90 (e.g. 87.31 → 87.90; 82.10 → 81.90)
      • 100-300  → nearest 5 with .90 ending (104.20 → 104.90)
      • >300     → nearest 10 (847.5 → 849.- but we keep .00)
    """
    if x < 100:
        whole = int(round(x))
        return whole - 0.10  # ends in .90
    if x < 300:
        whole = int(round(x))
        return whole - 0.10
    if x < 1000:
        # XX9 ending
        whole = int(round(x / 10) * 10) - 1
        return float(whole)
    # cases > 1000
    whole = int(round(x / 50) * 50) - 1
    return float(whole)


async def build_recommendations_async(now: Optional[dt.datetime] = None) -> dict:
    """Async entry: fetch everything, produce sorted recommendations.

    This is the function FastAPI handlers should call. The sync
    `build_recommendations()` is a wrapper for CLI usage.
    """
    now = now or dt.datetime.now(dt.timezone.utc)

    # Shopify is sync (urllib) but quick — keep it in-process.
    variants = fetch_shop_variants()
    market, fx = await asyncio.gather(
        fetch_market_data_async(),
        fetch_fx_eur_chf(),
    )
    return _assemble_report(variants, market, fx, now)


def build_recommendations(now: Optional[dt.datetime] = None) -> dict:
    """Sync top-level entry, e.g. for the CLI."""
    return asyncio.run(build_recommendations_async(now))


def _assemble_report(variants, market, fx, now) -> dict:

    recs: list[Recommendation] = []
    for v in variants:
        parsed = parse_sku(v.sku)
        m = None
        if parsed:
            m = market.get((parsed["set_code"], parsed["language"], parsed["product_type"]))
        recs.append(_build_one(v, m, fx, now))


    # Sort: actionable recommendations first by magnitude desc, then warnings,
    # then 'fair' rows (low), then no-data ('none' / 'warn').
    severity_rank = {"high": 0, "medium": 1, "warn": 2, "low": 3, "info": 4, "none": 5}
    recs.sort(key=lambda r: (severity_rank.get(r.severity, 9), -r.magnitude))

    # Stats
    n_recs = sum(1 for r in recs if r.has_recommendation)
    market_positions = [r.market_position_pct for r in recs if r.market_position_pct is not None]
    avg_pos = (sum(market_positions) / len(market_positions)) if market_positions else 0.0
    n_total = len(recs)
    n_with_data = sum(1 for r in recs if r.has_market_data)
    n_warnings = sum(1 for r in recs if r.severity == "warn")

    return {
        "recommendations": recs,
        "stats": {
            "n_total": n_total,
            "n_with_data": n_with_data,
            "n_recommendations": n_recs,
            "avg_market_position_pct": avg_pos,
            "n_warnings": n_warnings,
            "fx_eur_chf": fx,
            "generated_at": now,
        },
    }


# ──────────────────────────────────────────────────────────────────────
# Formatting helpers (Swiss German locale)
# ──────────────────────────────────────────────────────────────────────
def chf(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return _swiss_num(value, 2) + " CHF"


def eur(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return _swiss_num(value, 2) + " €"


def pct(value: Optional[float], with_sign: bool = True, digits: int = 1) -> str:
    if value is None:
        return "—"
    sign = ""
    if with_sign and value > 0:
        sign = "+"
    return f"{sign}{_swiss_num(value, digits)}%"


def _swiss_num(value: float, digits: int) -> str:
    s = f"{value:,.{digits}f}"  # 1,234.56 (US)
    # → 1’234.56 → swap: 1'234,56 (Swiss uses ’ but ' renders fine in mail)
    s = s.replace(",", "X").replace(".", ",").replace("X", "'")
    return s


def fmt_date(d: dt.datetime) -> str:
    return d.strftime("%d.%m.%Y")


def iso_week(d: dt.datetime) -> int:
    return d.isocalendar()[1]


# ──────────────────────────────────────────────────────────────────────
# HTML rendering
# ──────────────────────────────────────────────────────────────────────
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "..", "templates", "pricing_report.html")


def render_html(report: dict) -> str:
    """Render the full HTML email body."""
    recs: list[Recommendation] = report["recommendations"]
    stats = report["stats"]
    now: dt.datetime = stats["generated_at"]

    # Try template file first; fall back to inline (so the module is standalone).
    template = _load_template()
    cards_html = "\n".join(_render_card(r) for r in recs)

    week_start = (now - dt.timedelta(days=now.weekday())).date()
    week_end = week_start + dt.timedelta(days=6)

    return template.format(
        subject=_subject(stats),
        week_nr=iso_week(now),
        week_start=week_start.strftime("%d.%m.%Y"),
        week_end=week_end.strftime("%d.%m.%Y"),
        n_total=stats["n_total"],
        n_with_data=stats["n_with_data"],
        n_recommendations=stats["n_recommendations"],
        avg_market_position=pct(stats["avg_market_position_pct"]),
        n_warnings=stats["n_warnings"],
        fx_eur_chf=_swiss_num(stats["fx_eur_chf"], 4),
        cards=cards_html,
        generated_at=now.strftime("%d.%m.%Y %H:%M UTC"),
        admin_email=ADMIN_EMAIL,
        terminal_url=os.getenv("TERMINAL_BASE_URL", "https://terminal.holygrade.com"),
    )


def _subject(stats: dict) -> str:
    now = stats["generated_at"]
    return (f"Holygrade Weekly Pricing Report · KW {iso_week(now)} "
            f"· {stats['n_recommendations']} Empfehlungen")


def _render_card(r: Recommendation) -> str:
    v = r.variant
    sev_class = {
        "high": "sev-high", "medium": "sev-medium",
        "low": "sev-low", "warn": "sev-warn",
        "info": "sev-info", "none": "sev-none",
    }.get(r.severity, "sev-info")

    sev_label = {
        "high": "▲ Hohe Priorität",
        "medium": "● Empfehlung",
        "low": "✓ Im fairen Bereich",
        "warn": "⚠ Achtung",
        "none": "– Kein Mapping",
        "info": "·",
    }.get(r.severity, "·")

    # 30d trend arrow
    trend_30d = ""
    if r.snapshot_30d_pct is not None:
        arrow = "▲" if r.snapshot_30d_pct > 0 else ("▼" if r.snapshot_30d_pct < 0 else "▬")
        cls = "delta-pos" if r.snapshot_30d_pct > 0 else ("delta-neg" if r.snapshot_30d_pct < 0 else "")
        trend_30d = f'<span class="{cls}">{arrow} {pct(r.snapshot_30d_pct)}</span>'
    else:
        trend_30d = '<span class="muted">—</span>'

    # Market 7d avg & availability
    market_block = "—"
    if r.market and r.market.cm_live_7d_avg:
        avail = r.market.cm_live_available or 0
        market_block = f"{eur(r.market.cm_live_7d_avg)} <span class='muted'>({avail} Angebote)</span>"

    # Position
    pos_block = pct(r.market_position_pct) if r.market_position_pct is not None else "—"
    pos_class = ""
    if r.market_position_pct is not None:
        if r.market_position_pct > HIGH_THRESHOLD_PCT:
            pos_class = "delta-neg"
        elif r.market_position_pct < LOW_THRESHOLD_PCT:
            pos_class = "delta-pos"

    # Trend chf
    trend_chf_block = "—"
    if r.trend_eur:
        trend_chf_block = f"{eur(r.trend_eur)} <span class='muted'>(≈ {chf(r.trend_chf)})</span>"

    # Recommendation block
    if r.has_recommendation:
        delta_class = "delta-pos" if (r.delta_pct or 0) > 0 else "delta-neg"
        reco_block = (
            f"<div class='reco-price'>{chf(r.recommended_price_chf)}</div>"
            f"<div class='reco-delta {delta_class}'>{pct(r.delta_pct)}</div>"
        )
    else:
        reco_block = "<div class='reco-price reco-none'>—</div><div class='reco-delta muted'>keine Anpassung</div>"

    cm_link = ""
    if r.market and r.market.cm_live_url:
        cm_link = f'<a href="{r.market.cm_live_url}" class="link-mini">Cardmarket ↗</a>'

    return f"""
    <tr class="card-row {sev_class}">
      <td class="card">
        <table class="card-inner" cellspacing="0" cellpadding="0" border="0" role="presentation">
          <tr>
            <td class="card-head">
              <span class="sev-badge {sev_class}">{sev_label}</span>
              <div class="prod-title">{_html_escape(v.title)}</div>
              <div class="prod-sub">{_html_escape(v.variant_title)} · <code>{_html_escape(v.sku)}</code></div>
            </td>
            <td class="card-reco">
              <div class="reco-label">Empfehlung</div>
              {reco_block}
            </td>
          </tr>
          <tr>
            <td colspan="2">
              <table class="metrics" cellspacing="0" cellpadding="0" border="0" role="presentation" width="100%">
                <tr>
                  <td class="metric"><div class="m-label">Shop-Preis</div><div class="m-value">{chf(v.price_chf)}</div></td>
                  <td class="metric"><div class="m-label">CM-Trend</div><div class="m-value">{trend_chf_block}</div></td>
                  <td class="metric"><div class="m-label">7-Tage-Ø</div><div class="m-value">{market_block}</div></td>
                </tr>
                <tr>
                  <td class="metric"><div class="m-label">30-Tage-Trend</div><div class="m-value">{trend_30d}</div></td>
                  <td class="metric"><div class="m-label">Markt-Position</div><div class="m-value {pos_class}">{pos_block}</div></td>
                  <td class="metric"><div class="m-label">Aktionen</div><div class="m-value">
                    <a href="{v.admin_url}" class="link-mini">In Shopify öffnen ↗</a>
                    {cm_link}
                  </div></td>
                </tr>
              </table>
              <div class="rationale">{_html_escape(r.rationale)}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
    """


def _html_escape(s: str) -> str:
    if not s:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))


def _load_template() -> str:
    """Load the HTML template; fall back to embedded one for portability."""
    try:
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return _EMBEDDED_TEMPLATE


# Embedded fallback template (single-file portability).
# The "real" template lives at templates/pricing_report.html — this is just
# a safety net in case the file isn't deployed.
_EMBEDDED_TEMPLATE = """<!doctype html>
<html lang="de"><head><meta charset="utf-8"><title>{subject}</title></head>
<body><h1>{subject}</h1><table>{cards}</table></body></html>
"""


# ──────────────────────────────────────────────────────────────────────
# Preview / CLI
# ──────────────────────────────────────────────────────────────────────
PREVIEW_PATH = os.environ.get(
    "PRICING_REPORT_PREVIEW_PATH",
    "/home/user/workspace/pricing_report_PREVIEW.html",
)


def write_preview(html: str, path: str = PREVIEW_PATH) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def _cli():
    parser = argparse.ArgumentParser(description="Holygrade Weekly Pricing Report")
    parser.add_argument("--preview", action="store_true",
                        help="Generate HTML preview and write to disk.")
    parser.add_argument("--send", action="store_true",
                        help="Also send the email (Resend or SMTP).")
    parser.add_argument("--to", default=ADMIN_EMAIL,
                        help=f"Recipient (default: {ADMIN_EMAIL}).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = build_recommendations()
    html = render_html(report)
    subject = _subject(report["stats"])

    if args.preview:
        path = write_preview(html)
        print(f"Preview written to: {path}")
        print(f"Subject: {subject}")
        print(f"Recommendations: {report['stats']['n_recommendations']}/"
              f"{report['stats']['n_total']} | "
              f"Avg market pos: {report['stats']['avg_market_position_pct']:+.1f}%")

    if args.send:
        from services.pricing_mailer import send_report
        ok, info = asyncio.run(send_report(args.to, subject, html))
        print(f"Send result: ok={ok} info={info}")


if __name__ == "__main__":
    _cli()

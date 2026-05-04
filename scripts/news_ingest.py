"""News ingest pipeline — fetches from all active sources and writes to news_items.

Run:  python scripts/news_ingest.py          (one-shot)
Cron: called from main.py daily sync loop or via Render cron

Sources:
  1. Bandai EN official site — HTML parse
  2. YouTube RSS (Bandai Card Games channel)
  3. Limitless TCG — tournament results HTML parse
  4. Market signals — generated from sealed_unified + snapshots
"""
import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import asyncpg
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Add parent dir for imports when running standalone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("news_ingest")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.gwddradbzpsygmzlrrcm:GQiCNTE5gg1IHfwR5sMf6SbppIlOaiYR@aws-1-eu-central-1.pooler.supabase.com:5432/postgres",
)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
SCRAPFLY_KEY = os.getenv(
    "SCRAPFLY_API_KEY",
    "scp-live-a2fc83ff48e244a7a72912b93a2c405b",
)

SET_CODE_RE = re.compile(r"\b(OP-?\d{2}|EB-?\d{2}|ST-?\d{2}|PRB-?\d{2})\b", re.IGNORECASE)
UA = "Mozilla/5.0 (compatible; HolygradeTerminal/1.0; +https://terminal.holygrade.com)"


# ═══════════════════════════════════════════════════════════════════
# DB helpers
# ═══════════════════════════════════════════════════════════════════

async def get_conn():
    p = urlparse(DATABASE_URL)
    return await asyncpg.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        database=p.path.lstrip("/") or "postgres",
        ssl="require",
    )


def compute_featured_score(source, category, published_at, related_set):
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


def extract_set_code(text):
    m = SET_CODE_RE.search(text)
    if m:
        code = m.group(1).upper().replace("-", "")
        # Normalize: OP13 -> OP-13
        if len(code) >= 4 and code[:2].isalpha() and code[2:].isdigit():
            return code[:2] + "-" + code[2:]
        return code
    return None


def categorize(title, source_type, related_set):
    title_lower = title.lower()
    if source_type == "holygrade":
        return "shop"
    if source_type == "market":
        return "market"
    tournament_kw = ["tournament", "regional", "top 8", "top8", "champion", "turnier",
                     "treasure cup", "deckliste", "decklist", "finals", "regionals"]
    if any(k in title_lower for k in tournament_kw):
        return "tournament"
    if related_set:
        return "set_release"
    market_kw = ["+%", "preis", "price", "trend", "markt", "stock", "lager"]
    if any(k in title_lower for k in market_kw):
        return "market"
    return "other"


async def translate_to_de(title_en):
    """Translate an English headline to German using Claude Haiku."""
    if not ANTHROPIC_API_KEY:
        return title_en  # fallback: return English
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 150,
                    "messages": [{"role": "user", "content": (
                        "Übersetze diese One Piece TCG News-Überschrift ins Deutsche. "
                        "Max 120 Zeichen. Behalte Kartennamen (z.B. Monkey.D.Luffy) und "
                        "Set-Codes (z.B. OP-13, EB-04) auf Englisch/Original. "
                        "Gib NUR die Übersetzung zurück, keine Erklärung.\n\n"
                        f'Headline: "{title_en}"'
                    )}],
                },
            )
            data = resp.json()
            text = data.get("content", [{}])[0].get("text", "").strip()
            # Remove quotes if the model wrapped the translation
            if text.startswith('"') and text.endswith('"'):
                text = text[1:-1]
            return text[:120] if text else title_en
    except Exception as e:
        logger.warning(f"Translation failed: {e}")
        return title_en


async def translate_jp_to_de(text_jp: str) -> str:
    """Translate a Japanese headline to German using available LLM."""
    if not text_jp or not text_jp.strip():
        return text_jp
    sys_prompt = (
        "Translate this Japanese One Piece TCG news headline to German. "
        "Keep set codes (OP-XX, EB-XX) untouched. Be concise. "
        "Return ONLY the translation, no explanation."
    )
    # Try OpenAI first (cheaper), then Anthropic
    if OPENAI_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    json={
                        "model": "gpt-4o-mini",
                        "max_tokens": 150,
                        "messages": [
                            {"role": "system", "content": sys_prompt},
                            {"role": "user", "content": text_jp},
                        ],
                    },
                )
                data = resp.json()
                text = data["choices"][0]["message"]["content"].strip()
                if text.startswith('"') and text.endswith('"'):
                    text = text[1:-1]
                return text[:120] if text else text_jp
        except Exception as e:
            logger.warning(f"OpenAI JP→DE translation failed: {e}")

    if ANTHROPIC_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 150,
                        "messages": [{"role": "user", "content": f"{sys_prompt}\n\n{text_jp}"}],
                    },
                )
                data = resp.json()
                text = data.get("content", [{}])[0].get("text", "").strip()
                if text.startswith('"') and text.endswith('"'):
                    text = text[1:-1]
                return text[:120] if text else text_jp
        except Exception as e:
            logger.warning(f"Anthropic JP→DE translation failed: {e}")

    logger.warning("No LLM key available for JP→DE translation")
    return text_jp


async def insert_news_item(conn, source, source_key, source_url, title_de,
                           title_en, teaser_de, category, language,
                           related_set, published_at, override_score=None):
    """Insert a news item, skip if URL already exists."""
    # Check dedup first (cheap)
    exists = await conn.fetchval(
        "SELECT 1 FROM news_items WHERE source_url = $1", source_url
    )
    if exists:
        return False

    score = override_score if override_score is not None else compute_featured_score(source, category, published_at, related_set)
    try:
        await conn.execute(
            "INSERT INTO news_items "
            "(source, source_key, source_url, title_de, title_en, teaser_de, "
            " category, language, related_set, featured_score, published_at) "
            "VALUES ($1::news_source_type, $2, $3, $4, $5, $6, "
            "        $7::news_category, $8::news_language, $9, $10, $11) "
            "ON CONFLICT (source_url) DO NOTHING",
            source, source_key, source_url, title_de[:120], title_en,
            teaser_de[:240] if teaser_de else None,
            category, language, related_set, score, published_at,
        )
        return True
    except Exception as e:
        logger.error(f"Insert failed for {source_url}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════
# Source: Bandai EN official site
# ═══════════════════════════════════════════════════════════════════

async def ingest_bandai_en(conn):
    """Parse https://en.onepiece-cardgame.com/news/ for news items."""
    logger.info("Ingesting Bandai EN news...")
    count = 0
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.get(
                "https://en.onepiece-cardgame.com/news/",
                headers={"User-Agent": UA},
            )
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Parse news items — they are <a class="newsListLink">
        for link in soup.find_all("a", class_="newsListLink"):
            href = link.get("href", "")
            if not href:
                continue
            # Make absolute URL
            if href.startswith("/"):
                source_url = "https://en.onepiece-cardgame.com" + href
            elif href.startswith("http"):
                source_url = href
            else:
                source_url = "https://en.onepiece-cardgame.com/" + href

            # Extract title
            title_el = link.find(class_="newsTitle") or link.find("h3") or link.find("h2")
            if not title_el:
                # Use full link text as fallback
                raw_title = link.get_text(strip=True)
            else:
                raw_title = title_el.get_text(strip=True)

            if not raw_title or len(raw_title) < 5:
                continue

            # Extract date
            date_el = link.find(class_="newsDate")
            pub_at = datetime.now(timezone.utc)
            if date_el:
                date_text = date_el.get_text(strip=True)
                for fmt in ["%B %d, %Y", "%Y.%m.%d", "%Y-%m-%d", "%m/%d/%Y"]:
                    try:
                        pub_at = datetime.strptime(date_text, fmt).replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        continue

            # Extract teaser
            teaser_el = link.find(class_="newsLead")
            teaser = teaser_el.get_text(strip=True)[:240] if teaser_el else None

            # Set code and category
            related_set = extract_set_code(raw_title)
            category = categorize(raw_title, "bandai", related_set)

            # Translate to German
            title_de = await translate_to_de(raw_title)
            title_en = raw_title[:120]

            inserted = await insert_news_item(
                conn, "bandai", "bandai_op_official", source_url,
                title_de, title_en, teaser, category, "en",
                related_set, pub_at,
            )
            if inserted:
                count += 1
                logger.info(f"  + {title_de[:60]}")

        # Skip events page — too noisy, main /news/ page is sufficient

    except Exception as e:
        logger.error(f"Bandai EN ingest failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='bandai_op_official'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='bandai_op_official'",
        count,
    )
    logger.info(f"Bandai EN: {count} new items ingested.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Source: YouTube RSS (Bandai Card Games)
# ═══════════════════════════════════════════════════════════════════

async def ingest_youtube_rss(conn):
    """Parse YouTube RSS feed for One Piece TCG videos."""
    import feedparser

    logger.info("Ingesting YouTube RSS...")
    count = 0

    # Get feed URL from DB
    row = await conn.fetchrow(
        "SELECT feed_url FROM news_sources WHERE source_key='youtube_bandai'"
    )
    if not row or not row["feed_url"]:
        logger.warning("No YouTube feed URL configured.")
        return 0

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(row["feed_url"])
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)

        for entry in feed.entries:
            title = entry.title
            link = entry.link

            # Filter: only One Piece related videos
            title_lower = title.lower()
            if not any(k in title_lower for k in ["one piece", "op-", "op ", "optcg", "opcg"]):
                continue

            # Parse published date
            pub_at = datetime.now(timezone.utc)
            if hasattr(entry, "published"):
                try:
                    from email.utils import parsedate_to_datetime
                    pub_at = parsedate_to_datetime(entry.published)
                except Exception:
                    pass
            if pub_at.tzinfo is None:
                pub_at = pub_at.replace(tzinfo=timezone.utc)

            related_set = extract_set_code(title)
            category = categorize(title, "bandai", related_set)

            title_de = await translate_to_de(title)

            inserted = await insert_news_item(
                conn, "bandai", "youtube_bandai", link,
                title_de, title[:120], None, category, "en",
                related_set, pub_at,
            )
            if inserted:
                count += 1
                logger.info(f"  + (YT) {title_de[:60]}")

    except Exception as e:
        logger.error(f"YouTube RSS ingest failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='youtube_bandai'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='youtube_bandai'",
        count,
    )
    logger.info(f"YouTube: {count} new items ingested.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Source: Limitless TCG — tournament results
# ═══════════════════════════════════════════════════════════════════

async def ingest_limitless(conn):
    """Parse onepiece.limitlesstcg.com for tournament & meta news."""
    logger.info("Ingesting Limitless TCG...")
    count = 0

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            resp = await client.get(
                "https://onepiece.limitlesstcg.com/",
                headers={"User-Agent": UA},
            )
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Skip navigation / generic links
        _SKIP_TITLES = {
            "all upcoming tournaments", "tournament platform",
            "all tournaments", "see all", "view all",
            "home", "about", "contact", "login", "sign up",
        }

        # Extract tournament links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if not text or len(text) < 5:
                continue

            # Filter out navigation links and junk
            text_lower = text.lower().strip()
            if text_lower in _SKIP_TITLES:
                continue
            if "event period:" in text_lower:
                continue
            # Skip pure navigation paths (no specific tournament/deck ID)
            if href.rstrip("/") in ("/tournaments", "/decks", "/"):
                continue

            source_url = None
            category = "tournament"

            if "/tournaments/" in href:
                # Tournament result or event — must have a specific ID after /tournaments/
                path_after = href.split("/tournaments/")[-1].strip("/")
                if not path_after:
                    continue  # Skip index page link
                if href.startswith("/"):
                    source_url = "https://onepiece.limitlesstcg.com" + href
                elif href.startswith("http"):
                    source_url = href
            elif "/decks/" in href and "%" in text:
                # Meta deck share (e.g., "1. Purple Enel 44.03%")
                if href.startswith("/"):
                    source_url = "https://onepiece.limitlesstcg.com" + href
                elif href.startswith("http"):
                    source_url = href

            if not source_url:
                continue

            related_set = extract_set_code(text)
            title_de = await translate_to_de(text)

            inserted = await insert_news_item(
                conn, "community", "limitless_tcg", source_url,
                title_de, text[:120], "Quelle: Limitless TCG", category, "en",
                related_set, datetime.now(timezone.utc),
            )
            if inserted:
                count += 1
                logger.info(f"  + (LT) {title_de[:60]}")

    except Exception as e:
        logger.error(f"Limitless TCG ingest failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='limitless_tcg'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='limitless_tcg'",
        count,
    )
    logger.info(f"Limitless: {count} new items ingested.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Source: Market signals from our own DB
# ═══════════════════════════════════════════════════════════════════

async def ingest_market_signals(conn):
    """Generate news items from sealed_unified price movements."""
    logger.info("Generating market signals...")
    count = 0
    max_signals = 3  # Max per run to avoid spam

    try:
        # Strong movers: 7d trend vs 30d avg, threshold >=15%
        movers = await conn.fetch("""
            SELECT set_code, product_name, cm_live_trend, cm_live_30d_avg,
                   cm_live_available, cm_live_7d_avg, language,
                   ROUND(((cm_live_trend - cm_live_30d_avg) / NULLIF(cm_live_30d_avg, 0) * 100)::numeric, 1) as trend_pct
            FROM sealed_unified
            WHERE language = 'JP'
              AND cm_live_trend IS NOT NULL
              AND cm_live_30d_avg IS NOT NULL
              AND cm_live_30d_avg > 0
              AND ABS(cm_live_trend - cm_live_30d_avg) / cm_live_30d_avg >= 0.15
            ORDER BY ABS(cm_live_trend - cm_live_30d_avg) / cm_live_30d_avg DESC
            LIMIT 5
        """)

        for row in movers:
            if count >= max_signals:
                break

            set_code = row["set_code"]
            pct = float(row["trend_pct"])
            price = round(float(row["cm_live_trend"]), 2)
            name = row["product_name"][:50]
            avail = row["cm_live_available"] or 0

            # Build headline
            direction = "+" if pct > 0 else ""
            title_de = f"{set_code} {direction}{pct}% in 7 Tagen — {name}"[:120]

            # Build teaser
            # Hinweis: Wir nennen externe Marktplätze NIEMALS namentlich (legal/branding-Linie
            # aus Phase 1+2). Stattdessen neutral "EU-Markt" und ggf. Holygrade-Lager.
            teaser_parts = [f"EU-Markttrend: CHF {price:.2f}"]

            # Holygrade-Shop-Status prüfen — falls im Lager, das prominent zeigen
            shop_row = await conn.fetchrow("""
                SELECT in_stock, qty FROM sealed_unified WHERE set_code=$1 AND language='JP'
                  AND product_type IN ('booster box','case','set','carton')
                ORDER BY (CASE WHEN in_stock THEN 0 ELSE 1 END), qty DESC NULLS LAST LIMIT 1
            """, set_code)
            if shop_row and shop_row["in_stock"] and (shop_row["qty"] or 0) > 0:
                teaser_parts.append(f"{shop_row['qty']} bei Holygrade auf Lager")
            elif avail > 0:
                teaser_parts.append(f"{avail} Angebote verfügbar")
            teaser_de = " · ".join(teaser_parts)[:240]

            source_url = f"https://terminal.holygrade.com/preview/sealed/{set_code}"

            inserted = await insert_news_item(
                conn, "market", "market_signals", source_url,
                title_de, None, teaser_de, "market", "de",
                set_code, datetime.now(timezone.utc),
            )
            if inserted:
                count += 1
                logger.info(f"  + (MKT) {title_de[:60]}")

        # Low-price alerts: current price < 30d min (new lows)
        if count < max_signals:
            lows = await conn.fetch("""
                SELECT s.set_code, s.product_name, s.cm_live_trend, s.cm_live_30d_avg,
                       s.cm_live_lowest, s.cm_live_available
                FROM sealed_unified s
                WHERE s.language = 'JP'
                  AND s.cm_live_trend IS NOT NULL
                  AND s.cm_live_30d_avg IS NOT NULL
                  AND s.cm_live_trend < s.cm_live_30d_avg * 0.85
                  AND s.cm_live_trend > 10
                ORDER BY (s.cm_live_30d_avg - s.cm_live_trend) DESC
                LIMIT 3
            """)

            for row in lows:
                if count >= max_signals:
                    break
                set_code = row["set_code"]
                price = round(float(row["cm_live_trend"]), 2)
                name = row["product_name"][:50]

                title_de = f"{name} — neuer 30-Tage-Tiefstand"[:120]
                teaser_de = f"Aktuell CHF {price:.2f}, unter dem 30-Tage-Durchschnitt"[:240]
                source_url = f"https://terminal.holygrade.com/preview/sealed/{set_code}#low"

                inserted = await insert_news_item(
                    conn, "market", "market_signals", source_url,
                    title_de, None, teaser_de, "market", "de",
                    set_code, datetime.now(timezone.utc),
                )
                if inserted:
                    count += 1
                    logger.info(f"  + (MKT low) {title_de[:60]}")

    except Exception as e:
        logger.error(f"Market signals failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='market_signals'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='market_signals'",
        count,
    )
    logger.info(f"Market signals: {count} new items generated.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Source: Reddit r/OnePieceTCG (via Scrapfly)
# ═══════════════════════════════════════════════════════════════════

# Flair matching — Reddit flairs contain emojis, e.g. "📰 One Piece TCG News"
# We match on substring to handle varying formatting
_REDDIT_GOOD_FLAIR_KW = ["news", "spoiler", "discussion", "meta", "card reveal",
                          "deck tech", "original content", "leak", "announcement"]
_REDDIT_SKIP_FLAIR_KW = ["pull", "trade", "help", "meme", "humor",
                          "collection flex", "question", "buying", "selling"]

def _reddit_flair_matches(flair: str, keywords: list) -> bool:
    f = flair.lower()
    return any(kw in f for kw in keywords)

def _reddit_flair_to_category(flair: str) -> str:
    f = flair.lower()
    if any(kw in f for kw in ("spoiler", "leak", "card reveal", "announcement")):
        return "set_release"
    if any(kw in f for kw in ("meta", "deck tech")):
        return "tournament"
    return "other"

def _reddit_featured_score(upvotes: int) -> int:
    if upvotes >= 1000:
        return 70
    if upvotes >= 500:
        return 60
    if upvotes >= 200:
        return 50
    if upvotes >= 100:
        return 40
    return 35


async def ingest_reddit(conn):
    """Fetch top weekly posts from r/OnePieceTCG via Scrapfly."""
    logger.info("Ingesting Reddit r/OnePieceTCG...")
    count = 0
    max_items = 5

    if not SCRAPFLY_KEY:
        logger.warning("SCRAPFLY_KEY not set, skipping Reddit ingest")
        return 0

    try:
        url = "https://www.reddit.com/r/OnePieceTCG/top.json?t=week&limit=25"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                "https://api.scrapfly.io/scrape",
                params={"key": SCRAPFLY_KEY, "url": url, "asp": "true"},
            )
            resp.raise_for_status()

        content = resp.json()["result"]["content"]
        data = __import__("json").loads(content)

        posts = data.get("data", {}).get("children", [])
        for post_wrap in posts:
            if count >= max_items:
                break

            post = post_wrap.get("data", {})
            title = post.get("title", "")
            score = post.get("score", 0)
            flair = (post.get("link_flair_text") or "").lower().strip()
            permalink = post.get("permalink", "")

            # Filter: score >= 50 (lowered from 100 — good posts often 50-100)
            if score < 50:
                continue
            # Filter: skip flairs with skip keywords
            if flair and _reddit_flair_matches(flair, _REDDIT_SKIP_FLAIR_KW):
                continue
            # If flair is present, must match at least one good keyword
            if flair and not _reddit_flair_matches(flair, _REDDIT_GOOD_FLAIR_KW):
                continue
            # Title length
            if len(title) < 20:
                continue

            source_url = f"https://www.reddit.com{permalink}"

            # Category and score
            category = _reddit_flair_to_category(flair) if flair else "other"
            feat_score = _reddit_featured_score(score)
            related_set = extract_set_code(title)

            # Translate to German (usually English already)
            title_de = await translate_to_de(title)
            teaser_de = f"{score} Upvotes · r/OnePieceTCG"

            # Parse time
            created_utc = post.get("created_utc", 0)
            pub_at = datetime.fromtimestamp(created_utc, tz=timezone.utc) if created_utc else datetime.now(timezone.utc)

            inserted = await insert_news_item(
                conn, "community", "reddit_optcg", source_url,
                title_de, title[:120], teaser_de, category, "en",
                related_set, pub_at, override_score=feat_score,
            )
            if inserted:
                count += 1
                logger.info(f"  + (Reddit) [{score}↑] {title_de[:60]}")

    except Exception as e:
        logger.error(f"Reddit ingest failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='reddit_optcg'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='reddit_optcg'",
        count,
    )
    logger.info(f"Reddit: {count} new items ingested.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Source: Bandai JP site (via Scrapfly + LLM translation)
# ═══════════════════════════════════════════════════════════════════

async def ingest_bandai_jp(conn):
    """Fetch Bandai JP news page via Scrapfly, translate JP→DE."""
    logger.info("Ingesting Bandai JP news...")
    count = 0
    max_items = 10

    if not SCRAPFLY_KEY:
        logger.warning("SCRAPFLY_KEY not set, skipping Bandai JP ingest")
        return 0

    jp_urls = [
        "https://www.onepiece-cardgame.com/jp/news/",
        "https://www.onepiece-cardgame.com/jp/products/",
    ]

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            for jp_url in jp_urls:
                if count >= max_items:
                    break

                try:
                    resp = await client.get(
                        "https://api.scrapfly.io/scrape",
                        params={
                            "key": SCRAPFLY_KEY,
                            "url": jp_url,
                            "render_js": "true",
                        },
                    )
                    resp.raise_for_status()
                except Exception as url_err:
                    logger.warning(f"Bandai JP fetch failed for {jp_url}: {url_err}")
                    continue

                html = resp.json()["result"]["content"]
                soup = BeautifulSoup(html, "html.parser")

                # Look for news/product links — common Bandai patterns
                for link in soup.find_all("a", href=True):
                    if count >= max_items:
                        break

                    href = link["href"]
                    text = link.get_text(strip=True)

                    if not text or len(text) < 5:
                        continue

                    # Only relevant paths
                    if not any(p in href for p in ["/news/", "/products/", "/cardlist/"]):
                        continue
                    # Skip pure index links
                    path_after = href.rstrip("/").split("/")[-1]
                    if not path_after or path_after in ("news", "products", "cardlist", "jp"):
                        continue

                    # Build absolute URL
                    if href.startswith("/"):
                        source_url = "https://www.onepiece-cardgame.com" + href
                    elif href.startswith("http"):
                        source_url = href
                    else:
                        source_url = "https://www.onepiece-cardgame.com/jp/" + href

                    # Detect category from URL
                    if "/products/" in href:
                        category = "set_release"
                    else:
                        category = "other"

                    related_set = extract_set_code(text + " " + href)

                    # Translate JP → DE
                    title_de = await translate_jp_to_de(text)
                    teaser_de = "Quelle: Bandai Japan (übersetzt)"

                    # NO image_url (Bandai card art constraint)
                    inserted = await insert_news_item(
                        conn, "bandai", "bandai_jp", source_url,
                        title_de, text[:120], teaser_de, category, "de",
                        related_set, datetime.now(timezone.utc),
                        override_score=80,
                    )
                    if inserted:
                        count += 1
                        logger.info(f"  + (Bandai JP) {title_de[:60]}")

    except Exception as e:
        logger.error(f"Bandai JP ingest failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='bandai_jp'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='bandai_jp'",
        count,
    )
    logger.info(f"Bandai JP: {count} new items ingested.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Source: Limitless TCG Articles (via Scrapfly, filtered)
# ═══════════════════════════════════════════════════════════════════

_LIMITLESS_KEYWORDS = {
    "spoiler", "preview", "meta", "format", "tier list",
    "ban", "errata", "metagame", "analysis", "guide",
    "featured decklist", "deck profile",
}
# Navigation/tool links that match keywords but aren't articles
_LIMITLESS_NAV_SKIP = {
    "deck builder", "metafy", "deck search", "meta hub",
    "card database", "sign up", "log in", "pricing",
}


async def ingest_limitless_articles(conn):
    """Fetch Limitless TCG articles/blog via Scrapfly, filter for spoiler/meta."""
    logger.info("Ingesting Limitless TCG articles...")
    count = 0
    max_items = 5

    if not SCRAPFLY_KEY:
        logger.warning("SCRAPFLY_KEY not set, skipping Limitless articles ingest")
        return 0

    urls_to_try = [
        "https://onepiece.limitlesstcg.com/articles",
        "https://onepiece.limitlesstcg.com/",
    ]

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for page_url in urls_to_try:
                if count >= max_items:
                    break

                resp = await client.get(
                    "https://api.scrapfly.io/scrape",
                    params={
                        "key": SCRAPFLY_KEY,
                        "url": page_url,
                        "render_js": "true",
                    },
                )
                resp.raise_for_status()
                html = resp.json()["result"]["content"]
                soup = BeautifulSoup(html, "html.parser")

                for link in soup.find_all("a", href=True):
                    if count >= max_items:
                        break

                    href = link["href"]
                    text = link.get_text(strip=True)

                    if not text or len(text) < 10:
                        continue

                    # Must match article-like keywords
                    text_lower = text.lower()
                    if not any(kw in text_lower for kw in _LIMITLESS_KEYWORDS):
                        continue

                    # Skip navigation / tool links
                    if any(nav in text_lower for nav in _LIMITLESS_NAV_SKIP):
                        continue

                    # Skip pure tournament result pages
                    if "/tournaments/" in href and not any(kw in text_lower for kw in ("meta", "format", "tier")):
                        continue

                    # Build URL
                    if href.startswith("/"):
                        source_url = "https://onepiece.limitlesstcg.com" + href
                    elif href.startswith("http"):
                        source_url = href
                    else:
                        continue

                    related_set = extract_set_code(text)
                    title_de = await translate_to_de(text)

                    inserted = await insert_news_item(
                        conn, "community", "limitless_articles", source_url,
                        title_de, text[:120], "Quelle: Limitless TCG",
                        "tournament", "en", related_set,
                        datetime.now(timezone.utc), override_score=60,
                    )
                    if inserted:
                        count += 1
                        logger.info(f"  + (LT-Art) {title_de[:60]}")

    except Exception as e:
        logger.error(f"Limitless articles ingest failed: {e}")
        await conn.execute(
            "UPDATE news_sources SET last_fetch_status='error', last_error_msg=$1 "
            "WHERE source_key='limitless_articles'",
            str(e)[:500],
        )
        return 0

    await conn.execute(
        "UPDATE news_sources SET last_fetched_at=NOW(), last_fetch_count=$1, "
        "last_fetch_status='ok', last_error_msg=NULL "
        "WHERE source_key='limitless_articles'",
        count,
    )
    logger.info(f"Limitless articles: {count} new items ingested.")
    return count


# ═══════════════════════════════════════════════════════════════════
# Featured score recompute
# ═══════════════════════════════════════════════════════════════════

async def recompute_featured_scores(conn):
    """Recompute all featured scores (nightly decay)."""
    logger.info("Recomputing featured scores...")
    result = await conn.execute("""
        UPDATE news_items SET featured_score = GREATEST(0, LEAST(100,
            CASE WHEN source IN ('bandai','twitter') THEN 40 ELSE 0 END
          + CASE WHEN published_at >= NOW() - INTERVAL '6h'  THEN 20
                 WHEN published_at >= NOW() - INTERVAL '24h' THEN 10
                 ELSE 0 END
          + CASE WHEN category = 'set_release'  THEN 15
                 WHEN category = 'tournament'   THEN 10
                 ELSE 0 END
          + CASE WHEN related_set IS NOT NULL THEN 5 ELSE 0 END
          + CASE WHEN published_at < NOW() - INTERVAL '14d' THEN -100 ELSE 0 END
        ))
        WHERE is_published = TRUE
    """)
    logger.info(f"Featured scores recomputed: {result}")


# ═══════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════

async def run_full_ingest():
    """Run all ingest pipelines."""
    conn = await get_conn()
    try:
        results = {}
        results["bandai"] = await ingest_bandai_en(conn)
        results["youtube"] = await ingest_youtube_rss(conn)
        results["limitless"] = await ingest_limitless(conn)
        results["market"] = await ingest_market_signals(conn)
        # New Scrapfly-based sources (Phase 5)
        results["reddit"] = await ingest_reddit(conn)
        results["bandai_jp"] = await ingest_bandai_jp(conn)
        results["limitless_articles"] = await ingest_limitless_articles(conn)
        await recompute_featured_scores(conn)

        total = sum(results.values())
        logger.info(f"Full ingest complete: {total} new items. Breakdown: {results}")
        return results
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_full_ingest())

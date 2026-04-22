"""Scrape PriceCharting.com for accurate OPTCG sealed prices.

PriceCharting maintains separate JP and EN product pages with prices derived
from eBay sold listings — real transaction prices, not listings.

URL patterns:
  JP: pricecharting.com/game/one-piece-japanese-{slug}/{product-type}
  EN: pricecharting.com/game/one-piece-{slug}/{product-type}
"""
import asyncio
import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USD_TO_EUR = 0.92

# Set code → PriceCharting slug (verified against pricecharting.com/category/one-piece-cards)
PC_SET_SLUGS: dict[str, str] = {
    "OP01": "romance-dawn",
    "OP02": "paramount-war",
    "OP03": "pillars-of-strength",
    "OP04": "kingdoms-of-intrigue",
    "OP05": "awakening-of-the-new-era",
    "OP06": "wings-of-the-captain",
    "OP07": "500-years-in-the-future",
    "OP08": "two-legends",
    "OP09": "emperors-in-the-new-world",
    "OP10": "royal-blood",
    "OP11": "fist-of-divine-speed",
    "OP12": "legacy-of-the-master",
    "OP13": "carrying-on-his-will",
    "OP14": "azure-sea's-seven",
    "OP15": "adventure-on-kami's-island",
    "EB01": "extra-booster-memorial-collection",
    "EB02": "extra-booster-anime-25th-collection",
    "EB03": "extra-booster-heroines-edition",
    "EB04": "extra-booster-egghead-crisis",
    "PRB01": "premium-booster",
    "PRB02": "premium-booster-2",
}

# Product type → URL slug
_PT_SLUGS: dict[str, str] = {
    "booster box": "booster-box",
    "booster": "booster-pack",
    "case": "booster-box-case",
}


async def _fetch_page(url: str) -> Optional[str]:
    """Fetch a PriceCharting page. Returns HTML or None."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; HolygradeBot/1.0; +https://holygrade.com)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        try:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return None
            return resp.text
        except Exception as e:
            logger.warning(f"PriceCharting fetch failed for {url}: {e}")
            return None


def _parse_price(html: str) -> Optional[float]:
    """Extract the Ungraded price from a PriceCharting game page.

    Primary selector: #used_price .price.js-price  (the "Ungraded" column
    in the price_data table).  Falls back to regex search.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Check this isn't a generic redirect/list page (no real product)
    used_td = soup.select_one("#used_price")
    if not used_td:
        return None

    # Primary: first .price.js-price inside #used_price
    price_span = used_td.select_one(".price.js-price")
    if price_span:
        text = price_span.get_text(strip=True)
        if text and text != "-":
            match = re.search(r"\$?([\d,]+\.?\d*)", text)
            if match:
                try:
                    return float(match.group(1).replace(",", ""))
                except ValueError:
                    pass

    # Fallback: any .price.js-price on the page
    for elem in soup.select(".price.js-price"):
        text = elem.get_text(strip=True)
        if text and text != "-":
            match = re.search(r"\$?([\d,]+\.?\d*)", text)
            if match:
                try:
                    return float(match.group(1).replace(",", ""))
                except ValueError:
                    continue

    return None


async def fetch_sealed_price(
    set_code: str,
    product_type: str,
    language: str = "JP",
) -> Optional[dict]:
    """Fetch the current price for a specific sealed product.

    Args:
        set_code: e.g. "OP13"
        product_type: "booster box", "booster", or "case"
        language: "JP" or "EN"

    Returns:
        {"price_usd": float, "price_eur": float, "source_url": str} or None
    """
    slug = PC_SET_SLUGS.get(set_code)
    if not slug:
        return None

    pt_slug = _PT_SLUGS.get(product_type.lower())
    if not pt_slug:
        return None

    prefix = "one-piece-japanese" if language == "JP" else "one-piece"
    url = f"https://www.pricecharting.com/game/{prefix}-{slug}/{pt_slug}"

    html = await _fetch_page(url)
    if not html:
        return None

    price_usd = _parse_price(html)
    if price_usd is None or price_usd <= 0:
        return None

    return {
        "price_usd": round(price_usd, 2),
        "price_eur": round(price_usd * USD_TO_EUR, 2),
        "source_url": url,
    }


async def test_all_sets() -> list[dict]:
    """Test fetching all known sets — JP booster boxes only (fast scan).

    Returns a list of results with prices for verification.
    """
    results = []
    for set_code in PC_SET_SLUGS:
        for lang in ["JP", "EN"]:
            result = await fetch_sealed_price(set_code, "booster box", lang)
            entry = {
                "set_code": set_code,
                "language": lang,
                "product_type": "booster box",
                "price_usd": None,
                "price_eur": None,
                "source_url": None,
            }
            if result:
                entry.update(result)
            results.append(entry)
            logger.info(
                f"PriceCharting test: {set_code} {lang} box = "
                f"${entry['price_usd']}" if entry["price_usd"] else
                f"PriceCharting test: {set_code} {lang} box = N/A"
            )
            await asyncio.sleep(1.2)
    return results

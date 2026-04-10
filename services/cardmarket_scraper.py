"""
Cardmarket scraper for OPTCG sealed products and singles.
Used as secondary data source to verify API prices and fill gaps.
Scrapes public product pages — no login required.
"""
import httpx
from bs4 import BeautifulSoup
import asyncio
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

CARDMARKET_BASE = "https://www.cardmarket.com/en/OnePiece"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


async def scrape_cardmarket_search(query: str, category: str = "Singles") -> list[dict]:
    """Search Cardmarket for cards or products matching query.
    category: 'Singles' or 'Sealed'
    Returns list of {name, url, price_eur, trend_30d, available_items}
    """
    search_url = f"{CARDMARKET_BASE}/Products/Search?searchString={query}&mode=gallery"
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(search_url, headers=HEADERS)
            if resp.status_code != 200:
                logger.warning(f"Cardmarket search returned {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            results = []

            # Parse product tiles
            for tile in soup.select(".col-12.col-md-8.col-lg-6.col-xl-4, .table-body .row"):
                try:
                    name_el = tile.select_one("a.col")
                    if not name_el:
                        name_el = tile.select_one(".col a")
                    if not name_el:
                        continue

                    name = name_el.get_text(strip=True)
                    url = name_el.get("href", "")
                    if url and not url.startswith("http"):
                        url = "https://www.cardmarket.com" + url

                    # Price
                    price_el = tile.select_one(".price-container .price, .col-price")
                    price = None
                    if price_el:
                        price_text = price_el.get_text(strip=True)
                        price_match = re.search(r"([\d.,]+)", price_text.replace("€", "").replace(",", "."))
                        if price_match:
                            price = float(price_match.group(1))

                    results.append({
                        "name": name,
                        "url": url,
                        "price_eur": price,
                        "source": "cardmarket_scraper"
                    })
                except Exception as e:
                    logger.debug(f"Error parsing tile: {e}")
                    continue

            return results
    except Exception as e:
        logger.error(f"Cardmarket scrape error: {e}")
        return []


async def scrape_cardmarket_product(url: str) -> Optional[dict]:
    """Scrape a single Cardmarket product page for detailed pricing.
    Returns {name, price_lowest, price_trend_30d, price_trend_7d, available_from, url}
    """
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=HEADERS)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            name = ""
            title_el = soup.select_one("h1")
            if title_el:
                name = title_el.get_text(strip=True)

            # Price info from the info-list
            prices = {}
            for dt in soup.select("dt"):
                label = dt.get_text(strip=True).lower()
                dd = dt.find_next_sibling("dd")
                if dd:
                    val_text = dd.get_text(strip=True)
                    price_match = re.search(r"([\d.,]+)", val_text.replace("€", "").replace(",", "."))
                    if price_match:
                        try:
                            prices[label] = float(price_match.group(1))
                        except ValueError:
                            pass

            return {
                "name": name,
                "url": url,
                "price_lowest": prices.get("price from", prices.get("from", None)),
                "price_trend_30d": prices.get("30-day avg. price", prices.get("30-days average price", None)),
                "price_trend_7d": prices.get("7-day avg. price", prices.get("7-days average price", None)),
                "available_from": prices.get("available items", None),
                "source": "cardmarket_scraper"
            }
    except Exception as e:
        logger.error(f"Cardmarket product scrape error: {e}")
        return None


async def scrape_sealed_prices(set_code: str) -> list[dict]:
    """Scrape Cardmarket sealed product prices for a specific set.
    e.g. set_code='OP13' → searches for 'OP13 booster box', 'OP13 booster case'
    """
    queries = [
        f"{set_code} booster box",
        f"{set_code} booster case",
        f"{set_code} display",
    ]
    all_results = []
    for q in queries:
        results = await scrape_cardmarket_search(q, category="Sealed")
        all_results.extend(results)
        await asyncio.sleep(1)  # Rate limit

    return all_results

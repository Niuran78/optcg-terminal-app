"""Cross-check prices between API and scraper to ensure data quality."""
import logging

logger = logging.getLogger(__name__)

PRICE_DIVERGENCE_THRESHOLD = 0.30  # 30% difference triggers a warning


def verify_price(api_price: float, scraped_price: float, item_name: str = "") -> dict:
    """Compare API price vs scraped price.
    Returns verification result with confidence score.
    """
    if not api_price or not scraped_price:
        return {"verified": False, "reason": "missing_price", "confidence": 0}

    diff_pct = abs(api_price - scraped_price) / max(api_price, scraped_price)

    if diff_pct <= 0.05:
        return {"verified": True, "confidence": 1.0, "diff_pct": diff_pct}
    elif diff_pct <= 0.15:
        return {"verified": True, "confidence": 0.8, "diff_pct": diff_pct}
    elif diff_pct <= PRICE_DIVERGENCE_THRESHOLD:
        return {"verified": True, "confidence": 0.5, "diff_pct": diff_pct, "warning": "moderate_divergence"}
    else:
        logger.warning(
            f"Price divergence for {item_name}: API={api_price}, Scraped={scraped_price}, Diff={diff_pct:.1%}"
        )
        return {"verified": False, "confidence": 0.2, "diff_pct": diff_pct, "warning": "high_divergence"}


def choose_best_price(api_price: float, scraped_price: float) -> tuple[float, str]:
    """Choose the most reliable price between API and scraper.
    Returns (price, source).
    API 7d average is preferred if available, otherwise scraped.
    """
    if api_price and scraped_price:
        # If close, prefer API (more structured data)
        diff_pct = abs(api_price - scraped_price) / max(api_price, scraped_price)
        if diff_pct <= 0.20:
            return api_price, "api"
        else:
            # Large divergence — prefer scraped (more real-time)
            return scraped_price, "scraper"
    elif api_price:
        return api_price, "api"
    elif scraped_price:
        return scraped_price, "scraper"
    return 0, "none"

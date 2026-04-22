"""Technical & TCG-specific indicators for Holygrade Terminal.

Computes trading signals from price history + cross-sectional data.

Indicators:
  - Moving Averages (7d, 30d, 90d)
  - RSI (Relative Strength Index, 14-period)
  - Bollinger Bands (20-period, 2 std dev)
  - TCG-Custom:
      * JP/EN Spread Ratio (historical norm tracking)
      * Sealed-to-Single Ratio (value density)
      * Reprint Risk Score (age-based)
      * Liquidity Score (listing count proxy)
"""
from __future__ import annotations
from typing import Optional
from datetime import date, timedelta
import math


# ─────────────────────────────────────────────────────────────────────────────
# Classic technical indicators
# ─────────────────────────────────────────────────────────────────────────────

def moving_average(prices: list[float], period: int) -> Optional[float]:
    """Simple moving average over the last `period` prices."""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def rsi(prices: list[float], period: int = 14) -> Optional[float]:
    """Relative Strength Index (0-100).

    <30 = oversold (BUY signal)
    >70 = overbought (SELL signal)
    50  = neutral
    """
    if len(prices) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    # Wilder's smoothing
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def bollinger_bands(
    prices: list[float], period: int = 20, std_devs: float = 2.0
) -> Optional[dict]:
    """Bollinger Bands: middle = SMA, upper/lower = ±std_devs std deviations.

    Signals:
      - price near upper band → overbought
      - price near lower band → oversold
      - bandwidth widening → volatility increase
    """
    if len(prices) < period:
        return None

    window = prices[-period:]
    mean = sum(window) / period
    variance = sum((p - mean) ** 2 for p in window) / period
    std = math.sqrt(variance)
    current = prices[-1]

    upper = mean + std_devs * std
    lower = mean - std_devs * std
    bandwidth = (upper - lower) / mean if mean else 0
    # Percent position within the bands (0 = lower, 100 = upper)
    if upper == lower:
        pct_b = 50.0
    else:
        pct_b = (current - lower) / (upper - lower) * 100

    return {
        "middle": round(mean, 2),
        "upper": round(upper, 2),
        "lower": round(lower, 2),
        "bandwidth": round(bandwidth, 4),
        "pct_b": round(pct_b, 1),  # 0 = on lower band, 100 = on upper band
    }


def price_change_pct(prices: list[float], days_back: int) -> Optional[float]:
    """% change vs. price days_back ago."""
    if len(prices) < days_back + 1:
        return None
    past = prices[-days_back - 1]
    current = prices[-1]
    if past == 0:
        return None
    return round((current - past) / past * 100, 2)


# ─────────────────────────────────────────────────────────────────────────────
# TCG-specific indicators
# ─────────────────────────────────────────────────────────────────────────────

# Historical norm for JP/EN ratio — observed market range
# JP is typically 2.5-4× cheaper than EN for the same sealed product
JP_EN_NORMAL_MIN = 2.5
JP_EN_NORMAL_MAX = 4.0


def jp_en_ratio(en_price: Optional[float], jp_price: Optional[float]) -> Optional[dict]:
    """EN price ÷ JP price.

    Signals when ratio is outside historical 2.5-4.0 band:
      - ratio < 2.5 → EN anomaly cheap OR JP overheated
      - ratio > 4.0 → EN hype or JP mispricing (arbitrage opportunity)
    """
    if not en_price or not jp_price or jp_price <= 0:
        return None
    ratio = en_price / jp_price
    if ratio < JP_EN_NORMAL_MIN:
        signal = "BELOW_NORMAL"
    elif ratio > JP_EN_NORMAL_MAX:
        signal = "ABOVE_NORMAL"
    else:
        signal = "NORMAL"
    return {
        "ratio": round(ratio, 2),
        "signal": signal,
        "normal_range": [JP_EN_NORMAL_MIN, JP_EN_NORMAL_MAX],
    }


def sealed_to_singles_ratio(
    sealed_box_price: Optional[float],
    top_cards_total: Optional[float],
) -> Optional[dict]:
    """Box price ÷ sum of top-N most valuable cards in the set.

    Interpretation:
      ratio < 0.4 → Box undervalued vs chase cards (good box buy)
      ratio 0.4-0.7 → Fair pricing
      ratio > 0.7 → Box overpriced; buy singles instead
    """
    if not sealed_box_price or not top_cards_total or top_cards_total <= 0:
        return None
    ratio = sealed_box_price / top_cards_total
    if ratio < 0.4:
        signal = "BOX_UNDERVALUED"
    elif ratio < 0.7:
        signal = "FAIR"
    else:
        signal = "SINGLES_BETTER"
    return {
        "ratio": round(ratio, 3),
        "signal": signal,
        "box_price": sealed_box_price,
        "top_singles_total": top_cards_total,
    }


def reprint_risk(
    set_release_date: Optional[date],
    today: Optional[date] = None,
) -> Optional[dict]:
    """Heuristic: older sets have higher reprint risk.

    OPTCG history shows sets aged 18-30 months often get reprinted
    (Romance Dawn 2024 reprint, EB01 reprint, etc.).

    Returns:
      score 0-100, where 100 = imminent reprint risk
      level: LOW | MODERATE | HIGH | IMMINENT
    """
    if not set_release_date:
        return None
    if today is None:
        today = date.today()
    months_old = (today - set_release_date).days / 30.44

    if months_old < 12:
        score = 5
        level = "LOW"
    elif months_old < 18:
        score = 25
        level = "LOW"
    elif months_old < 24:
        score = 55
        level = "MODERATE"
    elif months_old < 30:
        score = 80
        level = "HIGH"
    else:
        score = 95
        level = "IMMINENT"

    return {
        "score": score,
        "level": level,
        "months_since_release": round(months_old, 1),
    }


def liquidity_score(
    listing_count: Optional[int],
    eu_price: Optional[float],
    eu_lowest: Optional[float],
) -> Optional[dict]:
    """Liquidity proxy: listing count + bid-ask spread.

    High liquidity = easy to buy/sell at market price.
    """
    if listing_count is None and (eu_price is None or eu_lowest is None):
        return None

    # Spread-based (tight = more liquid)
    spread_pct = None
    if eu_price and eu_lowest and eu_price > 0:
        spread_pct = (eu_price - eu_lowest) / eu_price * 100

    # Volume score (heuristic)
    vol_score = None
    if listing_count is not None:
        if listing_count > 200:
            vol_score = "VERY_HIGH"
        elif listing_count > 50:
            vol_score = "HIGH"
        elif listing_count > 10:
            vol_score = "MODERATE"
        else:
            vol_score = "LOW"

    return {
        "listing_count": listing_count,
        "spread_pct": round(spread_pct, 1) if spread_pct is not None else None,
        "volume": vol_score,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Signal generator — combines indicators into BUY / SELL / HOLD
# ─────────────────────────────────────────────────────────────────────────────

def generate_signal(ind: dict) -> dict:
    """Combine indicators into a single actionable signal.

    Input:  ind dict from build_indicators()
    Output: {'action': 'BUY'|'SELL'|'HOLD', 'strength': 0-100, 'reasons': [...]}
    """
    buy_score = 0
    sell_score = 0
    reasons = []

    # RSI
    r = ind.get("rsi")
    if r is not None:
        if r < 30:
            buy_score += 30
            reasons.append(f"RSI {r} oversold")
        elif r > 70:
            sell_score += 30
            reasons.append(f"RSI {r} overbought")

    # Moving average trend
    ma7 = ind.get("ma_7d")
    ma30 = ind.get("ma_30d")
    if ma7 and ma30:
        if ma7 > ma30 * 1.03:
            buy_score += 15
            reasons.append("7d MA above 30d MA (uptrend)")
        elif ma7 < ma30 * 0.97:
            sell_score += 15
            reasons.append("7d MA below 30d MA (downtrend)")

    # Bollinger Bands
    bb = ind.get("bollinger")
    if bb:
        if bb["pct_b"] < 10:
            buy_score += 20
            reasons.append("Near lower Bollinger band")
        elif bb["pct_b"] > 90:
            sell_score += 20
            reasons.append("Near upper Bollinger band")

    # Price momentum
    chg_7d = ind.get("change_7d")
    if chg_7d is not None:
        if chg_7d < -10:
            buy_score += 10
            reasons.append(f"7d down {chg_7d}%")
        elif chg_7d > 15:
            sell_score += 10
            reasons.append(f"7d up {chg_7d}%")

    # Reprint risk dampens BUY
    rr = ind.get("reprint_risk")
    if rr and rr["level"] in ("HIGH", "IMMINENT"):
        sell_score += 15
        reasons.append(f"Reprint risk {rr['level'].lower()}")

    # Decide
    if buy_score > sell_score and buy_score >= 20:
        return {
            "action": "BUY",
            "strength": min(100, buy_score),
            "reasons": reasons,
        }
    if sell_score > buy_score and sell_score >= 20:
        return {
            "action": "SELL",
            "strength": min(100, sell_score),
            "reasons": reasons,
        }
    return {"action": "HOLD", "strength": 0, "reasons": reasons or ["No strong signal"]}


def build_indicators(
    history: list[dict],
    current: Optional[dict] = None,
    set_release_date: Optional[date] = None,
) -> dict:
    """Build the full indicator panel from a list of historical price points.

    history: list of {'snap_date': date, 'eu_cardmarket_7d_avg': float, ...}
             sorted ascending by snap_date
    current: optional override for latest price (if not in history)

    Returns dict with all indicators + overall signal.
    """
    # Extract EU price series as primary time-series
    prices = [
        float(h.get("eu_cardmarket_7d_avg") or h.get("price") or 0)
        for h in history
        if h.get("eu_cardmarket_7d_avg") or h.get("price")
    ]
    prices = [p for p in prices if p > 0]

    if current and current.get("eu_cardmarket_7d_avg"):
        # Ensure latest price is the final point
        prices.append(float(current["eu_cardmarket_7d_avg"]))

    ind = {
        "ma_7d": moving_average(prices, 7),
        "ma_30d": moving_average(prices, 30),
        "ma_90d": moving_average(prices, 90),
        "rsi": rsi(prices, 14),
        "bollinger": bollinger_bands(prices, 20, 2.0),
        "change_7d": price_change_pct(prices, 7),
        "change_30d": price_change_pct(prices, 30),
        "change_90d": price_change_pct(prices, 90),
        "data_points": len(prices),
    }

    if set_release_date:
        ind["reprint_risk"] = reprint_risk(set_release_date)

    ind["signal"] = generate_signal(ind)
    return ind

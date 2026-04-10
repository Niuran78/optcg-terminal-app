"""Arbitrage calculation engine for OPTCG Market Terminal.

Compares Cardmarket (EU) and TCGPlayer (US) prices.
Factors in:
  - EUR→USD/USD→EUR FX spread (~3%)
  - Shipping EU→US: ~€15-20
  - Shipping US→EU: ~€20-25
  - Marketplace fees: ~13% TCGPlayer, ~5% Cardmarket
"""
import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional


# Configurable cost parameters (can override via env)
FX_SPREAD = float(os.getenv("ARB_FX_SPREAD", "0.03"))           # 3% FX conversion cost
SHIPPING_EU_TO_US = float(os.getenv("ARB_SHIPPING_EU_US", "17.0"))  # EUR
SHIPPING_US_TO_EU = float(os.getenv("ARB_SHIPPING_US_EU", "22.0"))  # EUR
CARDMARKET_FEE = float(os.getenv("ARB_CM_FEE", "0.05"))          # 5%
TCGPLAYER_FEE = float(os.getenv("ARB_TCG_FEE", "0.13"))          # 13%
PROFIT_THRESHOLD = float(os.getenv("ARB_PROFIT_THRESHOLD", "0.05"))  # 5% min profit
WATCH_THRESHOLD = float(os.getenv("ARB_WATCH_THRESHOLD", "0.02"))    # 2% watch zone


class ArbSignal(str, Enum):
    BUY_EU = "BUY_EU"       # Buy on Cardmarket, sell on TCGPlayer
    BUY_US = "BUY_US"       # Buy on TCGPlayer, sell on Cardmarket
    WATCH = "WATCH"          # Near profitable
    NEUTRAL = "NEUTRAL"      # No opportunity


@dataclass
class ArbResult:
    signal: ArbSignal
    cardmarket_price: float
    tcgplayer_price: float
    profit_eur: float          # Net profit in EUR after all costs
    profit_pct: float          # Profit as % of buy price
    buy_price: float           # Where to buy
    sell_price: float          # Where to sell (in EUR equivalent)
    buy_market: str            # "cardmarket" or "tcgplayer"
    sell_market: str           # "tcgplayer" or "cardmarket"
    cost_breakdown: dict       # Detailed cost breakdown

    def to_dict(self) -> dict:
        return {
            "signal": self.signal.value,
            "cardmarket_price": round(self.cardmarket_price, 2),
            "tcgplayer_price": round(self.tcgplayer_price, 2),
            "profit_eur": round(self.profit_eur, 2),
            "profit_pct": round(self.profit_pct * 100, 1),
            "buy_price": round(self.buy_price, 2),
            "sell_price": round(self.sell_price, 2),
            "buy_market": self.buy_market,
            "sell_market": self.sell_market,
            "cost_breakdown": self.cost_breakdown,
        }


def calculate_arbitrage(
    cardmarket_price: Optional[float],
    tcgplayer_price: Optional[float],
    item_type: str = "card",  # "card" or "product"
) -> Optional[ArbResult]:
    """
    Calculate arbitrage opportunity between Cardmarket and TCGPlayer.
    Both prices should be in EUR. TCGPlayer prices from the API are already
    EUR-converted.

    Returns None if prices are unavailable or below minimum thresholds.
    """
    if cardmarket_price is None or tcgplayer_price is None:
        return None
    if cardmarket_price <= 0 or tcgplayer_price <= 0:
        return None
    # Skip items below €1 (noise)
    if cardmarket_price < 1.0 and tcgplayer_price < 1.0:
        return None

    # Determine shipping cost based on item type
    # Cards: individual shipping is factored per batch
    # Products (boxes): use fixed shipping
    if item_type == "product":
        ship_eu_us = SHIPPING_EU_TO_US
        ship_us_eu = SHIPPING_US_TO_EU
    else:
        # For individual cards, amortize shipping over assumed lot size of 10
        ship_eu_us = SHIPPING_EU_TO_US / 10
        ship_us_eu = SHIPPING_US_TO_EU / 10

    # === Scenario 1: Buy on Cardmarket, sell on TCGPlayer ===
    # Cost: CM buy price + FX spread + shipping EU→US + TCG listing fee
    buy_cm = cardmarket_price
    cm_buy_fee = buy_cm * CARDMARKET_FEE
    tcg_sell_price_eur = tcgplayer_price
    tcg_sell_fee = tcg_sell_price_eur * TCGPLAYER_FEE
    fx_cost_1 = tcg_sell_price_eur * FX_SPREAD

    profit_1 = (tcg_sell_price_eur
                - cm_buy_fee
                - ship_eu_us
                - tcg_sell_fee
                - fx_cost_1
                - buy_cm)
    pct_1 = profit_1 / buy_cm if buy_cm > 0 else 0

    # === Scenario 2: Buy on TCGPlayer, sell on Cardmarket ===
    buy_tcg = tcgplayer_price
    tcg_buy_fee = buy_tcg * TCGPLAYER_FEE
    cm_sell_price_eur = cardmarket_price
    cm_sell_fee = cm_sell_price_eur * CARDMARKET_FEE
    fx_cost_2 = buy_tcg * FX_SPREAD

    profit_2 = (cm_sell_price_eur
                - tcg_buy_fee
                - ship_us_eu
                - cm_sell_fee
                - fx_cost_2
                - buy_tcg)
    pct_2 = profit_2 / buy_tcg if buy_tcg > 0 else 0

    # Pick best scenario
    best_profit = max(profit_1, profit_2)
    best_pct = max(pct_1, pct_2)

    if pct_1 >= pct_2:
        signal_profit = profit_1
        signal_pct = pct_1
        buy_market = "cardmarket"
        sell_market = "tcgplayer"
        buy_price = buy_cm
        sell_price = tcg_sell_price_eur
        cost_breakdown = {
            "buy_price": round(buy_cm, 2),
            "buy_fee": round(cm_buy_fee, 2),
            "shipping": round(ship_eu_us, 2),
            "sell_fee": round(tcg_sell_fee, 2),
            "fx_cost": round(fx_cost_1, 2),
            "sell_price": round(tcg_sell_price_eur, 2),
            "net_profit": round(profit_1, 2),
        }
    else:
        signal_profit = profit_2
        signal_pct = pct_2
        buy_market = "tcgplayer"
        sell_market = "cardmarket"
        buy_price = buy_tcg
        sell_price = cm_sell_price_eur
        cost_breakdown = {
            "buy_price": round(buy_tcg, 2),
            "buy_fee": round(tcg_buy_fee, 2),
            "shipping": round(ship_us_eu, 2),
            "sell_fee": round(cm_sell_fee, 2),
            "fx_cost": round(fx_cost_2, 2),
            "sell_price": round(cm_sell_price_eur, 2),
            "net_profit": round(profit_2, 2),
        }

    # Determine signal
    if signal_pct >= PROFIT_THRESHOLD and signal_profit > 0:
        if buy_market == "cardmarket":
            signal = ArbSignal.BUY_EU
        else:
            signal = ArbSignal.BUY_US
    elif signal_pct >= WATCH_THRESHOLD:
        signal = ArbSignal.WATCH
    else:
        signal = ArbSignal.NEUTRAL

    return ArbResult(
        signal=signal,
        cardmarket_price=cardmarket_price,
        tcgplayer_price=tcgplayer_price,
        profit_eur=signal_profit,
        profit_pct=signal_pct,
        buy_price=buy_price,
        sell_price=sell_price,
        buy_market=buy_market,
        sell_market=sell_market,
        cost_breakdown=cost_breakdown,
    )


def analyze_items(items: list[dict], item_type: str = "card") -> list[dict]:
    """
    Run arbitrage analysis on a list of items (cards or products).
    Items must have _cardmarket_price and _tcgplayer_price fields.
    Returns list of dicts with item data + arbitrage results, sorted by profit desc.
    """
    results = []
    for item in items:
        cm_price = item.get("_cardmarket_price")
        tcp_price = item.get("_tcgplayer_price")
        arb = calculate_arbitrage(cm_price, tcp_price, item_type)
        if arb is None:
            continue

        name = (item.get("name") or item.get("title") or
                item.get("card_name") or item.get("product_name") or "Unknown")
        code = item.get("code", item.get("card_code", item.get("id", "")))
        rarity = item.get("rarity", item.get("card_rarity", ""))
        image = item.get("image", item.get("img", item.get("image_url", "")))

        results.append({
            "id": str(item.get("id", item.get("_id", code))),
            "name": name,
            "code": str(code),
            "rarity": rarity,
            "image": image,
            "item_type": item_type,
            **arb.to_dict(),
        })

    # Sort by absolute profit descending
    results.sort(key=lambda x: x["profit_eur"], reverse=True)
    return results

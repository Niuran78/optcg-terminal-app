# OPTCG Market Terminal

A Bloomberg Terminal for One Piece TCG — professional market data tool for collectors, investors, and traders.

**Features:**
- Live arbitrage scanner (Cardmarket EU ↔ TCGPlayer US)
- Sealed product tracker with price history
- EV Calculator (JP & EN boxes, community pull rates)
- Tier-based access (Free / Pro / Elite)
- JWT auth + Stripe subscriptions
- Full JP + EN set support (🇯🇵 Japanese focus)

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone <your-repo>
cd optcg-terminal-app
pip install -r requirements.txt
```

### 2. Set up environment variables

```bash
cp .env.example .env
# Edit .env with your keys
```

**Required variables:**
- `RAPIDAPI_KEY` — Your RapidAPI key for one-piece-card-game1 API
- `JWT_SECRET` — Strong random secret (see `.env.example`)
- `STRIPE_SECRET_KEY` — Stripe secret key (sk_test_... for dev)
- `STRIPE_PRO_PRICE_ID` — Stripe Price ID for Pro tier
- `STRIPE_ELITE_PRICE_ID` — Stripe Price ID for Elite tier

### 3. Run the server

```bash
python main.py
# or
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000` in your browser.

---

## API Keys Setup

### RapidAPI (One Piece Card Game API)

1. Go to [RapidAPI — one-piece-card-game1](https://rapidapi.com/api-sports/api/one-piece-card-game1)
2. Subscribe to the **Ultra plan** ($24.90/mo → 15,000 req/day, 300 req/min)
3. Copy your `X-RapidAPI-Key` into `.env` as `RAPIDAPI_KEY`

The host is pre-configured as `one-piece-card-game1.p.rapidapi.com`.

### Stripe Setup

1. Create a [Stripe account](https://dashboard.stripe.com)
2. Get your **Secret key** from Dashboard → Developers → API keys
3. Create two Products in [Stripe Dashboard → Products](https://dashboard.stripe.com/products):
   - **OPTCG Pro** — CHF 19/month recurring
   - **OPTCG Elite** — CHF 69/month recurring
4. Copy the **Price IDs** (e.g. `price_1Abc...`) into `.env`
5. Set up a Stripe **Webhook**:
   - Go to Dashboard → Developers → Webhooks
   - Add endpoint: `https://yourdomain.com/api/billing/webhook`
   - Select events: `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`
   - Copy the **Webhook signing secret** into `.env` as `STRIPE_WEBHOOK_SECRET`

**Local webhook testing (Stripe CLI):**
```bash
stripe listen --forward-to localhost:8000/api/billing/webhook
```

---

## Architecture

```
optcg-terminal-app/
├── main.py                  # FastAPI app, startup, routing
├── requirements.txt
├── .env.example
├── db/
│   └── init.py              # SQLite schema (users, sets, cache, history)
├── api/
│   ├── auth.py              # Register, login, JWT (/api/auth/*)
│   ├── cards.py             # Card endpoints (/api/cards/*)
│   ├── sets.py              # Set/episode listing (/api/sets/*)
│   ├── arbitrage.py         # Arbitrage scanner (/api/arbitrage/*)
│   ├── sealed.py            # Sealed products (/api/sealed/*)
│   ├── ev.py                # EV calculator (/api/ev/*)
│   └── stripe_billing.py    # Stripe checkout + webhooks (/api/billing/*)
├── services/
│   ├── opcg_api.py          # API adapter + SQLite cache
│   ├── arbitrage_engine.py  # Arbitrage math
│   └── ev_engine.py         # EV calculation + pull rates
├── middleware/
│   └── tier_gate.py         # JWT parsing + tier enforcement
└── static/
    ├── index.html           # Main dashboard (SPA)
    ├── login.html           # Auth page
    ├── style.css            # Terminal dark theme
    ├── app.js               # Dashboard JS
    └── auth.js              # Login/register JS
```

---

## Tier Access Matrix

| Feature | Free | Pro | Elite |
|---|---|---|---|
| Sets coverage | Latest 3 | All sets | All sets |
| Data freshness | 24h cache | 15-min cache | 15-min cache |
| Arbitrage signals | ✓ (no $ amounts) | ✓ Full | ✓ Full |
| Sealed product tracker | Latest 3 sets | All sets | All sets |
| Price history | — | 30 days | 1 year |
| EV Calculator | — | ✓ | ✓ |
| Price alerts | — | — | ✓ (email) |

---

## API Reference

### Authentication

```bash
# Register
POST /api/auth/register
{"email": "user@example.com", "password": "yourpassword"}

# Login
POST /api/auth/login
{"email": "user@example.com", "password": "yourpassword"}

# Get current user
GET /api/auth/me
Authorization: Bearer <token>
```

### Arbitrage Scanner

```bash
# Get top arbitrage opportunities
GET /api/arbitrage/scanner?item_type=product&language=JP&limit=50

# Arbitrage for specific set
GET /api/arbitrage/set/{set_id}?item_type=product
```

### Sealed Products

```bash
# List all sealed products
GET /api/sealed/products?language=JP&sort=price_highest

# Price history (Pro+)
GET /api/sealed/products/{id}/history?days=30
Authorization: Bearer <token>
```

### EV Calculator (Pro+)

```bash
# Calculate EV for a set
GET /api/ev/calculate/{set_id}?box_cost=120.00
Authorization: Bearer <token>

# Custom EV calculation
POST /api/ev/custom
Authorization: Bearer <token>
{"set_id": "OP-01", "language": "JP", "box_cost": 115.00}
```

### Sets

```bash
GET /api/sets                    # All sets
GET /api/sets?language=JP        # JP sets only
GET /api/sets/{set_id}           # Single set
```

---

## Caching Strategy

| Tier | Cache Duration | API Calls |
|---|---|---|
| Free (no auth) | 24 hours | Minimal |
| Pro | 15 minutes | Normal |
| Elite | 15 minutes | Normal |

The API adapter (`services/opcg_api.py`) checks SQLite cache age before making API calls. On cache miss, it fetches fresh data, stores it, and records a price history entry for charts.

---

## Arbitrage Calculation

The engine computes two scenarios per item:
1. **BUY EU**: Buy on Cardmarket → sell on TCGPlayer
2. **BUY US**: Buy on TCGPlayer → sell on Cardmarket

**Costs factored in:**
- FX spread (EUR/USD conversion): ~3% (configurable via `ARB_FX_SPREAD`)
- Shipping EU→US: ~€17 per shipment (configurable via `ARB_SHIPPING_EU_US`)
- Shipping US→EU: ~€22 per shipment (configurable via `ARB_SHIPPING_US_EU`)
- Cardmarket seller fee: ~5% (`ARB_CM_FEE`)
- TCGPlayer seller fee: ~13% (`ARB_TCG_FEE`)

**Signal thresholds:**
- `BUY_EU` / `BUY_US`: >5% profit after all costs
- `WATCH`: 2–5% profit
- `NEUTRAL`: <2% or no opportunity

---

## EV Calculation

Pull rates are hardcoded community data in `services/ev_engine.py`:

| Rarity | JP (10 packs) | EN (24 packs) |
|---|---|---|
| SR (Super Rare) | 3.5× | 7.0× |
| SEC (Secret Rare) | 0.5× | 0.67× |
| Leader AA | ~1 in 3 boxes | 2× |
| Manga AA | ~1 in 54 boxes | ~1 in 54 boxes |

**Verdict thresholds:**
- `OPEN`: EV/Box ≥ 115% (clearly worth opening)
- `BORDERLINE`: EV/Box 90–115%
- `HOLD_SEALED`: EV/Box < 90% (sealed worth more)

---

## Production Deployment

```bash
# Install production dependencies
pip install gunicorn

# Run with Gunicorn
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000

# Or with uvicorn directly (no --reload in production)
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

**Environment checklist for production:**
- [ ] Set `JWT_SECRET` to a strong random value
- [ ] Use live Stripe keys (`sk_live_...`)
- [ ] Set `APP_URL` to your production domain
- [ ] Configure Stripe webhook with production URL
- [ ] Set `DATABASE_PATH` to a persistent volume path
- [ ] Tighten CORS in `main.py` (`allow_origins=[your-domain]`)

---

## Development Notes

- **No React/Vue** — pure vanilla HTML/CSS/JS
- **No CSS frameworks** — custom terminal dark theme
- **Fonts**: Cabinet Grotesk (headings) + JetBrains Mono (data) via Google Fonts + Fontshare
- **Charts**: TradingView Lightweight Charts for price sparklines
- **Database**: SQLite (single file, no setup required)
- **Payments**: Stripe Checkout Sessions (no custom payment forms)

---

## License

Private — all rights reserved.

<!-- deploy-trigger 2026-04-24 -->

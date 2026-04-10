/* ============================================================
   OPTCG MARKET TERMINAL — app.js
   State management, API, filter logic, render pipeline
   ============================================================ */

'use strict';

/* ============================================================
   STATE — single source of truth
   ============================================================ */
const State = {
  // Auth
  token: null,
  user: null,

  // Data (loaded once from API)
  sets: [],
  cards: [],          // 6,000+ cards from arbitrage scanner
  sealedProducts: [], // all sealed products

  // Arbitrage filters
  arbFilters: {
    search: '',
    region: 'all',    // 'all' | 'JP' | 'EN'
    rarity: 'all',    // 'all' | 'sec' | 'manga' | 'regular'
    signal: 'all',    // 'all' | 'BUY_EU' | 'BUY_US' | 'WATCH'
    set:    'all',    // 'all' | set_name
  },
  arbSort: { col: 'profit_eur', dir: 'desc' },
  arbPage: 50,        // how many rows currently visible

  // Sealed filters
  sealedFilters: {
    type:   'all',    // 'all' | 'case' | 'box' | 'booster'
    set:    'all',
    region: 'all',    // 'all' | 'JP' | 'EN'
  },
  sealedSort: 'price',

  // EV
  evSetId: null,
  evLanguage: 'JP',
  evBoxCost: 100,

  // UI
  activeTab: 'arbitrage',
  currency: 'EUR',

  // Loading
  loading: {
    cards: false,
    sealed: false,
    sets: false,
    ev: false,
  },
};

/* ============================================================
   FX / FORMATTING
   ============================================================ */
const FX = { EUR: 1.0, CHF: 0.96, USD: 1.08 };

function fmt(eurValue) {
  if (eurValue == null || isNaN(eurValue)) return '—';
  const amount = parseFloat(eurValue) * FX[State.currency];
  const sym = { EUR: '€', CHF: 'CHF ', USD: '$' }[State.currency];
  if (amount >= 1000) return sym + amount.toLocaleString('en-US', { maximumFractionDigits: 0 });
  if (amount >= 10)   return sym + amount.toFixed(0);
  return sym + amount.toFixed(2);
}

function fmtPct(val) {
  if (val == null || isNaN(val)) return '—';
  return (parseFloat(val) >= 0 ? '+' : '') + parseFloat(val).toFixed(1) + '%';
}

function esc(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g,  '&amp;')
    .replace(/</g,  '&lt;')
    .replace(/>/g,  '&gt;')
    .replace(/"/g,  '&quot;')
    .replace(/'/g,  '&#39;');
}

/* ============================================================
   AUTH HEADERS
   ============================================================ */
function authHeaders() {
  const hdrs = { 'Content-Type': 'application/json' };
  if (State.token) hdrs['Authorization'] = 'Bearer ' + State.token;
  return hdrs;
}

/* ============================================================
   FILTER LOGIC
   ============================================================ */
function getFilteredCards() {
  let filtered = [...State.cards];

  // Search
  if (State.arbFilters.search) {
    const q = State.arbFilters.search.toLowerCase();
    filtered = filtered.filter(c =>
      (c.name     || '').toLowerCase().includes(q) ||
      (c.set_name || '').toLowerCase().includes(q) ||
      (c.code     || '').toLowerCase().includes(q)
    );
  }

  // Region: JP → JP+BOTH, EN → EN+BOTH, all → everything
  if (State.arbFilters.region === 'JP') {
    filtered = filtered.filter(c => c._region === 'JP' || c._region === 'BOTH');
  } else if (State.arbFilters.region === 'EN') {
    filtered = filtered.filter(c => c._region === 'EN' || c._region === 'BOTH');
  }

  // Set
  if (State.arbFilters.set !== 'all') {
    filtered = filtered.filter(c => c.set_name === State.arbFilters.set);
  }

  // Rarity
  if (State.arbFilters.rarity === 'sec') {
    filtered = filtered.filter(c => /LEADER|SECRET|SEC/i.test(c.rarity || ''));
  } else if (State.arbFilters.rarity === 'manga') {
    filtered = filtered.filter(c =>
      /SP|MANGA|SPECIAL/i.test(c.rarity || '') ||
      parseFloat((c.version || '').replace('v', '')) >= 3
    );
  } else if (State.arbFilters.rarity === 'regular') {
    filtered = filtered.filter(c =>
      !/LEADER|SECRET|SEC|SP|MANGA|SPECIAL/i.test(c.rarity || '')
    );
  }

  // Signal
  if (State.arbFilters.signal !== 'all') {
    filtered = filtered.filter(c => c.signal === State.arbFilters.signal);
  }

  // Sort — map display column names to data field names
  const COL_MAP = { eu_price: 'cardmarket_price', us_price: 'tcgplayer_price' };
  const { col, dir } = State.arbSort;
  const dataCol = COL_MAP[col] || col;
  filtered.sort((a, b) => {
    let va = a[dataCol] ?? 0;
    let vb = b[dataCol] ?? 0;
    if (typeof va === 'string') va = va.toLowerCase();
    if (typeof vb === 'string') vb = vb.toLowerCase();
    return dir === 'desc'
      ? (vb > va ? 1 : vb < va ? -1 : 0)
      : (va > vb ? 1 : va < vb ? -1 : 0);
  });

  return filtered;
}

function getFilteredSealed() {
  let filtered = [...State.sealedProducts];

  // Set
  if (State.sealedFilters.set !== 'all') {
    filtered = filtered.filter(p => p.set_name === State.sealedFilters.set);
  }

  // Type
  const type = State.sealedFilters.type;
  if (type === 'case') {
    filtered = filtered.filter(p => /case/i.test(p.name));
  } else if (type === 'box') {
    filtered = filtered.filter(p => /booster box/i.test(p.name) && !/case/i.test(p.name));
  } else if (type === 'booster') {
    filtered = filtered.filter(p =>
      /booster/i.test(p.name) && !/box/i.test(p.name) && !/case/i.test(p.name)
    );
  }

  // Region
  if (State.sealedFilters.region === 'EN') {
    filtered = filtered.filter(p => p._tcgplayer_price && p._tcgplayer_price > 0);
  }

  // Sort
  if (State.sealedSort === 'price') {
    filtered.sort((a, b) => (b._cardmarket_price || 0) - (a._cardmarket_price || 0));
  } else if (State.sealedSort === 'trend') {
    filtered.sort((a, b) => {
      const score = t => t === 'up' ? 1 : t === 'down' ? -1 : 0;
      return score(b.trend) - score(a.trend);
    });
  } else if (State.sealedSort === 'set') {
    filtered.sort((a, b) => (a.set_name || '').localeCompare(b.set_name || ''));
  }

  return filtered;
}

/* ============================================================
   STAT CALCULATIONS
   ============================================================ */
function calcArbStats(filtered) {
  const total = filtered.length;
  const bestProfit = filtered.reduce((best, c) => {
    const p = parseFloat(c.profit_eur) || 0;
    return p > best ? p : best;
  }, 0);
  const buyEU = filtered.filter(c => c.signal === 'BUY_EU').length;
  const buyUS = filtered.filter(c => c.signal === 'BUY_US').length;
  return { total, bestProfit, buyEU, buyUS };
}

/* ============================================================
   RENDER — ARBITRAGE TABLE
   ============================================================ */
function isPaidUser() {
  return State.user?.tier === 'pro' || State.user?.tier === 'elite';
}

function renderSignalBadge(signal) {
  const map = {
    'BUY_EU':  ['buy-eu',  'BUY EU'],
    'BUY_US':  ['buy-us',  'BUY US'],
    'WATCH':   ['watch',   'WATCH'],
    'NEUTRAL': ['neutral', 'NEUTRAL'],
  };
  const [cls, label] = map[signal] || ['neutral', signal || 'NEUTRAL'];
  return `<span class="signal-badge ${cls}">${esc(label)}</span>`;
}

function renderRegionFlag(region) {
  if (!region) return '';
  if (region === 'JP')   return '<span class="region-flag" title="Japan">🇯🇵</span>';
  if (region === 'EN')   return '<span class="region-flag" title="US/English">🇺🇸</span>';
  if (region === 'BOTH') return '<span class="region-flag" title="Both regions">🇯🇵🇺🇸</span>';
  return '';
}

function renderBuyLink(card) {
  // Determine which market to buy on based on signal
  const signal = card.signal || '';
  if (signal === 'BUY_EU' || (signal !== 'BUY_US' && card.cardmarket_url)) {
    return `<a class="buy-link" href="${esc(card.cardmarket_url || '#')}" target="_blank" rel="noopener">🇪🇺 Buy EU</a>`;
  }
  if (signal === 'BUY_US' || card.tcgplayer_url) {
    return `<a class="buy-link us" href="${esc(card.tcgplayer_url || '#')}" target="_blank" rel="noopener">🇺🇸 Buy US</a>`;
  }
  return '—';
}

function renderThumb(src) {
  if (src) {
    return `<img class="card-thumb" src="${esc(src)}" alt="" loading="lazy" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'" /><span class="card-thumb-placeholder" style="display:none">🃏</span>`;
  }
  return `<span class="card-thumb-placeholder">🃏</span>`;
}

function renderArbRow(card) {
  const profit     = parseFloat(card.profit_eur) || 0;
  const profitPct  = parseFloat(card.profit_pct) || 0;
  const profitCls  = profit >= 0 ? 'pos' : 'neg';
  const pctCls     = profitPct >= 0 ? 'pos' : 'neg';
  const euPrice    = fmt(card.cardmarket_price);
  const usPrice    = fmt(card.tcgplayer_price);

  return `<tr>
    <td>
      <div class="cell-product">
        ${renderThumb(card.image)}
        <div class="cell-product-info">
          <span class="cell-product-name" title="${esc(card.name)}">${esc(card.name || '—')}</span>
          <span class="cell-product-code">${esc(card.code || '')} ${esc(card.rarity || '')}</span>
        </div>
      </div>
    </td>
    <td>${esc(card.set_name || '—')}</td>
    <td>${renderRegionFlag(card._region)}</td>
    <td class="cell-price">${card.cardmarket_url ? `<a href="${esc(card.cardmarket_url)}" target="_blank" rel="noopener" class="price-link">${euPrice}</a>` : euPrice}</td>
    <td class="cell-price">${card.tcgplayer_url ? `<a href="${esc(card.tcgplayer_url)}" target="_blank" rel="noopener" class="price-link">${usPrice}</a>` : usPrice}</td>
    <td class="cell-profit ${profitCls}">${fmt(profit)}</td>
    <td class="cell-pct ${pctCls}">${fmtPct(profitPct)}</td>
    <td>${renderSignalBadge(card.signal)}</td>
    <td>${renderBuyLink(card)}</td>
  </tr>`;
}

function renderArbTable(filtered) {
  const tbody = document.getElementById('arb-tbody');
  const tfoot = document.getElementById('arb-tfoot');
  if (!tbody) return;

  const paid    = isPaidUser();
  const visible = filtered.slice(0, State.arbPage);
  const locked  = !paid && filtered.length > 10 ? filtered.slice(10, State.arbPage) : [];
  const free    = !paid ? Math.min(10, filtered.length) : filtered.length;

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9">
      <div class="empty-state">
        <span class="empty-icon">🔍</span>
        <span class="empty-title">No results</span>
        <span class="empty-sub">Try adjusting your filters</span>
      </div>
    </td></tr>`;
    if (tfoot) tfoot.innerHTML = '';
    return;
  }

  let html = '';

  if (paid) {
    // All visible rows
    for (let i = 0; i < visible.length; i++) {
      html += renderArbRow(visible[i]);
    }
  } else {
    // First 10 rows
    for (let i = 0; i < Math.min(10, visible.length); i++) {
      html += renderArbRow(visible[i]);
    }
  }

  tbody.innerHTML = html;

  // Pro lock overlay
  const proWrap = document.getElementById('arb-pro-wrap');
  if (proWrap) {
    proWrap.style.display = (!paid && filtered.length > 10) ? 'block' : 'none';
  }

  // Footer
  if (tfoot) {
    const showing = paid ? Math.min(State.arbPage, filtered.length) : Math.min(10, filtered.length);
    const total   = filtered.length;
    const hasMore = paid && State.arbPage < total;

    tfoot.innerHTML = `
      <div class="table-footer">
        <span class="table-count">Showing ${showing.toLocaleString()} of ${total.toLocaleString()} results</span>
        <div class="table-pagination">
          ${hasMore
            ? `<button class="btn-load-more" id="btn-load-more">Load 50 more</button>`
            : paid ? `<span style="color:var(--muted);font-size:12px">All results shown</span>` : ''
          }
          ${!paid
            ? `<button class="btn-upgrade btn-load-more" onclick="window.location.href='/api/billing/checkout'">Upgrade to see all ${total.toLocaleString()} results</button>`
            : ''
          }
        </div>
      </div>`;

    const loadMoreBtn = document.getElementById('btn-load-more');
    if (loadMoreBtn) {
      loadMoreBtn.addEventListener('click', () => {
        State.arbPage += 50;
        renderArbTable(filtered);
      });
    }
  }
}

/* ── Sort header click ── */
function initArbSortHeaders() {
  document.querySelectorAll('#arb-table th[data-col]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (State.arbSort.col === col) {
        State.arbSort.dir = State.arbSort.dir === 'desc' ? 'asc' : 'desc';
      } else {
        State.arbSort.col = col;
        State.arbSort.dir = 'desc';
      }
      State.arbPage = 50; // reset pagination on sort change
      applyFilters();
    });
  });
}

function updateSortIcons() {
  document.querySelectorAll('#arb-table th[data-col]').forEach(th => {
    const col  = th.dataset.col;
    const icon = th.querySelector('.sort-icon');
    if (!icon) return;
    if (State.arbSort.col === col) {
      th.classList.add('sorted');
      icon.textContent = State.arbSort.dir === 'desc' ? '▼' : '▲';
    } else {
      th.classList.remove('sorted');
      icon.textContent = '↕';
    }
  });
}

/* ============================================================
   RENDER — STAT CARDS
   ============================================================ */
function renderArbStats(filtered) {
  const { total, bestProfit, buyEU, buyUS } = calcArbStats(filtered);
  const el = id => document.getElementById(id);

  const tot = el('stat-total');
  const bp  = el('stat-best-profit');
  const eu  = el('stat-buy-eu');
  const us  = el('stat-buy-us');

  if (tot) tot.textContent = total.toLocaleString();
  if (bp)  bp.textContent  = fmt(bestProfit);
  if (eu)  eu.textContent  = buyEU.toLocaleString();
  if (us)  us.textContent  = buyUS.toLocaleString();
}

/* ============================================================
   RENDER — SEALED GRID
   ============================================================ */
function renderTrendBadge(trend) {
  if (!trend) return '';
  const map = { up: ['up', '↑', 'Up'], down: ['down', '↓', 'Down'], flat: ['flat', '→', 'Flat'] };
  const [cls, arrow, label] = map[trend] || ['flat', '—', trend];
  return `<span class="trend-badge ${cls}">${arrow} ${esc(label)}</span>`;
}

function renderSealedCard(p) {
  const euPrice  = p._cardmarket_price;
  const avg30    = p.cm_30d_average;
  const avg7     = p.cm_7d_average;
  const lang     = (p.set_language || 'JP') === 'JP' ? '🇯🇵' : '🇺🇸';
  const setCode  = p.set_code ? `(${esc(p.set_code)})` : '';

  let imgHtml;
  if (p.image_url) {
    imgHtml = `<img class="product-img" src="${esc(p.image_url)}" alt="${esc(p.name)}" loading="lazy" onerror="this.parentElement.innerHTML='<div class=product-img-placeholder>📦</div>'" />`;
  } else {
    imgHtml = `<div class="product-img-placeholder">📦</div>`;
  }

  return `<div class="product-card">
    <div class="product-img-wrap">${imgHtml}</div>
    <div class="product-body">
      <div class="product-name">${esc(p.name)}</div>
      <div class="product-meta">
        <span>${lang}</span>
        <span>📦</span>
        <span>${esc(p.set_name || '')} ${setCode}</span>
      </div>
      <div class="product-divider"></div>
      <div class="product-prices">
        <div class="price-row">
          <span class="price-label">EU</span>
          <span class="price-value">${euPrice ? fmt(euPrice) : '—'}</span>
        </div>
        ${euPrice ? `<div class="price-avg">
          ${avg30 ? `30d avg ${fmt(avg30)}` : ''}
          ${avg7  ? `&nbsp; 7d avg ${fmt(avg7)}`  : ''}
          &nbsp; ${renderTrendBadge(p.trend)}
        </div>` : ''}
      </div>
    </div>
    <div class="product-footer">
      ${p.cardmarket_url
        ? `<a class="view-link" href="${esc(p.cardmarket_url)}" target="_blank" rel="noopener">🔗 View listing</a>`
        : '<span style="color:var(--muted);font-size:12px">No listing</span>'
      }
    </div>
  </div>`;
}

function renderSealedGrid(filtered) {
  const grid = document.getElementById('sealed-grid');
  if (!grid) return;

  if (filtered.length === 0) {
    grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1">
      <span class="empty-icon">📦</span>
      <span class="empty-title">No products found</span>
      <span class="empty-sub">Try adjusting your filters</span>
    </div>`;
    return;
  }

  grid.innerHTML = filtered.map(renderSealedCard).join('');
}

/* ============================================================
   RENDER — TICKER
   ============================================================ */
function renderTicker() {
  const track = document.getElementById('ticker-track');
  if (!track) return;

  // Take top 20 by profit
  const top = [...State.cards]
    .filter(c => c.profit_eur && parseFloat(c.profit_eur) > 0)
    .sort((a, b) => parseFloat(b.profit_eur) - parseFloat(a.profit_eur))
    .slice(0, 20);

  if (top.length === 0) {
    track.innerHTML = `<div class="ticker-item"><span class="ticker-item-name">Loading market data…</span></div>`;
    return;
  }

  const items = top.map(c => {
    const signal  = c.signal || 'NEUTRAL';
    const sigCls  = signal === 'BUY_EU' ? 'buy-eu' : signal === 'BUY_US' ? 'buy-us' : 'watch';
    const profit  = parseFloat(c.profit_eur);
    const profCls = profit >= 0 ? '' : 'neg';

    return `<div class="ticker-item">
      <span class="ticker-item-name">${esc(c.name || c.code || '—')}</span>
      <span class="ticker-item-price">${fmt(c.cardmarket_price || c.tcgplayer_price || 0)}</span>
      <span class="ticker-item-signal ${sigCls}">${esc(signal)}</span>
      <span class="ticker-item-profit ${profCls}">+${fmt(profit)}</span>
    </div>`;
  }).join('');

  // Duplicate for seamless loop
  track.innerHTML = items + items;
}

/* ============================================================
   RENDER — MARKET OVERVIEW TAB
   ============================================================ */
function renderMarketOverview() {
  // Stat cards
  const setsEl    = document.getElementById('ov-sets');
  const updatedEl = document.getElementById('ov-updated');
  const topSigEl  = document.getElementById('ov-top-signal');
  const recentEl  = document.getElementById('ov-recent');

  if (setsEl)    setsEl.textContent    = State.sets.length || 49;
  if (updatedEl) updatedEl.textContent = new Date().toLocaleString('en-US', { hour: '2-digit', minute: '2-digit' });
  if (topSigEl) {
    const buyEU = State.cards.filter(c => c.signal === 'BUY_EU').length;
    const buyUS = State.cards.filter(c => c.signal === 'BUY_US').length;
    topSigEl.textContent = buyEU > buyUS ? `BUY EU (${buyEU})` : `BUY US (${buyUS})`;
  }
  if (recentEl) {
    const sorted = [...State.sets]
      .filter(s => s.release_date)
      .sort((a, b) => new Date(b.release_date) - new Date(a.release_date));
    recentEl.textContent = sorted[0]?.name || '—';
  }

  // Top movers
  const moversEl = document.getElementById('ov-movers');
  if (moversEl) {
    const top5 = [...State.cards]
      .filter(c => c.profit_eur)
      .sort((a, b) => parseFloat(b.profit_eur) - parseFloat(a.profit_eur))
      .slice(0, 5);

    if (top5.length === 0) {
      moversEl.innerHTML = `<li class="overview-list-item"><span class="empty-sub">No data yet</span></li>`;
    } else {
      moversEl.innerHTML = top5.map((c, i) => {
        const profit = parseFloat(c.profit_eur);
        return `<li class="overview-list-item">
          <div class="overview-list-left">
            <span class="rank-badge">${i + 1}</span>
            <div>
              <div class="overview-list-name">${esc(c.name || c.code || '—')}</div>
              <div class="overview-list-sub">${esc(c.set_name || '')} · ${esc(c._region || '')}</div>
            </div>
          </div>
          <div class="overview-list-right">
            ${renderSignalBadge(c.signal)}
            <span class="cell-profit pos" style="font-size:13px;font-family:var(--font-mono)">${fmt(profit)}</span>
          </div>
        </li>`;
      }).join('');
    }
  }

  // Recent sets
  const setsListEl = document.getElementById('ov-sets-list');
  if (setsListEl) {
    const recent10 = [...State.sets]
      .filter(s => s.release_date)
      .sort((a, b) => new Date(b.release_date) - new Date(a.release_date))
      .slice(0, 10);

    if (recent10.length === 0) {
      setsListEl.innerHTML = `<li class="overview-list-item"><span class="empty-sub">Loading sets…</span></li>`;
    } else {
      setsListEl.innerHTML = recent10.map(s => {
        const lang = s.language === 'JP' ? '🇯🇵' : '🇺🇸';
        const date = s.release_date ? new Date(s.release_date).toLocaleDateString('en-US', { year: 'numeric', month: 'short' }) : '—';
        return `<li class="overview-list-item">
          <div class="overview-list-left">
            <span>${lang}</span>
            <div>
              <div class="overview-list-name">${esc(s.name)}</div>
              <div class="overview-list-sub">${esc(s.code || '')} · ${s.card_count ? s.card_count + ' cards' : ''}</div>
            </div>
          </div>
          <div class="overview-list-right">
            <span class="overview-list-sub">${date}</span>
          </div>
        </li>`;
      }).join('');
    }
  }
}

/* ============================================================
   RENDER — EV RESULTS
   ============================================================ */
function renderEvResults(data) {
  const el = id => document.getElementById(id);
  const results = el('ev-results');
  if (!results) return;

  if (!data || data.error) {
    results.className = 'ev-results visible';
    results.innerHTML = `<div class="ev-verdict error">
      <span class="ev-verdict-icon">⚠️</span>
      <div class="ev-verdict-text">
        <h3>Error</h3>
        <p>${esc(data?.error || 'Failed to calculate EV')}</p>
      </div>
    </div>`;
    return;
  }

  const verdict     = data.verdict || 'HOLD';
  const verdictCls  = verdict === 'OPEN' ? 'open' : verdict === 'BORDERLINE' ? 'borderline' : 'hold';
  const verdictIcon = verdict === 'OPEN' ? '✅' : verdict === 'BORDERLINE' ? '📊' : '🔒';

  const evValue  = parseFloat(data.box_ev) || 0;
  const pl       = evValue - (parseFloat(State.evBoxCost) || 0);
  const returnPct = State.evBoxCost > 0 ? (pl / State.evBoxCost * 100) : 0;

  results.className = 'ev-results visible';
  results.innerHTML = `
    <div class="ev-verdict ${verdictCls}">
      <span class="ev-verdict-icon">${verdictIcon}</span>
      <div class="ev-verdict-text">
        <h3>${esc(verdict)}</h3>
        <p>${esc(data.verdict_text || (verdict === 'OPEN' ? 'This box has positive expected value.' : verdict === 'BORDERLINE' ? 'Near breakeven — proceed with caution.' : 'Expected value is below box cost.'))}</p>
      </div>
    </div>
    <div class="ev-stats-grid">
      <div class="ev-stat">
        <div class="ev-stat-label">Box EV</div>
        <div class="ev-stat-value">${fmt(evValue)}</div>
      </div>
      <div class="ev-stat">
        <div class="ev-stat-label">Profit / Loss</div>
        <div class="ev-stat-value ${pl >= 0 ? 'pos' : 'neg'}">${fmt(pl)}</div>
      </div>
      <div class="ev-stat">
        <div class="ev-stat-label">Return</div>
        <div class="ev-stat-value ${returnPct >= 0 ? 'pos' : 'neg'}">${fmtPct(returnPct)}</div>
      </div>
    </div>
    ${data.hits && data.hits.length > 0 ? `
    <div class="ev-hits-table">
      <div class="ev-hits-title">Hit Breakdown</div>
      <table class="data-table" style="min-width:auto">
        <thead>
          <tr>
            <th style="cursor:default">Card</th>
            <th style="cursor:default">Rarity</th>
            <th style="cursor:default;text-align:right">Price</th>
            <th style="cursor:default;text-align:right">Pull Rate</th>
            <th style="cursor:default;text-align:right">EV Contribution</th>
          </tr>
        </thead>
        <tbody>
          ${data.hits.map(h => `
            <tr>
              <td>${esc(h.name || '—')}</td>
              <td><span class="signal-badge neutral">${esc(h.rarity || '—')}</span></td>
              <td class="cell-price">${fmt(h.price)}</td>
              <td class="cell-price">${h.pull_rate != null ? parseFloat(h.pull_rate).toFixed(3) + '%' : '—'}</td>
              <td class="cell-profit pos">${fmt(h.ev_contribution)}</td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>` : ''}`;
}

/* ============================================================
   APPLY FILTERS — main pipeline
   ============================================================ */
function applyFilters() {
  if (State.activeTab === 'arbitrage') {
    const filtered = getFilteredCards();
    renderArbStats(filtered);
    updateSortIcons();
    renderArbTable(filtered);
  } else if (State.activeTab === 'sealed') {
    const filtered = getFilteredSealed();
    renderSealedGrid(filtered);
  } else if (State.activeTab === 'overview') {
    renderMarketOverview();
  }
}

/* ============================================================
   SET DROPDOWNS — populate from State.sets
   ============================================================ */
function populateSetDropdowns() {
  // Arbitrage set dropdown
  const arbSetSel = document.getElementById('filter-set');
  if (arbSetSel) {
    const names = [...new Set(State.cards.map(c => c.set_name).filter(Boolean))].sort();
    arbSetSel.innerHTML = `<option value="all">All Sets</option>` +
      names.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
    arbSetSel.value = State.arbFilters.set;
  }

  // Sealed set dropdown
  const sealedSetSel = document.getElementById('sealed-filter-set');
  if (sealedSetSel) {
    const names = [...new Set(State.sealedProducts.map(p => p.set_name).filter(Boolean))].sort();
    sealedSetSel.innerHTML = `<option value="all">All Sets</option>` +
      names.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
    sealedSetSel.value = State.sealedFilters.set;
  }

  // EV set dropdown
  const evSetSel = document.getElementById('ev-set');
  if (evSetSel) {
    const evSets = State.sets.filter(s => s.card_count > 0 || true);
    evSetSel.innerHTML = `<option value="">Select a set…</option>` +
      evSets.map(s => `<option value="${esc(s.api_id)}">${esc(s.name)} (${esc(s.language || 'JP')})</option>`).join('');
  }
}

/* ============================================================
   LOADING SKELETONS
   ============================================================ */
function showArbSkeleton() {
  const tbody = document.getElementById('arb-tbody');
  if (!tbody) return;
  tbody.innerHTML = Array.from({ length: 10 }, () =>
    `<tr><td colspan="9"><div class="skeleton skeleton-row"></div></td></tr>`
  ).join('');
}

function showSealedSkeleton() {
  const grid = document.getElementById('sealed-grid');
  if (!grid) return;
  grid.innerHTML = Array.from({ length: 8 }, () =>
    `<div class="skeleton skeleton-card"></div>`
  ).join('');
}

/* ============================================================
   API — DATA LOADING
   ============================================================ */
async function loadSets() {
  try {
    const resp = await Auth.authFetch('/api/sets');
    if (!resp.ok) return;
    const data = await resp.json();
    State.sets = Array.isArray(data) ? data : (data.sets || []);
  } catch (e) {
    console.error('[OPTCG] Failed to load sets:', e);
  }
}

async function loadCards() {
  if (State.loading.cards) return;
  State.loading.cards = true;
  showArbSkeleton();

  try {
    const resp = await Auth.authFetch('/api/arbitrage/scanner?item_type=card');
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      console.error('[OPTCG] Scanner error:', err);
      return;
    }
    const data = await resp.json();
    State.cards = Array.isArray(data.opportunities) ? data.opportunities : [];
    populateSetDropdowns();
    renderTicker();
    if (State.activeTab === 'arbitrage') applyFilters();
    if (State.activeTab === 'overview')  renderMarketOverview();
  } catch (e) {
    console.error('[OPTCG] Failed to load cards:', e);
  } finally {
    State.loading.cards = false;
  }
}

async function loadSealed() {
  if (State.loading.sealed) return;
  State.loading.sealed = true;
  showSealedSkeleton();

  try {
    const resp = await Auth.authFetch('/api/sealed/products');
    if (!resp.ok) return;
    const data = await resp.json();
    State.sealedProducts = Array.isArray(data.products) ? data.products : [];
    populateSetDropdowns();
    if (State.activeTab === 'sealed') applyFilters();
  } catch (e) {
    console.error('[OPTCG] Failed to load sealed:', e);
  } finally {
    State.loading.sealed = false;
  }
}

async function calcEV() {
  const setId   = State.evSetId;
  const lang    = State.evLanguage;
  const boxCost = State.evBoxCost;

  if (!setId) {
    showToast('Please select a set first.', 'error');
    return;
  }

  const btn = document.getElementById('btn-calculate');
  if (btn) { btn.disabled = true; btn.textContent = 'Calculating…'; }

  try {
    const resp = await Auth.authFetch(
      `/api/ev/calculate/${encodeURIComponent(setId)}?language=${lang}&box_cost=${boxCost}`
    );
    const data = await resp.json();
    renderEvResults(data);
  } catch (e) {
    renderEvResults({ error: e.message });
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Calculate EV'; }
  }
}

/* ============================================================
   NAV / TABS
   ============================================================ */
function switchTab(tab) {
  State.activeTab = tab;

  document.querySelectorAll('.nav-tab, .mobile-nav-btn').forEach(el => {
    el.classList.toggle('active', el.dataset.tab === tab);
  });

  document.querySelectorAll('.tab-panel').forEach(el => {
    el.classList.toggle('active', el.id === `tab-${tab}`);
  });

  // Load data on demand
  if (tab === 'arbitrage' && State.cards.length === 0) {
    loadCards();
  } else if (tab === 'sealed' && State.sealedProducts.length === 0) {
    loadSealed();
  } else if (tab === 'overview') {
    renderMarketOverview();
  }

  applyFilters();
}

/* ============================================================
   USER MENU
   ============================================================ */
function updateNavUserState() {
  const navRight    = document.getElementById('nav-right');
  if (!navRight) return;

  if (State.user) {
    const initial = (State.user.email || 'U')[0].toUpperCase();
    const tier    = State.user.tier || 'free';

    navRight.innerHTML = `
      <div class="currency-selector">
        <button class="currency-btn ${State.currency === 'EUR' ? 'active' : ''}" data-cur="EUR">EUR</button>
        <button class="currency-btn ${State.currency === 'CHF' ? 'active' : ''}" data-cur="CHF">CHF</button>
        <button class="currency-btn ${State.currency === 'USD' ? 'active' : ''}" data-cur="USD">USD</button>
      </div>
      <div class="status-pill"><span class="status-dot"></span>15MIN</div>
      <div class="user-menu">
        <button class="user-menu-trigger" id="user-menu-trigger">
          <span class="user-avatar">${esc(initial)}</span>
          <span>${esc(State.user.email || 'Account')}</span>
          <span class="user-tier-badge">${esc(tier.toUpperCase())}</span>
          <span style="color:var(--muted);font-size:11px">▾</span>
        </button>
        <div class="user-dropdown" id="user-dropdown">
          <div class="user-email">${esc(State.user.email || '')}</div>
          ${tier === 'free' ? `<div class="user-dropdown-item" onclick="window.location.href='/api/billing/checkout'">⚡ Upgrade to Elite</div>` : ''}
          <div class="user-dropdown-divider"></div>
          <div class="user-dropdown-item danger" id="btn-logout">Sign Out</div>
        </div>
      </div>`;

    // Currency toggle
    navRight.querySelectorAll('.currency-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        State.currency = btn.dataset.cur;
        navRight.querySelectorAll('.currency-btn').forEach(b =>
          b.classList.toggle('active', b.dataset.cur === State.currency)
        );
        applyFilters();
        renderTicker();
      });
    });

    // User menu toggle
    const trigger  = document.getElementById('user-menu-trigger');
    const dropdown = document.getElementById('user-dropdown');
    if (trigger && dropdown) {
      trigger.addEventListener('click', (e) => {
        e.stopPropagation();
        dropdown.classList.toggle('open');
      });
    }

    const logoutBtn = document.getElementById('btn-logout');
    if (logoutBtn) {
      logoutBtn.addEventListener('click', () => {
        Auth.logout();
      });
    }

  } else {
    // Not logged in
    navRight.innerHTML = `
      <div class="currency-selector">
        <button class="currency-btn ${State.currency === 'EUR' ? 'active' : ''}" data-cur="EUR">EUR</button>
        <button class="currency-btn ${State.currency === 'CHF' ? 'active' : ''}" data-cur="CHF">CHF</button>
        <button class="currency-btn ${State.currency === 'USD' ? 'active' : ''}" data-cur="USD">USD</button>
      </div>
      <div class="status-pill"><span class="status-dot"></span>15MIN</div>
      <a href="/static/login.html" class="btn-signin">Sign In</a>
      <a href="/static/login.html#register" class="btn-elite">ELITE ⚡</a>`;

    navRight.querySelectorAll('.currency-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        State.currency = btn.dataset.cur;
        navRight.querySelectorAll('.currency-btn').forEach(b =>
          b.classList.toggle('active', b.dataset.cur === State.currency)
        );
        applyFilters();
        renderTicker();
      });
    });
  }
}

/* ============================================================
   TOAST NOTIFICATIONS
   ============================================================ */
function showToast(msg, type = 'success', duration = 3000) {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = msg;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity 0.3s';
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

/* ============================================================
   FILTER EVENT WIRING
   ============================================================ */
function initFilters() {
  /* ── Arbitrage filters ── */

  // Region pills
  document.querySelectorAll('[data-arb-region]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.arbFilters.region = btn.dataset.arbRegion;
      State.arbPage = 50;
      document.querySelectorAll('[data-arb-region]').forEach(b =>
        b.classList.toggle('active', b.dataset.arbRegion === State.arbFilters.region)
      );
      applyFilters();
    });
  });

  // Rarity pills
  document.querySelectorAll('[data-arb-rarity]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.arbFilters.rarity = btn.dataset.arbRarity;
      State.arbPage = 50;
      document.querySelectorAll('[data-arb-rarity]').forEach(b =>
        b.classList.toggle('active', b.dataset.arbRarity === State.arbFilters.rarity)
      );
      applyFilters();
    });
  });

  // Signal pills
  document.querySelectorAll('[data-arb-signal]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.arbFilters.signal = btn.dataset.arbSignal;
      State.arbPage = 50;
      document.querySelectorAll('[data-arb-signal]').forEach(b =>
        b.classList.toggle('active', b.dataset.arbSignal === State.arbFilters.signal)
      );
      applyFilters();
    });
  });

  // Set dropdown
  const arbSetSel = document.getElementById('filter-set');
  if (arbSetSel) {
    arbSetSel.addEventListener('change', () => {
      State.arbFilters.set = arbSetSel.value;
      State.arbPage = 50;
      applyFilters();
    });
  }

  // Search input (debounced)
  const searchIn = document.getElementById('arb-search');
  if (searchIn) {
    let debounceTimer;
    searchIn.addEventListener('input', () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        State.arbFilters.search = searchIn.value.trim();
        State.arbPage = 50;
        applyFilters();
      }, 300);
    });
  }

  /* ── Sealed filters ── */

  // Set dropdown
  const sealedSetSel = document.getElementById('sealed-filter-set');
  if (sealedSetSel) {
    sealedSetSel.addEventListener('change', () => {
      State.sealedFilters.set = sealedSetSel.value;
      applyFilters();
    });
  }

  // Type pills
  document.querySelectorAll('[data-sealed-type]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.sealedFilters.type = btn.dataset.sealedType;
      document.querySelectorAll('[data-sealed-type]').forEach(b =>
        b.classList.toggle('active', b.dataset.sealedType === State.sealedFilters.type)
      );
      applyFilters();
    });
  });

  // Region pills
  document.querySelectorAll('[data-sealed-region]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.sealedFilters.region = btn.dataset.sealedRegion;
      document.querySelectorAll('[data-sealed-region]').forEach(b =>
        b.classList.toggle('active', b.dataset.sealedRegion === State.sealedFilters.region)
      );
      applyFilters();
    });
  });

  // Sort pills
  document.querySelectorAll('[data-sealed-sort]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.sealedSort = btn.dataset.sealedSort;
      document.querySelectorAll('[data-sealed-sort]').forEach(b =>
        b.classList.toggle('active', b.dataset.sealedSort === State.sealedSort)
      );
      applyFilters();
    });
  });

  /* ── EV calculator ── */
  const evSetSel = document.getElementById('ev-set');
  if (evSetSel) {
    evSetSel.addEventListener('change', () => {
      State.evSetId = evSetSel.value;
    });
  }

  const evLangBtns = document.querySelectorAll('[data-ev-lang]');
  evLangBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      State.evLanguage = btn.dataset.evLang;
      evLangBtns.forEach(b => b.classList.toggle('active', b.dataset.evLang === State.evLanguage));
      const hintEl = document.getElementById('ev-packs-hint');
      if (hintEl) hintEl.textContent = State.evLanguage === 'JP' ? '10 packs per box' : '24 packs per box';
    });
  });

  const evCostIn = document.getElementById('ev-box-cost');
  if (evCostIn) {
    evCostIn.addEventListener('input', () => {
      State.evBoxCost = parseFloat(evCostIn.value) || 0;
    });
  }

  const calcBtn = document.getElementById('btn-calculate');
  if (calcBtn) {
    calcBtn.addEventListener('click', calcEV);
  }

  /* ── Close user dropdown on outside click ── */
  document.addEventListener('click', () => {
    const dd = document.getElementById('user-dropdown');
    if (dd) dd.classList.remove('open');
  });

  /* ── Nav tabs ── */
  document.querySelectorAll('.nav-tab, .mobile-nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      switchTab(btn.dataset.tab);
    });
  });
}

/* ============================================================
   INIT
   ============================================================ */
async function init() {
  // Restore auth from localStorage
  State.token = Auth.getToken();
  State.user  = Auth.getUser();

  // Render nav with current user state
  updateNavUserState();

  // Wire all filter events
  initFilters();
  initArbSortHeaders();

  // Load initial data
  switchTab('arbitrage');

  // Also load sets and sealed in background
  loadSets().then(() => {
    populateSetDropdowns();
  });

  // Silently refresh user from server
  if (State.token) {
    Auth.fetchMe().then(user => {
      if (user) {
        State.user = user;
        updateNavUserState();
      }
    });
  }
}

// Start when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}

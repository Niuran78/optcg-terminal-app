/* ═══════════════════════════════════════════════════════════════
   OPTCG MARKET TERMINAL — Application v2
   ═══════════════════════════════════════════════════════════════ */

'use strict';

/* ── Global State ───────────────────────────────────────────────── */
const State = {
  token: null,
  user: null,   // { email, tier }

  activeTab: 'browser',

  browser: {
    cards:   [],
    total:   0,
    offset:  0,
    loading: false,
    filters: { set: 'all', rarity: 'all', search: '' },
    sort:    { col: 'eu_cardmarket_7d_avg', order: 'desc' },
  },

  sealed: {
    products: [],
    total:    0,
    loading:  false,
    filters:  { set: 'all', type: 'all' },
    sort:     'eu_price',
  },

  arbitrage: {
    opportunities: [],
    total:   0,
    offset:  0,
    loading: false,
    filters: { signal: 'all', minSpread: 5, set: 'all' },
  },

  overview: {
    data: null,
    loading: false,
  },

  // FX
  usdToEur: 0.92,
  displayCurrency: 'EUR', // 'EUR' | 'USD' for arbitrage calculated fields

  // Known sets (populated from first browse call)
  sets: [],
};

/* ── DOM References ──────────────────────────────────────────────── */
const $ = id => document.getElementById(id);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

/* ══════════════════════════════════════════════════════════════════
   BOOTSTRAP
   ══════════════════════════════════════════════════════════════════ */
document.addEventListener('DOMContentLoaded', async () => {
  // Restore session
  if (typeof Auth !== 'undefined') {
    try {
      const user = await Auth.restoreSession();
      State.user = user;
      State.token = Auth.getToken();
    } catch (_) {
      State.user = null;
    }
  }

  // Render nav user info
  renderNavUser();

  // Bind nav
  bindNav();

  // Load market summary bar
  loadMarketSummary();

  // Load default tab
  switchTab('browser');
});

/* ══════════════════════════════════════════════════════════════════
   MARKET SUMMARY BAR
   ══════════════════════════════════════════════════════════════════ */
async function loadMarketSummary() {
  const bar = $('market-summary-bar');
  if (!bar) return;

  try {
    const data = await apiFetch('/api/cards/market-summary');
    const updated = data.last_updated
      ? new Date(data.last_updated).toLocaleString()
      : 'N/A';
    bar.innerHTML =
      `<span>${fmt.int(data.total_cards)} Cards Tracked</span>` +
      `<span class="market-summary-sep"></span>` +
      `<span>${fmt.int(data.total_sets)} Sets</span>` +
      `<span class="market-summary-sep"></span>` +
      `<span>${fmt.int(data.cards_with_eu_prices)} EU Priced</span>` +
      `<span class="market-summary-sep"></span>` +
      `<span>Last Updated: ${escHtml(updated)}</span>`;
  } catch (_) {
    bar.innerHTML = '<span>Market data unavailable</span>';
  }
}

/* ══════════════════════════════════════════════════════════════════
   NAV
   ══════════════════════════════════════════════════════════════════ */
function bindNav() {
  // Tab buttons
  $$('[data-tab]').forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      switchTab(tab);
    });
  });

  // FX toggle
  $$('[data-fx]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.displayCurrency = btn.dataset.fx;
      $$('[data-fx]').forEach(b => b.classList.toggle('active', b.dataset.fx === State.displayCurrency));
      // Re-render current tab to reflect currency change in arbitrage
      if (State.activeTab === 'arbitrage') renderArbitrageTable();
    });
  });

  // User menu toggle
  const userMenu = $('user-menu');
  if (userMenu) {
    $('user-btn')?.addEventListener('click', (e) => {
      e.stopPropagation();
      userMenu.classList.toggle('open');
    });
    document.addEventListener('click', () => userMenu.classList.remove('open'));
  }

  // Logout
  $('logout-btn')?.addEventListener('click', () => {
    if (typeof Auth !== 'undefined') Auth.logout();
    else { localStorage.clear(); window.location.href = '/login.html'; }
  });
}

function switchTab(tab) {
  State.activeTab = tab;

  // Update nav tabs
  $$('[data-tab]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });

  // Update panels
  $$('.tab-panel').forEach(panel => {
    panel.classList.toggle('active', panel.id === `panel-${tab}`);
  });

  // Load data
  if (tab === 'browser')   loadBrowserData();
  if (tab === 'sealed')    loadSealedData();
  if (tab === 'arbitrage') loadArbitrageData();
  if (tab === 'overview')  loadOverviewData();
}

function renderNavUser() {
  const emailEl = $('nav-user-email');
  const tierEl  = $('nav-user-tier');

  if (!State.user) {
    if (emailEl) emailEl.textContent = 'Guest';
    if (tierEl)  tierEl.textContent = 'Free';
    return;
  }

  if (emailEl) emailEl.textContent = State.user.email || 'User';
  const tier = Auth?.getTier(State.user) || 'free';
  if (tierEl) tierEl.textContent = tier.toUpperCase();
}

/* ══════════════════════════════════════════════════════════════════
   API FETCH HELPER
   ══════════════════════════════════════════════════════════════════ */
async function apiFetch(url) {
  const headers = { 'Content-Type': 'application/json' };
  const token = State.token || (typeof Auth !== 'undefined' ? Auth.getToken() : null);
  if (token) headers['Authorization'] = `Bearer ${token}`;

  try {
    const res = await fetch(url, { headers });

    if (res.status === 401) {
      // Session expired – don't hard-redirect, just clear & show as guest
      if (typeof Auth !== 'undefined') Auth.clearToken();
      State.user  = null;
      State.token = null;
      renderNavUser();
      throw new Error('Session expired. Please sign in again.');
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || err.message || `HTTP ${res.status}`);
    }

    return await res.json();
  } catch (err) {
    if (err.name === 'TypeError') throw new Error('Network error — is the server running?');
    throw err;
  }
}

/* ══════════════════════════════════════════════════════════════════
   FORMATTING HELPERS
   ══════════════════════════════════════════════════════════════════ */
const fmt = {
  usd: (v) => v == null ? '—' : `$${Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
  eur: (v) => v == null ? '—' : `€${Number(v).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
  pct: (v) => v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(1)}%`,
  int: (v) => v == null ? '—' : Number(v).toLocaleString(),
};

function spreadClass(pct) {
  if (pct == null) return 'spread-neutral';
  if (pct > 0) return 'spread-positive';
  if (pct < 0) return 'spread-negative';
  return 'spread-neutral';
}

function signalBadge(signal) {
  if (!signal) return `<span class="signal signal-none">—</span>`;
  const map = {
    'BUY_EU': ['signal-buy-eu', 'BUY EU'],
    'BUY_EN': ['signal-buy-en', 'BUY EN'],
    'WATCH':  ['signal-watch',  'WATCH'],
  };
  const [cls, label] = map[signal] || ['signal-none', signal];
  return `<span class="signal ${cls}">${label}</span>`;
}

function rarityBadge(rarity) {
  if (!rarity) return '';
  const r = rarity.toLowerCase();
  const cls = r === 'sec' ? 'sec' : r === 'sr' ? 'sr' : r === 'leader' ? 'leader' : r === 'rare' ? 'rare' : '';
  return `<span class="rarity-badge ${cls}">${rarity}</span>`;
}

function trendIcon(trend) {
  if (!trend) return '';
  if (trend === 'up')   return `<span class="product-trend up">↑ Up</span>`;
  if (trend === 'down') return `<span class="product-trend down">↓ Down</span>`;
  return `<span class="product-trend flat">→ Stable</span>`;
}

function computeSpread(enUsd, euEur) {
  if (enUsd == null || euEur == null || euEur === 0) return null;
  const enInEur = enUsd * State.usdToEur;
  return ((enInEur - euEur) / euEur) * 100;
}

function cardThumb(url, name) {
  if (url) {
    return `<img class="card-thumb" src="${escHtml(url)}" alt="${escHtml(name || '')}" loading="lazy" onerror="this.outerHTML='<div class=\\'card-thumb-placeholder\\'>🃏</div>'" />`;
  }
  return `<div class="card-thumb-placeholder">🃏</div>`;
}

function escHtml(str) {
  return String(str ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

/* ══════════════════════════════════════════════════════════════════
   TOAST
   ══════════════════════════════════════════════════════════════════ */
function showToast(msg, type = 'info', duration = 3500) {
  const container = $('toast-container');
  if (!container) return;

  const icons = { success: '✓', error: '✗', warning: '⚠', info: 'ℹ' };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span class="toast-icon">${icons[type] || 'ℹ'}</span><span>${escHtml(msg)}</span>`;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(8px)';
    toast.style.transition = '200ms ease';
    setTimeout(() => toast.remove(), 250);
  }, duration);
}

/* ══════════════════════════════════════════════════════════════════
   LOADING BAR
   ══════════════════════════════════════════════════════════════════ */
function showLoadingBar() {
  let bar = $('loading-bar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'loading-bar';
    bar.className = 'loading-bar';
    document.body.appendChild(bar);
  }
  bar.style.display = 'block';
}

function hideLoadingBar() {
  const bar = $('loading-bar');
  if (bar) bar.style.display = 'none';
}

/* ══════════════════════════════════════════════════════════════════
   SKELETON ROWS
   ══════════════════════════════════════════════════════════════════ */
function skeletonRows(count, cols) {
  return Array.from({ length: count }, () => `
    <tr class="skeleton-row">
      ${Array.from({ length: cols }, () => `
        <td><div class="skeleton skeleton-cell" style="width:${60 + Math.random()*30}%"></div></td>
      `).join('')}
    </tr>
  `).join('');
}

function skeletonProductCards(count) {
  return Array.from({ length: count }, () => `
    <div class="skeleton-product-card">
      <div class="skeleton skeleton-card-img"></div>
      <div style="padding:16px; display:flex; flex-direction:column; gap:10px;">
        <div class="skeleton" style="height:13px; width:80%;"></div>
        <div class="skeleton" style="height:11px; width:50%;"></div>
        <div class="skeleton" style="height:20px; width:60%;"></div>
        <div class="skeleton" style="height:10px; width:90%;"></div>
      </div>
    </div>
  `).join('');
}

/* ══════════════════════════════════════════════════════════════════
   TAB 1: CARD BROWSER
   ══════════════════════════════════════════════════════════════════ */
function initBrowserFilters() {
  // Set filter
  const setSelect = $('browser-set');
  setSelect?.addEventListener('change', () => {
    State.browser.filters.set = setSelect.value;
    State.browser.offset = 0;
    loadBrowserData();
  });

  // Rarity pills
  $$('[data-rarity]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.browser.filters.rarity = btn.dataset.rarity;
      $$('[data-rarity]').forEach(b => b.classList.toggle('active', b.dataset.rarity === btn.dataset.rarity));
      State.browser.offset = 0;
      loadBrowserData();
    });
  });

  // Search
  let searchTimer;
  $('browser-search')?.addEventListener('input', (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      State.browser.filters.search = e.target.value.trim();
      State.browser.offset = 0;
      loadBrowserData();
    }, 400);
  });

  // Sort headers
  $$('[data-sort]', $('panel-browser')).forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.sort;
      if (State.browser.sort.col === col) {
        State.browser.sort.order = State.browser.sort.order === 'desc' ? 'asc' : 'desc';
      } else {
        State.browser.sort.col = col;
        State.browser.sort.order = 'desc';
      }
      State.browser.offset = 0;
      loadBrowserData();
    });
  });
}

async function loadBrowserData() {
  if (State.browser.loading) return;
  State.browser.loading = true;
  showLoadingBar();

  const tbody = $('browser-tbody');
  if (tbody) tbody.innerHTML = skeletonRows(10, 8);

  const { filters, sort, offset } = State.browser;

  const params = new URLSearchParams({
    limit:  50,
    offset: offset,
    sort:   sort.col,
    order:  sort.order,
  });
  if (filters.set    && filters.set    !== 'all') params.set('set_code', filters.set);
  if (filters.rarity && filters.rarity !== 'all') params.set('rarity', filters.rarity);
  if (filters.search) params.set('search', filters.search);

  try {
    const data = await apiFetch(`/api/cards/browse?${params}`);
    State.browser.cards = data.cards || [];
    State.browser.total = data.total || 0;

    // Collect sets for filter dropdowns
    if (data.cards?.length && !State.sets.length) {
      const seen = new Set();
      data.cards.forEach(c => { if (c.set_code) seen.add(JSON.stringify({ code: c.set_code, name: c.set_name })); });
      State.sets = Array.from(seen).map(s => JSON.parse(s)).sort((a,b) => a.code.localeCompare(b.code));
      populateSetSelects();
    }

    renderBrowserTable(data);
  } catch (err) {
    showToast(err.message, 'error');
    if (tbody) tbody.innerHTML = `<tr><td colspan="8" style="text-align:center; padding:40px;">
      <div class="empty-state">
        <div class="empty-icon">⚠️</div>
        <div class="empty-title">Failed to load</div>
        <div class="empty-desc">${escHtml(err.message)}</div>
      </div>
    </td></tr>`;
  } finally {
    State.browser.loading = false;
    hideLoadingBar();
  }
}

function renderBrowserTable(data) {
  const cards   = data.cards || [];
  const total   = data.total || 0;
  const tier    = data.tier || (State.user ? (Auth?.getTier(State.user) || 'free') : 'free');
  const isElite = tier === 'elite' || tier === 'pro';
  const limit   = isElite ? cards.length : Math.min(cards.length, 10);

  // Update sort headers
  $$('[data-sort]', $('panel-browser')).forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.sort === State.browser.sort.col) {
      th.classList.add(State.browser.sort.order === 'asc' ? 'sort-asc' : 'sort-desc');
    }
    const icon = th.querySelector('.th-sort-icon');
    if (icon) {
      if (th.dataset.sort === State.browser.sort.col) {
        icon.textContent = State.browser.sort.order === 'asc' ? '↑' : '↓';
      } else {
        icon.textContent = '↕';
      }
    }
  });

  // Summary bar
  const summaryEl = $('browser-summary');
  if (summaryEl) {
    summaryEl.innerHTML = `
      <span><strong>${fmt.int(total)}</strong> cards</span>
      <span>Page <strong>${Math.floor(State.browser.offset / 50) + 1}</strong></span>
      <span style="margin-left:auto;">Sorted by <strong>${State.browser.sort.col.replace(/_/g,' ')}</strong> ${State.browser.sort.order}</span>
    `;
  }

  // Table body
  const tbody = $('browser-tbody');
  if (!tbody) return;

  if (!cards.length) {
    tbody.innerHTML = `<tr><td colspan="8">
      <div class="empty-state">
        <div class="empty-icon">🔍</div>
        <div class="empty-title">No cards found</div>
        <div class="empty-desc">Try adjusting your filters or search query.</div>
      </div>
    </td></tr>`;
    renderBrowserPagination(total);
    return;
  }

  let rows = '';
  cards.forEach((card, i) => {
    const spread = computeSpread(card.en_tcgplayer_market, card.eu_cardmarket_7d_avg);
    const signal = card.arbitrage?.signal || null;
    const variant = card.variant && card.variant !== 'Normal' ? ` <span style="font-size:10px;color:var(--muted);">(${escHtml(card.variant)})</span>` : '';

    rows += `
      <tr data-idx="${i}" class="${i >= limit ? 'blurred-rows' : ''}">
        <td>
          <div class="card-cell">
            ${cardThumb(card.image_url, card.name)}
            <div class="card-info">
              <div class="card-name">${escHtml(card.name)}${variant}</div>
              <div class="card-id">${escHtml(card.card_id || '')}</div>
            </div>
          </div>
        </td>
        <td>
          <span style="font-family:var(--font-mono);font-size:11px;color:var(--accent);">${escHtml(card.set_code || '')}</span>
          ${card.set_name ? `<div style="font-size:10px;color:var(--muted);margin-top:2px;">${escHtml(card.set_name)}</div>` : ''}
        </td>
        <td>${rarityBadge(card.rarity)}</td>
        <td class="col-en">
          <div class="price-cell">
            <div class="price-val">${fmt.usd(card.en_tcgplayer_market)}</div>
            ${card.en_tcgplayer_low != null ? `<div class="price-sub">Low ${fmt.usd(card.en_tcgplayer_low)}</div>` : ''}
          </div>
        </td>
        <td class="col-eu">
          <div class="price-cell">
            <div class="price-val">${fmt.eur(card.eu_cardmarket_7d_avg)}</div>
            ${card.eu_cardmarket_30d_avg != null ? `<div class="price-sub">30d ${fmt.eur(card.eu_cardmarket_30d_avg)}</div>` : ''}
          </div>
        </td>
        <td>
          <span class="${spreadClass(spread)}">${spread != null ? fmt.pct(spread) : '<span class="spread-neutral">—</span>'}</span>
        </td>
        <td>${signalBadge(signal)}</td>
      </tr>
    `;
  });

  tbody.innerHTML = rows;

  // Free tier overlay
  const tableWrap = $('browser-table-wrap');
  const existingOverlay = tableWrap?.querySelector('.upgrade-cta');
  if (existingOverlay) existingOverlay.remove();

  if (!isElite && cards.length > 10) {
    const cta = document.createElement('div');
    cta.className = 'upgrade-cta';
    cta.innerHTML = `
      <div class="upgrade-lock-icon">🔒</div>
      <h3>${fmt.int(total - 10)} more cards locked</h3>
      <p>Upgrade to Elite to unlock all ${fmt.int(total)} cards, full arbitrage scanner, and live price feeds.</p>
      <a href="/login.html" class="btn btn-primary">Upgrade to Elite</a>
    `;
    tableWrap?.appendChild(cta);
  }

  renderBrowserPagination(total);
}

function renderBrowserPagination(total) {
  const paginationEl = $('browser-pagination');
  if (!paginationEl) return;

  const limit   = 50;
  const offset  = State.browser.offset;
  const pages   = Math.ceil(total / limit);
  const current = Math.floor(offset / limit) + 1;

  const start = offset + 1;
  const end   = Math.min(offset + limit, total);

  paginationEl.innerHTML = `
    <div class="pagination-bar">
      <div class="pagination-info">
        Showing <span>${fmt.int(start)}–${fmt.int(end)}</span> of <span>${fmt.int(total)}</span> cards
      </div>
      <div class="pagination-controls">
        <button class="page-btn" ${current <= 1 ? 'disabled' : ''} onclick="goToPage(${current - 2})">←</button>
        ${Array.from({ length: Math.min(pages, 7) }, (_, i) => {
          // Show pages around current
          let p;
          if (pages <= 7) p = i + 1;
          else if (current <= 4) p = i + 1;
          else if (current >= pages - 3) p = pages - 6 + i;
          else p = current - 3 + i;
          if (p < 1 || p > pages) return '';
          return `<button class="page-btn ${p === current ? 'active' : ''}" onclick="goToPage(${p - 1})">${p}</button>`;
        }).join('')}
        <button class="page-btn" ${current >= pages ? 'disabled' : ''} onclick="goToPage(${current})">→</button>
      </div>
    </div>
  `;
}

window.goToPage = function(pageIndex) {
  State.browser.offset = pageIndex * 50;
  loadBrowserData();
  $('panel-browser')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
};

function populateSetSelects() {
  const selects = $$('[data-set-select]');
  const options = `<option value="all">All Sets</option>` +
    State.sets.map(s => `<option value="${escHtml(s.code)}">${escHtml(s.code)} — ${escHtml(s.name || s.code)}</option>`).join('');
  selects.forEach(sel => { sel.innerHTML = options; });
}

/* ══════════════════════════════════════════════════════════════════
   TAB 2: SEALED TRACKER
   ══════════════════════════════════════════════════════════════════ */
function initSealedFilters() {
  $('sealed-set')?.addEventListener('change', () => {
    State.sealed.filters.set = $('sealed-set').value;
    loadSealedData();
  });

  $$('[data-type]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.sealed.filters.type = btn.dataset.type;
      $$('[data-type]').forEach(b => b.classList.toggle('active', b.dataset.type === btn.dataset.type));
      loadSealedData();
    });
  });

  $$('[data-sort-sealed]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.sealed.sort = btn.dataset.sortSealed;
      $$('[data-sort-sealed]').forEach(b => b.classList.toggle('active', b.dataset.sortSealed === btn.dataset.sortSealed));
      loadSealedData();
    });
  });
}

async function loadSealedData() {
  if (State.sealed.loading) return;
  State.sealed.loading = true;
  showLoadingBar();

  const grid = $('sealed-grid');
  if (grid) grid.innerHTML = skeletonProductCards(8);

  const { filters, sort } = State.sealed;
  const params = new URLSearchParams({ sort });
  if (filters.set  !== 'all') params.set('set_code', filters.set);
  if (filters.type !== 'all') params.set('product_type', filters.type);

  try {
    const data = await apiFetch(`/api/cards/sealed?${params}`);
    State.sealed.products = data.products || [];
    State.sealed.total    = data.total || 0;
    renderSealedGrid(data);
  } catch (err) {
    showToast(err.message, 'error');
    if (grid) grid.innerHTML = `<div style="grid-column:1/-1;"><div class="empty-state">
      <div class="empty-icon">⚠️</div>
      <div class="empty-title">Failed to load</div>
      <div class="empty-desc">${escHtml(err.message)}</div>
    </div></div>`;
  } finally {
    State.sealed.loading = false;
    hideLoadingBar();
  }
}

function renderSealedGrid(data) {
  const products = data.products || [];
  const grid = $('sealed-grid');
  if (!grid) return;

  const countEl = $('sealed-count');
  if (countEl) countEl.textContent = `${fmt.int(data.total || products.length)} products`;

  if (!products.length) {
    grid.innerHTML = `<div style="grid-column:1/-1;"><div class="empty-state">
      <div class="empty-icon">📦</div>
      <div class="empty-title">No sealed products found</div>
      <div class="empty-desc">Try changing the set or product type filter.</div>
    </div></div>`;
    return;
  }

  const typeEmoji = { case: '📦', 'booster box': '🎴', booster: '🃏', box: '🎴', tin: '🗃️' };
  function getTypeEmoji(type) { return typeEmoji[type?.toLowerCase()] || '📦'; }

  grid.innerHTML = products.map(p => `
    <div class="product-card">
      <div class="product-img-wrap">
        ${p.image_url
          ? `<img src="${escHtml(p.image_url)}" alt="${escHtml(p.product_name || '')}" loading="lazy" onerror="this.style.display='none'" />`
          : `<div class="product-img-placeholder">${getTypeEmoji(p.product_type)}</div>`
        }
        <div class="product-type-tag">${escHtml(p.product_type || 'product')}</div>
      </div>
      <div class="product-body">
        <div>
          <div class="product-name">${escHtml(p.product_name || 'Unknown Product')}</div>
          <div class="product-set">${getTypeEmoji(p.product_type)} ${escHtml(p.set_name || '')} (${escHtml(p.set_code || '')})</div>
        </div>
        <div class="product-price-section">
          <div class="product-price-label">
            <span>🇪🇺</span>
            <span>EU PRICE</span>
          </div>
          <div class="product-price-main">${fmt.eur(p.eu_price)}</div>
          <div class="product-price-stats">
            ${p.eu_30d_avg != null ? `<span>30d ${fmt.eur(p.eu_30d_avg)}</span>` : ''}
            ${p.eu_7d_avg  != null ? `<span>7d ${fmt.eur(p.eu_7d_avg)}</span>`  : ''}
            ${p.eu_trend   ? trendIcon(p.eu_trend) : ''}
          </div>
          ${p.eu_source ? `<div style="font-family:var(--font-mono);font-size:9px;color:var(--muted);margin-top:4px;">${escHtml(p.eu_source)}</div>` : ''}
        </div>
        <a class="product-link" href="#" onclick="return false;">
          <span>↗</span> View listing
        </a>
      </div>
    </div>
  `).join('');
}

/* ══════════════════════════════════════════════════════════════════
   TAB 3: ARBITRAGE SCANNER
   ══════════════════════════════════════════════════════════════════ */
function initArbitrageFilters() {
  $$('[data-signal]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.arbitrage.filters.signal = btn.dataset.signal;
      $$('[data-signal]').forEach(b => b.classList.toggle('active', b.dataset.signal === btn.dataset.signal));
      State.arbitrage.offset = 0;
      loadArbitrageData();
    });
  });

  $('arb-min-spread')?.addEventListener('change', () => {
    State.arbitrage.filters.minSpread = Number($('arb-min-spread').value) || 5;
    State.arbitrage.offset = 0;
    loadArbitrageData();
  });

  $('arb-set')?.addEventListener('change', () => {
    State.arbitrage.filters.set = $('arb-set').value;
    State.arbitrage.offset = 0;
    loadArbitrageData();
  });
}

async function loadArbitrageData() {
  if (State.arbitrage.loading) return;
  State.arbitrage.loading = true;
  showLoadingBar();

  const tbody = $('arb-tbody');
  if (tbody) tbody.innerHTML = skeletonRows(8, 8);

  const { filters, offset } = State.arbitrage;
  const params = new URLSearchParams({
    min_profit_pct: filters.minSpread,
    limit:  50,
    offset: offset,
  });
  if (filters.signal !== 'all') params.set('signal', filters.signal);
  if (filters.set    !== 'all') params.set('set_code', filters.set);

  try {
    const data = await apiFetch(`/api/cards/arbitrage?${params}`);
    State.arbitrage.opportunities = data.opportunities || [];
    State.arbitrage.total = data.total || 0;
    renderArbitrageView(data);
  } catch (err) {
    showToast(err.message, 'error');
    if (tbody) tbody.innerHTML = `<tr><td colspan="8">
      <div class="empty-state">
        <div class="empty-icon">⚠️</div>
        <div class="empty-title">Failed to load</div>
        <div class="empty-desc">${escHtml(err.message)}</div>
      </div>
    </td></tr>`;
  } finally {
    State.arbitrage.loading = false;
    hideLoadingBar();
  }
}

function renderArbitrageView(data) {
  const opps = data.opportunities || [];

  // Stat cards
  const buyEu = opps.filter(o => o.signal === 'BUY_EU').length;
  const buyEn = opps.filter(o => o.signal === 'BUY_EN').length;
  const spreads = opps.map(o => o.profit_pct).filter(v => v != null);
  const avgSpread = spreads.length ? spreads.reduce((a,b) => a+b, 0) / spreads.length : 0;
  const bestSpread = spreads.length ? Math.max(...spreads) : 0;

  const statsEl = $('arb-stats');
  if (statsEl) {
    statsEl.innerHTML = `
      <div class="stat-card">
        <div class="stat-label">Total Opportunities</div>
        <div class="stat-value accent">${fmt.int(data.total || opps.length)}</div>
        <div class="stat-sub">above ${State.arbitrage.filters.minSpread}% spread</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Best Spread</div>
        <div class="stat-value positive">${fmt.pct(bestSpread)}</div>
        <div class="stat-sub">highest single card</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Avg Spread</div>
        <div class="stat-value">${fmt.pct(avgSpread)}</div>
        <div class="stat-sub">across all signals</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Buy EU Signals</div>
        <div class="stat-value positive">${buyEu}</div>
        <div class="stat-sub">EN > EU price</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Buy EN Signals</div>
        <div class="stat-value" style="color:var(--info)">${buyEn}</div>
        <div class="stat-sub">EU > EN price</div>
      </div>
    `;
  }

  renderArbitrageTable(opps);
}

function renderArbitrageTable(opps) {
  opps = opps || State.arbitrage.opportunities;
  const tbody = $('arb-tbody');
  if (!tbody) return;

  if (!opps.length) {
    tbody.innerHTML = `<tr><td colspan="8">
      <div class="empty-state">
        <div class="empty-icon">📊</div>
        <div class="empty-title">No arbitrage opportunities</div>
        <div class="empty-desc">Lower the minimum spread or check back soon.</div>
      </div>
    </td></tr>`;
    return;
  }

  tbody.innerHTML = opps.map(o => {
    const profitDisplay = State.displayCurrency === 'USD'
      ? fmt.usd(o.profit_eur != null ? o.profit_eur / State.usdToEur : null)
      : fmt.eur(o.profit_eur);

    const variant = o.variant && o.variant !== 'Normal' ? ` (${o.variant})` : '';

    return `
      <tr>
        <td>
          <div class="card-cell">
            ${cardThumb(o.image_url, o.name)}
            <div class="card-info">
              <div class="card-name">${escHtml(o.name || '')}${escHtml(variant)}</div>
              <div class="card-id">${escHtml(o.card_id || '')}</div>
            </div>
          </div>
        </td>
        <td>
          <span style="font-family:var(--font-mono);font-size:11px;color:var(--accent);">${escHtml(o.set_code || '')}</span>
          ${o.rarity ? `<div style="margin-top:4px;">${rarityBadge(o.rarity)}</div>` : ''}
        </td>
        <td class="col-en">
          <div class="price-cell">
            <div class="price-val">${fmt.usd(o.en_price_usd)}</div>
            <div class="price-sub">${escHtml(o.sell_market || 'TCGPlayer')}</div>
          </div>
        </td>
        <td class="col-eu">
          <div class="price-cell">
            <div class="price-val">${fmt.eur(o.eu_price_eur)}</div>
            <div class="price-sub">${escHtml(o.buy_market || 'Cardmarket')}</div>
          </div>
        </td>
        <td>
          <span class="${o.profit_pct >= 0 ? 'spread-positive' : 'spread-negative'}">${fmt.pct(o.profit_pct)}</span>
        </td>
        <td>
          <span class="${o.profit_eur >= 0 ? 'spread-positive' : 'spread-negative'}" style="font-family:var(--font-mono);font-size:13px;font-weight:600;">
            ${profitDisplay}
          </span>
        </td>
        <td>${signalBadge(o.signal)}</td>
        <td>
          <button class="action-btn" onclick="showToast('Card detail view coming soon', 'info')">
            ↗ Detail
          </button>
        </td>
      </tr>
    `;
  }).join('');
}

/* ══════════════════════════════════════════════════════════════════
   TAB 4: MARKET OVERVIEW
   ══════════════════════════════════════════════════════════════════ */
async function loadOverviewData() {
  if (State.overview.loading) return;
  State.overview.loading = true;
  showLoadingBar();

  try {
    const data = await apiFetch('/api/market/overview');
    State.overview.data = data;
    renderOverview(data);
  } catch (err) {
    showToast(err.message, 'error');
    const el = $('overview-content');
    if (el) el.innerHTML = `<div class="empty-state">
      <div class="empty-icon">⚠️</div>
      <div class="empty-title">Failed to load overview</div>
      <div class="empty-desc">${escHtml(err.message)}</div>
    </div>`;
  } finally {
    State.overview.loading = false;
    hideLoadingBar();
  }
}

function renderOverview(data) {
  const heroEl = $('overview-hero');
  if (heroEl) {
    heroEl.innerHTML = `
      <div class="overview-hero-card accent-border">
        <div class="stat-label">Sets Tracked</div>
        <div class="stat-value accent">${fmt.int(data.sets_tracked || data.total_sets || '—')}</div>
        <div class="stat-sub">active in market</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Cards Indexed</div>
        <div class="stat-value">${fmt.int(data.cards_indexed || data.total_cards || '—')}</div>
        <div class="stat-sub">with price data</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Arb Opportunities</div>
        <div class="stat-value positive">${fmt.int(data.arb_opportunities || '—')}</div>
        <div class="stat-sub">above 5% spread</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Last Updated</div>
        <div class="stat-value" style="font-size:14px;">${data.last_updated ? new Date(data.last_updated).toLocaleTimeString() : '—'}</div>
        <div class="stat-sub">${data.last_updated ? new Date(data.last_updated).toLocaleDateString() : 'Unknown'}</div>
      </div>
    `;
  }

  // Top movers
  const moversEl = $('overview-movers');
  if (moversEl) {
    const movers = data.top_movers || data.top_arbitrage || [];
    if (movers.length) {
      moversEl.innerHTML = movers.slice(0,5).map((m, i) => `
        <div class="overview-mover">
          <div class="overview-mover-rank">#${i+1}</div>
          <div class="overview-mover-info">
            <div class="overview-mover-name">${escHtml(m.name || m.card_name || 'Unknown')}</div>
            <div class="overview-mover-set">${escHtml(m.set_code || '')} · ${escHtml(m.rarity || '')}</div>
          </div>
          <div class="overview-mover-spread">${fmt.pct(m.profit_pct || m.spread_pct || 0)}</div>
        </div>
      `).join('');
    } else {
      moversEl.innerHTML = `<div class="empty-state" style="padding:24px 0;">
        <div style="color:var(--muted);font-size:13px;">No movers data available</div>
      </div>`;
    }
  }

  // Sets list
  const setsEl = $('overview-sets');
  if (setsEl) {
    const sets = data.sets || data.recent_sets || [];
    if (sets.length) {
      setsEl.innerHTML = sets.slice(0,10).map(s => `
        <div class="set-item">
          <div class="set-item-code">${escHtml(s.set_code || s.code || '')}</div>
          <div class="set-item-name">${escHtml(s.set_name || s.name || '')}</div>
          <div class="set-item-count">${s.card_count ? `${s.card_count} cards` : ''}</div>
        </div>
      `).join('');
    } else {
      setsEl.innerHTML = `<div style="color:var(--muted);font-size:13px;padding:16px 0;">No sets data</div>`;
    }
  }
}

/* ══════════════════════════════════════════════════════════════════
   INIT ALL FILTERS once DOM is ready
   ══════════════════════════════════════════════════════════════════ */
document.addEventListener('DOMContentLoaded', () => {
  initBrowserFilters();
  initSealedFilters();
  initArbitrageFilters();
});

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

  portfolio: {
    id: null,          // active portfolio id
    name: '',
    items: [],
    summary: null,
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
// ============================================================
// Dynamic currency headers — update column labels when toggle changes
// ============================================================
function updateCurrencyHeaders() {
  const sym = State.displayCurrency === 'USD' ? '$' : '€';
  // Browser tab & Arbitrage tab: EN / EU price columns
  document.querySelectorAll('th.col-en .th-main').forEach(el => {
    el.innerHTML = `EN Price (${sym})${el.querySelector('.th-sort-icon') ? ' <span class="th-sort-icon" aria-hidden="true">↕</span>' : ''}`;
  });
  document.querySelectorAll('th.col-eu .th-main').forEach(el => {
    el.innerHTML = `EU Price (${sym})${el.querySelector('.th-sort-icon') ? ' <span class="th-sort-icon" aria-hidden="true">↕</span>' : ''}`;
  });
}

function bindNav() {
  // Tab buttons
  $$('[data-tab]').forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      switchTab(tab);
    });
  });

  // FX toggle — re-render current tab for currency conversion
  $$('[data-fx]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.displayCurrency = btn.dataset.fx;
      $$('[data-fx]').forEach(b => b.classList.toggle('active', b.dataset.fx === State.displayCurrency));
      updateCurrencyHeaders();
      // Re-render whichever tab is active
      if (State.activeTab === 'browser' && State.browser.lastData) renderBrowserTable(State.browser.lastData);
      if (State.activeTab === 'sealed' && State.sealed.lastData) renderSealedGrid(State.sealed.lastData);
      if (State.activeTab === 'arbitrage') renderArbitrageTable();
      if (State.activeTab === 'portfolio') { renderPortfolioSummary(State.portfolio.summary); renderPortfolioItems(State.portfolio.items); }
    });
  });

  // Apply initial header state on load
  updateCurrencyHeaders();

  // User menu: only attach the outside-click closer.
  // The toggle itself is bound via onclick in renderNavUser() to avoid
  // double-firing (addEventListener + onclick would cancel each other out).
  const userMenu = $('user-menu');
  if (userMenu) {
    document.addEventListener('click', (e) => {
      if (userMenu.contains(e.target)) return;
      userMenu.classList.remove('open');
    });
  }

  // Logout — stopPropagation so the outside-click handler doesn't
  // interfere, and force-clear localStorage before redirect.
  $('logout-btn')?.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    try {
      localStorage.removeItem('optcg_token');
      // Also clear any legacy keys just in case
      localStorage.removeItem('token');
      localStorage.removeItem('user');
    } catch (err) { /* private mode */ }
    if (typeof Auth !== 'undefined') Auth.logout();
    else window.location.href = '/login.html';
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
  if (tab === 'portfolio') loadPortfolioData();
}

function renderNavUser() {
  const emailEl = $('nav-user-email');
  const tierEl  = $('nav-user-tier');
  const userBtn = $('user-btn');

  if (!State.user) {
    // Not logged in — turn the user button into a Sign In link
    if (emailEl) emailEl.textContent = 'Sign In';
    if (tierEl)  tierEl.textContent = '';
    if (userBtn) {
      userBtn.onclick = (e) => {
        e.preventDefault();
        e.stopPropagation();
        window.location.href = '/login.html';
      };
    }
    return;
  }

  // Logged in — restore dropdown behavior
  if (emailEl) emailEl.textContent = State.user.email || 'User';
  const tier = Auth?.getTier(State.user) || 'free';
  if (tierEl) tierEl.textContent = tier.toUpperCase();
  if (userBtn) {
    userBtn.onclick = (e) => {
      e.stopPropagation();
      const menu = $('user-menu');
      if (menu) {
        const isOpen = menu.classList.toggle('open');
        userBtn.setAttribute('aria-expanded', isOpen);
      }
    };
  }
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
  // Currency-aware: converts EUR↔USD based on State.displayCurrency
  eurAuto: (v) => {
    if (v == null) return '—';
    if (State.displayCurrency === 'USD') return fmt.usd(v / State.usdToEur);
    return fmt.eur(v);
  },
  usdAuto: (v) => {
    if (v == null) return '—';
    if (State.displayCurrency === 'EUR') return fmt.eur(v * State.usdToEur);
    return fmt.usd(v);
  },
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

    State.browser.lastData = data;
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
      <tr data-idx="${i}" class="clickable-row ${i >= limit ? 'blurred-rows' : ''}" onclick="openPriceHistory(${i})" title="Click for price history">
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
            ${card.links?.tcgplayer
              ? `<a class="price-val price-link" href="${card.links.tcgplayer}" target="_blank" rel="noopener nofollow" title="Buy on TCGPlayer">${fmt.usdAuto(card.en_tcgplayer_market)} ↗</a>`
              : `<div class="price-val">${fmt.usdAuto(card.en_tcgplayer_market)}</div>`}
            ${card.en_tcgplayer_low != null ? `<div class="price-sub">Low ${fmt.usdAuto(card.en_tcgplayer_low)}</div>` : ''}
          </div>
        </td>
        <td class="col-eu">
          <div class="price-cell">
            ${card.links?.cardmarket_en
              ? `<a class="price-val price-link" href="${card.links.cardmarket_en}" target="_blank" rel="noopener nofollow" title="Buy on Cardmarket EN">${fmt.eurAuto(card.eu_cardmarket_7d_avg)} ↗</a>`
              : `<div class="price-val">${fmt.eurAuto(card.eu_cardmarket_7d_avg)}</div>`}
            ${card.eu_cardmarket_30d_avg != null ? `<div class="price-sub">30d ${fmt.eurAuto(card.eu_cardmarket_30d_avg)}</div>` : ''}
            ${card.links?.cardmarket_jp ? `<a class="cm-jp-mini" href="${card.links.cardmarket_jp}" target="_blank" rel="noopener nofollow" title="Also check JP market">JP↗</a>` : ''}
          </div>
        </td>
        <td>
          <span class="${spreadClass(spread)}">${spread != null ? fmt.pct(spread) : '<span class="spread-neutral">—</span>'}</span>
        </td>
        <td>
          <div style="display:flex;align-items:center;gap:4px;">
            ${signalBadge(signal)}
            <button class="btn-alert-bell" title="Set price alert" onclick="event.stopPropagation();openAlertMini(${i})" aria-label="Set price alert">
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M7 1.5a3.5 3.5 0 00-3.5 3.5c0 2.1-.7 3.3-1.3 4a.5.5 0 00.4.75h8.8a.5.5 0 00.4-.75c-.6-.7-1.3-1.9-1.3-4A3.5 3.5 0 007 1.5z" stroke="currentColor" stroke-width="1.1"/><path d="M5.5 10.5a1.5 1.5 0 003 0" stroke="currentColor" stroke-width="1.1"/></svg>
            </button>
          </div>
        </td>
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
  const params = new URLSearchParams({ sort, limit: '200' });
  if (filters.set  !== 'all') params.set('set_code', filters.set);
  if (filters.type !== 'all') params.set('product_type', filters.type);

  try {
    const data = await apiFetch(`/api/cards/sealed?${params}`);
    State.sealed.products = data.products || [];
    State.sealed.total    = data.total || 0;
    State.sealed.lastData = data;
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

  grid.innerHTML = products.map(p => {
    const lang = (p.language || 'JP').toUpperCase();
    const langBadge = lang === 'EN'
      ? `<span style="background:rgba(59,130,246,0.15);color:#60a5fa;padding:2px 6px;border-radius:4px;font-size:9px;font-weight:700;letter-spacing:0.05em;">🇬🇧 EN</span>`
      : `<span style="background:rgba(201,168,76,0.15);color:#c9a84c;padding:2px 6px;border-radius:4px;font-size:9px;font-weight:700;letter-spacing:0.05em;">🇯🇵 JP</span>`;
    return `
    <div class="product-card">
      <div class="product-img-wrap">
        ${p.image_url
          ? `<img src="${escHtml(p.image_url)}" alt="${escHtml(p.product_name || '')}" loading="lazy" onerror="this.style.display='none'" />`
          : `<div class="product-img-placeholder">${getTypeEmoji(p.product_type)}</div>`
        }
        <div class="product-type-tag">${escHtml(p.product_type || 'product')}</div>
        <div style="position:absolute;top:8px;left:8px;">${langBadge}</div>
      </div>
      <div class="product-body">
        <div>
          <div class="product-name">${escHtml(p.product_name || 'Unknown Product')}</div>
          <div class="product-set">${getTypeEmoji(p.product_type)} ${escHtml(p.set_name || '')} (${escHtml(p.set_code || '')})</div>
        </div>
        <div class="product-price-section">
          <div class="product-price-label">
            <span>${lang === 'EN' ? '🇬🇧' : '🇯🇵'}</span>
            <span>${lang} MARKET</span>
          </div>
          <div class="product-price-main">${fmt.eurAuto(p.eu_price)}</div>
          <div class="product-price-stats">
            ${p.en_price_usd != null ? `<span>USD ${fmt.usd(p.en_price_usd)}</span>` : ''}
            ${p.eu_7d_avg != null && p.eu_7d_avg !== p.eu_price ? `<span>7d ${fmt.eurAuto(p.eu_7d_avg)}</span>` : ''}
            ${p.eu_trend ? trendIcon(p.eu_trend) : ''}
          </div>
          ${p.eu_source ? `<div style="font-family:var(--font-mono);font-size:9px;color:var(--muted);margin-top:4px;">${escHtml(p.eu_source)}</div>` : ''}
        </div>
        ${p.links?.cardmarket
          ? `<a class="product-link" href="${p.links.cardmarket}" target="_blank" rel="noopener nofollow" title="Buy on Cardmarket"><span>↗</span> Buy on Cardmarket</a>`
          : p.links?.pricecharting
            ? `<a class="product-link" href="${p.links.pricecharting}" target="_blank" rel="noopener nofollow" title="View on PriceCharting"><span>↗</span> View on PriceCharting</a>`
            : `<span class="product-link" style="opacity:0.4;cursor:default;">No listing available</span>`}
      </div>
    </div>`;
  }).join('');
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
            ${o.links?.tcgplayer
              ? `<a class="price-val price-link" href="${o.links.tcgplayer}" target="_blank" rel="noopener nofollow" title="Buy on TCGPlayer">${fmt.usdAuto(o.en_price_usd)} ↗</a>`
              : `<div class="price-val">${fmt.usdAuto(o.en_price_usd)}</div>`}
            <div class="price-sub">${escHtml(o.sell_market || 'TCGPlayer')}</div>
          </div>
        </td>
        <td class="col-eu">
          <div class="price-cell">
            ${o.links?.cardmarket
              ? `<a class="price-val price-link" href="${o.links.cardmarket}" target="_blank" rel="noopener nofollow" title="Buy on Cardmarket">${fmt.eurAuto(o.eu_price_eur)} ↗</a>`
              : `<div class="price-val">${fmt.eurAuto(o.eu_price_eur)}</div>`}
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
          <div style="display:flex;flex-direction:column;gap:2px;">
            ${o.links?.tcgplayer ? `<a class="action-btn action-btn-tcg" href="${o.links.tcgplayer}" target="_blank" rel="noopener nofollow" title="Buy on TCGPlayer (EN)">TCGPlayer ↗</a>` : ''}
            ${o.links?.cardmarket_en ? `<a class="action-btn action-btn-cm-en" href="${o.links.cardmarket_en}" target="_blank" rel="noopener nofollow" title="Buy on Cardmarket (EN)">CM EN ↗</a>` : ''}
            ${o.links?.cardmarket_jp ? `<a class="action-btn action-btn-cm-jp" href="${o.links.cardmarket_jp}" target="_blank" rel="noopener nofollow" title="Buy on Cardmarket (JP)">CM JP ↗</a>` : ''}
          </div>
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
    // Fetch both endpoints in parallel: market-summary (reliable) + overview (for sets/movers)
    const [summary, overview] = await Promise.all([
      apiFetch('/api/cards/market-summary').catch(() => null),
      apiFetch('/api/market/overview').catch(() => null),
    ]);

    // Merge into a single data object for renderOverview
    const data = {
      total_sets:    summary?.total_sets || 0,
      total_cards:   summary?.total_cards || 0,
      cards_with_eu: summary?.cards_with_eu_prices || 0,
      cards_with_en: summary?.cards_with_en_prices || 0,
      last_updated:  summary?.last_updated || null,
      top_movers:    overview?.top_movers || [],
      recent_sets:   overview?.recent_sets || [],
      arb_opportunities: overview?.stats?.arb_opportunities || 0,
    };

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
    const setsTracked = Number(data.total_sets) || 0;
    const cardsIndexed = Number(data.total_cards) || 0;
    const arbOpps = Number(data.arb_opportunities) || 0;

    heroEl.innerHTML = `
      <div class="overview-hero-card accent-border">
        <div class="stat-label">Sets Tracked</div>
        <div class="stat-value accent">${fmt.int(setsTracked)}</div>
        <div class="stat-sub">active in market</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Cards Indexed</div>
        <div class="stat-value">${fmt.int(cardsIndexed)}</div>
        <div class="stat-sub">with price data</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">EU Priced</div>
        <div class="stat-value positive">${fmt.int(Number(data.cards_with_eu) || 0)}</div>
        <div class="stat-sub">cards with EU prices</div>
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
        <div style="color:var(--muted);font-size:13px;">Coming soon — EN prices needed for arbitrage movers</div>
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
   PRICE HISTORY MODAL
   ══════════════════════════════════════════════════════════════════ */
let priceChart = null;
let priceModalCard = null;

function initPriceModal() {
  const overlay = $('price-modal-overlay');
  const closeBtn = $('price-modal-close');
  const daysSelect = $('price-modal-days');

  if (!overlay) return;

  closeBtn?.addEventListener('click', closePriceModal);
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closePriceModal();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && overlay.getAttribute('aria-hidden') === 'false') closePriceModal();
  });

  daysSelect?.addEventListener('change', () => {
    if (priceModalCard) loadPriceHistory(priceModalCard, Number(daysSelect.value));
  });
}

function closePriceModal() {
  const overlay = $('price-modal-overlay');
  if (overlay) {
    overlay.setAttribute('aria-hidden', 'true');
    overlay.classList.remove('open');
  }
  if (priceChart) { priceChart.destroy(); priceChart = null; }
  priceModalCard = null;
}

function showPriceHistoryModal(card) {
  priceModalCard = card;
  const overlay = $('price-modal-overlay');
  if (!overlay) return;

  // Set header info
  const titleEl = $('price-modal-title');
  const subEl = $('price-modal-subtitle');
  if (titleEl) titleEl.textContent = card.name || card.card_id || 'Price History';
  if (subEl) subEl.textContent = [card.card_id, card.set_code, card.rarity].filter(Boolean).join(' · ');

  overlay.setAttribute('aria-hidden', 'false');
  overlay.classList.add('open');

  const days = Number($('price-modal-days')?.value || 30);
  loadPriceHistory(card, days);
}

async function loadPriceHistory(card, days) {
  const chartCanvas = $('price-history-chart');
  const summaryEl = $('price-modal-summary');
  if (!chartCanvas) return;

  // Destroy previous chart
  if (priceChart) { priceChart.destroy(); priceChart = null; }

  const variant = card.variant || 'Normal';
  const params = new URLSearchParams({ variant, days });

  try {
    const data = await apiFetch(`/api/cards/price-history/${encodeURIComponent(card.card_id)}?${params}`);
    const history = data.history || [];

    const labels = history.map(h => h.date);
    const euPrices = history.map(h => h.eu_cardmarket_7d_avg);
    const enPrices = history.map(h => h.en_tcgplayer_market);

    const datasets = [];
    if (euPrices.some(v => v != null)) {
      datasets.push({
        label: 'EU Cardmarket 7d Avg',
        data: euPrices,
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59,130,246,0.1)',
        borderWidth: 2,
        pointRadius: history.length > 60 ? 0 : 3,
        pointHoverRadius: 5,
        tension: 0.3,
        fill: true,
      });
    }
    if (enPrices.some(v => v != null)) {
      datasets.push({
        label: 'EN TCGPlayer Market',
        data: enPrices,
        borderColor: '#00e5c0',
        backgroundColor: 'rgba(0,229,192,0.08)',
        borderWidth: 2,
        pointRadius: history.length > 60 ? 0 : 3,
        pointHoverRadius: 5,
        tension: 0.3,
        fill: true,
      });
    }

    if (!datasets.length) {
      chartCanvas.parentElement.innerHTML = '<div class="empty-state" style="padding:40px 0;"><div class="empty-icon">📊</div><div class="empty-title">No price data</div></div>';
      return;
    }

    priceChart = new Chart(chartCanvas, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            labels: { color: '#b0ada6', font: { family: "'JetBrains Mono', monospace", size: 11 } }
          },
          tooltip: {
            backgroundColor: '#1a2420',
            borderColor: '#263430',
            borderWidth: 1,
            titleColor: '#e8e6e0',
            bodyColor: '#b0ada6',
            titleFont: { family: "'JetBrains Mono', monospace", size: 12 },
            bodyFont: { family: "'JetBrains Mono', monospace", size: 11 },
            callbacks: {
              label: (ctx) => {
                const v = ctx.parsed.y;
                if (v == null) return '';
                const prefix = ctx.dataset.label.startsWith('EU') ? '€' : '$';
                return ` ${ctx.dataset.label}: ${prefix}${v.toFixed(2)}`;
              }
            }
          }
        },
        scales: {
          x: {
            ticks: { color: '#4a6660', font: { family: "'JetBrains Mono', monospace", size: 10 }, maxRotation: 45 },
            grid: { color: 'rgba(30,42,40,0.4)' }
          },
          y: {
            ticks: { color: '#4a6660', font: { family: "'JetBrains Mono', monospace", size: 10 } },
            grid: { color: 'rgba(30,42,40,0.4)' }
          }
        }
      }
    });

    // Summary below chart
    if (summaryEl) {
      const latestEu = euPrices.filter(v => v != null);
      const latestEn = enPrices.filter(v => v != null);
      const tierNote = data.tier === 'free' ? ' <span style="color:var(--warning);font-size:10px;">(Free: 7d limit — upgrade for full history)</span>' : '';
      summaryEl.innerHTML =
        `<span>${history.length} data point${history.length !== 1 ? 's' : ''} · ${data.days}d range${tierNote}</span>` +
        (latestEu.length ? `<span>EU Latest: ${fmt.eur(latestEu[latestEu.length-1])}</span>` : '') +
        (latestEn.length ? `<span>EN Latest: ${fmt.usd(latestEn[latestEn.length-1])}</span>` : '');
    }

  } catch (err) {
    showToast('Failed to load price history: ' + err.message, 'error');
    closePriceModal();
  }
}

// Make card rows clickable in browser table
window.openPriceHistory = function(idx) {
  const card = State.browser.cards[idx];
  if (card) showPriceHistoryModal(card);
};

/* ══════════════════════════════════════════════════════════════════
   PORTFOLIO TAB
   ══════════════════════════════════════════════════════════════════ */

async function loadPortfolioData() {
  const authGate = $('portfolio-auth-gate');
  const content  = $('portfolio-content');

  // Auth gate: must be signed in
  if (!State.user || !State.token) {
    if (authGate) authGate.style.display = '';
    if (content)  content.style.display = 'none';
    return;
  }
  if (authGate) authGate.style.display = 'none';
  if (content)  content.style.display = '';

  State.portfolio.loading = true;
  showLoadingBar();
  $('portfolio-tbody').innerHTML = skeletonRows(5, 8);

  try {
    // Get or create portfolio
    const listRes = await apiFetch('/api/portfolio');
    let pf = listRes.portfolios[0];

    if (!pf) {
      // Auto-create default portfolio
      const createRes = await apiFetchMut('/api/portfolio', 'POST', { name: 'My Portfolio' });
      pf = createRes;
    }

    State.portfolio.id   = pf.id;
    State.portfolio.name = pf.name;

    // Load items and summary in parallel
    const [itemsRes, summaryRes] = await Promise.all([
      apiFetch(`/api/portfolio/${pf.id}/items`),
      apiFetch(`/api/portfolio/${pf.id}/summary`),
    ]);

    State.portfolio.items   = itemsRes.items || [];
    State.portfolio.summary = summaryRes;

    renderPortfolioSummary(summaryRes);
    renderPortfolioItems(State.portfolio.items);

    // Load alerts list
    loadAlertsList();

    // Export button visibility
    const exportBtn = $('export-csv-btn');
    if (exportBtn) {
      exportBtn.style.display = (State.user.tier === 'elite') ? '' : 'none';
    }
  } catch (err) {
    const detail = err.message || '';
    if (detail.includes('PRO_REQUIRED')) {
      $('portfolio-tbody').innerHTML = `<tr><td colspan="8" class="empty-state" style="padding:40px 0;">
        <div style="margin-bottom:8px;font-weight:600;">Pro Required</div>
        <div style="color:var(--muted);font-size:13px;">Portfolio tracking requires a Pro (CHF 19/mo) or Elite subscription.</div>
        <a href="/login.html#upgrade" class="btn-primary" style="margin-top:12px;display:inline-block;">Upgrade</a>
      </td></tr>`;
    } else {
      showToast('Portfolio: ' + detail, 'error');
      $('portfolio-tbody').innerHTML = `<tr><td colspan="8" class="empty-state" style="padding:40px 0;color:var(--muted);">Failed to load portfolio</td></tr>`;
    }
  } finally {
    State.portfolio.loading = false;
    hideLoadingBar();
  }
}

/** POST/PUT/DELETE helper — apiFetch only does GET */
async function apiFetchMut(url, method, body) {
  const headers = { 'Content-Type': 'application/json' };
  const token = State.token || (typeof Auth !== 'undefined' ? Auth.getToken() : null);
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined });

  if (res.status === 401) {
    if (typeof Auth !== 'undefined') Auth.clearToken();
    State.user = null; State.token = null; renderNavUser();
    throw new Error('Session expired. Please sign in again.');
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    const msg = typeof err.detail === 'string' ? err.detail : (err.detail?.message || err.detail?.error || `HTTP ${res.status}`);
    throw new Error(msg);
  }
  // CSV export returns blob, not JSON
  const ct = res.headers.get('content-type') || '';
  if (ct.includes('text/csv')) return res;
  return res.json();
}

function renderPortfolioSummary(s) {
  const grid = $('portfolio-stat-grid');
  if (!grid || !s) return;

  const pnlClass = (s.total_pnl_eur || 0) >= 0 ? 'pnl-positive' : 'pnl-negative';
  const roiClass = (s.total_roi_pct || 0) >= 0 ? 'pnl-positive' : 'pnl-negative';

  grid.innerHTML = `
    <div class="stat-card"><div class="stat-label">Invested</div><div class="stat-value">${fmt.eurAuto(s.total_invested_eur)}</div></div>
    <div class="stat-card"><div class="stat-label">Current Value</div><div class="stat-value">${fmt.eurAuto(s.current_value_eur)}</div></div>
    <div class="stat-card"><div class="stat-label">P&L</div><div class="stat-value ${pnlClass}">${fmt.eurAuto(s.total_pnl_eur)}</div></div>
    <div class="stat-card"><div class="stat-label">ROI</div><div class="stat-value ${roiClass}">${fmt.pct(s.total_roi_pct)}</div></div>
  `;
}

function renderPortfolioItems(items) {
  const tbody = $('portfolio-tbody');
  if (!tbody) return;

  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="empty-state" style="padding:40px 0;">
      <div style="margin-bottom:8px;font-weight:600;">No cards yet</div>
      <div style="color:var(--muted);font-size:13px;">Click "Add Card" to start tracking your collection.</div>
    </td></tr>`;
    return;
  }

  tbody.innerHTML = items.map((it, idx) => {
    const pnlClass = (it.pnl_eur || 0) >= 0 ? 'pnl-positive' : 'pnl-negative';
    const roiClass = (it.roi_pct || 0) >= 0 ? 'pnl-positive' : 'pnl-negative';
    const marketPrice = it.eu_cardmarket_7d_avg != null ? fmt.eurAuto(it.eu_cardmarket_7d_avg)
                      : it.en_tcgplayer_market != null ? fmt.usdAuto(it.en_tcgplayer_market)
                      : '—';

    return `<tr>
      <td>
        <div style="display:flex;align-items:center;gap:8px;">
          ${cardThumb(it.image_url, it.name)}
          <div>
            <div style="font-weight:500;">${escHtml(it.name)}</div>
            <div style="color:var(--muted);font-size:11px;">${escHtml(it.card_id)} ${rarityBadge(it.rarity)}</div>
          </div>
        </div>
      </td>
      <td>${escHtml(it.set_code || '')}</td>
      <td>${it.quantity}</td>
      <td>${fmt.eurAuto(it.buy_price)}</td>
      <td class="col-eu">${marketPrice}</td>
      <td class="${pnlClass}">${it.pnl_eur != null ? fmt.eurAuto(it.pnl_eur) : '—'}</td>
      <td class="${roiClass}">${it.roi_pct != null ? fmt.pct(it.roi_pct) : '—'}</td>
      <td>
        <button class="btn-icon" title="Remove" onclick="deletePortfolioItem(${it.id})">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M3 4h8l-.75 8.25a1 1 0 01-1 .75h-4.5a1 1 0 01-1-.75L3 4z" stroke="currentColor" stroke-width="1.2"/><path d="M2 4h10M5.5 2h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
        </button>
      </td>
    </tr>`;
  }).join('');
}

// ─── Add Card Modal ────────────────────────────────────────────────

let _acDebounce = null;

function openAddCardModal() {
  const modal = $('add-card-modal');
  if (!modal) { showToast('Add card modal not found', 'error'); return; }
  modal.style.display = 'flex';
  modal.classList.add('open');
  modal.setAttribute('aria-hidden', 'false');
  // Reset form
  $('ac-search').value = '';
  $('ac-dropdown').style.display = 'none';
  $('ac-selected').style.display = 'none';
  $('ac-card-id').value = '';
  $('ac-variant').value = '';
  $('ac-qty').value = '1';
  $('ac-price').value = '';
  $('ac-date').value = '';
  $('ac-notes').value = '';
  setTimeout(() => $('ac-search').focus(), 100);
}
window.openAddCardModal = openAddCardModal;

function closeAddCardModal() {
  const modal = $('add-card-modal');
  if (!modal) return;
  modal.style.display = 'none';
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
}
window.closeAddCardModal = closeAddCardModal;

// Autocomplete search
function initPortfolioAutocomplete() {
  const input = $('ac-search');
  if (!input) return;

  input.addEventListener('input', () => {
    clearTimeout(_acDebounce);
    const q = input.value.trim();
    if (q.length < 2) {
      $('ac-dropdown').style.display = 'none';
      return;
    }
    _acDebounce = setTimeout(async () => {
      try {
        const data = await apiFetch(`/api/cards/search-autocomplete?q=${encodeURIComponent(q)}`);
        renderAutocomplete(data.results || []);
      } catch (_) {
        $('ac-dropdown').style.display = 'none';
      }
    }, 300);
  });

  // Close dropdown on outside click
  document.addEventListener('click', (e) => {
    if (!e.target.closest('#ac-search') && !e.target.closest('#ac-dropdown')) {
      $('ac-dropdown').style.display = 'none';
    }
  });
}

function renderAutocomplete(results) {
  const dd = $('ac-dropdown');
  if (!results.length) {
    dd.innerHTML = '<div class="autocomplete-empty">No cards found</div>';
    dd.style.display = '';
    return;
  }

  dd.innerHTML = results.map(r => `
    <div class="autocomplete-item" onclick="selectCard(${escHtml(JSON.stringify(JSON.stringify(r)))})">
      ${cardThumb(r.image_url, r.name)}
      <div class="ac-item-info">
        <div class="ac-item-name">${escHtml(r.name)}</div>
        <div class="ac-item-meta">${escHtml(r.card_id)} &middot; ${escHtml(r.set_code || '')} &middot; ${escHtml(r.rarity || '')}</div>
      </div>
      <div class="ac-item-price">${r.eu_cardmarket_7d_avg != null ? fmt.eur(r.eu_cardmarket_7d_avg) : '—'}</div>
    </div>
  `).join('');
  dd.style.display = '';
}

window.selectCard = function(jsonStr) {
  const card = JSON.parse(jsonStr);
  $('ac-card-id').value = card.card_id;
  $('ac-variant').value = card.variant || 'Normal';
  $('ac-search').value = card.name;
  $('ac-dropdown').style.display = 'none';

  // Show selected card details
  $('ac-card-thumb').innerHTML = cardThumb(card.image_url, card.name);
  $('ac-card-info').innerHTML = `
    <div style="font-weight:600;">${escHtml(card.name)}</div>
    <div style="color:var(--muted);font-size:12px;">${escHtml(card.card_id)} &middot; ${escHtml(card.set_code || '')} &middot; ${escHtml(card.rarity || '')} &middot; ${escHtml(card.variant || 'Normal')}</div>
    <div style="margin-top:4px;font-size:13px;">Market: ${card.eu_cardmarket_7d_avg != null ? fmt.eur(card.eu_cardmarket_7d_avg) : '—'}</div>
  `;
  $('ac-selected').style.display = '';

  // Pre-fill price with market price
  if (card.eu_cardmarket_7d_avg != null) {
    $('ac-price').value = Number(card.eu_cardmarket_7d_avg).toFixed(2);
  }
  // Default date to today
  if (!$('ac-date').value) {
    $('ac-date').value = new Date().toISOString().slice(0, 10);
  }
};

async function confirmAddCard() {
  const cardId = $('ac-card-id').value;
  if (!cardId) { showToast('Please search and select a card first.', 'warning'); return; }

  const qty   = parseInt($('ac-qty').value, 10) || 1;
  const price = parseFloat($('ac-price').value);
  if (isNaN(price) || price < 0) { showToast('Please enter a valid buy price.', 'warning'); return; }

  const body = {
    card_id: cardId,
    variant: $('ac-variant').value || 'Normal',
    quantity: qty,
    buy_price: price,
    buy_currency: 'EUR',
    buy_date: $('ac-date').value || null,
    notes: $('ac-notes').value || null,
  };

  try {
    const res = await apiFetchMut(`/api/portfolio/${State.portfolio.id}/items`, 'POST', body);
    showToast(`Card ${res.action}: ${cardId} x${res.quantity}`, 'success');
    closeAddCardModal();
    loadPortfolioData();
  } catch (err) {
    showToast('Add card failed: ' + err.message, 'error');
  }
}
window.confirmAddCard = confirmAddCard;

async function deletePortfolioItem(itemId) {
  if (!confirm('Remove this card from your portfolio?')) return;
  try {
    await apiFetchMut(`/api/portfolio/${State.portfolio.id}/items/${itemId}`, 'DELETE');
    showToast('Card removed', 'success');
    loadPortfolioData();
  } catch (err) {
    showToast('Delete failed: ' + err.message, 'error');
  }
}
window.deletePortfolioItem = deletePortfolioItem;

async function exportPortfolioCSV() {
  try {
    const res = await apiFetchMut(`/api/portfolio/${State.portfolio.id}/export`, 'GET');
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `portfolio_export_${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('CSV downloaded', 'success');
  } catch (err) {
    showToast('Export failed: ' + err.message, 'error');
  }
}
window.exportPortfolioCSV = exportPortfolioCSV;


/* ══════════════════════════════════════════════════════════════════
   PRICE ALERTS
   ══════════════════════════════════════════════════════════════════ */

let _alertCardIdx = null;

function openAlertMini(idx) {
  const card = State.browser.cards[idx];
  if (!card) return;

  if (!State.user || !State.token) {
    showToast('Sign in to set price alerts', 'warning');
    return;
  }

  _alertCardIdx = idx;
  const modal = $('alert-mini-modal');
  if (!modal) { showToast('Alert modal not found', 'error'); return; }
  modal.style.display = 'flex';
  modal.classList.add('open');
  modal.setAttribute('aria-hidden', 'false');

  const currentPrice = card.eu_cardmarket_7d_avg ?? card.en_tcgplayer_market;
  $('alert-card-info').innerHTML = `
    <div style="display:flex;align-items:center;gap:10px;">
      ${cardThumb(card.image_url, card.name)}
      <div>
        <div style="font-weight:600;">${escHtml(card.name)}</div>
        <div style="color:var(--muted);font-size:12px;">${escHtml(card.card_id)} &middot; ${escHtml(card.set_code || '')}</div>
        <div style="margin-top:4px;font-size:13px;">Current: ${currentPrice != null ? fmt.eur(currentPrice) : '—'}</div>
      </div>
    </div>
  `;

  // Default target: current price - 10% for "below", + 10% for "above"
  const dir = $('alert-direction').value;
  if (currentPrice != null) {
    const factor = dir === 'below' ? 0.9 : 1.1;
    $('alert-target').value = (currentPrice * factor).toFixed(2);
  } else {
    $('alert-target').value = '';
  }
}
window.openAlertMini = openAlertMini;

function closeAlertMini() {
  const modal = $('alert-mini-modal');
  if (!modal) return;
  modal.style.display = 'none';
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
  _alertCardIdx = null;
}
window.closeAlertMini = closeAlertMini;

async function confirmCreateAlert() {
  if (_alertCardIdx == null) return;
  const card = State.browser.cards[_alertCardIdx];
  if (!card) return;

  const target = parseFloat($('alert-target').value);
  if (isNaN(target) || target <= 0) {
    showToast('Enter a valid target price', 'warning');
    return;
  }

  try {
    await apiFetchMut('/api/alerts', 'POST', {
      card_id: card.card_id,
      variant: card.variant || 'Normal',
      target_price: target,
      direction: $('alert-direction').value,
      currency: 'EUR',
    });
    showToast(`Alert set: ${card.name} ${$('alert-direction').value} €${target.toFixed(2)}`, 'success');
    closeAlertMini();
  } catch (err) {
    showToast('Alert failed: ' + err.message, 'error');
  }
}
window.confirmCreateAlert = confirmCreateAlert;

// Alerts list in portfolio tab
async function loadAlertsList() {
  const container = $('alerts-list');
  if (!container) return;
  if (!State.user || !State.token) {
    container.innerHTML = '<div style="color:var(--muted);font-size:13px;">Sign in to see your alerts.</div>';
    return;
  }

  try {
    const data = await apiFetch('/api/alerts');
    const alerts = data.alerts || [];
    if (!alerts.length) {
      container.innerHTML = '<div style="color:var(--muted);font-size:13px;">No alerts set. Use the bell icon in the Card Browser to create one.</div>';
      return;
    }

    container.innerHTML = `<div class="alerts-grid">${alerts.map(a => {
      const statusClass = a.is_active ? 'alert-active' : 'alert-triggered';
      const statusLabel = a.is_active ? 'Active' : `Triggered at ${fmt.eurAuto(a.triggered_price)}`;
      const dirIcon = a.direction === 'below' ? '↓' : '↑';
      const currentDisplay = a.current_price_eur != null ? fmt.eurAuto(a.current_price_eur) : '—';

      return `<div class="alert-card ${statusClass}">
        <div class="alert-card-header">
          <div style="display:flex;align-items:center;gap:8px;">
            ${cardThumb(a.image_url, a.name)}
            <div>
              <div style="font-weight:500;font-size:13px;">${escHtml(a.name)}</div>
              <div style="color:var(--muted);font-size:11px;">${escHtml(a.card_id)} &middot; ${escHtml(a.set_code || '')}</div>
            </div>
          </div>
          ${a.is_active ? `<button class="btn-icon" title="Delete alert" onclick="deleteAlert(${a.id})">
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M3 4h8l-.75 8.25a1 1 0 01-1 .75h-4.5a1 1 0 01-1-.75L3 4z" stroke="currentColor" stroke-width="1.2"/><path d="M2 4h10M5.5 2h3" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>
          </button>` : ''}
        </div>
        <div class="alert-card-body">
          <span class="alert-direction">${dirIcon} ${a.direction} ${fmt.eurAuto(a.target_price)}</span>
          <span style="color:var(--muted);font-size:12px;">Now: ${currentDisplay}</span>
          <span class="alert-status ${statusClass}">${statusLabel}</span>
        </div>
      </div>`;
    }).join('')}</div>`;
  } catch (err) {
    container.innerHTML = `<div style="color:var(--muted);font-size:13px;">Failed to load alerts.</div>`;
  }
}

async function deleteAlert(alertId) {
  try {
    await apiFetchMut(`/api/alerts/${alertId}`, 'DELETE');
    showToast('Alert deleted', 'success');
    loadAlertsList();
  } catch (err) {
    showToast('Delete failed: ' + err.message, 'error');
  }
}
window.deleteAlert = deleteAlert;


/* ══════════════════════════════════════════════════════════════════
   INIT ALL FILTERS once DOM is ready
   ══════════════════════════════════════════════════════════════════ */
document.addEventListener('DOMContentLoaded', () => {
  initBrowserFilters();
  initSealedFilters();
  initArbitrageFilters();
  initPriceModal();
  initPortfolioAutocomplete();

  // Portfolio buttons
  $('add-card-btn')?.addEventListener('click', openAddCardModal);
  $('export-csv-btn')?.addEventListener('click', exportPortfolioCSV);

  // Close add-card modal on overlay click
  $('add-card-modal')?.addEventListener('click', (e) => {
    if (e.target === $('add-card-modal')) closeAddCardModal();
  });

  // Close alert mini-modal on overlay click
  $('alert-mini-modal')?.addEventListener('click', (e) => {
    if (e.target === $('alert-mini-modal')) closeAlertMini();
  });

  // Update default target when direction changes
  $('alert-direction')?.addEventListener('change', () => {
    if (_alertCardIdx == null) return;
    const card = State.browser.cards[_alertCardIdx];
    if (!card) return;
    const currentPrice = card.eu_cardmarket_7d_avg ?? card.en_tcgplayer_market;
    if (currentPrice != null) {
      const factor = $('alert-direction').value === 'below' ? 0.9 : 1.1;
      $('alert-target').value = (currentPrice * factor).toFixed(2);
    }
  });
});

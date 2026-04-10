/* ═══════════════════════════════════════════════════════════════
   OPTCG Market Terminal — Main Dashboard JS
   ═══════════════════════════════════════════════════════════════ */

// ─── State ────────────────────────────────────────────────────
const State = {
  user: null,           // { id, email, tier }
  token: null,          // JWT
  currency: 'EUR',      // Current display currency
  arbData: [],          // Current arbitrage results
  sealedData: [],       // Current sealed products
  arbMarket: 'both',    // 'both' | 'cardmarket' | 'tcgplayer'
  arbRarity: 'all',     // 'all' | 'sec' | 'manga' | 'regular'
  arbSignal: 'all',
  arbSort: { col: 'profit_eur', dir: 'desc' },
  sealedSort: 'price',  // 'price' | 'trend' | 'set'
  sealedSetFilter: 'all',
  lastRefresh: null,
};

// FX Rates (EUR base)
const FX = { EUR: 1.0, CHF: 0.96, USD: 1.08 };

// ─── Init ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadAuth();
  setupTabs();
  setupFilters();
  setupCurrencySwitcher();
  setupSearch();
  setupSortHeaders();

  setupSealedFilters();

  // Load initial data
  loadArbitrageData();
  loadOverview();

  // Check subscription success
  const params = new URLSearchParams(window.location.search);
  if (params.get('subscription') === 'success') {
    showToast(`🎉 Welcome to ${params.get('tier') || 'Pro'}! Refreshing your session...`, 'success');
    window.history.replaceState({}, '', '/');
    setTimeout(() => location.reload(), 2000);
  }
});

// ─── Auth ─────────────────────────────────────────────────────
function loadAuth() {
  const token = localStorage.getItem('optcg_token');
  const user = JSON.parse(localStorage.getItem('optcg_user') || 'null');
  if (token && user) {
    State.token = token;
    State.user = user;
    updateUserBadge();
  }
}

function updateUserBadge() {
  const badge = document.getElementById('userBadge');
  const emailEl = document.getElementById('userEmail');
  const tierEl = document.getElementById('tierBadge');
  const evLock = document.getElementById('evLockIcon');

  if (State.user) {
    emailEl.textContent = State.user.email.split('@')[0];
    tierEl.textContent = State.user.tier.toUpperCase();
    tierEl.className = `tier-badge ${State.user.tier}`;

    // Hide upgrade button for pro/elite
    if (canAccess('pro')) {
      document.getElementById('arbUpgradeBtn')?.classList.add('hidden');
      if (evLock) evLock.style.display = 'none';
    }
  } else {
    emailEl.textContent = 'Sign In';
    tierEl.textContent = 'FREE';
    tierEl.className = 'tier-badge free';
  }
}

function handleUserBadgeClick() {
  if (State.user) {
    // Show user menu or portal
    if (State.user.tier !== 'free') {
      openBillingPortal();
    } else {
      openUpgradeModal();
    }
  } else {
    window.location.href = '/login.html';
  }
}

async function openBillingPortal() {
  try {
    const data = await apiCall('/api/billing/portal');
    if (data.portal_url) window.open(data.portal_url, '_blank');
  } catch (e) {
    showToast('Could not open billing portal', 'error');
  }
}

function canAccess(tier) {
  const hierarchy = { free: 0, pro: 1, elite: 2 };
  const userTier = State.user?.tier || 'free';
  return (hierarchy[userTier] || 0) >= (hierarchy[tier] || 0);
}

// ─── API Helper ───────────────────────────────────────────────
async function apiCall(path, options = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (State.token) headers['Authorization'] = `Bearer ${State.token}`;

  const resp = await fetch(path, { ...options, headers: { ...headers, ...(options.headers || {}) } });

  if (resp.status === 401) {
    localStorage.removeItem('optcg_token');
    localStorage.removeItem('optcg_user');
    State.token = null;
    State.user = null;
    updateUserBadge();
  }

  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw Object.assign(new Error(err.detail?.message || err.detail || 'API error'), { status: resp.status, data: err });
  }

  return resp.json();
}

// ─── Currency Formatting ──────────────────────────────────────
function fmt(eur, decimals = 2) {
  if (eur === null || eur === undefined) return '—';
  const amount = eur * FX[State.currency];
  const sym = { EUR: '€', CHF: 'CHF ', USD: '$' }[State.currency];
  return `${sym}${amount.toFixed(decimals)}`;
}

function fmtChange(eur) {
  if (eur === null || eur === undefined) return '—';
  const amount = eur * FX[State.currency];
  const sign = amount >= 0 ? '+' : '';
  const sym = { EUR: '€', CHF: 'CHF ', USD: '$' }[State.currency];
  return `${sign}${sym}${amount.toFixed(2)}`;
}

// ─── Tabs ─────────────────────────────────────────────────────
function setupTabs() {
  const allTabs = document.querySelectorAll('[data-tab]');
  allTabs.forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      switchTab(tab);
    });
  });
}

function switchTab(tab) {
  // Update buttons
  document.querySelectorAll('[data-tab]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });
  // Update panels
  document.querySelectorAll('.tab-panel').forEach(panel => {
    panel.classList.toggle('active', panel.id === `tab-${tab}`);
  });

  // Lazy load tab data
  if (tab === 'sealed' && State.sealedData.length === 0) {
    loadSealedData();
  }
  if (tab === 'ev') {
    setupEVTab();
  }
  if (tab === 'overview') {
    loadOverview();
  }
}

// ─── Currency Switcher ─────────────────────────────────────────
function setupCurrencySwitcher() {
  document.querySelectorAll('.currency-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      State.currency = btn.dataset.currency;
      document.querySelectorAll('.currency-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      // Re-render current data
      renderArbTable(State.arbData);
      renderSealedGrid(State.sealedData);
    });
  });
}

// ─── Search ───────────────────────────────────────────────────
function setupSearch() {
  const input = document.getElementById('arbSearch');
  input.addEventListener('input', () => {
    renderArbTable(State.arbData);
  });
}

// ─── Filters ─────────────────────────────────────────────────
function setupFilters() {
  // Marketplace filter
  document.querySelectorAll('[data-arb-market]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-arb-market]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      State.arbMarket = btn.dataset.arbMarket;
      renderArbTable(State.arbData);
    });
  });

  // Rarity filter
  document.querySelectorAll('[data-arb-rarity]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-arb-rarity]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      State.arbRarity = btn.dataset.arbRarity;
      renderArbTable(State.arbData);
    });
  });

  // Arbitrage signal filter
  document.querySelectorAll('[data-arb-signal]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-arb-signal]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      State.arbSignal = btn.dataset.arbSignal;
      renderArbTable(State.arbData);
    });
  });
}

// ─── Sealed Tracker Filters ───────────────────────────────────
function setupSealedFilters() {
  // Sort buttons
  document.querySelectorAll('[data-sealed-sort]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-sealed-sort]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      State.sealedSort = btn.dataset.sealedSort;
      renderSealedGrid(State.sealedData);
    });
  });

  // Set filter dropdown
  const setFilter = document.getElementById('sealedSetFilter');
  if (setFilter) {
    setFilter.addEventListener('change', () => {
      State.sealedSetFilter = setFilter.value;
      renderSealedGrid(State.sealedData);
    });
  }
}

// ─── Sort Headers ─────────────────────────────────────────────
function setupSortHeaders() {
  document.querySelectorAll('.data-table th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.sort;
      if (State.arbSort.col === col) {
        State.arbSort.dir = State.arbSort.dir === 'asc' ? 'desc' : 'asc';
      } else {
        State.arbSort = { col, dir: 'desc' };
      }
      document.querySelectorAll('.data-table th').forEach(h => h.classList.remove('sorted'));
      th.classList.add('sorted');
      th.querySelector('.sort-arrow').textContent = State.arbSort.dir === 'asc' ? '↑' : '↓';
      renderArbTable(State.arbData);
    });
  });
}

// ═══════════════════════════════════════════════════════════════
// ARBITRAGE SCANNER
// ═══════════════════════════════════════════════════════════════
async function loadArbitrageData() {
  setArbLoading(true);
  try {
    const params = new URLSearchParams({
      item_type: 'card',
      limit: 200,
    });

    const data = await apiCall(`/api/arbitrage/scanner?${params}`);
    State.arbData = data.opportunities || [];
    State.lastRefresh = new Date();

    // Update free tier banner — only show if API says tier_limited AND user is not pro/elite
    const banner = document.getElementById('arbFreeLimitBanner');
    const isPaidUser = State.user && (State.user.tier === 'pro' || State.user.tier === 'elite');
    if (data.tier_limited && !isPaidUser) {
      banner?.classList.remove('hidden');
      document.getElementById('arbUpgradeBtn')?.classList.remove('hidden');
    } else {
      banner?.classList.add('hidden');
      if (isPaidUser) {
        document.getElementById('arbUpgradeBtn')?.classList.add('hidden');
      }
    }

    updateArbStats();
    renderArbTable(State.arbData);
    updateTickerBar(State.arbData.slice(0, 12));
    updateFreshness();
  } catch (e) {
    showArbError(e.message || 'Failed to load arbitrage data');
  } finally {
    setArbLoading(false);
  }
}

function refreshArbitrage() {
  State.arbData = [];
  loadArbitrageData();
}

function setArbLoading(loading) {
  if (loading) {
    document.getElementById('arbTableBody').innerHTML = `
      <tr><td colspan="8">
        <div class="loading-state">
          <span class="loading-spinner"></span>
          Scanning singles across Cardmarket &amp; TCGPlayer...
        </div>
      </td></tr>`;
  }
}

function showArbError(msg) {
  document.getElementById('arbTableBody').innerHTML = `
    <tr><td colspan="8">
      <div class="empty-state">
        <div class="empty-state-icon">⚠️</div>
        <div class="empty-state-title">Could not load data</div>
        <div class="empty-state-sub">${escHtml(msg)}</div>
      </div>
    </td></tr>`;
}

function updateArbStats() {
  const data = State.arbData;
  const buyEU = data.filter(d => d.signal === 'BUY_EU').length;
  const buyUS = data.filter(d => d.signal === 'BUY_US').length;
  const best = data[0];

  document.getElementById('statTotalOpps').textContent = data.length;
  document.getElementById('statBuyEU').textContent = buyEU;
  document.getElementById('statBuyUS').textContent = buyUS;

  if (best) {
    const profit = best.profit_eur;
    document.getElementById('statBestProfit').textContent = profit ? fmt(profit) : 'Pro only';
    document.getElementById('statBestProfit').className = `stat-value ${profit > 0 ? 'accent' : ''}`;
    document.getElementById('statProfitCurrency').textContent = State.currency;
    document.getElementById('statBestName').textContent = (best.name || 'Unknown').substring(0, 30);
  }
}

function filterArbData(data) {
  let filtered = [...data];

  // Marketplace filter
  if (State.arbMarket === 'cardmarket') {
    filtered = filtered.filter(d => d.cardmarket_price !== null && d.cardmarket_price !== undefined);
  } else if (State.arbMarket === 'tcgplayer') {
    filtered = filtered.filter(d => d.tcgplayer_price !== null && d.tcgplayer_price !== undefined);
  } else {
    // 'both' — only items with prices on both markets (true arbitrage)
    filtered = filtered.filter(d =>
      (d.cardmarket_price !== null && d.cardmarket_price !== undefined) &&
      (d.tcgplayer_price !== null && d.tcgplayer_price !== undefined)
    );
  }

  // Rarity filter
  if (State.arbRarity === 'sec') {
    filtered = filtered.filter(d => {
      const r = (d.rarity || '').toUpperCase();
      return r.includes('LEADER') || r.includes('SECRET') || r.includes('SEC');
    });
  } else if (State.arbRarity === 'manga') {
    filtered = filtered.filter(d => {
      const r = (d.rarity || '').toUpperCase();
      const n = (d.name || '').toUpperCase();
      return r.includes('MANGA') || r.includes('SP') || n.includes('MANGA') || n.includes(' SP');
    });
  } else if (State.arbRarity === 'regular') {
    filtered = filtered.filter(d => {
      const r = (d.rarity || '').toUpperCase();
      const n = (d.name || '').toUpperCase();
      return !r.includes('LEADER') && !r.includes('SECRET') && !r.includes('SEC') &&
             !r.includes('MANGA') && !r.includes('SP') && !n.includes('MANGA') && !n.includes(' SP');
    });
  }

  // Signal filter
  if (State.arbSignal && State.arbSignal !== 'all') {
    filtered = filtered.filter(d => d.signal === State.arbSignal);
  }

  // Search filter
  const query = document.getElementById('arbSearch')?.value?.toLowerCase() || '';
  if (query) {
    filtered = filtered.filter(d =>
      (d.name || '').toLowerCase().includes(query) ||
      (d.set_name || '').toLowerCase().includes(query) ||
      (d.code || '').toLowerCase().includes(query)
    );
  }

  // Sort
  filtered.sort((a, b) => {
    const col = State.arbSort.col;
    let av = a[col], bv = b[col];
    if (av === null || av === undefined) av = -Infinity;
    if (bv === null || bv === undefined) bv = -Infinity;
    if (State.arbSort.dir === 'asc') return av > bv ? 1 : -1;
    return av < bv ? 1 : -1;
  });

  return filtered;
}

function renderArbTable(data) {
  const filtered = filterArbData(data);
  const tbody = document.getElementById('arbTableBody');

  if (filtered.length === 0) {
    tbody.innerHTML = `
      <tr><td colspan="8">
        <div class="empty-state">
          <div class="empty-state-icon">🔍</div>
          <div class="empty-state-title">No results found</div>
          <div class="empty-state-sub">Try adjusting your filters or refresh data</div>
        </div>
      </td></tr>`;
    return;
  }

  tbody.innerHTML = filtered.map(item => {
    const signalClass = item.signal || 'NEUTRAL';
    const signalLabels = {
      BUY_EU: '▲ BUY EU',
      BUY_US: '▲ BUY US',
      WATCH: '◉ WATCH',
      NEUTRAL: '— NEUTRAL',
    };

    // Marketplace availability indicators
    const hasCM = item.cardmarket_price !== null && item.cardmarket_price !== undefined;
    const hasTCG = item.tcgplayer_price !== null && item.tcgplayer_price !== undefined;
    const marketLabel = hasCM && hasTCG
      ? `<span style="font-size:10px; color:var(--accent)">CM+TCG</span>`
      : hasCM
        ? `<span style="font-size:10px; color:var(--text-dim)">CM only</span>`
        : hasTCG
          ? `<span style="font-size:10px; color:var(--text-dim)">TCG only</span>`
          : `<span style="font-size:10px; color:var(--text-dim)">—</span>`;

    const profitEUR = item.profit_eur;
    const profitPct = item.profit_pct;
    let profitHTML;
    if (profitEUR === null || profitEUR === undefined) {
      profitHTML = `<span style="color:var(--text-dim); font-size:11px">Pro only</span>`;
    } else {
      const profitClass = profitEUR >= 0 ? 'price-positive' : 'price-negative';
      profitHTML = `
        <span class="mono ${profitClass}">${fmt(profitEUR)}</span>
        ${profitPct !== null ? `<br><span style="font-size:10px; color:var(--text-dim)">${profitPct}%</span>` : ''}
      `;
    }

    const buyOnHTML = item.buy_market
      ? `<span style="font-size:11px; color:var(--text-muted)">${item.buy_market === 'cardmarket' ? '🇪🇺 Cardmarket' : '🇺🇸 TCGPlayer'}</span>`
      : '—';

    return `
      <tr>
        <td>
          <div class="product-cell">
            <div>
              <div class="product-name">${escHtml(item.name || 'Unknown')}</div>
              <div class="product-meta">${escHtml(item.code || '')}</div>
            </div>
          </div>
        </td>
        <td>
          <div style="font-size:12px; font-weight:500">${escHtml(item.set_name || '—')}</div>
        </td>
        <td>${marketLabel}</td>
        <td class="mono">${item.cardmarket_price !== null ? fmt(item.cardmarket_price) : '—'}</td>
        <td class="mono">${item.tcgplayer_price !== null ? fmt(item.tcgplayer_price) : '—'}</td>
        <td>${profitHTML}</td>
        <td>
          <span class="signal-badge ${signalClass}">
            <span class="signal-dot"></span>
            ${signalLabels[signalClass] || signalClass}
          </span>
        </td>
        <td class="hide-mobile">${buyOnHTML}</td>
      </tr>
    `;
  }).join('');
}

function updateFreshness() {
  const el = document.getElementById('dataFreshness');
  if (!State.lastRefresh) return;
  const tier = State.user?.tier || 'free';
  el.textContent = tier === 'free' ? '24H CACHE' : '15MIN';
}

// ═══════════════════════════════════════════════════════════════
// SEALED TRACKER
// ═══════════════════════════════════════════════════════════════
async function loadSealedData() {
  document.getElementById('sealedGrid').innerHTML = `
    <div style="grid-column:1/-1">
      <div class="loading-state"><span class="loading-spinner"></span>Loading sealed products...</div>
    </div>`;

  try {
    const data = await apiCall('/api/sealed/products?sort=price_highest');
    State.sealedData = data.products || [];
    populateSealedSetFilter(State.sealedData);
    renderSealedGrid(State.sealedData);
  } catch (e) {
    document.getElementById('sealedGrid').innerHTML = `
      <div style="grid-column:1/-1">
        <div class="empty-state">
          <div class="empty-state-icon">⚠️</div>
          <div class="empty-state-title">Could not load sealed products</div>
          <div class="empty-state-sub">${escHtml(e.message)}</div>
        </div>
      </div>`;
  }
}

function populateSealedSetFilter(data) {
  const select = document.getElementById('sealedSetFilter');
  if (!select) return;
  // Collect unique set names
  const sets = [...new Set(data.map(p => p.set_name || '').filter(Boolean))].sort();
  select.innerHTML = '<option value="all">All Sets</option>' +
    sets.map(s => `<option value="${escAttr(s)}">${escHtml(s)}</option>`).join('');
  // Restore current selection if still valid
  if (State.sealedSetFilter !== 'all' && sets.includes(State.sealedSetFilter)) {
    select.value = State.sealedSetFilter;
  } else {
    State.sealedSetFilter = 'all';
    select.value = 'all';
  }
}

function refreshSealed() {
  State.sealedData = [];
  loadSealedData();
}

function filterSealedData(data) {
  let filtered = [...data];

  // Set filter
  if (State.sealedSetFilter && State.sealedSetFilter !== 'all') {
    filtered = filtered.filter(p => (p.set_name || '') === State.sealedSetFilter);
  }

  // Sort
  if (State.sealedSort === 'price') {
    filtered.sort((a, b) => (b._cardmarket_price || 0) - (a._cardmarket_price || 0));
  } else if (State.sealedSort === 'trend') {
    const trendOrder = { up: 2, down: 1, null: 0, undefined: 0 };
    filtered.sort((a, b) => (trendOrder[b.trend] || 0) - (trendOrder[a.trend] || 0));
  } else if (State.sealedSort === 'set') {
    filtered.sort((a, b) => (a.set_name || '').localeCompare(b.set_name || ''));
  }

  return filtered;
}

function renderSealedGrid(data) {
  const grid = document.getElementById('sealedGrid');
  const filtered = filterSealedData(data);

  if (filtered.length === 0) {
    grid.innerHTML = `<div style="grid-column:1/-1">
      <div class="empty-state">
        <div class="empty-state-icon">📦</div>
        <div class="empty-state-title">No sealed products found</div>
        <div class="empty-state-sub">Try refreshing or check back later</div>
      </div>
    </div>`;
    return;
  }

  grid.innerHTML = filtered.map(p => {
    const name = p.name || p.product_name || 'Unknown Product';
    const cm = p._cardmarket_price;
    const tcp = p._tcgplayer_price;
    const cm30d = p.cm_30d_average;
    const cm7d = p.cm_7d_average;
    const trend = p.trend; // 'up', 'down', or null
    const trendArrow = trend === 'up' ? '↑' : trend === 'down' ? '↓' : '—';
    const trendClass = trend === 'up' ? 'price-positive' : trend === 'down' ? 'price-negative' : '';
    const imgUrl = p.image_url;
    const cmUrl = p.cardmarket_url;
    // Set logo: use set_logo_url if available, or derive from set code
    const setCode = (p.set_code || p.set_id || '').toUpperCase();
    const setLogoUrl = p.set_logo_url || (setCode
      ? `https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/one-piece/${setCode}/${setCode}_en_logo.png`
      : null);

    return `
      <div class="product-card">
        <div style="display:flex; align-items:flex-start; gap:var(--space-3); margin-bottom:var(--space-2)">
          ${setLogoUrl ? `<img src="${escAttr(setLogoUrl)}" alt="${escAttr(setCode)}" style="height:32px; max-width:64px; object-fit:contain; opacity:0.85; flex-shrink:0;" onerror="this.style.display='none'" />` : ''}
          ${imgUrl ? `<img src="${escAttr(imgUrl)}" alt="${escAttr(name)}" style="height:60px; max-width:50px; border-radius:4px; object-fit:contain; flex-shrink:0;" onerror="this.style.display='none'" />` : ''}
        </div>
        <div class="product-card-header">
          <div class="product-card-name">${escHtml(name)}</div>
        </div>
        <div style="font-size:11px; color:var(--text-dim); margin-bottom:var(--space-2)">
          📦 ${escHtml(p.set_name || '')}${setCode ? ` <span style="opacity:0.6">(${escHtml(setCode)})</span>` : ''}
        </div>
        <div class="product-card-prices">
          <div class="product-price-item">
            <div class="product-price-source">🇪🇺 Cardmarket</div>
            <div class="product-price-value">${cm ? fmt(cm) : '—'}</div>
          </div>
          ${tcp ? `<div style="color:var(--border-light); font-size:18px">↔</div>
          <div class="product-price-item">
            <div class="product-price-source">🇺🇸 TCGPlayer</div>
            <div class="product-price-value">${fmt(tcp)}</div>
          </div>` : ''}
        </div>
        <div style="display:flex; gap:var(--space-3); margin-top:var(--space-2); font-size:11px; color:var(--text-dim)">
          ${cm30d !== null && cm30d !== undefined ? `<span>30d avg: ${fmt(cm30d)}</span>` : ''}
          ${cm7d !== null && cm7d !== undefined ? `<span>7d avg: ${fmt(cm7d)}</span>` : ''}
          ${trend ? `<span class="${trendClass}" style="font-weight:600">${trendArrow} 7d trend</span>` : ''}
        </div>
        <div style="margin-top:var(--space-2); display:flex; gap:var(--space-2); flex-wrap:wrap">
          ${cmUrl ? `<a href="${escAttr(cmUrl)}" target="_blank" rel="noopener" style="font-size:11px; color:var(--accent); text-decoration:none">🔗 Cardmarket</a>` : ''}
          ${!canAccess('pro') ? `<a href="#" onclick="openUpgradeModal(); return false;" style="font-size:11px; color:var(--text-dim)">Pro: price charts</a>` : ''}
        </div>
      </div>
    `;
  }).join('');
}

// ═══════════════════════════════════════════════════════════════
// EV CALCULATOR
// ═══════════════════════════════════════════════════════════════
async function setupEVTab() {
  if (!canAccess('pro')) {
    // Show pro gate overlay
    const evProGate = document.getElementById('evProGate');
    const evPanel = document.getElementById('evPanel');
    evProGate?.classList.remove('hidden');
    evPanel?.classList.add('hidden');
    return;
  }

  // Load sets for dropdown
  const select = document.getElementById('evSetSelect');
  if (select.options.length <= 1) {
    try {
      const data = await apiCall('/api/sets');
      const sets = data.sets || [];
      select.innerHTML = '<option value="">Select a set...</option>' +
        sets.map(s => {
          const lang = s.language || 'EN';
          const flag = lang === 'JP' ? '🇯🇵' : '🇺🇸';
          return `<option value="${escAttr(s.api_id)}">${flag} ${escHtml(s.name || s.api_id)} (${lang})</option>`;
        }).join('');

      // Set language from selected set
      select.addEventListener('change', () => {
        const opt = select.selectedOptions[0];
        if (opt && opt.text.includes('🇯🇵')) {
          document.getElementById('evLangSelect').value = 'JP';
        } else if (opt) {
          document.getElementById('evLangSelect').value = 'EN';
        }
      });
    } catch (e) {
      select.innerHTML = '<option>Error loading sets</option>';
    }
  }
}

async function calculateEV() {
  const setId = document.getElementById('evSetSelect').value;
  const lang = document.getElementById('evLangSelect').value;
  const boxCost = document.getElementById('evBoxCost').value;

  if (!setId) {
    showToast('Please select a set', 'error');
    return;
  }

  document.getElementById('evEmptyState').classList.add('hidden');
  document.getElementById('evResultContent').classList.remove('hidden');
  document.getElementById('evBreakdown').innerHTML = `
    <div class="loading-state" style="padding:var(--space-6)">
      <span class="loading-spinner"></span> Calculating EV...
    </div>`;

  try {
    let url = `/api/ev/calculate/${setId}`;
    if (boxCost) url += `?box_cost=${boxCost}`;

    const data = await apiCall(url);
    const ev = data.ev;
    renderEVResult(ev);
  } catch (e) {
    if (e.status === 403) {
      document.getElementById('evProGate')?.classList.remove('hidden');
      document.getElementById('evPanel')?.classList.add('hidden');
    } else {
      document.getElementById('evBreakdown').innerHTML = `
        <div class="empty-state">
          <div class="empty-state-icon">⚠️</div>
          <div class="empty-state-title">Could not calculate EV</div>
          <div class="empty-state-sub">${escHtml(e.message)}</div>
        </div>`;
    }
  }
}

function renderEVResult(ev) {
  const verdictBox = document.getElementById('evVerdictBox');
  const verdictText = document.getElementById('evVerdictText');
  const verdictSub = document.getElementById('evVerdictSub');
  const breakdown = document.getElementById('evBreakdown');

  verdictBox.className = `ev-verdict ${ev.verdict}`;
  verdictText.textContent = ev.verdict.replace('_', ' ');
  verdictSub.textContent = `EV: ${fmt(ev.calculated_ev)} vs Box: ${fmt(ev.box_cost)}`;

  const rows = [
    { label: 'Box Cost', value: fmt(ev.box_cost), highlight: false },
    { label: 'Calculated EV', value: fmt(ev.calculated_ev), highlight: true },
    { label: 'EV Ratio', value: `${(ev.ev_ratio * 100).toFixed(1)}%`, highlight: false },
    { label: 'Profit/Loss per Box', value: fmtChange(ev.ev_minus_box), highlight: true },
    { label: 'Packs per Box', value: ev.packs_per_box, highlight: false },
    { label: 'Cards Sampled', value: ev.card_sample_size, highlight: false },
    { label: '─── Rarity Breakdown ───', value: '', highlight: false },
    ...(ev.breakdown || []).map(b => ({
      label: `${b.rarity} (${b.rate_per_box}×, avg ${fmt(b.avg_price)})`,
      value: fmt(b.ev_contribution),
      highlight: false,
    })),
  ];

  breakdown.innerHTML = rows.map(row => `
    <div class="ev-row">
      <span class="ev-row-label">${escHtml(String(row.label))}</span>
      <span class="ev-row-value ${row.highlight ? 'highlight' : ''}">${escHtml(String(row.value))}</span>
    </div>
  `).join('');
}

// ═══════════════════════════════════════════════════════════════
// MARKET OVERVIEW
// ═══════════════════════════════════════════════════════════════
async function loadOverview() {
  try {
    const data = await apiCall('/api/market/overview');

    // Stats
    document.getElementById('ovSetsTracked').textContent = data.stats?.sets_tracked || '—';
    document.getElementById('ovTopSignal').textContent = data.stats?.top_signal || '—';
    document.getElementById('ovRecentSets').textContent = (data.recent_sets || []).length;
    document.getElementById('ovLastUpdate').textContent = new Date().toLocaleTimeString();

    // Top movers
    const moversList = document.getElementById('topMoversList');
    const movers = data.top_movers || [];
    if (movers.length === 0) {
      moversList.innerHTML = `<div class="empty-state" style="padding:var(--space-5)">
        <div class="empty-state-sub">No movers data available</div></div>`;
    } else {
      moversList.innerHTML = movers.map(m => `
        <div class="mover-item">
          <div>
            <div class="mover-name">${escHtml(m.name || '—')}</div>
            <div class="mover-set">
              ${m.set_language === 'JP' ? '🇯🇵' : '🇺🇸'} ${escHtml(m.set_name || '')}
            </div>
          </div>
          <div style="text-align:right">
            <div class="mover-price">${m.cardmarket_price ? fmt(m.cardmarket_price) : '—'}</div>
            <span class="signal-badge ${m.signal || 'NEUTRAL'}" style="font-size:10px; padding:2px 5px">
              ${m.signal || '—'}
            </span>
          </div>
        </div>
      `).join('');
    }

    // Recent sets
    const setsList = document.getElementById('recentSetsList');
    const sets = data.recent_sets || [];
    if (sets.length === 0) {
      setsList.innerHTML = `<div class="empty-state" style="padding:var(--space-5)">
        <div class="empty-state-sub">No sets data available</div></div>`;
    } else {
      setsList.innerHTML = sets.map(s => `
        <div class="mover-item">
          <div>
            <div class="mover-name">
              ${s.language === 'JP' ? '🇯🇵' : '🇺🇸'} ${escHtml(s.name || s.api_id)}
            </div>
            <div class="mover-set">${escHtml(s.code || '')} · ${escHtml(s.release_date || 'Unknown release')}</div>
          </div>
          <div>
            <span class="lang-tag ${s.language || 'EN'}">${s.language || 'EN'}</span>
          </div>
        </div>
      `).join('');
    }
  } catch (e) {
    document.getElementById('topMoversList').innerHTML =
      `<div class="empty-state" style="padding:var(--space-5)">
        <div class="empty-state-sub">Error loading overview: ${escHtml(e.message)}</div>
       </div>`;
  }
}

function refreshOverview() {
  loadOverview();
}

// ═══════════════════════════════════════════════════════════════
// TICKER BAR
// ═══════════════════════════════════════════════════════════════
function updateTickerBar(items) {
  if (!items || items.length === 0) return;
  const track = document.getElementById('tickerTrack');

  const tickerItems = items.map(item => {
    const signal = item.signal || 'NEUTRAL';
    const price = item.cardmarket_price;
    const profitText = item.profit_eur !== null && item.profit_eur !== undefined
      ? ` · ${fmtChange(item.profit_eur)}`
      : '';

    return `
      <span class="ticker-item">
        <span class="ticker-name">${escHtml((item.name || 'Unknown').substring(0, 25))}</span>
        <span class="ticker-price">${price ? fmt(price) : '—'}</span>
        <span class="signal-badge ${signal}" style="padding:1px 5px; font-size:10px">${signal}</span>
        ${profitText ? `<span class="ticker-change ${item.profit_eur >= 0 ? 'up' : 'down'}">${profitText}</span>` : ''}
      </span>
    `;
  });

  // Duplicate for seamless scroll
  const content = tickerItems.join('') + tickerItems.join('');
  track.innerHTML = content;
}

// ═══════════════════════════════════════════════════════════════
// UPGRADE / CHECKOUT
// ═══════════════════════════════════════════════════════════════
function openUpgradeModal() {
  document.getElementById('upgradeModal').classList.add('open');
}

function closeUpgradeModal() {
  document.getElementById('upgradeModal').classList.remove('open');
}

// Close on backdrop click
document.getElementById('upgradeModal')?.addEventListener('click', (e) => {
  if (e.target === document.getElementById('upgradeModal')) closeUpgradeModal();
});

async function startCheckout(tier) {
  if (!State.user) {
    // Redirect to login first
    sessionStorage.setItem('post_login_upgrade', tier);
    window.location.href = '/login.html?redirect=/&upgrade=' + tier;
    return;
  }

  try {
    const data = await apiCall('/api/billing/checkout', {
      method: 'POST',
      body: JSON.stringify({ tier }),
    });
    if (data.checkout_url) {
      window.location.href = data.checkout_url;
    }
  } catch (e) {
    showToast('Checkout error: ' + e.message, 'error');
  }
}

// ═══════════════════════════════════════════════════════════════
// TOAST NOTIFICATIONS
// ═══════════════════════════════════════════════════════════════
function showToast(message, type = 'info', duration = 4000) {
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity 0.3s';
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// ═══════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════
function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function escAttr(str) {
  if (!str) return '';
  return String(str).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

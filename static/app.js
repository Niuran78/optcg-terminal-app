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
    filters: { set: 'all', rarity: 'all', search: '', minLiquidity: 60 },  // Liquid-only by default
    sort:    { col: 'liquidity_score', order: 'desc' },
  },

  sealed: {
    products: [],
    total:    0,
    loading:  false,
    filters:  { set: 'all', type: 'all', lang: 'all' },
    sort:     'cm_live_trend',
    showAll:  false,   // when true (Pro only) -> live_only=false on backend
  },

  arbitrage: {
    opportunities: [],
    total:   0,
    offset:  0,
    loading: false,
    filters: { signal: 'all', minSpread: 0, set: 'all', source: 'live' },  // default: LIVE-only, all profitable spreads
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

  // FX — hydrated from /api/fx/rate at startup (Frankfurter.dev ECB-sourced).
  // Conservative initial value used only for the first ~100ms before the live
  // rate arrives; do NOT compute final displayed prices off this default.
  usdToEur: 0.93,
  fxLoaded: false,
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

  // Hydrate live FX rate so all USD→EUR conversions match the backend.
  hydrateFxRate();

  // First-run welcome banner (auto-show for accounts <7 days old, dismiss persists)
  bindWelcomeBanner();

  // Wire upgrade modal
  bindUpgradeModal();

  // Handle ?upgrade=pro / ?upgrade=elite / ?subscription=success
  handleUpgradeQueryParams();

  // Load market summary bar
  loadMarketSummary();

  // Load default tab
  switchTab('browser');
});

/* ═════════════════════════════════════════════════════════════════
   UPGRADE FLOW — modal + Stripe Checkout
   ═════════════════════════════════════════════════════════════════ */
function openUpgradeModal(highlightTier) {
  const modal = $('upgrade-modal');
  if (!modal) return;
  // If user not logged in, send to login.html with return URL
  if (!State.user) {
    window.location.href = '/login.html?next=' + encodeURIComponent(window.location.pathname + '?upgrade=' + (highlightTier || 'pro'));
    return;
  }
  // Telemetry: top of funnel
  trackEvent('upgrade_modal_open', { highlight_tier: highlightTier || 'pro' });
  // Remember what was focused so we can restore it on close
  modal._a11y_restore = document.activeElement;
  const errBox = $('upgrade-error');
  if (errBox) errBox.textContent = '';
  modal.style.display = 'flex';
  modal.setAttribute('aria-hidden', 'false');
  // Hide "current plan" button on the tier the user already has
  const currentTier = (Auth?.getTier(State.user) || 'free').toLowerCase();
  $$('#upgrade-modal .plan-card').forEach(card => {
    const tier = card.dataset.tier;
    const btn = card.querySelector('button');
    if (!btn) return;
    if (tier === currentTier) {
      btn.disabled = true;
      btn.style.opacity = '0.6';
      btn.style.cursor = 'default';
      btn.textContent = 'Your current plan';
      btn.classList.remove('btn-primary');
      btn.classList.add('btn');
    } else if (tier !== 'free') {
      btn.disabled = false;
      btn.style.opacity = '';
      btn.style.cursor = '';
      btn.textContent = 'Upgrade to ' + tier.charAt(0).toUpperCase() + tier.slice(1);
      btn.classList.add('btn-primary');
    }
  });
  // Visually emphasize the targeted tier
  if (highlightTier) {
    const target = document.querySelector(`#upgrade-modal .plan-card[data-tier="${highlightTier}"]`);
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }
  // A11y: trap focus inside, auto-focus first interactive element
  trapModalFocus(modal);
}

function closeUpgradeModal() {
  const modal = $('upgrade-modal');
  if (!modal) return;
  modal.style.display = 'none';
  modal.setAttribute('aria-hidden', 'true');
  releaseModalFocus(modal);
}

async function startCheckout(tier) {
  const errBox = $('upgrade-error');
  if (errBox) errBox.textContent = '';
  if (!['pro', 'elite'].includes(tier)) return;
  if (!State.user) {
    window.location.href = '/login.html?next=' + encodeURIComponent('/?upgrade=' + tier);
    return;
  }
  // Disable all checkout buttons, show loading state on clicked one
  const btns = $$('.upgrade-checkout-btn');
  btns.forEach(b => b.disabled = true);
  const target = document.querySelector(`.upgrade-checkout-btn[data-tier="${tier}"]`);
  const originalLabel = target ? target.textContent : '';
  if (target) target.textContent = 'Redirecting to Stripe…';
  try {
    const res = await apiFetchMut('/api/billing/checkout', 'POST', { tier });
    if (res && res.checkout_url) {
      window.location.href = res.checkout_url;
      return;
    }
    throw new Error('No checkout URL received');
  } catch (err) {
    const msg = (err && err.message) ? String(err.message) : 'Checkout unavailable';
    if (errBox) errBox.textContent = 'Checkout failed: ' + msg + '. Please try again or contact support.';
    btns.forEach(b => b.disabled = false);
    if (target) target.textContent = originalLabel;
  }
}

async function openStripePortal() {
  try {
    const res = await apiFetch('/api/billing/portal');
    if (res && res.portal_url) {
      window.location.href = res.portal_url;
    }
  } catch (err) {
    showMiniToast('Could not open subscription portal: ' + (err.message || 'unknown error'), 'error');
  }
}

// Minimal toast helper — uses existing .toast-container if present
function showMiniToast(message, type, ttl) {
  const container = document.getElementById('toast-container');
  if (!container) { console.log('[toast]', type || 'info', message); return; }
  const el = document.createElement('div');
  el.className = 'toast toast-' + (type || 'info');
  el.style.cssText = 'background:var(--surface);border:1px solid var(--border);border-left:3px solid ' +
    (type === 'success' ? 'var(--accent)' : type === 'error' ? 'var(--danger, #ff5c5c)' : 'var(--muted)') +
    ';padding:12px 16px;margin-top:8px;border-radius:var(--r-md);color:var(--text);font-size:13px;box-shadow:0 4px 12px rgba(0,0,0,0.3);max-width:360px;';
  el.textContent = message;
  container.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity 0.3s'; }, ttl || 4000);
  setTimeout(() => { el.remove(); }, (ttl || 4000) + 300);
}

function bindUpgradeModal() {
  // Header Upgrade button in user dropdown
  $('upgrade-btn')?.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    $('user-menu')?.classList.remove('open');
    const currentTier = (State.user && Auth?.getTier(State.user) || 'free').toLowerCase();
    // Open Stripe portal only if user has an ACTIVE subscription record.
    // stripe_customer_id alone is not enough — shop-bonus Elite users may have
    // a dangling customer row without a paid subscription.
    const hasActiveSub = State.user && State.user.subscription && State.user.subscription.status === 'active';
    if (currentTier === 'elite' && hasActiveSub) {
      openStripePortal();
      return;
    }
    // Highlight: Pro → Elite, everything else → Pro
    openUpgradeModal(currentTier === 'pro' ? 'elite' : 'pro');
  });

  // Manage Subscription link → Stripe portal
  $('manage-sub-link')?.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    openStripePortal();
  });

  // Show "Manage Subscription" link only for users with an active paid subscription
  const hasActiveSub = State.user && State.user.subscription && State.user.subscription.status === 'active';
  if (hasActiveSub) {
    const link = $('manage-sub-link');
    if (link) link.style.display = '';
  }

  // Close button
  $('upgrade-modal-close')?.addEventListener('click', closeUpgradeModal);

  // Backdrop click
  $('upgrade-modal')?.addEventListener('click', (e) => {
    if (e.target.id === 'upgrade-modal') closeUpgradeModal();
  });

  // Escape
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && $('upgrade-modal')?.style.display !== 'none') {
      closeUpgradeModal();
    }
  });

  // Checkout buttons
  $$('.upgrade-checkout-btn').forEach(btn => {
    btn.addEventListener('click', () => startCheckout(btn.dataset.tier));
  });
}

function handleUpgradeQueryParams() {
  const params = new URLSearchParams(window.location.search);
  const upgradeTier = params.get('upgrade');
  const subStatus = params.get('subscription');
  const subTier = params.get('tier');

  if (subStatus === 'success' && subTier) {
    // Telemetry: client-confirmed success (server-side already fired via webhook)
    trackEvent('checkout_success', { tier: subTier, source: 'redirect' });
    // Show success toast and clean URL
    setTimeout(() => {
      showMiniToast(`Welcome to ${subTier.charAt(0).toUpperCase() + subTier.slice(1)}! Your subscription is active.`, 'success', 7000);
    }, 400);
    const clean = window.location.pathname + window.location.hash;
    window.history.replaceState({}, document.title, clean);
    return;
  }

  if (upgradeTier && ['pro', 'elite'].includes(upgradeTier)) {
    setTimeout(() => openUpgradeModal(upgradeTier), 200);
    // Clean URL so reload doesn't re-trigger
    const clean = window.location.pathname + window.location.hash;
    window.history.replaceState({}, document.title, clean);
  }
}

// Expose globally so backend-provided upgrade_url handlers (via window.location or inline onclick) could call it
window.openUpgradeModal = openUpgradeModal;

/* ═════════════════════════════════════════════════════════════════
   A11Y — Modal Focus Management
   Generic helpers any modal can use. Trap Tab inside, restore focus on close.
   ═════════════════════════════════════════════════════════════════ */
const FOCUSABLE_SEL = [
  'button:not([disabled])',
  '[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
  '[contenteditable="true"]',
].join(',');

function _focusableIn(modal) {
  return Array.from(modal.querySelectorAll(FOCUSABLE_SEL))
    .filter(el => el.offsetParent !== null);  // visible only
}

function trapModalFocus(modal) {
  if (!modal) return;
  // Auto-focus first interactive element
  const focusables = _focusableIn(modal);
  if (focusables.length) {
    setTimeout(() => focusables[0].focus(), 50);
  }
  // Tab/Shift+Tab loop inside modal
  const handler = (e) => {
    if (e.key !== 'Tab') return;
    const f = _focusableIn(modal);
    if (!f.length) return;
    const first = f[0], last = f[f.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault(); last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault(); first.focus();
    }
  };
  modal._a11y_trap = handler;
  document.addEventListener('keydown', handler);
}

function releaseModalFocus(modal) {
  if (!modal) return;
  if (modal._a11y_trap) {
    document.removeEventListener('keydown', modal._a11y_trap);
    modal._a11y_trap = null;
  }
  // Restore focus to whatever opened the modal
  if (modal._a11y_restore && typeof modal._a11y_restore.focus === 'function') {
    try { modal._a11y_restore.focus(); } catch (_) {}
    modal._a11y_restore = null;
  }
}
window.trapModalFocus = trapModalFocus;
window.releaseModalFocus = releaseModalFocus;

/* ═════════════════════════════════════════════════════════════════
   TELEMETRY — fire-and-forget event tracking
   ═════════════════════════════════════════════════════════════════ */
function trackEvent(name, properties) {
  try {
    const headers = { 'Content-Type': 'application/json' };
    const tok = State.token || (typeof Auth !== 'undefined' && Auth.getToken && Auth.getToken());
    if (tok) headers['Authorization'] = 'Bearer ' + tok;
    fetch('/api/telemetry/event', {
      method: 'POST',
      headers,
      body: JSON.stringify({ event: name, properties: properties || {} }),
      keepalive: true,  // survives page navigation (e.g. checkout redirect)
    }).catch(() => {});
  } catch (_) {}
}
window.trackEvent = trackEvent;

/* ═════════════════════════════════════════════════════════════════
   MARKET RADAR — daily personalized signals
   ═════════════════════════════════════════════════════════════════ */
async function loadRadarData() {
  const content = $('radar-content');
  const meta = $('radar-meta');
  const badge = $('radar-badge');
  if (!content) return;
  content.innerHTML = '<div class="radar-loading">Loading…</div>';

  try {
    const data = await apiFetch('/api/radar/today');
    if (data.upgrade_required) {
      content.innerHTML = `
        <div class="radar-paywall">
          <div class="radar-paywall-title">Market Radar is a Pro feature</div>
          <div class="radar-paywall-desc">${data.message || 'Get personalized daily signals — price drops, fair-value opportunities, portfolio P&L — with Pro.'}</div>
          <button onclick="openUpgradeModal('pro')" class="btn-primary" style="border:none;cursor:pointer;">Upgrade to Pro</button>
        </div>`;
      if (meta) meta.textContent = '';
      if (badge) badge.style.display = 'none';
      return;
    }

    const signals = data.signals || [];
    if (signals.length === 0) {
      content.innerHTML = `
        <div class="radar-empty">
          <div class="radar-empty-title">No signals today</div>
          <div class="radar-empty-desc">The market is quiet. Check back tomorrow — signals are computed each night after the data sync.</div>
        </div>`;
      if (meta) meta.textContent = '0 SIGNALS';
      if (badge) badge.style.display = 'none';
      return;
    }

    if (meta) meta.textContent = `${signals.length} SIGNAL${signals.length === 1 ? '' : 'S'}`;
    if (badge) {
      const urgent = signals.filter(s => s.severity === 'urgent').length;
      if (urgent > 0) {
        badge.textContent = String(urgent);
        badge.style.display = '';
      } else {
        badge.style.display = 'none';
      }
    }

    content.innerHTML = '<div class="radar-list">' +
      signals.map(s => renderRadarRow(s)).join('') + '</div>';

    // Bind clicks
    $$('.radar-row', content).forEach(row => {
      row.addEventListener('click', () => onRadarRowClick(row));
    });
  } catch (err) {
    content.innerHTML = `<div class="radar-empty"><div class="radar-empty-title">Couldn't load signals</div><div class="radar-empty-desc">${(err && err.message) || 'Try again in a moment.'}</div></div>`;
  }
}

function _radarLocale() {
  // Honor browser language (de* -> de, otherwise en). Future: per-user pref.
  const lang = (navigator.language || 'en').toLowerCase();
  return lang.startsWith('de') ? 'de' : 'en';
}

function renderRadarRow(s) {
  const p = s.payload || {};
  const loc = _radarLocale();
  const word = (p.wording && (p.wording[loc] || p.wording.en || p.wording.de))
               || `${s.signal_type} on ${s.entity_id}`;
  const setPill = p.set_code ? `<span class="pill">${escapeHtml(p.set_code)}</span>` : '';
  const typeLabel = ({
    'price_drop': 'Price Drop',
    'fv_deviation': 'Below Fair Value',
    'portfolio_pnl': 'Portfolio P&L',
  })[s.signal_type] || s.signal_type;
  return `
    <div class="radar-row" data-signal-id="${s.id}" data-entity-type="${s.entity_type}" data-entity-id="${escapeHtml(s.entity_id)}" tabindex="0" role="button">
      <span class="radar-severity-dot ${s.severity}" aria-label="${s.severity}"></span>
      <div class="radar-row-body">
        <div class="radar-row-title">${escapeHtml(word)}</div>
        <div class="radar-row-meta">${setPill}<span>${typeLabel}</span></div>
      </div>
      <span class="radar-row-cta">View →</span>
    </div>`;
}

function onRadarRowClick(row) {
  const sigId = row.dataset.signalId;
  const entityType = row.dataset.entityType;
  const entityId = row.dataset.entityId;
  trackEvent('radar_signal_clicked', { signal_id: Number(sigId), entity_type: entityType, entity_id: entityId });
  if (entityType === 'card' && entityId) {
    // Reset filters so the card-id search isn't constrained by current selection
    if (State.browser && State.browser.filters) {
      State.browser.filters.set = 'all';
      State.browser.filters.rarity = 'all';
      State.browser.filters.minLiquidity = 0;  // Show even illiquid signals
      State.browser.filters.search = entityId;
      State.browser.offset = 0;
    }
    // Reset visible filter UI to match
    const setSel = $('browser-set');
    if (setSel) setSel.value = 'all';
    $$('#panel-browser .pill-group .pill').forEach(p => p.classList.remove('active'));
    const allPill = document.querySelector('#panel-browser .pill-group .pill[data-rarity="all"]');
    if (allPill) allPill.classList.add('active');

    switchTab('browser');
    const search = $('browser-search');
    if (search) {
      search.value = entityId;
      // Trigger backend search directly
      if (typeof loadBrowserData === 'function') loadBrowserData();
    }
  } else if (entityType === 'portfolio') {
    switchTab('portfolio');
  }
}

function escapeHtml(s) {
  if (s == null) return '';
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}
window.loadRadarData = loadRadarData;

/* ═════════════════════════════════════════════════════════════════
   FIRST-RUN WELCOME BANNER
   ═════════════════════════════════════════════════════════════════ */
function bindWelcomeBanner() {
  const banner = $('welcome-banner');
  if (!banner) return;
  if (!State.user) return;

  const dismissKey = 'hg_welcome_dismissed_v1';
  if (localStorage.getItem(dismissKey) === '1') return;

  // Show only for accounts created in the last 7 days
  const created = State.user.created_at ? new Date(State.user.created_at) : null;
  if (created && !isNaN(created)) {
    const ageDays = (Date.now() - created.getTime()) / (1000 * 60 * 60 * 24);
    if (ageDays > 7) return;
  }

  banner.style.display = 'block';

  // Step CTAs route to the right tab
  $$('.welcome-step-cta', banner).forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      if (typeof switchTab === 'function' && tab) {
        switchTab(tab);
      }
      // Don't auto-dismiss — user might want to come back to the list.
    });
  });

  $('welcome-close')?.addEventListener('click', () => {
    banner.style.display = 'none';
    localStorage.setItem(dismissKey, '1');
    trackEvent('welcome_banner_dismissed');
  });
}

/* ═════════════════════════════════════════════════════════════════
   FX — fetch live USD→EUR rate from /api/fx/rate
   ═════════════════════════════════════════════════════════════════ */
async function hydrateFxRate() {
  try {
    const r = await fetch('/api/fx/rate');
    if (!r.ok) return;
    const data = await r.json();
    if (data && typeof data.rate === 'number' && data.rate > 0.7 && data.rate < 1.3) {
      State.usdToEur = data.rate;
      State.fxLoaded = true;
      // Update tooltips that mention the rate, if rendered already.
      updateFxTooltips();
    }
  } catch (_) {
    // Keep conservative default already in State
  }
}

function updateFxTooltips() {
  const rate = State.usdToEur;
  document.querySelectorAll('[data-fx-tooltip]').forEach(el => {
    const tpl = el.getAttribute('data-fx-tooltip');
    if (tpl) el.title = tpl.replace('{rate}', rate.toFixed(4));
  });
}
window.hydrateFxRate = hydrateFxRate;

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
// Headers are now static language-based (🇯🇵 JP / 🇬🇧 EN) — no dynamic updates needed.
function updateCurrencyHeaders() { /* no-op: JP/EN headers are static */ }

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

  // Update nav tabs (aria-selected reflects WAI-ARIA tabs pattern)
  $$('.nav-tab[data-tab]').forEach(btn => {
    const isActive = btn.dataset.tab === tab;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
    btn.setAttribute('tabindex', isActive ? '0' : '-1');
  });

  // Update panels
  $$('.tab-panel').forEach(panel => {
    const isActive = panel.id === `panel-${tab}`;
    panel.classList.toggle('active', isActive);
    panel.setAttribute('aria-hidden', isActive ? 'false' : 'true');
  });

  // Load data
  if (tab === 'browser')   loadBrowserData();
  if (tab === 'sealed')    loadSealedData();
  if (tab === 'arbitrage') loadArbitrageData();
  if (tab === 'overview')  loadOverviewData();
  if (tab === 'portfolio') loadPortfolioData();
  if (tab === 'radar')     loadRadarData();
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
  const r = String(rarity).toLowerCase();
  // Normalized short codes: L, R, C, UC, SR, SEC, TR, P
  const map = {
    'l': { cls: 'leader', label: 'LEADER' },
    'leader': { cls: 'leader', label: 'LEADER' },
    'sr': { cls: 'sr', label: 'SR' },
    'super rare': { cls: 'sr', label: 'SR' },
    'sec': { cls: 'sec', label: 'SEC' },
    'secret rare': { cls: 'sec', label: 'SEC' },
    'tr': { cls: 'sec', label: 'TR' },
    'treasure rare': { cls: 'sec', label: 'TR' },
    'r': { cls: 'rare', label: 'R' },
    'rare': { cls: 'rare', label: 'R' },
    'uc': { cls: '', label: 'UC' },
    'uncommon': { cls: '', label: 'UC' },
    'c': { cls: '', label: 'C' },
    'common': { cls: '', label: 'C' },
    'p': { cls: '', label: 'PROMO' },
    'promo': { cls: '', label: 'PROMO' },
  };
  const entry = map[r] || { cls: '', label: rarity };
  return `<span class="rarity-badge ${entry.cls}">${entry.label}</span>`;
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

// External card-image CDNs (Bandai, TCGGO, TCGPriceLookup) send
// `Cross-Origin-Resource-Policy: same-site` which blocks cross-origin <img>
// loading in Chrome/Safari. We proxy them through our own origin instead.
function proxyImg(url) {
  if (!url) return '';
  // Only proxy external HTTP(S) URLs; leave data URIs and our own paths alone.
  if (!/^https?:\/\//i.test(url)) return url;
  return '/api/image/proxy?url=' + encodeURIComponent(url);
}

function cardThumb(url, name) {
  if (url) {
    const proxied = proxyImg(url);
    return `<img class="card-thumb" src="${escHtml(proxied)}" alt="${escHtml(name || '')}" loading="lazy" onerror="this.outerHTML='<div class=\\'card-thumb-placeholder\\'>🃏</div>'" />`;
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

  // Liquidity pills (Markets pivot)
  $$('[data-liq]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.browser.filters.minLiquidity = parseInt(btn.dataset.liq, 10);
      $$('[data-liq]').forEach(b => b.classList.toggle('active', b.dataset.liq === btn.dataset.liq));
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
  // Markets endpoint — investment-grade card list, live Cardmarket data only.
  if (State.browser.loading) return;
  State.browser.loading = true;
  showLoadingBar();

  const tbody = $('browser-tbody');
  if (tbody) tbody.innerHTML = skeletonRows(10, 6);

  const { filters, sort, offset } = State.browser;
  const sortCol = sort.col === 'relevance' || sort.col === 'eu_cardmarket_7d_avg'
                  ? 'liquidity_score' : sort.col;

  const params = new URLSearchParams({
    limit:  50,
    offset: offset,
    sort:   sortCol,
    order:  sort.order,
  });
  if (filters.set    && filters.set    !== 'all') params.set('set_code', filters.set);
  if (filters.rarity && filters.rarity !== 'all') params.set('rarity', filters.rarity);
  if (filters.search) params.set('search', filters.search);
  if (filters.minLiquidity != null) params.set('min_liquidity', filters.minLiquidity);

  try {
    const data = await apiFetch(`/api/cards/markets?${params}`);
    State.browser.cards = data.items || [];
    State.browser.total = data.total || 0;
    State.browser.lastData = data;

    // Build set list from current page (good enough for filter dropdown bootstrap)
    if (data.items?.length && !State.sets.length) {
      const seen = new Set();
      data.items.forEach(c => { if (c.set_code) seen.add(JSON.stringify({ code: c.set_code, name: c.set_name })); });
      State.sets = Array.from(seen).map(s => JSON.parse(s)).sort((a,b) => a.code.localeCompare(b.code));
      populateSetSelects();
    }

    // Telemetry
    try { trackEvent('markets_view_load', { total: data.total, tier: data.tier, filters: { set: filters.set, rarity: filters.rarity } }); } catch (_) {}

    renderBrowserTable(data);
  } catch (err) {
    showToast(err.message, 'error');
    if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding:40px;">
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

function _liquidityPill(score, bucket, isPro) {
  // Pro sees number + color; Free sees coarse color only.
  const palette = {
    'liquid':   { bg: 'rgba(0,229,192,0.15)',  fg: '#0fd9b3', label: 'Liquid' },
    'thin':     { bg: 'rgba(255,179,71,0.15)', fg: '#ffb347', label: 'Thin'   },
    'illiquid': { bg: 'rgba(255,92,92,0.15)',  fg: '#ff5c5c', label: 'Illiquid' },
  };
  const b = bucket || (score >= 60 ? 'liquid' : score >= 30 ? 'thin' : 'illiquid');
  const p = palette[b] || palette.thin;
  const text = isPro && score != null ? String(score) : p.label;
  return `<span class="liq-pill" style="display:inline-block;padding:2px 9px;border-radius:10px;background:${p.bg};color:${p.fg};font-family:var(--font-mono);font-size:10px;font-weight:700;letter-spacing:0.04em;">${text}</span>`;
}

function _sevenDayDelta(card) {
  // Compute % change from cm_live_7d_avg vs cm_live_trend
  const t = card.cm_live_trend, a = card.cm_live_7d_avg;
  if (t == null || a == null || a <= 0) return null;
  return ((t - a) / a) * 100;
}

function renderBrowserTable(data) {
  const items  = data.items || [];
  const total  = data.total || 0;
  const isPro  = data.is_pro === true;

  // Sort headers
  $$('[data-sort]', $('panel-browser')).forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.sort === State.browser.sort.col) {
      th.classList.add(State.browser.sort.order === 'asc' ? 'sort-asc' : 'sort-desc');
    }
    const icon = th.querySelector('.th-sort-icon');
    if (icon) icon.textContent = th.dataset.sort === State.browser.sort.col
        ? (State.browser.sort.order === 'asc' ? '↑' : '↓') : '↕';
  });

  // Summary
  const summaryEl = $('browser-summary');
  if (summaryEl) {
    summaryEl.innerHTML = `
      <span><strong>${fmt.int(total)}</strong> tradable cards</span>
      <span>Page <strong>${Math.floor(State.browser.offset / 50) + 1}</strong></span>
      <span style="margin-left:auto;">All entries have verified Cardmarket data</span>
    `;
  }

  const tbody = $('browser-tbody');
  if (!tbody) return;

  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="6">
      <div class="empty-state">
        <div class="empty-icon">🔍</div>
        <div class="empty-title">No cards match your filters</div>
        <div class="empty-desc">Try clearing filters or widening the price range.</div>
      </div>
    </td></tr>`;
    renderBrowserPagination(total);
    return;
  }

  let rows = '';
  items.forEach((c, i) => {
    const variant = c.variant && c.variant !== 'Normal'
                    ? ` <span style="font-size:10px;color:var(--muted);">(${escHtml(c.variant)})</span>` : '';
    const langPill = c.language ? `<span class="pill" style="display:inline-block;padding:1px 6px;border-radius:3px;background:rgba(255,255,255,0.04);font-family:var(--font-mono);font-size:9px;color:var(--muted);margin-left:4px;">${c.language}</span>` : '';

    // Liquidity
    const liq = _liquidityPill(c.liquidity_score, c.liquidity_bucket, isPro);

    // Cardmarket price + link
    let priceCell = '<span style="color:var(--muted);">—</span>';
    if (isPro && c.cm_live_trend != null) {
      const url = c.cm_live_url ? ` <a href="${escHtml(c.cm_live_url)}" target="_blank" rel="noopener" style="color:var(--accent);text-decoration:none;font-size:10px;margin-left:4px;">↗</a>` : '';
      const lowest = c.cm_live_lowest != null ? `<div style="font-size:10px;color:var(--muted);">low €${c.cm_live_lowest.toFixed(2)} · ${c.cm_live_available || 0} listings</div>` : '';
      priceCell = `<div><strong>€${c.cm_live_trend.toFixed(2)}</strong>${url}</div>${lowest}`;
    } else if (!isPro) {
      priceCell = `<button class="upgrade-inline" onclick="openUpgradeModal('pro')" style="background:none;border:1px dashed var(--border);color:var(--accent);padding:2px 8px;border-radius:4px;font-size:10px;cursor:pointer;">Upgrade to see</button>`;
    }

    // 7d delta
    let deltaCell = '<span style="color:var(--muted);">—</span>';
    if (isPro) {
      const d = _sevenDayDelta(c);
      if (d != null) {
        const color = d > 0 ? '#0fd9b3' : d < 0 ? '#ff5c5c' : 'var(--muted)';
        const sign = d > 0 ? '+' : '';
        deltaCell = `<span style="color:${color};font-family:var(--font-mono);font-size:12px;">${sign}${d.toFixed(1)}%</span>`;
      }
    }

    rows += `
      <tr data-idx="${i}" data-card-id="${escHtml(c.card_id)}" class="clickable-row" title="Click for price history">
        <td data-label="Card">
          <div class="card-cell">
            ${cardThumb(c.image_url, c.name)}
            <div class="card-info">
              <div class="card-name">${escHtml(c.name)}${variant}${langPill}</div>
              <div class="card-id">${escHtml(c.card_id || '')}</div>
            </div>
          </div>
        </td>
        <td data-label="Set"><span class="set-pill">${escHtml(c.set_code || '')}</span></td>
        <td data-label="Rarity">${escHtml(c.rarity || '')}</td>
        <td data-label="Liquidity">${liq}</td>
        <td data-label="Cardmarket">${priceCell}</td>
        <td data-label="7d Δ">${deltaCell}</td>
      </tr>`;
  });

  tbody.innerHTML = rows;

  // Click → price history modal (Pro only — Free users would see no chart data)
  $$('tr.clickable-row', tbody).forEach(tr => {
    tr.addEventListener('click', () => {
      const idx = parseInt(tr.dataset.idx, 10);
      const card = items[idx];
      if (!card) return;
      if (!isPro) {
        openUpgradeModal('pro');
        return;
      }
      if (typeof showPriceHistoryModal === 'function') showPriceHistoryModal(card);
    });
  });

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

  $$('[data-sealed-lang]').forEach(btn => {
    btn.addEventListener('click', () => {
      State.sealed.filters.lang = btn.dataset.sealedLang;
      $$('[data-sealed-lang]').forEach(b => b.classList.toggle('active', b.dataset.sealedLang === btn.dataset.sealedLang));
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

  // Show-all toggle (Pro-only). Flips live_only=false on backend.
  const showAllLink = $('sealed-show-all-toggle');
  if (showAllLink) {
    showAllLink.addEventListener('click', (e) => {
      e.preventDefault();
      const isPro = State.user && (State.user.tier === 'pro' || State.user.tier === 'elite');
      if (!isPro) {
        showUpgradeModal('pro');
        return;
      }
      State.sealed.showAll = !State.sealed.showAll;
      showAllLink.textContent = State.sealed.showAll
        ? 'Show live only'
        : 'Show all (Pro)';
      loadSealedData();
    });
  }
}

async function loadSealedData() {
  if (State.sealed.loading) return;
  State.sealed.loading = true;
  showLoadingBar();

  const sectionsEl = $('sealed-sections');
  if (sectionsEl) sectionsEl.innerHTML = skeletonProductCards(8);

  const { filters, sort, showAll } = State.sealed;
  const params = new URLSearchParams({ sort, limit: '200' });
  if (filters.set  !== 'all') params.set('set_code', filters.set);
  if (filters.type !== 'all') params.set('product_type', filters.type);
  if (filters.lang !== 'all') params.set('language', filters.lang);
  // Default: live_only=true. Pro+ may flip to false to see reference-only sealed.
  params.set('live_only', showAll ? 'false' : 'true');

  try {
    const data = await apiFetch(`/api/cards/sealed?${params}`);
    State.sealed.products = data.products || [];
    State.sealed.total    = data.total || 0;
    State.sealed.lastData = data;
    renderSealedSections(data);
    try {
      trackEvent('sealed_view_load', {
        total: data.total, tier: data.tier, live_only: data.live_only,
        filters: { set: filters.set, type: filters.type, lang: filters.lang },
      });
    } catch (_) {}
  } catch (err) {
    showToast(err.message, 'error');
    if (sectionsEl) sectionsEl.innerHTML = `<div class="empty-state">
      <div class="empty-icon">⚠️</div>
      <div class="empty-title">Failed to load</div>
      <div class="empty-desc">${escHtml(err.message)}</div>
    </div>`;
  } finally {
    State.sealed.loading = false;
    hideLoadingBar();
  }
}

/* Group products by lang+type, render a header per section + the cards grid. */
function renderSealedSections(data) {
  const products = data.products || [];
  const root = $('sealed-sections');
  if (!root) return;

  const countEl = $('sealed-count');
  if (countEl) {
    const liveLabel = data.live_only ? 'live' : 'all (incl. reference)';
    countEl.textContent = `${fmt.int(data.total || products.length)} ${liveLabel} products`;
  }

  if (!products.length) {
    root.innerHTML = `<div class="empty-state">
      <div class="empty-icon">📦</div>
      <div class="empty-title">No sealed products with live data</div>
      <div class="empty-desc">Try changing the set filter, or click "Show all" if you're on Pro.</div>
    </div>`;
    return;
  }

  // Bucket: 'box-EN', 'box-JP', 'case-JP', 'case-EN', or 'other'
  const buckets = { 'box-EN': [], 'box-JP': [], 'case-JP': [], 'case-EN': [], 'other': [] };
  products.forEach(p => {
    const lang = (p.language || 'JP').toUpperCase();
    const pt = (p.product_type || '').toLowerCase();
    if (pt === 'booster box' && lang === 'EN') buckets['box-EN'].push(p);
    else if (pt === 'booster box' && lang === 'JP') buckets['box-JP'].push(p);
    else if (pt === 'case' && lang === 'JP') buckets['case-JP'].push(p);
    else if (pt === 'case' && lang === 'EN') buckets['case-EN'].push(p);
    else buckets['other'].push(p);
  });

  const sectionDefs = [
    ['box-EN', '🇬🇧 Booster Boxes (EN)', 'Sealed English booster boxes with live Cardmarket data'],
    ['box-JP', '🇯🇵 Booster Boxes (JP)', 'Sealed Japanese booster boxes with live Cardmarket data'],
    ['case-JP', '📦 Cases (JP)', 'Full sealed cases (12 boxes) with live Cardmarket data'],
    ['case-EN', '📦 Cases (EN)', 'Full sealed cases (12 boxes) with live Cardmarket data'],
    ['other', '… Other Sealed', 'Reference-only or other sealed products'],
  ];

  const html = sectionDefs.map(([key, title, desc]) => {
    const items = buckets[key];
    if (!items.length) return '';
    const cards = items.map(renderSealedCard).join('');
    return `
      <div class="sealed-section">
        <div class="sealed-section-header">
          <h3 class="sealed-section-title">${escHtml(title)}
            <span class="sealed-section-count">${items.length}</span>
          </h3>
          <div class="sealed-section-desc">${escHtml(desc)}</div>
        </div>
        <div class="product-grid sealed-section-grid">${cards}</div>
      </div>
    `;
  }).join('');

  root.innerHTML = html;

  // Wire up card clicks for telemetry + EV modal
  root.querySelectorAll('[data-sealed-ev]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const sc = btn.dataset.setCode;
      const lang = btn.dataset.lang;
      const pt = btn.dataset.type;
      openSealedEvModal(sc, lang, pt);
    });
  });
  root.querySelectorAll('[data-sealed-card]').forEach(card => {
    card.addEventListener('click', () => {
      try { trackEvent('sealed_card_clicked', {
        set_code: card.dataset.setCode, language: card.dataset.lang,
        product_type: card.dataset.type,
      }); } catch (_) {}
    });
  });
}

/* Render a single sealed-card tile. */
function renderSealedCard(p) {
  const lang = (p.language || 'JP').toUpperCase();
  const langBadge = lang === 'EN'
    ? `<span class="sealed-badge sealed-badge-en">🇬🇧 EN</span>`
    : `<span class="sealed-badge sealed-badge-jp">🇯🇵 JP</span>`;
  const liveBadge = p.has_live
    ? `<span class="sealed-badge sealed-badge-live" title="Live Cardmarket data">🎯 LIVE</span>`
    : `<span class="sealed-badge sealed-badge-ref" title="Reference price only — no live Cardmarket data">REF</span>`;

  // Spread badge: only when >= 10%
  const spreadBadge = (p.spread_pct != null && p.spread_pct >= 10)
    ? `<span class="sealed-spread-badge" title="Lowest is ${p.spread_pct}% below trend">↓ ${p.spread_pct}%</span>`
    : '';

  // EV badge: only on booster boxes/cases with persisted EV
  let evBadge = '';
  if (p.ev_eur != null && p.ev_pct != null) {
    const pos = p.ev_pct >= 0;
    const sign = pos ? '+' : '';
    const cls = pos ? 'ev-positive' : 'ev-negative';
    const tip = `Estimated value if opened: €${(p.ev_eur || 0).toFixed(2)} vs box price €${(p.cm_live_trend || 0).toFixed(2)} (estimate)`;
    evBadge = `<button class="sealed-ev-badge ${cls}" data-sealed-ev data-set-code="${escHtml(p.set_code)}" data-lang="${escHtml(lang)}" data-type="${escHtml(p.product_type || 'booster box')}" title="${escHtml(tip)}">EV ${sign}${p.ev_pct.toFixed(0)}%</button>`;
  }

  const cmLink = p.cm_live_url || p.links?.cardmarket || '';
  const updated = p.cm_live_updated_at ? formatRelativeTime(p.cm_live_updated_at) : '';
  const listings = p.cm_live_available != null ? `${p.cm_live_available} listings` : '';
  const lowest = p.cm_live_lowest != null ? `Lowest ${fmt.eur(p.cm_live_lowest)}` : '';
  const avg7d = p.cm_live_7d_avg != null ? `7d Ø ${fmt.eur(p.cm_live_7d_avg)}` : '';
  const trendPrice = p.eu_price != null ? fmt.eur(p.eu_price) : '—';

  return `
    <div class="sealed-card" data-sealed-card data-set-code="${escHtml(p.set_code || '')}" data-lang="${escHtml(lang)}" data-type="${escHtml(p.product_type || '')}">
      <div class="sealed-card-img">
        ${p.image_url
          ? `<img src="${escHtml(proxyImg(p.image_url))}" alt="${escHtml(p.product_name || '')}" loading="lazy" onerror="this.style.display='none'" />`
          : `<div class="sealed-card-img-placeholder">📦</div>`
        }
        <div class="sealed-card-badges-tl">${langBadge}${liveBadge}</div>
        <div class="sealed-card-badges-tr">${spreadBadge}${evBadge}</div>
      </div>
      <div class="sealed-card-body">
        <div class="sealed-card-title">${escHtml(p.product_name || 'Sealed Product')}</div>
        <div class="sealed-card-set">
          <span class="set-pill">${escHtml(p.set_code || '')}</span>
          <span class="sealed-card-setname">${escHtml(p.set_name || '')}</span>
        </div>
        <div class="sealed-card-price-row">
          <div class="sealed-card-price-main">${trendPrice}</div>
          <div class="sealed-card-price-label">Cardmarket trend</div>
        </div>
        <div class="sealed-card-stats">
          ${lowest ? `<span>${lowest}</span>` : ''}
          ${avg7d ? `<span>${avg7d}</span>` : ''}
          ${listings ? `<span>${listings}</span>` : ''}
        </div>
        ${updated ? `<div class="sealed-card-updated">Updated ${escHtml(updated)}</div>` : ''}
        <div class="sealed-card-actions">
          ${cmLink
            ? `<a class="sealed-card-cta" href="${escHtml(cmLink)}" target="_blank" rel="noopener nofollow">View on Cardmarket ↗</a>`
            : `<span class="sealed-card-cta sealed-card-cta-disabled">No Cardmarket link</span>`}
        </div>
      </div>
    </div>
  `;
}

function formatRelativeTime(iso) {
  try {
    const t = new Date(iso).getTime();
    if (!t) return '';
    const diffMs = Date.now() - t;
    const m = Math.floor(diffMs / 60000);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  } catch (_) { return ''; }
}

/* EV detail modal: per-rarity breakdown */
async function openSealedEvModal(setCode, language, productType) {
  let modal = $('sealed-ev-modal');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'sealed-ev-modal';
    modal.className = 'sealed-ev-modal-overlay';
    document.body.appendChild(modal);
  }
  modal.innerHTML = `<div class="sealed-ev-modal">
    <div class="sealed-ev-modal-header">
      <div>
        <div class="sealed-ev-modal-title">Sealed EV — ${escHtml(setCode)} ${escHtml(language)}</div>
        <div class="sealed-ev-modal-sub">Loading per-rarity breakdown…</div>
      </div>
      <button class="sealed-ev-modal-close" aria-label="Close">×</button>
    </div>
    <div class="sealed-ev-modal-body"><div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-title">Computing…</div></div></div>
  </div>`;
  modal.style.display = 'flex';
  const closeFn = () => { modal.style.display = 'none'; };
  modal.querySelector('.sealed-ev-modal-close').addEventListener('click', closeFn);
  modal.addEventListener('click', (e) => { if (e.target === modal) closeFn(); });

  try {
    const params = new URLSearchParams({ language, product_type: productType || 'booster box' });
    const data = await apiFetch(`/api/cards/sealed/ev/${encodeURIComponent(setCode)}?${params}`);
    renderSealedEvModal(modal, data);
  } catch (err) {
    modal.querySelector('.sealed-ev-modal-body').innerHTML = `<div class="empty-state">
      <div class="empty-icon">⚠️</div>
      <div class="empty-title">Failed to compute EV</div>
      <div class="empty-desc">${escHtml(err.message || 'Unknown error')}</div>
    </div>`;
  }
}

function renderSealedEvModal(modal, data) {
  const sub = modal.querySelector('.sealed-ev-modal-sub');
  const body = modal.querySelector('.sealed-ev-modal-body');
  const evBox = data.ev_per_box_eur || 0;
  const boxPrice = data.box_price_eur;
  const evPct = data.ev_pct;
  const evMinus = data.ev_minus_box;
  const sign = (evPct != null && evPct >= 0) ? '+' : '';
  const cls = (evPct != null && evPct >= 0) ? 'ev-positive' : 'ev-negative';

  if (sub) sub.textContent = `${data.product_name || ''} · ${data.packs_per_unit} packs/${data.product_type === 'case' ? 'case' : 'box'} · estimate`;

  const headStats = `
    <div class="sealed-ev-stats">
      <div class="sealed-ev-stat">
        <div class="sealed-ev-stat-label">EV per ${data.product_type === 'case' ? 'case' : 'box'}</div>
        <div class="sealed-ev-stat-value">${fmt.eur(evBox)}</div>
      </div>
      <div class="sealed-ev-stat">
        <div class="sealed-ev-stat-label">Box price (Cardmarket trend)</div>
        <div class="sealed-ev-stat-value">${boxPrice != null ? fmt.eur(boxPrice) : '—'}</div>
      </div>
      <div class="sealed-ev-stat">
        <div class="sealed-ev-stat-label">EV − box</div>
        <div class="sealed-ev-stat-value ${cls}">${evMinus != null ? (evMinus >= 0 ? '+' : '') + fmt.eur(evMinus) : '—'} <span style="font-size:11px;font-weight:600;">${evPct != null ? `(${sign}${evPct.toFixed(1)}%)` : ''}</span></div>
      </div>
      <div class="sealed-ev-stat">
        <div class="sealed-ev-stat-label">EV per pack</div>
        <div class="sealed-ev-stat-value">${fmt.eur(data.ev_per_pack_eur)}</div>
      </div>
    </div>
  `;

  const breakdown = (data.rarities_breakdown || []).map(r => {
    const med = r.median_eur != null ? fmt.eur(r.median_eur) : '<span class="muted">no live data</span>';
    const contrib = r.ev_contribution != null ? fmt.eur(r.ev_contribution) : '€0.00';
    return `<tr>
      <td>${escHtml(r.rarity)}</td>
      <td style="text-align:right;">${(r.pull_rate * 100).toFixed(2)}%</td>
      <td style="text-align:right;">${med}</td>
      <td style="text-align:right;">${r.sample_size || 0}</td>
      <td style="text-align:right;font-weight:700;">${contrib}</td>
    </tr>`;
  }).join('');

  body.innerHTML = `
    ${headStats}
    <table class="sealed-ev-table">
      <thead>
        <tr>
          <th>Rarity</th>
          <th style="text-align:right;">Pull/pack</th>
          <th style="text-align:right;">Median €</th>
          <th style="text-align:right;">Sample</th>
          <th style="text-align:right;">EV/pack</th>
        </tr>
      </thead>
      <tbody>${breakdown}</tbody>
    </table>
    <div class="sealed-ev-disclaimer">
      ⚠ Estimate. Pull rates are community-derived (~50 box openings sample); Bandai does not
      publish official rates for OPTCG. Median trend is computed across
      <strong>cards_investable</strong> live Cardmarket data. Real-world realisation will be lower
      — you can't always sell every card at trend.
    </div>
  `;
}

/* @deprecated kept only to avoid breaking calls from other places */
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
    // LIVE badge: prominently show when we have a real Cardmarket trend
    const liveBadge = p.has_live
      ? `<span title="Live Cardmarket price (scraped within 24h)" style="background:rgba(0,229,192,0.18);color:#00e5c0;padding:2px 6px;border-radius:4px;font-size:9px;font-weight:800;letter-spacing:0.08em;border:1px solid rgba(0,229,192,0.35);">🎯 LIVE</span>`
      : `<span title="Reference price only — no live Cardmarket data yet" style="background:rgba(255,255,255,0.05);color:#9aa0a6;padding:2px 6px;border-radius:4px;font-size:9px;font-weight:700;letter-spacing:0.08em;border:1px solid rgba(255,255,255,0.08);">ref</span>`;
    // Prefer the direct scraped Cardmarket URL when we have one
    const cmLink = p.cm_live_url || p.links?.cardmarket;
    return `
    <div class="product-card">
      <div class="product-img-wrap">
        ${p.image_url
          ? `<img src="${escHtml(proxyImg(p.image_url))}" alt="${escHtml(p.product_name || '')}" loading="lazy" onerror="this.style.display='none'" />`
          : `<div class="product-img-placeholder">${getTypeEmoji(p.product_type)}</div>`
        }
        <div class="product-type-tag">${escHtml(p.product_type || 'product')}</div>
        <div style="position:absolute;top:8px;left:8px;display:flex;gap:4px;">${langBadge}${liveBadge}</div>
      </div>
      <div class="product-body">
        <div>
          <div class="product-name">${escHtml(p.product_name || 'Unknown Product')}</div>
          <div class="product-set">${getTypeEmoji(p.product_type)} ${escHtml(p.set_name || '')} (${escHtml(p.set_code || '')})</div>
        </div>
        <div class="product-price-section">
          <div class="product-price-label">
            <span>${lang === 'EN' ? '🇬🇧' : '🇯🇵'}</span>
            <span>${lang} · ${p.has_live ? 'CARDMARKET LIVE' : 'REFERENCE'}</span>
          </div>
          <div class="product-price-main">${fmt.eurAuto(p.eu_price)}</div>
          <div class="product-price-stats">
            ${p.cm_live_lowest != null ? `<span title="Lowest current listing">From ${fmt.eurAuto(p.cm_live_lowest)}</span>` : ''}
            ${p.cm_live_available != null ? `<span title="Items available on Cardmarket">${p.cm_live_available} offers</span>` : ''}
            ${p.eu_7d_avg != null && p.eu_7d_avg !== p.eu_price ? `<span>7d ${fmt.eurAuto(p.eu_7d_avg)}</span>` : ''}
            ${!p.has_live && p.en_price_usd != null ? `<span>USD ${fmt.usd(p.en_price_usd)}</span>` : ''}
            ${p.eu_trend ? trendIcon(p.eu_trend) : ''}
          </div>
          ${p.eu_source ? `<div style="font-family:var(--font-mono);font-size:9px;color:${p.has_live ? 'var(--accent,#00e5c0)' : 'var(--muted)'};margin-top:4px;">${escHtml(p.eu_source)}</div>` : ''}
        </div>
        ${cmLink
          ? `<a class="product-link" href="${cmLink}" target="_blank" rel="noopener nofollow" title="Buy on Cardmarket"><span>↗</span> Buy on Cardmarket</a>`
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
    limit:  100,
    offset: offset,
  });
  if (filters.source === 'live') params.set('live_only', 'true');
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
  let opps = data.opportunities || [];
  // Live-only is now applied server-side via live_only=true; no client-side
  // filtering needed. The 'source' filter purely controls which API call we made.

  // Stat cards
  const liveCount = opps.filter(o => o.is_live).length;
  const profits = opps.map(o => o.profit_eur).filter(v => v != null);
  const totalProfit = profits.reduce((a,b) => a+b, 0);
  const spreads = opps.map(o => o.profit_pct).filter(v => v != null);
  const bestSpread = spreads.length ? Math.max(...spreads) : 0;
  const bestProfitCard = opps.length ? opps.reduce((a,b) => (b.profit_eur || 0) > (a.profit_eur || 0) ? b : a) : null;

  const statsEl = $('arb-stats');
  if (statsEl) {
    statsEl.innerHTML = `
      <div class="stat-card">
        <div class="stat-label">Opportunities</div>
        <div class="stat-value accent">${fmt.int(opps.length)}</div>
        <div class="stat-sub">${State.arbitrage.filters.source === 'live' ? 'LIVE only' : 'all sources'} · ≥ ${State.arbitrage.filters.minSpread}% profit</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">LIVE Verified</div>
        <div class="stat-value positive">${fmt.int(liveCount)}</div>
        <div class="stat-sub">both JP+EN live prices</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Best Profit</div>
        <div class="stat-value positive">${bestProfitCard ? fmt.eurAuto(bestProfitCard.profit_eur) : '—'}</div>
        <div class="stat-sub">${bestProfitCard ? escHtml((bestProfitCard.name || '').slice(0, 24)) : 'no opportunities'}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Best Spread</div>
        <div class="stat-value">${fmt.pct(bestSpread)}</div>
        <div class="stat-sub">ROI after fees</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Total Profit Pool</div>
        <div class="stat-value">${fmt.eurAuto(totalProfit)}</div>
        <div class="stat-sub">sum of all opps</div>
      </div>
    `;
  }

  // Transparent note explaining the data source
  const noteEl = $('arb-note');
  if (noteEl) {
    const isLive = State.arbitrage.filters.source === 'live';
    if (isLive) {
      noteEl.innerHTML = `<strong>Verified live arbitrage only.</strong> ${data.total || 0} pairs where BOTH JP and EN sides have current Cardmarket listings. Switch to <a href="#" data-arb-source="all" style="color:var(--accent);">all sources</a> for a wider view (mix of live + reference).`;
      noteEl.style.display = 'block';
    } else {
      noteEl.innerHTML = `<strong>Showing all sources.</strong> ${data.total || 0} pairs incl. reference data (PriceCharting USD→EUR). Switch to <a href="#" data-arb-source="live" style="color:var(--accent);">live only</a> for fully verified opportunities.`;
      noteEl.style.display = 'block';
    }
    // Wire toggle links
    noteEl.querySelectorAll('[data-arb-source]').forEach(a => {
      a.onclick = (e) => {
        e.preventDefault();
        State.arbitrage.filters.source = a.dataset.arbSource;
        State.arbitrage.offset = 0;
        loadArbitrageData();
      };
    });
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
            ${o.links?.cardmarket_jp
              ? `<a class="price-val price-link" href="${o.links.cardmarket_jp}" target="_blank" rel="noopener nofollow" title="Buy JP on Cardmarket">${fmt.eurAuto(o.jp_price_eur)} ↗</a>`
              : `<div class="price-val">${fmt.eurAuto(o.jp_price_eur)}</div>`}
            <div class="price-sub">${o.jp_source === 'live' ? '<span class="live-badge">🎯 LIVE</span>' : '🇯🇵 Reference'}</div>
          </div>
        </td>
        <td class="col-eu">
          <div class="price-cell">
            ${o.links?.cardmarket_en
              ? `<a class="price-val price-link" href="${o.links.cardmarket_en}" target="_blank" rel="noopener nofollow" title="Sell on Cardmarket EN">${fmt.eurAuto(o.en_price_eur)} ↗</a>`
              : `<div class="price-val">${fmt.eurAuto(o.en_price_eur)}</div>`}
            <div class="price-sub">${o.en_source === 'live' ? '<span class="live-badge">🎯 LIVE</span>' : '🇬🇧 Reference'}</div>
          </div>
        </td>
        <td>
          <span class="${o.spread_ratio >= 2 ? 'spread-positive' : 'spread-neutral'}" style="font-family:var(--font-mono);font-weight:700;">${o.spread_ratio ? o.spread_ratio.toFixed(1) + 'x' : '—'}</span>
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
    // Fetch in parallel: market-summary, overview, AND today's radar signals
    const [summary, overview, radar] = await Promise.all([
      apiFetch('/api/cards/market-summary').catch(() => null),
      apiFetch('/api/market/overview').catch(() => null),
      apiFetch('/api/radar/today').catch(() => null),
    ]);

    const data = {
      stats:         overview?.stats || summary || {},
      top_valuable:  overview?.top_valuable || [],
      arbitrage:     overview?.arbitrage  || [],
      recent_sets:   overview?.recent_sets || [],
      radar:         radar || null,
    };

    State.overview.data = data;
    renderBriefingTop(radar);
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

function renderBriefingTop(radarData) {
  const el = $('briefing-signals');
  const dateEl = $('briefing-date');
  if (!el) return;

  // Update date label (e.g. "Today · Sun, Apr 26")
  if (dateEl) {
    const today = new Date();
    const opts = { weekday: 'short', month: 'short', day: 'numeric' };
    dateEl.textContent = `Today · ${today.toLocaleDateString('en-US', opts)}`;
  }

  // Wire "All signals →" link
  document.querySelectorAll('[data-tab-link]').forEach(a => {
    a.onclick = (e) => { e.preventDefault(); switchTab(a.dataset.tabLink); };
  });

  if (!radarData || radarData.upgrade_required) {
    const tier = radarData?.tier || 'free';
    if (tier === 'free') {
      el.innerHTML = `
        <div class="briefing-paywall">
          <div class="briefing-paywall-title">Daily signals are a Pro feature</div>
          <div class="briefing-paywall-desc">Get personalized price drops, fair-value opportunities and portfolio P&amp;L every day.</div>
          <button onclick="openUpgradeModal('pro')" class="btn-primary" style="border:none;cursor:pointer;">Upgrade to Pro</button>
        </div>`;
    } else {
      el.innerHTML = `<div class="briefing-empty">No signals computed yet today — check back after the nightly refresh.</div>`;
    }
    return;
  }

  const signals = (radarData.signals || []).slice(0, 3);
  if (!signals.length) {
    el.innerHTML = `<div class="briefing-empty">The market is quiet today. No signals fired — a good day to hold.</div>`;
    return;
  }

  // Locale
  const lang = (navigator.language || 'en').toLowerCase().startsWith('de') ? 'de' : 'en';
  const typeLabels = {
    price_drop:    { en: 'Price drop', de: 'Preisrückgang' },
    fv_deviation:  { en: 'Below fair value', de: 'Unter Fair Value' },
    portfolio_pnl: { en: 'Portfolio change', de: 'Änderung Portfolio' },
  };

  el.innerHTML = `<div class="briefing-grid">${signals.map((s, i) => {
    const p = s.payload || {};
    const text = (p.wording && (p.wording[lang] || p.wording.en || p.wording.de))
                 || `${s.signal_type} on ${s.entity_id}`;
    const tlabel = (typeLabels[s.signal_type] && typeLabels[s.signal_type][lang]) || s.signal_type;
    return `
      <div class="briefing-card" data-signal-id="${s.id}" data-entity-type="${s.entity_type}" data-entity-id="${escHtml(s.entity_id)}">
        <div class="briefing-card-rank">${i + 1}</div>
        <div class="briefing-card-body">
          <div class="briefing-card-type">
            <span class="briefing-dot ${s.severity}" aria-hidden="true"></span>
            ${tlabel}
          </div>
          <div class="briefing-card-headline">${escHtml(text)}</div>
          ${p.set_code ? `<div class="briefing-card-meta">${escHtml(p.set_code)} · ${escHtml(p.card_id || '')}</div>` : ''}
        </div>
      </div>`;
  }).join('')}</div>`;

  // Click handlers — reuse the radar click logic
  $$('.briefing-card', el).forEach(card => {
    card.addEventListener('click', () => {
      // Wire row to radar's onClick handler
      onRadarRowClick(card);
    });
  });
}
window.renderBriefingTop = renderBriefingTop;

function renderOverview(data) {
  const stats = data.stats || {};

  // ─── Hero bar: market-wide stats based on LIVE data ──────────────────
  const heroEl = $('overview-hero');
  if (heroEl) {
    const setsTracked = Number(stats.total_sets) || 0;
    const cardsIndexed = Number(stats.total_cards) || 0;
    const cardsLive = Number(stats.cards_with_live) || 0;
    const fresh = Number(stats.fresh_prices) || 0;
    const coverage = cardsIndexed > 0 ? Math.round((cardsLive / cardsIndexed) * 100) : 0;
    const lastScrape = stats.last_scrape;

    heroEl.innerHTML = `
      <div class="overview-hero-card accent-border">
        <div class="stat-label">Live Prices</div>
        <div class="stat-value accent">${fmt.int(cardsLive)}</div>
        <div class="stat-sub">cards with live Cardmarket data</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Fresh (&lt; 48h)</div>
        <div class="stat-value positive">${fmt.int(fresh)}</div>
        <div class="stat-sub">scraped in last 2 days</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Sets Covered</div>
        <div class="stat-value">${fmt.int(setsTracked)}</div>
        <div class="stat-sub">${fmt.int(cardsIndexed)} cards total</div>
      </div>
      <div class="overview-hero-card">
        <div class="stat-label">Last Scrape</div>
        <div class="stat-value" style="font-size:14px;">${lastScrape ? _relativeTime(lastScrape) : '—'}</div>
        <div class="stat-sub">${lastScrape ? new Date(lastScrape).toLocaleString() : 'Never'}</div>
      </div>
    `;
  }

  // ─── Top valuable live Alt-Arts ──────────────────────────────────────
  const valEl = $('overview-valuable');
  if (valEl) {
    if (data.top_valuable && data.top_valuable.length) {
      valEl.innerHTML = data.top_valuable.map((c, i) => {
        const imgProxied = c.image_url ? proxyImg(c.image_url) : '';
        const thumb = imgProxied
          ? `<img class="ov-card-thumb" src="${escHtml(imgProxied)}" alt="${escHtml(c.name)}" loading="lazy" onerror="this.style.display='none'"/>`
          : `<div class="ov-card-thumb ov-card-thumb-placeholder">🃏</div>`;
        return `
        <a class="ov-row" href="${escHtml(c.cm_live_url || '#')}" target="_blank" rel="noopener nofollow" title="Open on Cardmarket">
          <div class="ov-rank">#${i+1}</div>
          ${thumb}
          <div class="ov-info">
            <div class="ov-name">${escHtml(c.name || 'Unknown')} <span class="ov-variant">${escHtml(c.variant || '')}</span></div>
            <div class="ov-meta">${escHtml(c.set_code || '')} · ${escHtml(c.language || 'EN')} · ${fmt.int(c.cm_live_available || 0)} available</div>
          </div>
          <div class="ov-price">${fmt.eurAuto(c.cm_live_trend)}</div>
        </a>`;
      }).join('');
    } else {
      valEl.innerHTML = `<div class="empty-state" style="padding:24px 0;">
        <div style="color:var(--muted);font-size:13px;">No live-priced alt-arts yet — scraper is still backfilling.</div>
      </div>`;
    }
  }

  // ─── JP → EN Arbitrage movers (live spreads) ────────────────────────
  const moversEl = $('overview-movers');
  if (moversEl) {
    const movers = data.arbitrage || [];
    if (movers.length) {
      moversEl.innerHTML = movers.map((m, i) => {
        const imgProxied = m.image_url ? proxyImg(m.image_url) : '';
        const thumb = imgProxied
          ? `<img class="ov-card-thumb" src="${escHtml(imgProxied)}" alt="${escHtml(m.name)}" loading="lazy" onerror="this.style.display='none'"/>`
          : `<div class="ov-card-thumb ov-card-thumb-placeholder">🃏</div>`;
        const ratio = Number(m.ratio) || 0;
        return `
        <div class="ov-row">
          <div class="ov-rank">#${i+1}</div>
          ${thumb}
          <div class="ov-info">
            <div class="ov-name">${escHtml(m.name || 'Unknown')} <span class="ov-variant">${escHtml(m.variant || '')}</span></div>
            <div class="ov-meta">${escHtml(m.set_code || '')} · JP ${fmt.eurAuto(m.jp_price)} → EN ${fmt.eurAuto(m.en_price)}</div>
          </div>
          <div class="ov-price positive">+${fmt.eurAuto(m.spread_eur)}<div class="ov-meta" style="text-align:right;">${ratio.toFixed(1)}×</div></div>
        </div>`;
      }).join('');
    } else {
      moversEl.innerHTML = `<div class="empty-state" style="padding:24px 0;">
        <div style="color:var(--muted);font-size:13px;">No qualifying arbitrage pairs yet — need live prices on both JP+EN of the same card.</div>
      </div>`;
    }
  }

  // Sets coverage
  const setsEl = $('overview-sets');
  if (setsEl) {
    const sets = data.recent_sets || [];
    if (sets.length) {
      setsEl.innerHTML = sets.map(s => {
        const live = Number(s.live_count) || 0;
        const total = Number(s.card_count) || 0;
        const pct = total > 0 ? Math.round((live / total) * 100) : 0;
        const avg = s.avg_price ? `Ø ${fmt.eurAuto(Number(s.avg_price))}` : '';
        return `
        <div class="set-coverage-item">
          <div class="set-coverage-code">${escHtml(s.set_code || '')}</div>
          <div class="set-coverage-bar-wrap">
            <div class="set-coverage-bar" style="width:${pct}%"></div>
          </div>
          <div class="set-coverage-stats">
            <span class="set-coverage-live">${fmt.int(live)}</span>
            <span class="set-coverage-sep">/</span>
            <span class="set-coverage-total">${fmt.int(total)}</span>
            <span class="set-coverage-avg">${avg}</span>
          </div>
        </div>`;
      }).join('');
    } else {
      setsEl.innerHTML = `<div style="color:var(--muted);font-size:13px;padding:16px 0;">No sets data</div>`;
    }
  }
}

// Relative time: '5 min ago', '2 h ago', '1 d ago'
function _relativeTime(iso) {
  if (!iso) return '—';
  const diff = (Date.now() - new Date(iso).getTime()) / 1000; // seconds
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.round(diff/60)} min ago`;
  if (diff < 86400) return `${Math.round(diff/3600)} h ago`;
  return `${Math.round(diff/86400)} d ago`;
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
    releaseModalFocus(overlay);
  }
  if (priceChart) { priceChart.destroy(); priceChart = null; }
  priceModalCard = null;
}

function showPriceHistoryModal(card) {
  priceModalCard = card;
  const overlay = $('price-modal-overlay');
  if (!overlay) return;
  overlay._a11y_restore = document.activeElement;

  // Set header info
  const titleEl = $('price-modal-title');
  const subEl = $('price-modal-subtitle');
  if (titleEl) titleEl.textContent = card.name || card.card_id || 'Price History';
  if (subEl) subEl.textContent = [card.card_id, card.set_code, card.rarity].filter(Boolean).join(' · ');

  overlay.setAttribute('aria-hidden', 'false');
  overlay.classList.add('open');
  trapModalFocus(overlay);

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

    // Load Sealed Holdings (Phase C — free via Holygrade purchase hook)
    loadSealedPortfolio();

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
        <button onclick="openUpgradeModal('pro')" class="btn-primary" style="margin-top:12px;display:inline-block;border:none;cursor:pointer;">Upgrade</button>
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

    // Prefer live Cardmarket price; fall back to reference
    let priceHtml = '—';
    let sourceLabel = '';
    if (it.cm_live_trend != null) {
      const href = it.cm_live_url || '#';
      priceHtml = `<a class="price-link" href="${escHtml(href)}" target="_blank" rel="noopener nofollow" style="color:var(--accent);text-decoration:none;">${fmt.eurAuto(it.cm_live_trend)} ↗</a>`;
      sourceLabel = '<span class="live-badge" style="font-size:9px;">🎯 LIVE</span>';
    } else if (it.eu_cardmarket_7d_avg != null) {
      priceHtml = fmt.eurAuto(it.eu_cardmarket_7d_avg);
      sourceLabel = '<span style="font-size:9px;color:var(--muted);">Reference</span>';
    } else if (it.en_tcgplayer_market != null) {
      priceHtml = fmt.eurAuto(it.en_tcgplayer_market * State.usdToEur);
      sourceLabel = '<span style="font-size:9px;color:var(--muted);">Reference (EN)</span>';
    }

    return `<tr>
      <td data-label="Card">
        <div style="display:flex;align-items:center;gap:8px;">
          ${cardThumb(it.image_url, it.name)}
          <div>
            <div style="font-weight:500;">${escHtml(it.name)}</div>
            <div style="color:var(--muted);font-size:11px;">${escHtml(it.card_id)} ${rarityBadge(it.rarity)}</div>
          </div>
        </div>
      </td>
      <td data-label="Set">${escHtml(it.set_code || '')}</td>
      <td data-label="Qty">${it.quantity}</td>
      <td data-label="Buy Price">${fmt.eurAuto(it.buy_price)}</td>
      <td data-label="Market" class="col-eu">
        <div style="line-height:1.3;">${priceHtml}</div>
        <div style="margin-top:2px;">${sourceLabel}</div>
      </td>
      <td data-label="P&L" class="${pnlClass}">${it.pnl_eur != null ? fmt.eurAuto(it.pnl_eur) : '—'}</td>
      <td data-label="ROI" class="${roiClass}">${it.roi_pct != null ? fmt.pct(it.roi_pct) : '—'}</td>
      <td data-label="Action" class="actions">
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
  modal._a11y_restore = document.activeElement;
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
  trapModalFocus(modal);
}
window.openAddCardModal = openAddCardModal;

function closeAddCardModal() {
  const modal = $('add-card-modal');
  if (!modal) return;
  modal.style.display = 'none';
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
  releaseModalFocus(modal);
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

  dd.innerHTML = results.map(r => {
    // Prefer live Cardmarket trend, fall back to 7d reference
    const price = r.cm_live_trend != null
      ? r.cm_live_trend
      : (r.eu_cardmarket_7d_avg != null ? r.eu_cardmarket_7d_avg : null);
    const priceLabel = r.has_live
      ? `<span class="ac-live-badge" title="Live Cardmarket price">LIVE</span>`
      : `<span class="ac-ref-badge" title="Reference price (no live data yet)">ref</span>`;
    const langBadge = r.language ? `<span class="ac-lang">${escHtml(r.language)}</span>` : '';
    const metaParts = [r.card_id, r.set_code, r.rarity, r.variant].filter(Boolean).map(escHtml).join(' &middot; ');
    return `
    <div class="autocomplete-item" onclick="selectCard(${escHtml(JSON.stringify(JSON.stringify(r)))})">
      ${cardThumb(r.image_url, r.name)}
      <div class="ac-item-info">
        <div class="ac-item-name">${escHtml(r.name)} ${langBadge}</div>
        <div class="ac-item-meta">${metaParts}</div>
      </div>
      <div class="ac-item-price">
        ${price != null ? fmt.eur(price) : '—'}
        ${priceLabel}
      </div>
    </div>`;
  }).join('');
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

/* ══════════════════════════════════════════════════════════════════
   CHART MODAL — price history + TCG indicators
   ══════════════════════════════════════════════════════════════════ */
window.ChartModal = (function() {
  let chartInstance = null;
  let currentCardId = null;
  let currentVariant = 'Normal';
  let currentDays = 90;

  function fmtEur(v) {
    if (v == null) return '—';
    return '€' + Number(v).toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  function fmtPct(v) {
    if (v == null) return '—';
    const sign = v >= 0 ? '+' : '';
    return sign + Number(v).toFixed(1) + '%';
  }
  function tintPct(el, v) {
    el.classList.remove('positive', 'negative', 'neutral');
    if (v == null) { el.classList.add('neutral'); return; }
    el.classList.add(v >= 0.5 ? 'positive' : v <= -0.5 ? 'negative' : 'neutral');
  }

  async function open(cardId, variant = 'Normal', days = 90) {
    currentCardId = cardId;
    currentVariant = variant;
    currentDays = days;

    const modal = document.getElementById('chart-modal');
    modal.hidden = false;
    document.body.style.overflow = 'hidden';

    // Set initial loading state
    document.getElementById('chart-card-name').textContent = 'Loading…';
    document.getElementById('chart-card-meta').textContent = cardId;

    // Mark active range tab
    document.querySelectorAll('.range-tab').forEach(t => {
      t.classList.toggle('active', Number(t.dataset.range) === days);
    });

    await loadChartData();
  }

  function close() {
    document.getElementById('chart-modal').hidden = true;
    document.body.style.overflow = '';
    if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
  }

  async function loadChartData() {
    if (!currentCardId) return;
    const token = localStorage.getItem('optcg_token');
    const headers = token ? { 'Authorization': 'Bearer ' + token } : {};

    try {
      const url = `/api/cards/price-history/${encodeURIComponent(currentCardId)}?variant=${encodeURIComponent(currentVariant)}&days=${currentDays}`;
      const resp = await fetch(url, { headers });
      if (!resp.ok) throw new Error(`API ${resp.status}`);
      const data = await resp.json();
      renderAll(data);
    } catch (err) {
      document.getElementById('chart-card-name').textContent = 'Failed to load';
      document.getElementById('chart-card-meta').textContent = String(err);
      console.error('Chart load error:', err);
    }
  }

  function renderAll(d) {
    document.getElementById('chart-card-name').textContent = d.name || d.card_id;
    const metaParts = [d.card_id, d.set_code, d.rarity, d.variant].filter(Boolean);
    document.getElementById('chart-card-meta').textContent = metaParts.join(' · ');
    const img = document.getElementById('chart-card-img');
    if (d.image_url) { img.src = d.image_url; img.style.display = ''; }
    else { img.style.display = 'none'; }

    const ind = d.indicators || {};

    // Top stats
    document.getElementById('stat-eu').textContent = fmtEur(d.current_eu_price);
    document.getElementById('stat-en').textContent = d.current_en_price_usd != null ? '$' + Number(d.current_en_price_usd).toFixed(2) : '—';

    const s7 = document.getElementById('stat-7d');
    s7.textContent = fmtPct(ind.change_7d); tintPct(s7, ind.change_7d);
    const s30 = document.getElementById('stat-30d');
    s30.textContent = fmtPct(ind.change_30d); tintPct(s30, ind.change_30d);

    const rsiEl = document.getElementById('stat-rsi');
    if (ind.rsi != null) {
      rsiEl.textContent = ind.rsi.toFixed(1);
      rsiEl.className = 'stat-value ' + (ind.rsi < 30 ? 'positive' : ind.rsi > 70 ? 'negative' : 'neutral');
    } else { rsiEl.textContent = '—'; }

    // Signal badge
    const sig = ind.signal || {};
    const sigEl = document.getElementById('stat-signal');
    sigEl.textContent = sig.action || '—';
    sigEl.className = 'stat-value signal-' + (sig.action || 'hold').toLowerCase();

    // Indicator panels
    document.getElementById('ind-ma7').textContent = fmtEur(ind.ma_7d);
    document.getElementById('ind-ma30').textContent = fmtEur(ind.ma_30d);
    document.getElementById('ind-ma90').textContent = fmtEur(ind.ma_90d);
    const bb = ind.bollinger || {};
    document.getElementById('ind-bb-up').textContent = fmtEur(bb.upper);
    document.getElementById('ind-bb-mid').textContent = fmtEur(bb.middle);
    document.getElementById('ind-bb-lo').textContent = fmtEur(bb.lower);
    document.getElementById('ind-bb-pct').textContent = bb.pct_b != null ? bb.pct_b.toFixed(0) + '%' : '—';

    // TCG indicators
    const rr = ind.reprint_risk || {};
    const rrEl = document.getElementById('ind-reprint');
    rrEl.textContent = rr.level ? (rr.level + ' (' + rr.score + ')') : '—';
    rrEl.className = 'risk-' + (rr.level || '').toLowerCase();
    document.getElementById('ind-setage').textContent = rr.months_since_release != null ? rr.months_since_release.toFixed(1) + ' mo' : '—';

    const chg7 = document.getElementById('ind-chg7');
    chg7.textContent = fmtPct(ind.change_7d);
    chg7.className = ind.change_7d >= 0 ? 'positive' : 'negative';
    const chg30 = document.getElementById('ind-chg30');
    chg30.textContent = fmtPct(ind.change_30d);
    chg30.className = ind.change_30d >= 0 ? 'positive' : 'negative';

    // Signal reasons
    const reasonsEl = document.getElementById('signal-reasons');
    const reasons = sig.reasons || ['No strong signal'];
    reasonsEl.innerHTML = '<div class="signal-' + (sig.action || 'hold').toLowerCase() + '" style="font-size:14px;margin-bottom:8px;">' +
      (sig.action || 'HOLD') + ' · strength ' + (sig.strength || 0) + '/100</div>' +
      reasons.map(r => '<span class="reason">' + r + '</span>').join('');

    renderChart(d.history || [], ind);
  }

  function renderChart(history, ind) {
    const ctx = document.getElementById('price-chart').getContext('2d');
    if (chartInstance) chartInstance.destroy();

    const labels = history.map(h => h.date);
    const euPrices = history.map(h => h.eu_cardmarket_7d_avg);
    const enPrices = history.map(h => h.en_tcgplayer_market);

    // Compute rolling MAs for display (overlay)
    const ma7 = rollingMean(euPrices, 7);
    const ma30 = rollingMean(euPrices, 30);

    const datasets = [
      {
        label: 'EU · Cardmarket 7d',
        data: euPrices,
        borderColor: '#c9a84c',
        backgroundColor: 'rgba(201,168,76,0.08)',
        fill: true,
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 4,
        tension: 0.25,
      },
      {
        label: 'MA(7)',
        data: ma7,
        borderColor: '#60a5fa',
        backgroundColor: 'transparent',
        fill: false,
        borderWidth: 1,
        borderDash: [4, 4],
        pointRadius: 0,
        tension: 0.25,
      },
      {
        label: 'MA(30)',
        data: ma30,
        borderColor: '#a78bfa',
        backgroundColor: 'transparent',
        fill: false,
        borderWidth: 1,
        borderDash: [8, 4],
        pointRadius: 0,
        tension: 0.25,
      },
    ];

    if (enPrices.some(v => v != null)) {
      datasets.push({
        label: 'EN · TCGPlayer (USD)',
        data: enPrices,
        borderColor: '#4ade80',
        backgroundColor: 'transparent',
        fill: false,
        borderWidth: 1.5,
        pointRadius: 0,
        pointHoverRadius: 4,
        tension: 0.25,
        yAxisID: 'y1',
      });
    }

    const hasEn = enPrices.some(v => v != null);

    chartInstance = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            position: 'top',
            labels: {
              color: '#9ca3af',
              font: { family: 'IBM Plex Mono, monospace', size: 10 },
              boxWidth: 14,
              padding: 12,
            },
          },
          tooltip: {
            backgroundColor: '#0f1115',
            borderColor: '#2a2a2a',
            borderWidth: 1,
            titleColor: '#c9a84c',
            bodyColor: '#fff',
            titleFont: { family: 'IBM Plex Mono, monospace', size: 11 },
            bodyFont: { family: 'IBM Plex Mono, monospace', size: 11 },
            padding: 10,
            callbacks: {
              label: (ctx) => {
                const lbl = ctx.dataset.label || '';
                const v = ctx.parsed.y;
                if (v == null) return lbl + ': —';
                const isUsd = lbl.includes('USD');
                return lbl + ': ' + (isUsd ? '$' + v.toFixed(2) : '€' + v.toFixed(2));
              },
            },
          },
        },
        scales: {
          x: {
            ticks: {
              color: '#6b7280',
              font: { family: 'IBM Plex Mono, monospace', size: 9 },
              maxTicksLimit: 8,
            },
            grid: { color: 'rgba(255,255,255,0.03)' },
          },
          y: {
            position: 'left',
            ticks: {
              color: '#9ca3af',
              font: { family: 'IBM Plex Mono, monospace', size: 10 },
              callback: (v) => '€' + v.toFixed(0),
            },
            grid: { color: 'rgba(255,255,255,0.04)' },
          },
          y1: hasEn ? {
            position: 'right',
            ticks: {
              color: '#4ade80',
              font: { family: 'IBM Plex Mono, monospace', size: 10 },
              callback: (v) => '$' + v.toFixed(0),
            },
            grid: { drawOnChartArea: false },
          } : undefined,
        },
      },
    });
  }

  function rollingMean(arr, period) {
    const out = new Array(arr.length).fill(null);
    for (let i = period - 1; i < arr.length; i++) {
      let sum = 0, count = 0;
      for (let j = i - period + 1; j <= i; j++) {
        const v = arr[j];
        if (v != null) { sum += v; count++; }
      }
      if (count === period) out[i] = sum / period;
    }
    return out;
  }

  // Wire range tabs
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.range-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        const d = Number(tab.dataset.range);
        currentDays = d;
        document.querySelectorAll('.range-tab').forEach(t => t.classList.toggle('active', t === tab));
        loadChartData();
      });
    });
    // Escape closes
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && !document.getElementById('chart-modal').hidden) close();
    });
  });

  return { open, close };
})();

// Global helper for inline onclick
window.openChartModal = function(cardId, variant) { window.ChartModal.open(cardId, variant || 'Normal', 90); };
window.closeChartModal = function() { window.ChartModal.close(); };


/* ══════════════════════════════════════════════════════════════════
   SEALED PORTFOLIO (Phase C — Free, unlocked via Holygrade purchase)
   ══════════════════════════════════════════════════════════════════ */

async function loadSealedPortfolio() {
  const section = $('sealed-holdings-section');
  const summary = $('sealed-holdings-summary');
  const list    = $('sealed-holdings-list');
  if (!section || !summary || !list) return;

  // Hide by default; only show if user has at least one item.
  section.style.display = 'none';

  if (!State.user || !State.token) return;

  try {
    const res = await apiFetch('/api/portfolio/sealed');
    const items = (res && res.items) || [];
    if (!items.length) {
      section.style.display = 'none';
      return;
    }

    section.style.display = '';

    const s = res.summary || {};
    const fmt = (v) => fmtEurSafe(v);
    summary.innerHTML = `
      <div class="stat-card">
        <div class="stat-label">Boxes</div>
        <div class="stat-value">${items.length}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Bezahlt</div>
        <div class="stat-value">${fmt(s.total_paid_eur)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Aktueller Wert</div>
        <div class="stat-value">${fmt(s.total_value_eur)}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">P&L</div>
        <div class="stat-value" style="color:${(s.total_pl_eur ?? 0) >= 0 ? 'var(--positive,#4ade80)' : 'var(--negative,#e85a5a)'};">
          ${(s.total_pl_eur != null && s.total_pl_eur > 0) ? '+' : ''}${fmt(s.total_pl_eur)}
          ${s.total_pl_pct != null ? ` <span style="font-size:11px;opacity:.7;">(${s.total_pl_pct > 0 ? '+' : ''}${s.total_pl_pct.toFixed(1)}%)</span>` : ''}
        </div>
      </div>
    `;

    list.innerHTML = items.map((item) => renderSealedHoldingCard(item)).join('');
  } catch (err) {
    // Silent failure — sealed portfolio is optional/free, don't break main view.
    console.warn('loadSealedPortfolio failed:', err);
    section.style.display = 'none';
  }
}

function fmtEurSafe(v) {
  if (v == null) return '–';
  try {
    return new Intl.NumberFormat('de-DE', {
      style: 'currency', currency: 'EUR',
      minimumFractionDigits: 2, maximumFractionDigits: 2
    }).format(v);
  } catch (_) { return Number(v).toFixed(2) + ' €'; }
}

function renderSealedHoldingCard(item) {
  const plPositive = (item.pl_eur ?? 0) >= 0;
  const plColor = plPositive ? 'var(--positive,#4ade80)' : 'var(--negative,#e85a5a)';
  const plPrefix = (item.pl_eur != null && item.pl_eur > 0) ? '+' : '';
  const purchased = item.purchased_at
    ? new Date(item.purchased_at).toLocaleDateString('de-DE')
    : '–';
  const sparkline = renderSealedSparkline(item.history || []);

  return `
    <div style="display:flex;gap:12px;padding:12px;border:1px solid var(--border,#1e2a28);border-radius:8px;margin-bottom:8px;background:var(--surface,#161e1c);">
      ${item.image_url ? `<img src="${escAttr(item.image_url)}" alt="" style="width:60px;height:auto;border-radius:4px;flex-shrink:0;"/>` : ''}
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;font-size:14px;margin-bottom:2px;overflow:hidden;text-overflow:ellipsis;">
          ${escText(item.product_name || '–')}
        </div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--muted,#4a6660);text-transform:uppercase;letter-spacing:0.04em;margin-bottom:8px;">
          ${escText(item.set_code || '')} · ${escText(item.language || '')} · ${item.quantity}× · gekauft ${purchased}
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(80px,1fr));gap:8px;font-size:11px;">
          <div><div style="color:var(--muted,#4a6660);font-size:9px;text-transform:uppercase;letter-spacing:0.06em;">Bezahlt</div><div style="font-family:monospace;">${escText(fmtEurSafe(item.total_paid_eur))}</div></div>
          <div><div style="color:var(--muted,#4a6660);font-size:9px;text-transform:uppercase;letter-spacing:0.06em;">Heute</div><div style="font-family:monospace;">${escText(fmtEurSafe(item.current_value_eur))}</div></div>
          <div><div style="color:var(--muted,#4a6660);font-size:9px;text-transform:uppercase;letter-spacing:0.06em;">P&L</div><div style="font-family:monospace;color:${plColor};">${plPrefix}${escText(fmtEurSafe(item.pl_eur))}${item.pl_pct != null ? ` <span style="opacity:.7;">(${item.pl_pct > 0 ? '+' : ''}${item.pl_pct.toFixed(1)}%)</span>` : ''}</div></div>
        </div>
        ${sparkline}
      </div>
    </div>
  `;
}

function renderSealedSparkline(points) {
  const valid = (points || []).filter((p) => typeof p.trend_eur === 'number' && isFinite(p.trend_eur));
  if (valid.length < 7) {
    return `<div style="font-family:monospace;font-size:9px;color:var(--muted,#4a6660);margin-top:6px;">Verlauf wird aufgebaut · ${valid.length} Tag(e)</div>`;
  }
  const W = 240, H = 24, padX = 2, padY = 2;
  const values = valid.map((p) => p.trend_eur);
  let minV = Math.min.apply(null, values);
  let maxV = Math.max.apply(null, values);
  if (maxV === minV) maxV = minV + 1;
  const n = valid.length;
  const stepX = (W - 2 * padX) / Math.max(1, n - 1);
  const scaleY = (v) => H - padY - ((v - minV) / (maxV - minV)) * (H - 2 * padY);
  let d = '';
  for (let i = 0; i < n; i++) {
    const x = padX + i * stepX;
    const y = scaleY(values[i]);
    d += (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1) + ' ';
  }
  const firstV = values[0];
  const lastV = values[values.length - 1];
  const deltaPct = firstV > 0 ? ((lastV - firstV) / firstV * 100) : 0;
  const deltaCol = deltaPct > 0.5 ? 'var(--positive,#4ade80)' : (deltaPct < -0.5 ? 'var(--negative,#e85a5a)' : 'var(--muted,#4a6660)');
  const sign = deltaPct > 0 ? '+' : '';
  return `
    <div style="display:flex;align-items:center;gap:8px;margin-top:8px;">
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:140px;height:24px;flex-shrink:0;" aria-hidden="true">
        <path d="${d}" fill="none" stroke="#00e5c0" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
      <span style="font-family:monospace;font-size:10px;color:${deltaCol};">${sign}${deltaPct.toFixed(1)}% · 30T</span>
    </div>
  `;
}

function escAttr(s) {
  if (s == null) return '';
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;')
    .replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function escText(s) {
  if (s == null) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

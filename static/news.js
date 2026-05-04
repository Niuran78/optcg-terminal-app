/* ═══════════════════════════════════════════════════════════════
   NEWS PAGE — Holygrade Terminal /news
   Vanilla JS, mirrors existing app.js patterns.
   ═══════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  // ── State ───────────────────────────────────────────────────────
  let currentCat = 'all';
  let offset = 0;
  const LIMIT = 18;
  let totalItems = 0;
  let _loaded = false;

  // ── Helpers ─────────────────────────────────────────────────────

  function relativeTimeDE(iso) {
    if (!iso) return '';
    const diff = Date.now() - new Date(iso).getTime();
    const m = Math.floor(diff / 60000);
    if (m < 1) return 'gerade eben';
    if (m < 60) return 'vor ' + m + ' Min';
    const h = Math.floor(m / 60);
    if (h < 24) return 'vor ' + h + 'h';
    const d = Math.floor(h / 24);
    if (d === 1) return 'gestern';
    return 'vor ' + d + ' Tagen';
  }

  const BADGE_COLORS = {
    bandai:    '#00e5c0',
    twitter:   '#1DA1F2',
    holygrade: '#FF6B00',
    market:    '#FFD700',
    community: '#888',
  };

  const BADGE_LABELS = {
    bandai:    'BANDAI',
    twitter:   '@OPTCG',
    holygrade: 'HOLYGRADE',
    market:    'MARKT',
    community: 'COMMUNITY',
  };

  const CAT_LABELS = {
    set_release: 'SET-RELEASE',
    market:      'MARKT',
    tournament:  'TOURNAMENT',
    shop:        'SHOP',
    other:       'NEWS',
  };

  function badgeHTML(source) {
    const color = BADGE_COLORS[source] || '#888';
    const label = BADGE_LABELS[source] || source.toUpperCase();
    return '<span class="news-badge" style="color:' + color + '">' + label + '</span>';
  }

  function catChip(cat) {
    return '<span class="news-cat-chip">' + (CAT_LABELS[cat] || cat.toUpperCase()) + '</span>';
  }

  function isVisited(id) {
    try { return localStorage.getItem('hn_visited_' + id) === '1'; } catch (e) { return false; }
  }

  function markVisited(id) {
    try { localStorage.setItem('hn_visited_' + id, '1'); } catch (e) { /* ok */ }
  }

  function escapeHtml(s) {
    if (!s) return '';
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  // ── API calls ──────────────────────────────────────────────────

  async function apiFetch(url) {
    const token = localStorage.getItem('optcg_token');
    const headers = {};
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const res = await fetch(url, { headers: headers });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return res.json();
  }

  // ── Render ─────────────────────────────────────────────────────

  function renderFeaturedCard(item) {
    var visited = isVisited(item.id) ? ' visited' : '';
    var link = item.source_url || '#';
    var target = link.startsWith('http') ? ' target="_blank" rel="noopener"' : '';

    // If market signal links to /preview/sealed/..., open in same tab
    if (item.source === 'market' && item.source_url && item.source_url.includes('/preview/sealed/')) {
      target = '';
    }

    return '<a class="news-card featured' + visited + '" href="' + escapeHtml(link) + '"' + target +
      ' data-id="' + item.id + '" onclick="newsCardClick(' + item.id + ')">' +
      '<div class="news-card-top">' +
        badgeHTML(item.source) +
        '<span class="news-timestamp">' + escapeHtml(relativeTimeDE(item.published_at)) + '</span>' +
      '</div>' +
      '<div class="news-card-headline">' + escapeHtml(item.title_de) + '</div>' +
      (item.teaser_de ? '<div class="news-card-teaser">' + escapeHtml(item.teaser_de) + '</div>' : '') +
      '<div class="news-card-bottom">' +
        catChip(item.category) +
        '<span class="news-arrow">↗</span>' +
      '</div>' +
    '</a>';
  }

  function renderFeedCard(item) {
    var visited = isVisited(item.id) ? ' visited' : '';
    var link = item.source_url || '#';
    var target = link.startsWith('http') ? ' target="_blank" rel="noopener"' : '';

    if (item.source === 'market' && item.source_url && item.source_url.includes('/preview/sealed/')) {
      target = '';
    }

    return '<a class="news-card' + visited + '" href="' + escapeHtml(link) + '"' + target +
      ' data-id="' + item.id + '" onclick="newsCardClick(' + item.id + ')">' +
      '<div class="news-card-top">' +
        badgeHTML(item.source) +
        '<span class="news-card-dot">·</span>' +
        '<span class="news-timestamp">' + escapeHtml(relativeTimeDE(item.published_at)) + '</span>' +
        catChip(item.category) +
      '</div>' +
      '<div class="news-card-divider"></div>' +
      '<div class="news-card-headline">' + escapeHtml(item.title_de) + '</div>' +
      (item.teaser_de ? '<div class="news-card-teaser">' + escapeHtml(item.teaser_de) + '</div>' : '') +
      '<div class="news-card-bottom-right"><span class="news-arrow">↗</span></div>' +
    '</a>';
  }

  function renderSkeletons(container, count) {
    var html = '';
    for (var i = 0; i < count; i++) html += '<div class="news-skeleton"></div>';
    container.innerHTML = html;
  }

  // ── Data loading ───────────────────────────────────────────────

  async function loadFeatured() {
    var el = document.getElementById('news-featured');
    if (!el) return;
    renderSkeletons(el, 3);
    try {
      var data = await apiFetch('/api/news?featured=true');
      if (!data.items || data.items.length === 0) {
        el.innerHTML = '';
        return;
      }
      el.innerHTML = data.items.map(renderFeaturedCard).join('');
    } catch (e) {
      el.innerHTML = '<div class="news-error">Featured-News konnten nicht geladen werden.</div>';
    }
  }

  async function loadFeed(reset) {
    var feedEl = document.getElementById('news-feed');
    var emptyEl = document.getElementById('news-empty');
    var loadMoreEl = document.getElementById('news-load-more');
    var loadBtn = document.getElementById('news-load-btn');
    if (!feedEl) return;

    if (reset) {
      offset = 0;
      renderSkeletons(feedEl, 6);
      if (emptyEl) emptyEl.style.display = 'none';
    }

    var catParam = currentCat !== 'all' ? '&cat=' + currentCat : '';
    try {
      var data = await apiFetch('/api/news?limit=' + LIMIT + '&offset=' + offset + catParam);
      totalItems = data.total || 0;

      // KPI
      if (data.kpi) {
        var kpiToday = document.getElementById('kpi-today');
        var kpiWeek = document.getElementById('kpi-week');
        var kpiSources = document.getElementById('kpi-sources');
        if (kpiToday) kpiToday.textContent = data.kpi.today || 0;
        if (kpiWeek) kpiWeek.textContent = data.kpi.week || 0;
        if (kpiSources) kpiSources.textContent = data.kpi.active_sources || 0;
      }

      if (reset) feedEl.innerHTML = '';

      if (data.items.length === 0 && offset === 0) {
        feedEl.innerHTML = '';
        if (emptyEl) emptyEl.style.display = 'block';
        if (loadMoreEl) loadMoreEl.style.display = 'none';
        return;
      }

      if (emptyEl) emptyEl.style.display = 'none';
      feedEl.innerHTML += data.items.map(renderFeedCard).join('');

      // Load more button
      var shown = offset + data.items.length;
      if (shown < totalItems) {
        if (loadMoreEl) loadMoreEl.style.display = 'flex';
        if (loadBtn) loadBtn.textContent = 'Mehr laden — ' + (totalItems - shown) + ' weitere News';
      } else {
        if (loadMoreEl) loadMoreEl.style.display = 'none';
      }

      // Update timestamp
      var upd = document.getElementById('news-updated');
      if (upd) upd.textContent = 'Aktualisiert: gerade eben';

    } catch (e) {
      if (reset) feedEl.innerHTML = '<div class="news-error">News konnten nicht geladen werden.</div>';
    }
  }

  // ── Filter tabs ────────────────────────────────────────────────

  function bindFilters() {
    var filtersEl = document.getElementById('news-filters');
    if (!filtersEl) return;
    filtersEl.addEventListener('click', function (e) {
      var btn = e.target.closest('.news-filter');
      if (!btn) return;
      var cat = btn.dataset.cat;
      if (cat === currentCat) return;

      currentCat = cat;
      filtersEl.querySelectorAll('.news-filter').forEach(function (b) {
        b.classList.toggle('active', b.dataset.cat === cat);
      });
      loadFeed(true);
    });
  }

  // ── Load more ──────────────────────────────────────────────────

  function bindLoadMore() {
    var btn = document.getElementById('news-load-btn');
    if (!btn) return;
    btn.addEventListener('click', function () {
      offset += LIMIT;
      loadFeed(false);
    });
  }

  // ── Init ───────────────────────────────────────────────────────

  window.initNewsPage = function () {
    if (_loaded) return; // avoid double-load on rapid tab switches
    _loaded = true;

    // Check URL params for category
    var urlCat = new URLSearchParams(window.location.search).get('cat');
    if (urlCat && ['set_release', 'market', 'tournament', 'shop'].includes(urlCat)) {
      currentCat = urlCat;
      var filtersEl = document.getElementById('news-filters');
      if (filtersEl) {
        filtersEl.querySelectorAll('.news-filter').forEach(function (b) {
          b.classList.toggle('active', b.dataset.cat === currentCat);
        });
      }
    }

    bindFilters();
    bindLoadMore();
    loadFeatured();
    loadFeed(true);

    // Allow reload on subsequent tab switches
    setTimeout(function () { _loaded = false; }, 2000);
  };

  // ── Global helpers (called from onclick in HTML) ───────────────

  window.newsCardClick = function (id) {
    markVisited(id);
    var card = document.querySelector('.news-card[data-id="' + id + '"]');
    if (card) card.classList.add('visited');
  };

  window.newsFilterAll = function () {
    currentCat = 'all';
    var filtersEl = document.getElementById('news-filters');
    if (filtersEl) {
      filtersEl.querySelectorAll('.news-filter').forEach(function (b) {
        b.classList.toggle('active', b.dataset.cat === 'all');
      });
    }
    document.getElementById('news-empty').style.display = 'none';
    loadFeed(true);
  };

})();

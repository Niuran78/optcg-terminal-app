/* ═══════════════════════════════════════════════════════════════
   OPTCG MARKET TERMINAL — Auth Module
   Handles JWT token storage, login, register, session restore.
   ═══════════════════════════════════════════════════════════════ */

const Auth = (() => {
  const TOKEN_KEY = 'optcg_token';
  const USER_KEY  = 'optcg_user';

  /* ── Storage ─────────────────────────────────────────────────── */
  function setToken(token) {
    try { localStorage.setItem(TOKEN_KEY, token); } catch (e) { /* private mode */ }
  }

  function getToken() {
    try { return localStorage.getItem(TOKEN_KEY); } catch (e) { return null; }
  }

  function clearToken() {
    try {
      localStorage.removeItem(TOKEN_KEY);
      localStorage.removeItem(USER_KEY);
    } catch (e) { /* noop */ }
  }

  function setUser(user) {
    try { localStorage.setItem(USER_KEY, JSON.stringify(user)); } catch (e) { /* noop */ }
  }

  function getStoredUser() {
    try {
      const raw = localStorage.getItem(USER_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }

  /* ── API helpers ─────────────────────────────────────────────── */
  async function apiFetch(path, options = {}) {
    const token = getToken();
    const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
    if (token) headers['Authorization'] = `Bearer ${token}`;

    const res = await fetch(path, { ...options, headers });
    return res;
  }

  /* ── Login ───────────────────────────────────────────────────── */
  async function login(email, password) {
    const res = await apiFetch('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.detail || data.message || 'Login failed');
    }

    const token = data.access_token || data.token;
    if (!token) throw new Error('No token returned from server');

    setToken(token);

    // Fetch user profile
    const user = await fetchMe(token);
    setUser(user);
    return user;
  }

  /* ── Register ────────────────────────────────────────────────── */
  async function register(email, password) {
    const res = await apiFetch('/api/auth/register', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });

    const data = await res.json();

    if (!res.ok) {
      throw new Error(data.detail || data.message || 'Registration failed');
    }

    // Some APIs return a token on register; if so, store it
    if (data.access_token || data.token) {
      const token = data.access_token || data.token;
      setToken(token);
      const user = await fetchMe(token);
      setUser(user);
      return user;
    }

    // Otherwise return partial data
    return data;
  }

  /* ── Fetch /me ───────────────────────────────────────────────── */
  async function fetchMe(overrideToken) {
    const headers = { 'Content-Type': 'application/json' };
    const token = overrideToken || getToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;

    const res = await fetch('/api/auth/me', { headers });
    if (!res.ok) throw new Error('Session expired');

    const data = await res.json();
    return data;
  }

  /* ── Session restore ─────────────────────────────────────────── */
  async function restoreSession() {
    const token = getToken();
    if (!token) return null;

    try {
      const user = await fetchMe(token);
      setUser(user);
      return user;
    } catch (e) {
      clearToken();
      return null;
    }
  }

  /* ── Logout ──────────────────────────────────────────────────── */
  function logout() {
    clearToken();
    window.location.href = '/login.html';
  }

  /* ── Is authenticated ────────────────────────────────────────── */
  function isAuthenticated() {
    return !!getToken();
  }

  /* ── Guard: redirect to login if not authed ──────────────────── */
  function requireAuth() {
    if (!isAuthenticated()) {
      window.location.href = '/login.html?next=' + encodeURIComponent(window.location.pathname);
      return false;
    }
    return true;
  }

  /* ── Tier helpers ────────────────────────────────────────────── */
  function getTier(user) {
    return (user?.tier || user?.subscription_tier || 'free').toLowerCase();
  }

  function isElite(user) {
    const tier = getTier(user);
    return tier === 'elite' || tier === 'pro';
  }

  return {
    login,
    register,
    logout,
    restoreSession,
    isAuthenticated,
    requireAuth,
    getToken,
    getStoredUser,
    clearToken,
    fetchMe,
    getTier,
    isElite,
    apiFetch,
  };
})();

/* ═══════════════════════════════════════════════════════════════
   Login Page Controller (only active on login.html)
   ═══════════════════════════════════════════════════════════════ */
if (document.getElementById('auth-page')) {
  initAuthPage();
}

function initAuthPage() {
  let activeMode = 'login'; // 'login' | 'register'

  const tabLogin    = document.getElementById('tab-login');
  const tabRegister = document.getElementById('tab-register');
  const formLogin   = document.getElementById('form-login');
  const formRegister = document.getElementById('form-register');

  // If already logged in, redirect (honor ?next= so upgrade flow returns to modal)
  if (Auth.isAuthenticated()) {
    const next = new URLSearchParams(window.location.search).get('next');
    window.location.href = next || '/';
    return;
  }

  // Tab switching
  function switchMode(mode) {
    activeMode = mode;
    tabLogin.classList.toggle('active', mode === 'login');
    tabRegister.classList.toggle('active', mode === 'register');
    formLogin.classList.toggle('hidden', mode !== 'login');
    formRegister.classList.toggle('hidden', mode !== 'register');
  }

  tabLogin.addEventListener('click', () => switchMode('login'));
  tabRegister.addEventListener('click', () => switchMode('register'));

  // Handle login
  formLogin.addEventListener('submit', async (e) => {
    e.preventDefault();
    const email    = formLogin.querySelector('[name=email]').value.trim();
    const password = formLogin.querySelector('[name=password]').value;
    const errEl    = document.getElementById('login-error');
    const submitBtn = formLogin.querySelector('[type=submit]');

    errEl.textContent = '';
    submitBtn.disabled = true;
    submitBtn.textContent = 'Signing in…';

    try {
      await Auth.login(email, password);
      const next = new URLSearchParams(window.location.search).get('next');
      window.location.href = next || '/';
    } catch (err) {
      errEl.textContent = err.message;
      submitBtn.disabled = false;
      submitBtn.textContent = 'Sign In';
    }
  });

  // Handle register
  formRegister.addEventListener('submit', async (e) => {
    e.preventDefault();
    const email    = formRegister.querySelector('[name=email]').value.trim();
    const password = formRegister.querySelector('[name=password]').value;
    const confirm  = formRegister.querySelector('[name=confirm]').value;
    const errEl    = document.getElementById('register-error');
    const submitBtn = formRegister.querySelector('[type=submit]');

    errEl.textContent = '';

    if (password !== confirm) {
      errEl.textContent = 'Passwords do not match';
      return;
    }

    if (password.length < 8) {
      errEl.textContent = 'Password must be at least 8 characters';
      return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = 'Creating account…';

    try {
      await Auth.register(email, password);
      const next = new URLSearchParams(window.location.search).get('next');
      window.location.href = next || '/';
    } catch (err) {
      errEl.textContent = err.message;
      submitBtn.disabled = false;
      submitBtn.textContent = 'Create Account';
    }
  });
}

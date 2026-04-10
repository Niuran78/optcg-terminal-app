/* ============================================================
   OPTCG MARKET TERMINAL — auth.js
   Handles login, registration, token storage
   ============================================================ */

'use strict';

const Auth = (() => {
  const TOKEN_KEY = 'optcg_token';
  const USER_KEY  = 'optcg_user';

  /* ── Storage ── */
  function getToken() {
    return localStorage.getItem(TOKEN_KEY);
  }

  function getUser() {
    try {
      const raw = localStorage.getItem(USER_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }

  function saveSession(token, user) {
    localStorage.setItem(TOKEN_KEY, token);
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  }

  function clearSession() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  }

  function isLoggedIn() {
    return !!getToken();
  }

  /* ── API calls ── */
  async function login(email, password) {
    const resp = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      throw new Error(data.detail || data.message || 'Login failed');
    }

    saveSession(data.access_token, data.user);
    return data;
  }

  async function register(email, password) {
    const resp = await fetch('/api/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      throw new Error(data.detail || data.message || 'Registration failed');
    }

    saveSession(data.access_token, data.user);
    return data;
  }

  async function fetchMe() {
    const token = getToken();
    if (!token) return null;

    try {
      const resp = await fetch('/api/auth/me', {
        headers: { 'Authorization': 'Bearer ' + token },
      });

      if (!resp.ok) {
        if (resp.status === 401) {
          clearSession();
          return null;
        }
        return null;
      }

      const user = await resp.json();
      localStorage.setItem(USER_KEY, JSON.stringify(user));
      return user;
    } catch {
      return null;
    }
  }

  function logout() {
    clearSession();
    window.location.href = '/static/login.html';
  }

  /* ── Redirect guards ── */
  function requireAuth() {
    if (!isLoggedIn()) {
      window.location.href = '/static/login.html';
      return false;
    }
    return true;
  }

  function redirectIfAuthed() {
    if (isLoggedIn()) {
      window.location.href = '/static/index.html';
      return true;
    }
    return false;
  }

  /* ── authFetch — wrapper that adds Bearer token ── */
  async function authFetch(url, options = {}) {
    const token = getToken();
    const headers = {
      ...(options.headers || {}),
      ...(token ? { 'Authorization': 'Bearer ' + token } : {}),
    };

    const resp = await fetch(url, { ...options, headers });

    if (resp.status === 401) {
      clearSession();
      window.location.href = '/static/login.html';
      throw new Error('Session expired. Please log in again.');
    }

    return resp;
  }

  return {
    getToken,
    getUser,
    saveSession,
    clearSession,
    isLoggedIn,
    login,
    register,
    fetchMe,
    logout,
    requireAuth,
    redirectIfAuthed,
    authFetch,
  };
})();

/* ============================================================
   LOGIN PAGE LOGIC
   (Only runs on login.html)
   ============================================================ */
if (document.getElementById('login-form')) {
  // Redirect if already logged in
  Auth.redirectIfAuthed();

  const loginForm    = document.getElementById('login-form');
  const registerForm = document.getElementById('register-form');
  const tabLogin     = document.getElementById('tab-login');
  const tabRegister  = document.getElementById('tab-register');
  const errorEl      = document.getElementById('auth-error');
  const successEl    = document.getElementById('auth-success');

  function setError(msg) {
    if (!errorEl) return;
    errorEl.textContent = msg;
    errorEl.style.display = msg ? 'block' : 'none';
    if (successEl) successEl.style.display = 'none';
  }

  function setSuccess(msg) {
    if (!successEl) return;
    successEl.textContent = msg;
    successEl.style.display = msg ? 'block' : 'none';
    if (errorEl) errorEl.style.display = 'none';
  }

  function setLoading(form, loading) {
    const btn = form.querySelector('[type="submit"]');
    if (!btn) return;
    btn.disabled = loading;
    btn.textContent = loading ? 'Please wait…' : btn.dataset.label;
  }

  /* ── Tab switching ── */
  if (tabLogin && tabRegister) {
    tabLogin.addEventListener('click', () => {
      tabLogin.classList.add('active');
      tabRegister.classList.remove('active');
      loginForm.style.display = '';
      if (registerForm) registerForm.style.display = 'none';
      setError('');
    });

    tabRegister.addEventListener('click', () => {
      tabRegister.classList.add('active');
      tabLogin.classList.remove('active');
      if (registerForm) registerForm.style.display = '';
      loginForm.style.display = 'none';
      setError('');
    });
  }

  /* ── Login submit ── */
  loginForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    setError('');
    const email    = loginForm.querySelector('[name="email"]').value.trim();
    const password = loginForm.querySelector('[name="password"]').value;

    if (!email || !password) {
      setError('Please enter your email and password.');
      return;
    }

    setLoading(loginForm, true);
    try {
      await Auth.login(email, password);
      setSuccess('Logged in! Redirecting…');
      setTimeout(() => { window.location.href = '/static/index.html'; }, 500);
    } catch (err) {
      setError(err.message || 'Login failed. Check your credentials.');
    } finally {
      setLoading(loginForm, false);
    }
  });

  /* ── Register submit ── */
  if (registerForm) {
    registerForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      setError('');
      const email    = registerForm.querySelector('[name="email"]').value.trim();
      const password = registerForm.querySelector('[name="password"]').value;
      const confirm  = registerForm.querySelector('[name="confirm"]')?.value;

      if (!email || !password) {
        setError('Please fill in all fields.');
        return;
      }

      if (confirm !== undefined && password !== confirm) {
        setError('Passwords do not match.');
        return;
      }

      if (password.length < 8) {
        setError('Password must be at least 8 characters.');
        return;
      }

      setLoading(registerForm, true);
      try {
        await Auth.register(email, password);
        setSuccess('Account created! Redirecting…');
        setTimeout(() => { window.location.href = '/static/index.html'; }, 600);
      } catch (err) {
        setError(err.message || 'Registration failed. Try a different email.');
      } finally {
        setLoading(registerForm, false);
      }
    });
  }
}

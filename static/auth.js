/* ═══════════════════════════════════════════════════════════════
   OPTCG Market Terminal — Auth Page JS
   ═══════════════════════════════════════════════════════════════ */

// ─── Mode Switching ───────────────────────────────────────────
function switchAuthMode(mode) {
  const isLogin = mode === 'login';

  // Tabs
  document.getElementById('tabLogin').classList.toggle('active', isLogin);
  document.getElementById('tabRegister').classList.toggle('active', !isLogin);

  // Forms
  document.getElementById('loginForm').classList.toggle('hidden', !isLogin);
  document.getElementById('registerForm').classList.toggle('hidden', isLogin);

  // Features
  document.getElementById('authFeatures').classList.toggle('hidden', isLogin);

  // Header
  if (isLogin) {
    document.getElementById('authTitle').textContent = 'Welcome back';
    document.getElementById('authSubtitle').textContent = 'Sign in to access your market terminal';
  } else {
    document.getElementById('authTitle').textContent = 'Get started free';
    document.getElementById('authSubtitle').textContent = 'Create your account — no credit card required';
  }

  clearErrors();
}

// ─── Init ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Pre-fill from URL params
  const params = new URLSearchParams(window.location.search);
  if (params.get('upgrade')) {
    switchAuthMode('register');
  }

  // Auto-redirect if already logged in
  const token = localStorage.getItem('optcg_token');
  const user = JSON.parse(localStorage.getItem('optcg_user') || 'null');
  if (token && user) {
    const redirect = params.get('redirect') || '/';
    window.location.href = redirect;
  }
});

// ─── Login ────────────────────────────────────────────────────
async function handleLogin(event) {
  event.preventDefault();
  clearErrors();

  const email = document.getElementById('loginEmail').value.trim();
  const password = document.getElementById('loginPassword').value;

  if (!email || !password) {
    showFieldError('loginEmailErr', 'Email and password are required');
    return;
  }

  setSubmitLoading('loginSubmit', true);

  try {
    const resp = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      const msg = data.detail?.message || data.detail || 'Invalid email or password';
      showGlobalError(msg);
      return;
    }

    // Save auth state
    localStorage.setItem('optcg_token', data.access_token);
    localStorage.setItem('optcg_user', JSON.stringify(data.user));

    // Redirect
    const params = new URLSearchParams(window.location.search);
    const redirect = params.get('redirect') || '/';
    const upgrade = params.get('upgrade') || sessionStorage.getItem('post_login_upgrade');

    if (upgrade) {
      sessionStorage.removeItem('post_login_upgrade');
      // Redirect to checkout
      await startCheckout(data.access_token, upgrade);
    } else {
      window.location.href = redirect;
    }

  } catch (e) {
    showGlobalError('Network error. Please try again.');
  } finally {
    setSubmitLoading('loginSubmit', false);
  }
}

// ─── Register ─────────────────────────────────────────────────
async function handleRegister(event) {
  event.preventDefault();
  clearErrors();

  const email = document.getElementById('regEmail').value.trim();
  const password = document.getElementById('regPassword').value;
  const passwordConfirm = document.getElementById('regPasswordConfirm').value;

  // Validation
  let hasError = false;

  if (!email) {
    showFieldError('regEmailErr', 'Email is required');
    hasError = true;
  } else if (!isValidEmail(email)) {
    showFieldError('regEmailErr', 'Please enter a valid email address');
    hasError = true;
  }

  if (!password) {
    showFieldError('regPasswordErr', 'Password is required');
    hasError = true;
  } else if (password.length < 8) {
    showFieldError('regPasswordErr', 'Password must be at least 8 characters');
    hasError = true;
  }

  if (password !== passwordConfirm) {
    showFieldError('regPasswordConfirmErr', 'Passwords do not match');
    hasError = true;
  }

  if (hasError) return;

  setSubmitLoading('registerSubmit', true);

  try {
    const resp = await fetch('/api/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      const msg = data.detail?.message || data.detail || 'Registration failed';
      if (msg.toLowerCase().includes('email')) {
        showFieldError('regEmailErr', msg);
      } else {
        showGlobalError(msg);
      }
      return;
    }

    // Save auth state
    localStorage.setItem('optcg_token', data.access_token);
    localStorage.setItem('optcg_user', JSON.stringify(data.user));

    // Handle post-registration upgrade
    const params = new URLSearchParams(window.location.search);
    const upgrade = params.get('upgrade') || sessionStorage.getItem('post_login_upgrade');

    if (upgrade) {
      sessionStorage.removeItem('post_login_upgrade');
      await startCheckout(data.access_token, upgrade);
    } else {
      const redirect = params.get('redirect') || '/';
      window.location.href = redirect;
    }

  } catch (e) {
    showGlobalError('Network error. Please try again.');
  } finally {
    setSubmitLoading('registerSubmit', false);
  }
}

// ─── Stripe Checkout ──────────────────────────────────────────
async function startCheckout(token, tier) {
  try {
    const resp = await fetch('/api/billing/checkout', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
      body: JSON.stringify({ tier }),
    });
    const data = await resp.json();
    if (data.checkout_url) {
      window.location.href = data.checkout_url;
    } else {
      window.location.href = '/';
    }
  } catch (e) {
    window.location.href = '/';
  }
}

// ─── Password Strength ────────────────────────────────────────
function updatePasswordStrength(password) {
  const bar = document.getElementById('pwStrengthBar');
  if (!bar) return;

  let strength = 0;
  if (password.length >= 8) strength += 25;
  if (password.length >= 12) strength += 15;
  if (/[A-Z]/.test(password)) strength += 20;
  if (/[0-9]/.test(password)) strength += 20;
  if (/[^A-Za-z0-9]/.test(password)) strength += 20;

  bar.style.width = `${Math.min(strength, 100)}%`;

  if (strength < 40) {
    bar.style.background = 'var(--negative)';
  } else if (strength < 70) {
    bar.style.background = 'var(--warning)';
  } else {
    bar.style.background = 'var(--positive)';
  }
}

// ─── Error Helpers ────────────────────────────────────────────
function showGlobalError(message) {
  const el = document.getElementById('authError');
  el.textContent = message;
  el.classList.add('visible');
}

function showFieldError(id, message) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = message;
    el.classList.add('visible');
    // Mark parent input as error
    const input = el.previousElementSibling;
    if (input && input.classList.contains('field-input')) {
      input.classList.add('error');
    }
  }
}

function clearErrors() {
  document.getElementById('authError')?.classList.remove('visible');
  document.querySelectorAll('.field-error').forEach(el => {
    el.classList.remove('visible');
    el.textContent = '';
  });
  document.querySelectorAll('.field-input.error').forEach(el => {
    el.classList.remove('error');
  });
}

function setSubmitLoading(id, loading) {
  const btn = document.getElementById(id);
  if (!btn) return;
  btn.disabled = loading;
  btn.classList.toggle('loading', loading);
  if (loading) {
    btn.dataset.originalText = btn.childNodes[0]?.textContent?.trim() || '';
    btn.childNodes[0].textContent = 'Please wait...';
  } else {
    if (btn.dataset.originalText) {
      btn.childNodes[0].textContent = btn.dataset.originalText;
    }
  }
}

// ─── Utilities ────────────────────────────────────────────────
function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

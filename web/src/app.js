// app.js - hash router and shell (header, nav, server override, health).
import { api, serverURL, setServer } from './api.js';
import { el } from './util.js';
import { viewCoverage } from './views/coverage.js';
import { viewStreams } from './views/streams.js';
import { viewTimeline } from './views/timeline.js';
import { viewDiff } from './views/diff.js';
import { viewSnapshots } from './views/snapshots.js';
import { viewPlans } from './views/plans.js';

const routes = {
  coverage: viewCoverage,
  streams: viewStreams,
  timeline: viewTimeline,
  diff: viewDiff,
  snapshots: viewSnapshots,
  plans: viewPlans,
};

const NAV = [
  ['coverage', 'Coverage'],
  ['streams', 'Streams'],
  ['timeline', 'Timeline'],
  ['diff', 'Diff'],
  ['snapshots', 'Snapshots'],
  ['plans', 'Plans'],
];

function currentRoute() {
  const name = location.hash.replace(/^#\/?/, '').split('/')[0].toLowerCase();
  return name in routes ? name : 'coverage';
}

function renderNav() {
  const nav = document.getElementById('nav');
  for (const [name, label] of NAV) {
    nav.appendChild(el('a', { href: `#/${name}`, className: 'navlink', textContent: label, dataset: { name } }));
  }
}

function highlightNav() {
  const cur = currentRoute();
  for (const a of document.querySelectorAll('.navlink')) {
    const active = a.dataset.name === cur;
    a.classList.toggle('active', active);
    if (active) a.setAttribute('aria-current', 'page');
    else a.removeAttribute('aria-current');
  }
}

async function route() {
  highlightNav();
  const mount = document.getElementById('view');
  mount.textContent = '';
  const wrap = el('div');
  mount.appendChild(wrap);
  try {
    await routes[currentRoute()](wrap);
  } catch (err) {
    wrap.appendChild(el('div', { className: 'notice error', textContent: err && err.message ? err.message : String(err) }));
  }
}

async function checkHealth() {
  const dot = document.getElementById('health');
  try {
    const h = await api.health();
    dot.className = 'health ok';
    dot.title = `krply-server ${h.version || ''} · storage ${h.storage || ''} · ${h.status || ''}`;
  } catch (err) {
    dot.className = 'health bad';
    dot.title = 'server unreachable: ' + (err && err.message ? err.message : err);
  }
}

function initServer() {
  const form = document.getElementById('server-form');
  const input = document.getElementById('server-input');
  const foot = document.getElementById('foot-server');
  input.value = serverURL() || '';
  const sync = () => {
    foot.textContent = serverURL() ? `server: ${serverURL()}` : 'server: same origin';
  };
  sync();
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    setServer(input.value.trim());
    sync();
    checkHealth();
    route();
  });
}

renderNav();
initServer();
checkHealth();
route();
window.addEventListener('hashchange', route);

// api.js - thin fetch wrapper around the krply-server query API.
// The base URL is the same origin, or an optional ?server= override that is
// remembered in localStorage (krply.server) so the UI can point at a remote
// krply-server without CORS.

const LS_KEY = 'krply.server';
const fromURL = new URLSearchParams(location.search).get('server');

const PROD_HOSTS = new Set([
  'krply.lverma.com',
  'krply-web-lakshit-vermas-projects.vercel.app',
]);
const DEFAULT_SERVER = PROD_HOSTS.has(location.hostname) ? 'https://krply-server.onrender.com' : '';

let base = (fromURL || localStorage.getItem(LS_KEY) || DEFAULT_SERVER).replace(/\/+$/, '');

export function serverURL() { return base; }

export function setServer(url) {
  base = (url || '').replace(/\/+$/, '');
  if (url) localStorage.setItem(LS_KEY, base);
  else localStorage.removeItem(LS_KEY);
}

const enc = encodeURIComponent;
const qs = (q) =>
  Object.entries(q || {})
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${enc(k)}=${enc(v)}`)
    .join('&');

async function request(path, opts = {}) {
  const res = await fetch(base + path, {
    headers: {
      Accept: 'application/json',
      ...(opts.body ? { 'Content-Type': 'application/json' } : {}),
    },
    ...opts,
  });
  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const j = await res.json();
      msg = j.error || j.warning || j.message || msg;
    } catch { /* keep default */ }
    throw new Error(msg);
  }
  const text = await res.text();
  if (!text) return null;
  try { return JSON.parse(text); } catch { return text; }
}

export const api = {
  health: () => request('/v1/health'),
  clusters: () => request('/v1/clusters'),
  streams: () => request('/v1/streams'),
  events: (q) => request('/v1/events?' + qs(q)),
  // eventsAll follows cursor pagination so the whole journal is fetched, not
  // just the first (oldest) page. Bounded to 20 pages of 1000.
  eventsAll: async (q = {}) => {
    const all = [];
    let cursor = '';
    for (let i = 0; i < 20; i++) {
      const page = await api.events({ ...q, limit: 1000, ...(cursor ? { cursor } : {}) });
      all.push(...asList(page.items));
      if (!page.has_more || !page.next_cursor) break;
      cursor = page.next_cursor;
    }
    return all;
  },
  history: (cluster, stream, ns, name, since) =>
    request(`/v1/objects/${enc(objectRef({ cluster, stream, ns, name }))}/history` + (since ? '?' + qs({ since }) : '')),
  diff: (q) => request('/v1/diff?' + qs(q)),
  snapshots: () => request('/v1/snapshots'),
  plans: () => request('/v1/replay-plans'),
  createPlan: (body) => request('/v1/replay-plans', { method: 'POST', body: JSON.stringify(body) }),
  dryRun: (body) => request(`/v1/replay-plans/${enc(body.plan_id)}/dry-run`, {
    method: 'POST',
    body: JSON.stringify(body),
  }),
};

function asList(v) {
  return Array.isArray(v) ? v : [];
}

// objectRef encodes the composite object reference as a single base64url token
// (stream IDs contain slashes and cannot be path segments).
function objectRef({ cluster, stream, ns, name }) {
  const bytes = new TextEncoder().encode([cluster, stream, ns, name].join('\u0000'));
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

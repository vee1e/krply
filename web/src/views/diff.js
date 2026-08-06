// JSON diff: changed objects between two observed-time boundaries.
// The API returns per-field changes (path + before/after + added/removed), so
// we rebuild the before/after *changed-fields* trees into two JSON panes and
// list every path with added/removed/modified badges.
import { api } from '../api.js';
import { el, asList, badge, fmtTime, field, formCard } from '../util.js';

const toLocalInput = (d) => {
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
};

export async function viewDiff(mount) {
  mount.appendChild(el('h1', { textContent: 'JSON diff' }));
  const clusters = asList(await api.clusters().catch(() => []));

  const clusterSel = el('select');
  clusterSel.appendChild(el('option', { value: '', textContent: '— cluster —' }));
  for (const cl of clusters) clusterSel.appendChild(el('option', { value: cl.id, textContent: cl.context || cl.id }));

  const namespaceInput = el('input', { type: 'text', placeholder: 'namespace' });
  const sinceInput = el('input', { type: 'datetime-local', step: '1', value: toLocalInput(new Date(Date.now() - 3600e3)) });
  const untilInput = el('input', { type: 'datetime-local', step: '1', value: toLocalInput(new Date()) });

  const results = el('div');
  mount.appendChild(formCard([
    field('cluster', clusterSel),
    field('namespace', namespaceInput),
    field('before (observed)', sinceInput),
    field('after (observed)', untilInput),
  ], 'show diff', async () => {
    results.textContent = '';
    if (!clusterSel.value || !namespaceInput.value.trim() || !sinceInput.value || !untilInput.value) {
      results.appendChild(el('div', { className: 'notice warn', textContent: 'cluster, namespace, before and after are required.' }));
      return;
    }
    const q = {
      cluster_id: clusterSel.value,
      namespace: namespaceInput.value.trim(),
      since: new Date(sinceInput.value).toISOString(),
      until: new Date(untilInput.value).toISOString(),
    };
    results.appendChild(el('p', { className: 'muted', textContent: 'loading…' }));
    try {
      const data = await api.diff(q);
      results.textContent = '';
      renderDiff(results, data);
    } catch (err) {
      results.textContent = '';
      results.appendChild(el('div', { className: 'notice error', textContent: err.message || String(err) }));
    }
  }));
  mount.appendChild(results);
}

function renderDiff(results, data) {
  const changed = asList(data.changed);
  if (data.warning) results.appendChild(el('div', { className: 'notice warn', textContent: data.warning }));
  if (data.has_gaps) results.appendChild(el('div', { className: 'notice error', textContent: 'Coverage gaps detected — this diff may be incomplete.' }));
  results.appendChild(el('p', { className: 'muted' }, [
    el('span', { textContent: 'before ' }),
    el('code', { textContent: fmtTime(data.before_at) }),
    el('span', { textContent: ' → after ' }),
    el('code', { textContent: fmtTime(data.after_at) }),
    el('span', { textContent: ` · ${changed.length} changed object${changed.length === 1 ? '' : 's'}` }),
  ]));
  if (!changed.length) {
    results.appendChild(el('p', { className: 'muted', textContent: 'No changes between the two observed boundaries.' }));
    return;
  }

  for (const obj of changed) {
    const list = el('div', { className: 'changelist' });
    for (const ch of asList(obj.changes)) {
      const kind = ch.removed ? badge('REMOVED', 'gap') : ch.added ? badge('ADDED', 'ok') : badge('MODIFIED', 'degraded');
      list.appendChild(el('div', { className: 'chg' }, [
        el('span', { className: 'path', textContent: ch.path || '(root)' }),
        kind,
        el('span', { className: 'vals' }, [
          changeValue(ch.before, ch.added),
          el('span', { className: 'value-arrow', textContent: '→' }),
          changeValue(ch.after, ch.removed),
        ]),
      ]));
    }
    results.appendChild(el('div', { className: 'diff-obj' }, [
      el('div', { className: 'obj-head' }, [
        el('span', { className: 'name', textContent: obj.namespace ? `${obj.namespace}/${obj.name}` : obj.name }),
        badge(obj.kind || '?', 'neutral'),
        el('span', { className: 'muted', textContent: `${asList(obj.changes).length} field changes` }),
      ]),
      el('div', { className: 'diff-grid' }, [
        el('div', { className: 'diff-pane' }, [el('h4', { textContent: 'before (changed fields)' }), el('pre', { className: 'json', textContent: pretty(buildTree(asList(obj.changes), 'before')) })]),
        el('div', { className: 'diff-pane' }, [el('h4', { textContent: 'after (changed fields)' }), el('pre', { className: 'json', textContent: pretty(buildTree(asList(obj.changes), 'after')) })]),
      ]),
      list,
    ]));
  }
}

function changeValue(value, absent) {
  if (absent) return el('span', { className: 'value-empty', textContent: '∅' });
  if (value !== null && typeof value === 'object') {
    const count = Array.isArray(value) ? value.length : Object.keys(value).length;
    const details = el('details', { className: 'value-details' }, [
      el('summary', { textContent: 'inspect' }),
      el('pre', { textContent: pretty(value) }),
    ]);
    return el('span', { className: 'change-value' }, [
      el('span', { className: 'value-summary', textContent: `${Array.isArray(value) ? 'array' : 'object'} · ${count} ${count === 1 ? 'item' : 'items'}` }),
      details,
    ]);
  }
  return el('span', { className: 'change-value', textContent: val(value) });
}

// Build a nested {path -> value} tree for one side of a diff.
function buildTree(changes, side) {
  const root = {};
  for (const ch of changes) {
    if (side === 'before' ? ch.added : ch.removed) continue; // absent on this side
    const tokens = pathTokens(ch.path);
    const val = side === 'before' ? ch.before : ch.after;
    if (!tokens.length) {
      if (val && typeof val === 'object') Object.assign(root, val);
      continue;
    }
    setPath(root, tokens, val);
  }
  return root;
}

const pathTokens = (p) => {
  const out = [];
  const re = /([^\[\].]+)(?:\[(\d+)\])?/g;
  let m;
  while ((m = re.exec(p || ''))) {
    out.push({ key: m[1], index: m[2] != null ? Number(m[2]) : null });
  }
  return out;
};

function setPath(root, tokens, val) {
  let cur = root;
  for (let i = 0; i < tokens.length - 1; i++) {
    const t = tokens[i];
    if (t.index != null) {
      const arr = cur[t.key] || (cur[t.key] = []);
      while (arr.length <= t.index) arr.push({});
      cur = arr[t.index];
    } else {
      cur = cur[t.key] || (cur[t.key] = {});
    }
  }
  const t = tokens[tokens.length - 1];
  if (t.index != null) {
    const arr = cur[t.key] || (cur[t.key] = []);
    while (arr.length < t.index) arr.push(null);
    arr[t.index] = val;
  } else {
    cur[t.key] = val;
  }
}

const val = (v) => (v === undefined ? '∅' : JSON.stringify(v));
const pretty = (o) => JSON.stringify(o, null, 2) || '{}';

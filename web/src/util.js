// DOM, formatting, and monochrome primitives shared by the views.

export function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === undefined || v === null) continue;
    if (k === 'textContent') node.textContent = v;
    else if (k === 'className') node.className = v;
    else if (k === 'value') node.value = v;
    else if (k === 'dataset') Object.assign(node.dataset, v);
    else node.setAttribute(k, v);
  }
  for (const c of [].concat(children)) {
    if (c == null) continue;
    if (typeof c === 'string' || typeof c === 'number') {
      node.appendChild(document.createTextNode(String(c)));
    } else {
      node.appendChild(c);
    }
  }
  return node;
}

export const esc = (s) =>
  String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

export function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString(undefined, {
    year: 'numeric', month: 'short', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
}

export function fmtTimeShort(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function fmtRV(rv) { return rv ? rv : '—'; }

export function sq(kind, title) {
  return el('i', { className: `sq ${kind}`, 'aria-hidden': 'true', title: title || '' });
}

// chip(label, kind) — a bordered micro-tag. Monochrome; kind sets the square.
export function chip(label, kind = 'neutral') {
  return el('span', { className: `chip chip-${kind}` }, [sq(squareFor(kind)), label]);
}

function squareFor(kind) {
  switch (kind) {
    case 'ok': return 'ok';
    case 'degraded': return 'degraded';
    case 'gap': return 'gap';
    case 'synthetic': return 'synthetic';
    default: return 'void';
  }
}

export function badge(label, kind = 'neutral') {
  return chip(label, kind);
}

export function statusBadge(s) {
  if (s.available === false) return chip('UNAVAILABLE', 'gap');
  if (s.degraded) return chip('DEGRADED', 'degraded');
  if (s.has_gaps) return chip('HAS GAPS', 'gap');
  return chip('OK', 'ok');
}

const WGLYPH = {
  ADDED: '＋', MODIFIED: '~', DELETED: '−', BOOKMARK: '◆', ERROR: '✕', BASELINE: '≡',
};

export function watchBadge(watchType, synthetic) {
  const t = (watchType || '').toUpperCase() || 'EVENT';
  let kind = 'neutral';
  if (t === 'ADDED') kind = 'ok';
  else if (t === 'MODIFIED') kind = 'degraded';
  else if (t === 'DELETED' || t === 'ERROR') kind = 'gap';
  else if (t === 'BOOKMARK' || t === 'BASELINE') kind = 'synthetic';
  const glyph = WGLYPH[t] || '·';
  const b = el('span', { className: `chip chip-${kind}` }, [el('i', { className: 'wglyph', textContent: glyph }), t]);
  if (synthetic) b.title = 'synthetic (from initial relist)';
  return b;
}

export function streamLabel(s) {
  const ns = s.namespace ? `${s.namespace}/` : '';
  return `${ns}${s.resource}` + (s.kind && s.kind !== s.resource ? ` (${s.kind})` : '');
}

export function streamTitle(s) {
  const parts = [
    `stream ${s.id}`,
    s.group || s.version ? `gv: ${s.group ? s.group + '/' : ''}${s.version}` : '',
    s.kind ? `kind: ${s.kind}` : '',
    s.selector ? `selector: ${s.selector}` : '',
  ];
  return parts.filter(Boolean).join(' · ');
}

export function spinner(text = 'loading…') {
  return el('p', { className: 'loading', textContent: text });
}

export function asList(value, key = 'items') {
  if (value == null) return [];
  if (Array.isArray(value)) return value;
  if (Array.isArray(value[key])) return value[key];
  return [];
}

// emptyState(message, hint) — a direction, not a shrug.
export function emptyState(message, hint) {
  const box = el('div', { className: 'notice' }, [el('p', { className: 'muted', textContent: message, style: 'margin:0' })]);
  if (hint) box.appendChild(el('p', { style: 'margin:6px 0 0', textContent: hint }));
  return box;
}

export function field(label, control) {
  return el('div', { className: 'field' }, [el('label', { textContent: label }), control]);
}

export function formCard(fields, submitLabel, onSubmit) {
  const grid = el('div', { className: 'form-grid' }, fields);
  grid.appendChild(el('div', { className: 'actions' }, [el('button', { type: 'submit', className: 'primary', textContent: submitLabel })]));
  const form = el('form', {}, [grid]);
  const card = el('div', { className: 'panel' }, [el('div', { className: 'panel-body' }, [form])]);
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    onSubmit();
  });
  return card;
}

export function simpleTable(headers, rows) {
  const thead = el('thead', {}, [el('tr', {}, headers.map((h) => el('th', { textContent: h })))]);
  const tbody = el('tbody', {}, rows.map((cells) => el('tr', {}, cells)));
  return el('div', { className: 'table-wrap' }, [el('table', { className: 'data' }, [thead, tbody])]);
}

// coverageStrip(stream, span, events, buckets) — the signature graphic.
// A strip of time buckets over the stream's observed window:
//   solid   = at least one journal record in the bucket
//   hatched = a GAP record falls in the bucket
//   faint   = inside the observed window but idle
//   void    = outside the observed window
export function coverageStrip(stream, span, events, buckets = 32) {
  const start = span.start.getTime();
  const end = span.end.getTime();
  const dur = Math.max(end - start, 1);
  const first = stream.first_observed_at ? new Date(stream.first_observed_at).getTime() : null;
  const last = stream.last_observed_at ? new Date(stream.last_observed_at).getTime() : null;

  const cells = Array.from({ length: buckets }, () => ({ gap: false, cover: 0, inWindow: false }));
  for (let i = 0; i < buckets; i++) {
    const bStart = start + (dur * i) / buckets;
    const bEnd = start + (dur * (i + 1)) / buckets;
    cells[i].inWindow = first != null && last != null && bEnd >= first && bStart <= last;
  }
  for (const ev of events) {
    const t = ev.observed_at ? new Date(ev.observed_at).getTime() : null;
    if (t == null || t < start || t > end) continue;
    const idx = Math.min(buckets - 1, Math.floor(((t - start) / dur) * buckets));
    if (ev.record_type === 'GAP') cells[idx].gap = true;
    else cells[idx].cover++;
  }

  const track = el('span', { className: 'strip', role: 'img', 'aria-label': `coverage over time for ${streamLabel(stream)}` });
  for (const c of cells) {
    const cls = c.gap ? 'g' : c.cover > 0 ? 'c' : c.inWindow ? 'a' : 'e';
    track.appendChild(el('i', { className: `cell ${cls}` }));
  }
  return track;
}

// stripWithStats builds one stream row for the coverage surface.
export function stripRow(stream, span, events) {
  const label = el('div', { className: 'strip-label' }, [
    el('span', { className: 'name', textContent: stream.resource }),
    el('span', { className: 'sub', textContent: stream.namespace ? `${stream.namespace} · ${stream.kind || ''}` : stream.kind || 'cluster-scoped' }),
  ]);
  const stats = el('div', { className: 'strip-stats' }, [
    statusBadge(stream),
    el('span', { className: 'rv', textContent: `RV ${fmtRV(stream.last_resource_version)}` }),
    el('span', { textContent: `${stream.gap_count} gap${stream.gap_count === 1 ? '' : 's'}` }),
  ]);
  return el('div', { className: 'strip-row', title: streamTitle(stream) }, [
    label,
    coverageStrip(stream, span, events),
    stats,
  ]);
}

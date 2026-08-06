// defaults.js - demo-friendly defaults so views work with one click (or none).
// Each picker falls back to "" so the form still validates when data is absent.

export function pickFirst(items) {
  const list = Array.isArray(items) ? items : [];
  return list.length ? list[0] : null;
}

// pickLatestSnapshot returns the snapshot with the newest observed time.
export function pickLatestSnapshot(snaps) {
  let best = null;
  for (const s of Array.isArray(snaps) ? snaps : []) {
    const t = s.at ? new Date(s.at).getTime() : 0;
    if (!best || t > new Date(best.at).getTime()) best = s;
  }
  return best;
}

// pickNamespace returns the most common namespace across streams.
export function pickNamespace(streams) {
  const counts = new Map();
  for (const st of Array.isArray(streams) ? streams : []) {
    if (!st.namespace) continue;
    counts.set(st.namespace, (counts.get(st.namespace) || 0) + 1);
  }
  let best = '';
  let bestCount = 0;
  for (const [ns, c] of counts) {
    if (c > bestCount) { best = ns; bestCount = c; }
  }
  return best;
}

// pickObject returns the first observed object {namespace, name}.
export function pickObject(events) {
  for (const ev of Array.isArray(events) ? events : []) {
    if (ev.namespace && ev.name) return { namespace: ev.namespace, name: ev.name };
  }
  return null;
}

// setSelect assigns a value and fires change so dependent options rebuild.
export function setSelect(sel, value) {
  sel.value = value || '';
  sel.dispatchEvent(new Event('change'));
}

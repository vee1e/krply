// Stream health: observed window, coverage strip, gap count, last RV.
import { api } from '../api.js';
import { el, asList, simpleTable, statusBadge, streamTitle, streamLabel, fmtTime, fmtRV, coverageStrip, spinner, emptyState } from '../util.js';

export async function viewStreams(mount) {
  mount.appendChild(el('h1', {}, [
    el('span', { className: 'eyebrow', textContent: 'streams' }),
    el('span', { textContent: 'Stream health' }),
  ]));
  mount.appendChild(el('hr', { className: 'rule' }));

  let streams, events;
  try {
    streams = asList(await api.streams());
    events = asList(await api.eventsAll({}));
  } catch (err) {
    mount.appendChild(el('div', { className: 'notice error', textContent: `failed to load streams: ${err.message || String(err)}` }));
    return;
  }
  if (!streams.length) {
    mount.appendChild(emptyState(
      'No streams recorded yet.',
      'Start a recording with: krply record --namespace shop --resource deployments --store krply.db',
    ));
    return;
  }

  const span = observedSpan(events);

  mount.appendChild(simpleTable(
    ['stream', 'coverage over time', 'first observed', 'last observed', 'gaps', 'last RV', 'status'],
    streams.map((st) => {
      const stEvents = events.filter((e) => e.stream_id === st.id);
      return [
        el('td', { className: 'mono', textContent: streamLabel(st), title: streamTitle(st) }),
        el('td', { style: 'min-width:160px' }, [coverageStrip(st, span, stEvents, 20)]),
        el('td', { textContent: fmtTime(st.first_observed_at) }),
        el('td', { textContent: fmtTime(st.last_observed_at) }),
        el('td', { className: 'num', textContent: String(st.gap_count) }),
        el('td', { className: 'mono', textContent: fmtRV(st.last_resource_version) }),
        el('td', {}, [statusBadge(st)]),
      ];
    })
  ));
}

function observedSpan(events) {
  let min = null;
  let max = null;
  for (const ev of events) {
    const t = ev.observed_at ? new Date(ev.observed_at).getTime() : NaN;
    if (Number.isNaN(t)) continue;
    if (min == null || t < min) min = t;
    if (max == null || t > max) max = t;
  }
  const now = Date.now();
  return { start: new Date(min != null ? min : now), end: new Date(max != null ? max : now) };
}

// Coverage: the coverage surface. For each cluster, one block. For each
// stream, a strip chart of event cadence over time. Solid = activity,
// hatched = gap, faint = idle, void = outside the observed window.
import { api } from '../api.js';
import { el, asList, stripRow, fmtTime, emptyState } from '../util.js';

export async function viewCoverage(mount) {
  mount.appendChild(el('h1', {}, [
    el('span', { className: 'eyebrow', textContent: 'coverage' }),
    el('span', { textContent: 'Stream coverage over time' }),
  ]));
  mount.appendChild(el('hr', { className: 'rule' }));

  let clusters, streams, events;
  try {
    [clusters, streams, events] = await Promise.all([
      api.clusters(),
      api.streams(),
      api.eventsAll({}),
    ]);
  } catch (err) {
    mount.appendChild(el('div', { className: 'notice error', textContent: `failed to load coverage: ${err.message || String(err)}` }));
    return;
  }
  clusters = asList(clusters);
  streams = asList(streams);
  events = asList(events);

  if (!clusters.length && !streams.length) {
    mount.appendChild(emptyState(
      'No clusters or streams recorded yet.',
      'Start a recording with: krply record --namespace shop --resource deployments --resource configmaps --store krply.db',
    ));
    return;
  }

  const byCluster = new Map();
  for (const st of streams) {
    const list = byCluster.get(st.cluster_id) || [];
    list.push(st);
    byCluster.set(st.cluster_id, list);
  }
  for (const cl of clusters) if (!byCluster.has(cl.id)) byCluster.set(cl.id, []);
  const clusterIds = clusters.length ? clusters.map((cl) => cl.id) : [...byCluster.keys()];

  for (const id of clusterIds) {
    const sts = byCluster.get(id) || [];
    const cl = clusters.find((x) => x.id === id) || { id };
    const block = el('section', { className: 'cluster-block' });

    const span = observedSpan(events, id);
    const head = el('header', { className: 'cluster-head' }, [
      el('span', { className: 'cname', textContent: cl.context || cl.id }),
      el('span', { className: 'cmeta' }, [
        `${sts.length} stream${sts.length === 1 ? '' : 's'}`,
        sts.length ? ` · first ${fmtTime(earliestFirst(sts))}` : '',
        sts.length ? ` · last ${fmtTime(latestLast(sts))}` : '',
      ]),
    ]);
    block.appendChild(head);

    if (!sts.length) {
      block.appendChild(el('div', { className: 'strip-rows' }, [
        el('div', { className: 'strip-row', style: 'padding:14px 16px' }, [
          el('span', { className: 'muted', textContent: 'no streams known for this cluster yet' }),
        ]),
      ]));
      mount.appendChild(block);
      continue;
    }

    const rows = el('div', { className: 'strip-rows' });
    for (const st of sts) rows.appendChild(stripRow(st, span, events.filter((e) => e.stream_id === st.id)));
    block.appendChild(rows);
    block.appendChild(stripLegend());
    mount.appendChild(block);
  }

}

function stripLegend() {
  const legend = el('div', { className: 'strip-legend' });
  const item = (cls, label) =>
    el('span', {}, [el('i', { className: `sq ${cls}` }), el('span', { textContent: label })]);
  legend.appendChild(item('ok', 'events'));
  legend.appendChild(item('degraded', 'gap'));
  legend.appendChild(item('idle', 'idle'));
  legend.appendChild(item('void', 'outside window'));
  return legend;
}

function observedSpan(events, clusterId) {
  let min = null;
  let max = null;
  for (const ev of events) {
    if (ev.cluster_id && ev.cluster_id !== clusterId) continue;
    const t = ev.observed_at ? new Date(ev.observed_at).getTime() : NaN;
    if (Number.isNaN(t)) continue;
    if (min == null || t < min) min = t;
    if (max == null || t > max) max = t;
  }
  const now = Date.now();
  return { start: new Date(min != null ? min : now), end: new Date(max != null ? max : now) };
}

function earliestFirst(sts) {
  let min = null;
  for (const st of sts) {
    const t = st.first_observed_at ? new Date(st.first_observed_at).getTime() : NaN;
    if (!Number.isNaN(t) && (min == null || t < min)) min = t;
  }
  return min != null ? new Date(min).toISOString() : '';
}

function latestLast(sts) {
  let max = null;
  for (const st of sts) {
    const t = st.last_observed_at ? new Date(st.last_observed_at).getTime() : NaN;
    if (!Number.isNaN(t) && (max == null || t > max)) max = t;
  }
  return max != null ? new Date(max).toISOString() : '';
}

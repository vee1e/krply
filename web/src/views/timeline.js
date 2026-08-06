// Object timeline: observed events for one object, with GAP rows shown distinctly.
import { api } from '../api.js';
import { el, asList, badge, watchBadge, fmtTime, streamLabel, streamTitle, field, formCard } from '../util.js';
import { pickFirst, pickObject, setSelect } from '../defaults.js';

export async function viewTimeline(mount) {
  mount.appendChild(el('h1', { textContent: 'Object timeline' }));
  const [clusters, streams, evs] = await Promise.all([
    api.clusters().catch(() => []),
    api.streams().catch(() => []),
    api.events({ limit: 30 }).catch(() => []),
  ]);
  const clusterList = asList(clusters);
  const streamList = asList(streams);
  const events = asList(evs);

  const clusterSel = el('select');
  clusterSel.appendChild(el('option', { value: '', textContent: '— cluster —' }));
  for (const cl of clusterList) clusterSel.appendChild(el('option', { value: cl.id, textContent: cl.context || cl.id }));

  const streamSel = el('select');
  const fillStreams = () => {
    streamSel.textContent = '';
    streamSel.appendChild(el('option', { value: '', textContent: '— stream —' }));
    for (const st of streamList) {
      if (!clusterSel.value || st.cluster_id === clusterSel.value) {
        streamSel.appendChild(el('option', { value: st.id, textContent: streamLabel(st), title: streamTitle(st) }));
      }
    }
  };
  clusterSel.addEventListener('change', fillStreams);
  fillStreams();

  const namespaceInput = el('input', { type: 'text', placeholder: 'namespace' });
  const nameInput = el('input', { type: 'text', placeholder: 'name' });
  const sinceInput = el('input', { type: 'number', min: '1', max: '8760', value: '24' });

  const defCluster = pickFirst(clusterList);
  const defObject = pickObject(events);
  if (defCluster) setSelect(clusterSel, defCluster.id);
  fillStreams();
  const defStream = streamList.find((st) => st.cluster_id === (defCluster && defCluster.id)) || pickFirst(streamList);
  if (defStream) streamSel.value = defStream.id;
  if (defObject) {
    namespaceInput.value = defObject.namespace;
    nameInput.value = defObject.name;
  }

  const results = el('div');

  async function run() {
    results.textContent = '';
    const cluster = clusterSel.value;
    const stream = streamSel.value;
    const namespace = namespaceInput.value.trim();
    const name = nameInput.value.trim();
    const hours = Number(sinceInput.value || 24);
    if (!cluster || !stream || !namespace || !name) {
      results.appendChild(el('div', { className: 'notice warn', textContent: 'cluster, stream, namespace and name are required.' }));
      return;
    }
    results.appendChild(el('p', { className: 'muted', textContent: 'loading…' }));
    try {
      const since = new Date(Date.now() - hours * 3600e3).toISOString();
      const data = await api.history(cluster, stream, namespace, name, since);
      results.textContent = '';
      renderHistory(results, data);
    } catch (err) {
      results.textContent = '';
      results.appendChild(el('div', { className: 'notice error', textContent: err.message || String(err) }));
    }
  }

  mount.appendChild(formCard([
    field('cluster', clusterSel),
    field('stream', streamSel),
    field('namespace', namespaceInput),
    field('name', nameInput),
    field('since (hours)', sinceInput),
  ], 'show timeline', run));
  mount.appendChild(results);

  run();
}

function renderHistory(results, data) {
  if (data.warning) results.appendChild(el('div', { className: 'notice warn', textContent: data.warning }));
  if (data.has_gaps) results.appendChild(el('div', { className: 'notice error', textContent: 'Coverage gaps detected — missing spans are marked GAP below.' }));
  results.appendChild(el('p', { className: 'muted' }, [
    el('span', { textContent: 'kind ' }),
    el('code', { textContent: data.kind || '?' }),
    el('span', { textContent: ' · stream ' }),
    el('code', { textContent: data.stream_id || '?' }),
  ]));

  const items = asList(data.items);
  const gaps = asList(data.gaps);
  if (!items.length && !gaps.length) {
    results.appendChild(el('p', { className: 'muted', textContent: 'No observed events in this window.' }));
    return;
  }

  // Interleave events and gaps in observed-time order.
  const seq = [];
  for (const it of items) seq.push({ at: new Date(it.observed_at), type: 'item', it });
  for (const g of gaps) seq.push({ at: new Date(g.detected_at), type: 'gap', g });
  seq.sort((a, b) => a.at - b.at);

  const header = el('div', { className: 'tl-head' }, [
    el('span', { textContent: 'observed at' }),
    el('span', { textContent: 'event' }),
    el('span', { textContent: 'summary' }),
  ]);
  const body = el('div', { className: 'tl-body' });
  const timeline = el('div', { className: 'timeline' }, [header, body]);

  for (const entry of seq) {
    if (entry.type === 'gap') {
      body.appendChild(el('div', { className: 'tl-row gap' }, [
        el('span', { className: 'at', textContent: fmtTime(entry.g.detected_at) }),
        el('span', { className: 'badges' }, [badge('GAP', 'gap')]),
        el('span', { className: 'summary' }, [
          el('span', { textContent: entry.g.reason || 'history unavailable' }),
          el('span', { className: 'rv', textContent: `RV ${entry.g.from_resource_version || '?'} → ${entry.g.to_resource_version || '?'}` }),
        ]),
      ]));
      continue;
    }
    const it = entry.it;
    const badges = [watchBadge(it.watch_type, it.synthetic)];
    if (it.synthetic) badges.push(badge('SYNTHETIC', 'synthetic'));
    if (it.resource_version) badges.push(badge(`RV ${it.resource_version}`, 'neutral'));
    const summary = el('span', { className: 'summary', textContent: it.summary || '(no summary)' });
    if (it.changed_fields && it.changed_fields.length) summary.title = 'changed fields: ' + it.changed_fields.join(', ');
    body.appendChild(el('div', { className: 'tl-row' }, [
      el('span', { className: 'at', textContent: fmtTime(it.observed_at) }),
      el('span', { className: 'badges' }, badges),
      summary,
    ]));
  }

  results.appendChild(timeline);
}

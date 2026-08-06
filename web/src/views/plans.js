// Replay plans: create (POST /v1/replay-plans), review objects/exclusions, dry-run.
import { api } from '../api.js';
import { el, asList, simpleTable, badge, fmtTime, field, formCard } from '../util.js';
import { pickFirst, pickLatestSnapshot, pickNamespace, setSelect } from '../defaults.js';

export async function viewPlans(mount) {
  mount.appendChild(el('h1', { textContent: 'Replay plans' }));

  const [clusters, snaps, streams] = await Promise.all([
    api.clusters().catch(() => []),
    api.snapshots().catch(() => []),
    api.streams().catch(() => []),
  ]);
  const clusterList = asList(clusters);
  const snapshotList = asList(snaps);
  const streamList = asList(streams);

  const clusterSel = el('select');
  clusterSel.appendChild(el('option', { value: '', textContent: '— cluster —' }));
  for (const cl of clusterList) clusterSel.appendChild(el('option', { value: cl.id, textContent: cl.context || cl.id }));

  const snapshotSel = el('select');
  const fillSnapshots = () => {
    snapshotSel.textContent = '';
    snapshotSel.appendChild(el('option', { value: '', textContent: '— snapshot —' }));
    for (const sn of snapshotList) {
      if (!clusterSel.value || sn.cluster_id === clusterSel.value) {
        snapshotSel.appendChild(el('option', { value: sn.id, textContent: `${sn.name} · ${fmtTime(sn.at)}` }));
      }
    }
  };
  clusterSel.addEventListener('change', fillSnapshots);
  fillSnapshots();

  const sourceInput = el('input', { type: 'text', placeholder: 'source namespace' });
  const targetInput = el('input', { type: 'text', placeholder: 'target namespace' });
  const list = el('div', { className: 'plan-list' });

  const defCluster = pickFirst(clusterList);
  const defSnapshot = pickLatestSnapshot(snapshotList);
  const defNs = pickNamespace(streamList);
  if (defCluster) setSelect(clusterSel, defCluster.id);
  fillSnapshots();
  if (defSnapshot && (!clusterSel.value || defSnapshot.cluster_id === clusterSel.value)) snapshotSel.value = defSnapshot.id;
  if (defNs) {
    sourceInput.value = defNs;
    targetInput.value = defNs ? `${defNs}-copy` : '';
  }

  async function create(body) {
    list.textContent = '';
    list.appendChild(el('p', { className: 'muted', textContent: 'creating plan…' }));
    try {
      const plan = await api.createPlan(body);
      list.textContent = '';
      if (plan && plan.id) list.appendChild(planCard(plan));
      await load();
    } catch (err) {
      list.textContent = '';
      list.appendChild(el('div', { className: 'notice error', textContent: err.message || String(err) }));
    }
  }

  mount.appendChild(formCard([
    field('cluster', clusterSel),
    field('snapshot', snapshotSel),
    field('source namespace', sourceInput),
    field('target namespace', targetInput),
  ], 'create plan', () => {
    list.textContent = '';
    const body = {
      cluster_id: clusterSel.value,
      snapshot_id: snapshotSel.value,
      source_namespace: sourceInput.value.trim(),
      target_namespace: targetInput.value.trim(),
    };
    if (!body.cluster_id || !body.snapshot_id || !body.source_namespace || !body.target_namespace) {
      list.appendChild(el('div', { className: 'notice warn', textContent: 'cluster, snapshot, source and target namespace are required.' }));
      return;
    }
    create(body);
  }));
  mount.appendChild(list);

  async function load() {
    list.textContent = '';
    list.appendChild(el('p', { className: 'muted', textContent: 'loading plans…' }));
    try {
      const plans = asList(await api.plans());
      list.textContent = '';
      if (!plans.length) {
        list.appendChild(el('p', { className: 'muted', textContent: 'No plans yet — create one above.' }));
        return;
      }
      for (const p of plans) list.appendChild(planCard(p));
    } catch (err) {
      list.textContent = '';
      list.appendChild(el('div', { className: 'notice error', textContent: err.message || String(err) }));
    }
  }
  await load();
}

const itemName = (x) => (x.namespace ? `${x.namespace}/${x.name}` : x.name);

function planCard(p) {
  const head = el('div', { className: 'plan-head' }, [
    el('span', { className: 'id', textContent: p.id }),
    p.coverage_complete ? badge('COVERAGE OK', 'ok') : badge('COVERAGE INCOMPLETE', 'gap'),
    badge(p.status || 'PLANNED', 'neutral'),
    el('span', { className: 'muted', textContent: `created ${fmtTime(p.created_at)}` }),
  ]);
  const meta = el('div', { className: 'plan-meta' }, [
    el('span', {}, [el('span', { textContent: 'snapshot ' }), el('span', { className: 'mono', textContent: p.snapshot_id })]),
    el('span', {}, [el('span', { textContent: 'source ' }), el('span', { className: 'mono', textContent: p.source_namespace })]),
    el('span', {}, [el('span', { textContent: 'target ' }), el('span', { className: 'mono', textContent: p.target_namespace || 'same' })]),
    p.target_context ? el('span', {}, [el('span', { textContent: 'context ' }), el('span', { className: 'mono', textContent: p.target_context })]) : null,
    p.field_manager ? el('span', {}, [el('span', { textContent: 'manager ' }), el('span', { className: 'mono', textContent: p.field_manager })]) : null,
  ]);
  const body = el('div', { className: 'plan-body' }, [meta]);

  if (asList(p.warnings).length) {
    body.appendChild(el('div', { className: 'subsection' }, [
      el('h4', { textContent: 'warnings' }),
      el('ul', { className: 'clean' }, asList(p.warnings).map((w) => el('li', { textContent: w }))),
    ]));
  }

  const objects = asList(p.objects).slice().sort((a, b) => (a.order || 0) - (b.order || 0));
  if (objects.length) {
    body.appendChild(el('div', { className: 'subsection' }, [
      el('h4', { textContent: `objects (ordered, ${objects.length})` }),
      simpleTable(['#', 'kind', 'name', 'warnings'], objects.map((o) => [
        el('td', { className: 'num', textContent: String(o.order) }),
        el('td', { textContent: o.kind || '?' }),
        el('td', { className: 'mono', textContent: itemName(o) }),
        el('td', { className: 'muted', textContent: asList(o.warnings).length ? asList(o.warnings).join('; ') : '—' }),
      ])),
    ]));
  }

  const excluded = asList(p.excluded);
  if (excluded.length) {
    body.appendChild(el('div', { className: 'subsection' }, [
      el('h4', { textContent: `excluded (${excluded.length})` }),
      simpleTable(['kind', 'name', 'reason'], excluded.map((x) => [
        el('td', { textContent: x.kind || '?' }),
        el('td', { className: 'mono', textContent: itemName(x) }),
        el('td', { className: 'muted', textContent: x.reason || '—' }),
      ])),
    ]));
  }

  const drySection = el('div', { className: 'dryrun' });
  const dryBtn = el('button', { className: 'ghost', textContent: 'dry-run', type: 'button' });
  dryBtn.addEventListener('click', async () => {
    drySection.textContent = '';
    dryBtn.disabled = true;
    drySection.appendChild(el('p', { className: 'muted', textContent: 'running dry-run…' }));
    try {
      const r = await api.dryRun({ plan_id: p.id });
      // The server returns the full plan with the result nested under
      // dry_run_result; fall back to the top level defensively.
      const dr = r.dry_run_result || r;
      drySection.textContent = '';
      drySection.appendChild(el('div', {}, [
        el('span', { textContent: 'dry-run: ' }),
        dr.ok ? badge('OK', 'ok') : badge('NOT OK', 'gap'),
        el('span', { textContent: ` · applied ${dr.applied}` }),
        el('span', { textContent: ` · skipped ${asList(dr.skipped).length}` }),
      ]));
      for (const [label, list] of [['conflicts', dr.conflicts], ['errors', dr.errors], ['skipped', dr.skipped]]) {
        if (asList(list).length) {
          drySection.appendChild(el('div', { className: 'subsection' }, [
            el('h4', { textContent: label }),
            el('ul', { className: 'clean' }, asList(list).map((c) => el('li', { textContent: `${c.kind || '?'} ${itemName(c)} — ${c.message}` }))),
          ]));
        }
      }
    } catch (err) {
      drySection.textContent = '';
      drySection.appendChild(el('div', { className: 'notice error', textContent: err.message || String(err) }));
    } finally {
      dryBtn.disabled = false;
    }
  });
  drySection.appendChild(dryBtn);
  if (p.dry_run_result) {
    drySection.appendChild(el('p', { className: 'muted', textContent: `last dry-run: applied ${p.dry_run_result.applied} · ok: ${p.dry_run_result.ok}` }));
  }
  body.appendChild(drySection);

  return el('div', { className: 'plan-card' }, [head, body]);
}

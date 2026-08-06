// Snapshots: materialized-state snapshots with completeness + missing streams.
import { api } from '../api.js';
import { el, asList, simpleTable, badge, fmtTime } from '../util.js';

export async function viewSnapshots(mount) {
  mount.appendChild(el('h1', { textContent: 'Snapshots' }));
  const snaps = asList(await api.snapshots());
  if (!snaps.length) {
    mount.appendChild(el('p', { className: 'muted', textContent: 'No snapshots recorded yet.' }));
    return;
  }
  mount.appendChild(simpleTable(
    ['at (observed)', 'name', 'cluster', 'objects', 'streams', 'coverage', 'missing streams', 'warning'],
    snaps.map((sn) => {
      const missing = asList(sn.missing);
      return [
        el('td', { className: 'mono', textContent: fmtTime(sn.at) }),
        el('td', { className: 'mono', textContent: sn.name }),
        el('td', { className: 'mono', textContent: sn.cluster_id }),
        el('td', { className: 'num', textContent: String(sn.object_count) }),
        el('td', { className: 'num', textContent: String(sn.streams) }),
        el('td', {}, [sn.complete ? badge('COMPLETE', 'ok') : badge('INCOMPLETE', 'gap')]),
        el('td', {}, [missing.length ? el('span', { className: 'mono', textContent: missing.join(', ') }) : el('span', { className: 'muted', textContent: '—' })]),
        el('td', { className: 'muted', textContent: sn.warning || '—' }),
      ];
    })
  ));
}

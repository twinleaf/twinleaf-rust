// The Properties table's rows: every checked device's RPCs merged by name.
//
// A row is every device's entry of one name, grouped by what the entry is
// for (its channel), each group in the first device's order. It `differs`
// when the devices declare the entry with other types or access. What a row
// lets a person do, its control, is inferred from the entry's type and
// access, as the table carries no ranges or units.

/** The groups, by channel, in the order the table shows them. */
export const GROUPS = [
  ['setting', 'Settings'],
  ['report', 'Reports'],
  ['action', 'Actions'],
  ['diagnostic', 'Diagnostics'],
  [null, 'Other'],
];

/**
 * The control of an entry: `action` runs it, `toggle` turns a readable
 * bool on or off, `value` writes typed text, `display` only shows its value,
 * and `none` is an entry with no type to show.
 */
export function controlOf(rpc) {
  const read = rpc.access.includes('read'), write = rpc.access.includes('write');
  if (rpc.channel === 'action' || (!rpc.access.length && !rpc.type)) return 'action';
  if (!rpc.type) return 'none';
  if (rpc.type === 'bool' && write) return read ? 'toggle' : 'value';
  if (write) return 'value';
  return 'display';
}

/**
 * The rows of `devices`, each {route, table}, in group order, of the groups
 * whose channel is in `channels`: each row is {name, rpc, entries, differs,
 * group}, `rpc` the first device's entry, `entries` each device's by
 * route, and `group` the title of its group.
 */
export function mergeRows(devices, channels = GROUPS.map(([channel]) => channel)) {
  const byName = new Map();
  for (const d of devices) {
    for (const rpc of d.table?.rpcs ?? []) {
      let row = byName.get(rpc.name);
      if (!row) byName.set(rpc.name, (row = { name: rpc.name, rpc, entries: new Map(), differs: false }));
      row.entries.set(d.route, rpc);
      if (rpc.type !== row.rpc.type || rpc.access.join() !== row.rpc.access.join()) row.differs = true;
    }
  }
  const rows = [...byName.values()];
  return GROUPS.filter(([channel]) => channels.includes(channel)).flatMap(([channel, title]) => rows.filter((r) => (r.rpc.channel ?? null) === channel).map((r) => ({ ...r, group: title })));
}

/** The devices of `devices` a write of `row` reaches: those whose entry is writable, or an action. */
export function writersOf(row, devices) {
  return devices.filter((d) => {
    const rpc = row.entries.get(d.route);
    return rpc && (rpc.access.includes('write') || controlOf(rpc) === 'action');
  });
}

/** The order devices are listed in: by route, /2 before /10. */
export const byRoute = (a, b) => a.route.localeCompare(b.route, undefined, { numeric: true });

/** A device as the table and the list name it: its name at the root, else its route and name. */
export const deviceLabel = (d) => (d.route === '/' ? (d.name ?? 'device') : `${d.route.slice(1)} ${d.name ?? ''}`.trim());

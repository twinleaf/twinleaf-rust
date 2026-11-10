// The plot command table against its rules. Every entry is well formed, and
// the keys the page reads for a plot are the table's, in its order. For any
// plots, kinds, selection and origin, a command acts on what its origin and
// scope say (a plot's menu: the selection it is in, or it alone; the page:
// the selection, or with none every plot for a view command and none for a
// plot command), less the plots of kinds it does not take, and runs only
// when it has plots to act on, or takes none. Each command, run, reaches
// the plots through its context alone: the workspace it is given has no
// plots to read.
import { afterEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { COMMANDS, commandOf, commands } from './commands.js';
import { KEYMAP, actionFor, scopeOf } from './keymap.js';
import { KINDS } from './constants.js';
import { Selection } from './selection.svelte.js';

const IDS = ['a', 'b', 'c', 'd'];

describe('the command table', () => {
  it('has each command once, each entry of the fields it may have', () => {
    expect(new Set(COMMANDS.map((c) => c.id)).size).toBe(COMMANDS.length);
    const kinds = KINDS.map(([k]) => k);
    for (const c of COMMANDS) {
      expect(['view', 'plot'], c.id).toContain(c.scope);
      expect(c.kinds === undefined || c.kinds.every((k) => kinds.includes(k)), c.id).toBe(true);
      expect((c.needs ?? []).every((n) => ['workspace', 'axis'].includes(n)), c.id).toBe(true);
      expect([undefined, 'page'], c.id).toContain(c.place);
      expect(typeof c.run, c.id).toBe('function');
      // A key is shown and named; a row has an icon to go with its label.
      if (c.keys) expect(c.show?.length && c.what, c.id).toBeTruthy();
      if (c.label) expect(c.icon, c.id).toBeTruthy();
      // A command reached by no key, no row and no palette is reached by nothing.
      expect(!!(c.keys || c.label), c.id).toBe(true);
    }
  });

  it('is what the keys are: every plot key is a command’s, in the table’s order, and runs it', () => {
    const plot = KEYMAP.find((p) => p.place === 'plot').entries;
    expect(plot.map((e) => e.action)).toEqual(COMMANDS.filter((c) => c.keys && !c.place).map((c) => c.id));
    for (const c of COMMANDS.filter((c) => c.keys)) {
      for (const key of c.keys) expect(actionFor(c.place ?? 'plot', { key }), `${c.id} by ${key}`).toBe(c.id);
      expect(scopeOf(c.id)).toBe(c.scope);
    }
    // A key the sheet shows of a command and that the page reads elsewhere, as Ctrl+C, is shown as the table has it.
    const page = KEYMAP.find((p) => p.place === 'page').entries;
    for (const c of COMMANDS.filter((c) => c.place === 'page')) expect(page.some((e) => e.what === c.what && e.show === c.show), c.id).toBe(true);
  });
});

/** A plot of `kind` as a command takes it, its view and PlotView recording what is asked of them into `log`. */
function plotOf(id, kind, log, live = true) {
  const recorder = (name) => new Proxy({}, {
    get: (_, key) => {
      if (key === 'plotId') return id;
      if (key === 'live') return live;
      if (key === 'view') return recorder(`${id}.view`);
      if (key === 'config') return () => ({ kind });
      if (key === 'yAxes') return [{ key: 'shared' }];
      if (key === 'markers') return () => ({ a: null, b: null });
      if (key === 'cursorsPlaced') return () => 0;
      if (key === 'then') return undefined;
      return (...args) => (log.push(`${name}.${String(key)}`), key === 'axisUnits' ? 'V' : key === 'axisIn' ? 'shared' : args[0]);
    },
  });
  return { id, plot: { id, kind, legend: false, items: [{ key: 's' }] }, view: recorder(`${id}.view`), pv: recorder(`${id}.pv`) };
}

/**
 * Commands over plots of `kinds` (one per id) with `picked` selected and
 * `last` clicked last: every service records what it is asked into `log`,
 * and the workspace holds only whether plots are maximized.
 */
function page(kinds, picked, last, log, maximized = null) {
  const shown = IDS.slice(0, kinds.length);
  const selection = new Selection((id) => shown.includes(id), () => shown);
  selection.only(picked, last);
  const ws = new Proxy({ maximized }, {
    get: (t, key) => (key === 'maximized' ? t.maximized : expect.unreachable(`a command read ws.${String(key)}`)),
    set: (t, key, v) => (key === 'maximized' ? ((t.maximized = v), log.push(`maximized ${v}`), true) : false),
  });
  const record = (name) => (...args) => (log.push(`${name} ${JSON.stringify(args[0]?.map?.((t) => t.id ?? t) ?? args[0])}`), name === 'picker' ? true : Promise.resolve(null));
  const svc = {
    targets: (on) => selection.targets(on),
    pageTargets: (scope) => selection.pageTargets(scope),
    plotOf: (id) => plotOf(id, kinds[IDS.indexOf(id)], log),
    last: () => selection.last,
    ws,
    selection,
    viewHistory: { step: (fn) => fn(), burst: (kind, views, fn) => fn() },
    cursors: { set: (what, byId) => log.push(`cursors ${what} ${[...byId.keys()]}`), clear: record('clear'), followed: record('followed') },
    toast: { error: record('toast') },
    edit: record('edit'),
    split: record('split'),
    close: record('close'),
    copy: record('copy'),
    copyCheck: record('copyCheck'),
    openExport: record('openExport'),
    picker: record('picker'),
  };
  return { cmds: commands(svc) };
}

const runs = [];
afterEach(() => {
  for (const [c, run] of runs.splice(0)) c.run = run;
});
/** Has command `c` record the plots it is run on into `seen`, in place of running. */
function spy(c, seen) {
  runs.push([c, c.run]);
  c.run = (ctx) => void seen.push(ctx.plots.map((t) => t.id));
}

const setup = fc.record({
  kinds: fc.array(fc.constantFrom('time', 'spectrum'), { minLength: 1, maxLength: 4 }),
  picks: fc.subarray([0, 1, 2, 3]),
  on: fc.option(fc.nat(3)),
  last: fc.option(fc.nat(3)),
  id: fc.constantFrom(...COMMANDS.map((c) => c.id)),
});

describe('a command', () => {
  it('acts on what its origin and scope say, less the kinds it does not take, and runs only with plots to act on or where it takes none', () => {
    fc.assert(
      fc.property(setup, ({ kinds, picks, on, last, id }) => {
        const shown = IDS.slice(0, kinds.length);
        const picked = picks.map((i) => IDS[i]).filter((p) => shown.includes(p));
        const made = on !== null && shown.includes(IDS[on]) ? IDS[on] : null;
        const c = commandOf(id), seen = [];
        spy(c, seen);
        const p = page(kinds, picked, last === null ? null : IDS[last], []);
        // The oracle, from the rules alone.
        const ids = made ? (picked.includes(made) ? picked : [made]) : picked.length ? picked : c.scope === 'view' ? shown : [];
        const want = ids.filter((i) => !c.kinds || c.kinds.includes(kinds[IDS.indexOf(i)]));
        const taken = p.cmds.run(id, { on: made, from: made ? 'menu' : 'key', how: {} });
        expect(taken, `${id} taken`).toBe(ids.length > 0 || !!c.empty);
        expect(seen, `${id} run on`).toEqual(want.length || c.empty ? [want] : []);
      }),
    );
  });

  it('reaches the plots it acts on through its context alone, and hands the services only those', () => {
    fc.assert(
      fc.property(setup, fc.boolean(), ({ kinds, picks, on, last, id }, maximized) => {
        const log = [], shown = IDS.slice(0, kinds.length);
        const picked = picks.map((i) => IDS[i]).filter((p) => shown.includes(p));
        const made = on !== null && shown.includes(IDS[on]) ? IDS[on] : null;
        const p = page(kinds, picked, last === null ? null : IDS[last], log, maximized ? [shown[0]] : null);
        const ctx = p.cmds.context(id, { on: made });
        p.cmds.run(id, { on: made, from: made ? 'menu' : 'key', how: id === 'addYCursors' ? 'shared' : {} });
        const acted = new Set(ctx.plots.map((t) => t.id));
        // Each plot named in what was asked is one the command acts on.
        for (const line of log) for (const named of line.match(/\b[a-d]\b/g) ?? []) expect(acted.has(named), `${id}: ${line}`).toBe(true);
      }),
    );
  });

  it('reads its row from the same context: a value over the plots it acts on, made on a plot or from the page', () => {
    const log = [];
    const p = page(['time', 'time', 'spectrum'], ['a', 'b'], 'a', log);
    expect(p.cmds.row('close', { on: 'a' }).value).toBe('2 plots');
    expect(p.cmds.row('close', { on: 'c' }).value).toBe('1 signal');
    expect(p.cmds.row('exportData').value).toBe('2 selected');
    expect(p.cmds.row('exportData', { on: 'c' }).value).toBe(null);
    expect(p.cmds.row('maximize', { on: 'a' }).label).toBe('Maximize');
    expect(p.cmds.row('copyData', { on: 'a', how: { state: 'too-many', rows: 200000, reason: 'More' } })).toMatchObject({ disabled: true, value: { text: '200,000 rows', warn: true } });
    // The palette offers no command that needs an axis, and none with no plots to act on.
    const offered = p.cmds.offered().map((c) => c.id);
    expect(offered).not.toContain('addYCursors');
    expect(offered).toContain('splitRight');
    const none = page(['time'], [], null, log);
    expect(none.cmds.offered().map((c) => c.id)).toEqual(COMMANDS.filter((c) => c.label && c.scope === 'view' && !c.needs?.includes('axis')).map((c) => c.id));
  });
});

// The plot command table against its rules. Every entry is well formed, and
// the keys the page reads for a plot are the table's, in its order. For any
// plots, kinds, selection and origin, a command acts on what its origin and
// scope say (a plot's menu: the selection it is in, or it alone; the page:
// the selection, or with none every plot for a view command and none for a
// plot command), less the plots of kinds it does not take, and runs only
// when it has plots to act on, or takes none. Each command, run, reaches
// the plots through its context alone: the workspace it is given has no
// plots to read. A command that needs signals acts on those its origin
// says (a signal's row: the signals picked when it is one of them, or it
// alone; the palette: the one it names; the page: the signals picked),
// never runs with none, and its row counts what it acts on. The peak
// commands, on plots drawn from signals with known peaks, put an X cursor on
// the peak the rule says, step through every peak in order and past the
// view's edge, each as one edit and one view step, each plot of the lead's
// kind to a peak of its own; with none to go to, they do nothing and their
// rows say why. A command reads the same, by one label and one icon, from
// every origin; the palette lists what the rule says of the table, and no
// other; Window sets the views of the plots of the kind it is made on; and
// Zoom to X1–X2 cannot be chosen until its plot has both X cursors.
import { afterEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { COMMANDS, HOSTED, ORIGIN, PALETTE, PLOTLESS, commandOf, commands, listed } from './commands.js';
import { KEYMAP, actionFor, scopeOf } from './keymap.js';
import { KINDS } from './constants.js';
import { SIGNAL, Selection } from './selection.svelte.js';
import { CursorEdits, xOf } from './cursors.js';
import { PlotView } from './plot.js';

const IDS = ['a', 'b', 'c', 'd'];

describe('the command table', () => {
  it('has each command once, each entry of the fields it may have', () => {
    expect(new Set(COMMANDS.map((c) => c.id)).size).toBe(COMMANDS.length);
    const kinds = KINDS.map(([k]) => k);
    for (const c of COMMANDS) {
      expect(['view', 'plot', ...PLOTLESS], c.id).toContain(c.scope);
      // A command of a tab or the page acts on no plot, so runs with none.
      if (PLOTLESS.includes(c.scope)) expect(c.empty && !c.kinds, c.id).toBe(true);
      expect(c.kinds === undefined || c.kinds.every((k) => kinds.includes(k)), c.id).toBe(true);
      expect((c.needs ?? []).every((n) => [...HOSTED, 'axis', 'signals'].includes(n)), c.id).toBe(true);
      expect([undefined, 'page', 'list', 'tabs'], c.id).toContain(c.place);
      expect(typeof c.run, c.id).toBe('function');
      // A key is shown and named; a row has an icon to go with its label.
      if (c.keys) expect(c.show?.length && c.what, c.id).toBeTruthy();
      if (c.label) expect(c.icon, c.id).toBeTruthy();
      // The palette lists every command that has a label and needs nothing of where it is made, under a heading the palette has, and no other; a submenu marks one of the values it lists.
      expect(listed(c), c.id).toBe(!!c.label && !c.needs?.some((n) => ORIGIN.includes(n)));
      expect(c.palette === undefined ? !listed(c) : listed(c) && PALETTE.includes(c.palette), c.id).toBe(true);
      if (c.choices || c.chosen) expect(c.label && typeof c.choices, c.id).toBe('function');
    }
  });

  it('is what the keys are: every plot key is a command’s, in the table’s order, and runs it', () => {
    const plot = KEYMAP.find((p) => p.place === 'plot').entries;
    expect(plot.map((e) => e.action)).toEqual(COMMANDS.filter((c) => c.keys && !c.place).map((c) => c.id));
    for (const c of COMMANDS.filter((c) => c.keys)) {
      for (const key of c.keys) expect(actionFor(c.place ?? 'plot', { key, shiftKey: !!c.shift, ctrlKey: !!c.ctrl }), `${c.id} by ${key}`).toBe(c.id);
      expect(scopeOf(c.id)).toBe(c.scope);
    }
    // The Signals list's keys that are commands are the table's too, in its order.
    const list = KEYMAP.find((p) => p.place === 'list').entries.filter((e) => commandOf(e.action));
    expect(list.map((e) => e.action)).toEqual(COMMANDS.filter((c) => c.place === 'list').map((c) => c.id));
    // A key the sheet shows of a command and that the page reads elsewhere, as Ctrl+C, is shown as the table has it.
    const page = KEYMAP.find((p) => p.place === 'page').entries;
    for (const c of COMMANDS.filter((c) => c.place === 'page')) expect(page.some((e) => e.what === c.what && e.show === c.show), c.id).toBe(true);
  });
});

/** A plot of `kind` as a command takes it, its view and PlotView recording what is asked of them into `log`. */
function plotOf(id, kind, log, live = true, items = ['s']) {
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
  return { id, plot: { id, kind, legend: false, items: items.map((key) => ({ key })) }, view: recorder(`${id}.view`), pv: recorder(`${id}.pv`) };
}

/**
 * Commands over plots of `kinds` (one per id), each showing signals `items`, 's' unless given,
 * with `picked` selected, `last` clicked last and `signals` picked in the
 * Signals list: every service records what it is asked into `log`,
 * and the workspace holds no plot to read: only which plots are maximized,
 * which a command changes by `maximize` and `restoreLayout` alone, its tabs
 * by name, T open and U beside it, and `setView` and `movePlotToTab`, which
 * record the plot each is asked to change.
 */
function page(kinds, picked, last, log, maximized = null, signals = ['s'], items = ['s'], lacks = []) {
  const shown = IDS.slice(0, kinds.length);
  const selection = new Selection((id) => shown.includes(id), () => shown);
  selection.only(picked, last);
  if (signals.length) selection.pickRows(signals.map((k) => SIGNAL + k));
  const view = {
    maximized,
    maximize: (ids) => ((view.maximized = ids), void log.push(`maximize ${ids}`)),
    restoreLayout: () => ((view.maximized = null), void log.push('restoreLayout')),
    tabs: [{ id: 'T', name: 'T', layout: null }, { id: 'U', name: 'U', layout: null }],
    tab: { id: 'T', name: 'T', layout: null },
    activeId: 'T',
    duplicateTab: (t) => void log.push(`duplicateTab ${t.id}`),
    addTab: () => void log.push('addTab'),
    selectTab: (id) => void log.push(`selectTab ${id}`),
    arrangement: () => 'rows',
    arrange: (name) => void log.push(`arrange ${name}`),
    reset: () => void log.push('reset'),
    setView: (id) => void log.push(`setView ${id}`),
    movePlotToTab: (id, tab) => void log.push(`movePlotToTab ${id} ${tab}`),
  };
  const ws = new Proxy(view, {
    get: (t, key) => (key in t ? t[key] : expect.unreachable(`a command read ws.${String(key)}`)),
    set: (t, key) => expect.unreachable(`a command set ws.${String(key)}`),
  });
  const record = (name) => (...args) => (log.push(`${name} ${JSON.stringify(args[0]?.map?.((t) => t.id ?? t) ?? args[0])}`), name === 'picker' ? true : Promise.resolve(null));
  const svc = {
    has: (need) => !lacks.includes(need),
    targets: (on) => selection.targets(on),
    pageTargets: (scope) => selection.pageTargets(scope),
    signalTargets: (key) => selection.signalTargets(key),
    plotOf: (id) => plotOf(id, kinds[IDS.indexOf(id)], log, true, items),
    last: () => selection.last,
    ws,
    selection,
    viewHistory: { step: (fn) => fn(), burst: (kind, views, fn) => fn(), follow: record('follow') },
    cursors: { set: (what, byId) => log.push(`cursors ${what} ${[...byId.keys()]}`), clear: record('clear'), lastX: () => 'a' },
    toast: { error: record('toast'), success: record('toast') },
    edit: record('edit'),
    group: (what, fn) => (log.push(`group ${what}`), fn()),
    split: record('split'),
    close: record('close'),
    newPlot: record('newPlot'),
    copy: record('copy'),
    copyCheck: record('copyCheck'),
    copyImage: record('copyImage'),
    openExport: record('openExport'),
    picker: record('picker'),
    renameTab: (id) => void log.push(`renameTab ${id}`),
    closeTab: (t) => void log.push(`closeTab ${t.id}`),
    dockShown: (side) => side === 'left',
    toggleDock: (side) => void log.push(`toggleDock ${side}`),
    exportTabs: () => void log.push('exportTabs'),
    importTabs: () => void log.push('importTabs'),
  };
  return { cmds: commands(svc), svc };
}

const runs = [];
afterEach(() => {
  // Newest first, so a command replaced twice ends as it began.
  for (const [c, run] of runs.splice(0).reverse()) c.run = run;
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
        const ids = PLOTLESS.includes(c.scope) ? [] : made ? (picked.includes(made) ? picked : [made]) : picked.length ? picked : c.scope === 'view' ? shown : [];
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

  it('Maximize shows the plots it acts on alone, or leaves a maximized view whatever it acts on, from every origin, through the workspace’s two methods', () => {
    fc.assert(
      fc.property(setup, fc.boolean(), fc.constantFrom('menu', 'button', 'key', 'palette'), ({ kinds, picks, on }, maximized, from) => {
        const log = [], shown = IDS.slice(0, kinds.length);
        const picked = picks.map((i) => IDS[i]).filter((p) => shown.includes(p));
        // A menu row and a button are made on a plot; a key and the palette from the page.
        const made = from === 'menu' || from === 'button' ? IDS[(on ?? 0) % shown.length] : null;
        const p = page(kinds, picked, null, log, maximized ? [shown[0]] : null);
        const ids = made ? (picked.includes(made) ? picked : [made]) : picked;
        const taken = p.cmds.run('maximize', { on: made, from });
        expect(log, from).toEqual(maximized ? ['restoreLayout'] : ids.length ? [`maximize ${ids}`] : []);
        expect(taken, from).toBe(maximized || ids.length > 0);
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
    // Before the hub has counted, it waits: no count, no warning, and it cannot be chosen.
    expect(p.cmds.row('copyData', { on: 'a', how: { state: 'counting', rows: null } })).toMatchObject({ disabled: true, value: { text: 'Counting…', warn: false } });
    expect(p.cmds.row('copyData', { on: 'a', how: { state: 'ok', rows: 4000 } })).toMatchObject({ disabled: false, value: { text: '4,000 rows', warn: false } });
    // The palette offers the commands the table lists there, so none that needs an axis, and none with no plots to act on.
    const offered = p.cmds.offered().map((c) => c.id);
    expect(offered).not.toContain('addYCursors');
    expect(offered).toContain('splitRight');
    const none = page(['time'], [], null, log);
    // A command of the page's plots is offered while one of them is of its kinds: a capture plot's are not, with none.
    expect([...new Set(none.cmds.offered().map((c) => c.id))]).toEqual(COMMANDS.filter((c) => listed(c) && c.scope !== 'plot' && (!c.kinds || c.kinds.includes('time'))).map((c) => c.id));
    expect(page(['capture'], [], null, log).cmds.offered().map((c) => c.id)).toEqual(expect.arrayContaining(['captureNow', 'captureAuto']));
    // With none selected, Restore is offered while plots are maximized, as F then restores.
    expect(page(['time'], [], null, log, ['a']).cmds.offered()).toContainEqual({ id: 'maximize', how: undefined, label: 'Restore', find: 'plots Restore maximize', group: 'Plots' });
    // A command of choices has a row for each, in its own words, matched with its heading: a tab to go to, a dock to hide or show, an arrangement; a label of text reads the choice after it.
    const rows = (id, at = none) => at.cmds.offered().filter((c) => c.id === id).map((c) => [c.how.to, c.label, c.find, c.group]);
    expect(rows('openTab')).toEqual([['T', 'Go to T', 'tabs Go to T openTab', 'Tabs'], ['U', 'Go to U', 'tabs Go to U openTab', 'Tabs']]);
    expect(rows('dock')).toEqual([['left', 'Hide the left dock', 'docks Hide the left dock dock', 'Docks'], ['right', 'Show the right dock', 'docks Show the right dock dock', 'Docks']]);
    expect(rows('arrange')[0]).toEqual(['rows', 'Rows', 'arrange Rows arrange', 'Arrange']);
    expect(rows('setKind', p)).toEqual([['time', 'Kind › Time', 'plots Kind › Time setKind', 'Plots'], ['spectrum', 'Kind › Spectrum', 'plots Kind › Spectrum setKind', 'Plots']]);
    expect(rows('moveToTab', p)).toEqual([['U', 'Move to tab › U', 'plots Move to tab › U moveToTab', 'Plots']]);
    // A row made with a choice is that choice's own, with no submenu, and checks the arrangement the tab is in.
    expect(none.cmds.row('arrange', { how: { to: 'rows' } })).toMatchObject({ label: 'Rows', choices: null, value: { label: 'The layout now' } });
    expect(none.cmds.row('arrange', { how: { to: 'grid' } })).toMatchObject({ label: 'Grid', value: null });
    expect(none.cmds.row('exportTabs')).toMatchObject({ label: 'Export every tab…', value: '2 tabs' });
    expect(none.cmds.offered().find((c) => c.id === 'exportTabs')).toMatchObject({ label: 'Export every tab…', group: 'View' });
  });

  it('reads the same, one label and one icon, from every origin', () => {
    const ORIGINS = ['menu', 'button', 'key', 'palette', 'toolbar', 'row'];
    fc.assert(
      fc.property(setup, fc.option(fc.constantFrom('left', 'right', 'none', 'rows', 'time', 'T', 'U')), ({ kinds, picks, on, last, id }, to) => {
        const shown = IDS.slice(0, kinds.length);
        const picked = picks.map((i) => IDS[i]).filter((p) => shown.includes(p));
        const made = on !== null && shown.includes(IDS[on]) ? IDS[on] : null;
        const p = page(kinds, picked, last === null ? null : IDS[last], []);
        // A command of choices is made with one of them, or none; a y axis command on the axis.
        const c = commandOf(id), choice = c.choices && to !== null ? { to } : {};
        const how = c.needs?.includes('axis') ? { key: 'shared', ...choice } : c.choices && to !== null ? choice : undefined;
        const reads = ORIGINS.map((from) => {
          const r = p.cmds.row(id, { on: made, from, how });
          return [r.label, r.icon];
        });
        for (const r of reads) expect(r, `${id} with ${JSON.stringify(how)}`).toEqual(reads[0]);
      }),
    );
  });

  it('is in the palette as the rule says of the table: each listed command that can act now and be chosen, once for each choice, and no other', () => {
    fc.assert(
      fc.property(setup, fc.boolean(), ({ kinds, picks, last }, maximized) => {
        const shown = IDS.slice(0, kinds.length);
        const picked = picks.map((i) => IDS[i]).filter((p) => shown.includes(p));
        const p = page(kinds, picked, last === null ? null : IDS[last], [], maximized ? [shown[0]] : null);
        // The oracle, from the rule alone: a listed command, on the plots the page gives it, of its kinds.
        const want = COMMANDS.filter(listed).flatMap((c) => {
          const ctx = p.cmds.context(c.id, { from: 'palette' });
          const ids = PLOTLESS.includes(c.scope) ? [] : (picked.length ? picked : c.scope === 'view' ? shown : []).filter((i) => !c.kinds || c.kinds.includes(kinds[IDS.indexOf(i)]));
          const acts = PLOTLESS.includes(c.scope) || ids.length > 0 || (c.empty && !!c.active?.(ctx));
          if (!acts || c.disabled?.(ctx)) return [];
          return (c.choices?.(ctx) ?? [[undefined]]).map(([to]) => `${c.id} ${to}`);
        });
        expect(p.cmds.offered().map((o) => `${o.id} ${o.how?.to}`)).toEqual(want);
      }),
    );
  });

  it('Window sets the views of the plots it acts on of the kind of the plot it is made on, from the page of the time plots', () => {
    fc.assert(
      fc.property(setup, ({ kinds, picks, on, last }) => {
        const log = [], shown = IDS.slice(0, kinds.length);
        const picked = picks.map((i) => IDS[i]).filter((p) => shown.includes(p));
        const made = on !== null && shown.includes(IDS[on]) ? IDS[on] : null;
        const p = page(kinds, picked, last === null ? null : IDS[last], log);
        p.cmds.run('window', { on: made, from: made ? 'button' : 'toolbar', how: { s: 5, typed: true } });
        const ids = made ? (picked.includes(made) ? picked : [made]) : picked.length ? picked : shown;
        const kind = made ? kinds[IDS.indexOf(made)] : 'time';
        expect(log.filter((l) => l.endsWith('.setSpan')).map((l) => l[0]), `made on ${made} with ${picked}`).toEqual(ids.filter((i) => kinds[IDS.indexOf(i)] === kind));
      }),
    );
  });

  it('Zoom to X1–X2 cannot be chosen, and does nothing, until the plot it is made on has X1 and X2', () => {
    for (const [x, pair] of [[null, false], [{ a: '14 s', b: '—' }, false], [{ a: '—', b: '16 s' }, false], [{ a: '14 s', b: '16 s' }, true]]) {
      const log = [], p = page(['time', 'time'], ['a', 'b'], 'a', log);
      const plotOf = p.svc.plotOf;
      p.svc.plotOf = (id) => {
        const t = plotOf(id);
        return { ...t, pv: new Proxy(t.pv, { get: (pv, key) => (key === 'legend' ? { cursors: x && { x, ys: [] } } : pv[key]) }) };
      };
      for (const where of [{ on: 'a' }, { from: 'palette' }]) expect(p.cmds.row('zoomToCursors', where).disabled, `${JSON.stringify(x)} ${JSON.stringify(where)}`).toBe(!pair);
      expect(p.cmds.offered().some((o) => o.id === 'zoomToCursors')).toBe(pair);
      expect(p.cmds.run('zoomToCursors', { on: 'a', from: 'menu' })).toBe(pair);
      expect(log.filter((l) => l.endsWith('zoomToCursors'))).toEqual(pair ? ['a.pv.zoomToCursors'] : []);
    }
  });

  it('whose host has not what it needs does not run, has no row, asks nothing and is not in the palette, and every other command is as on a host that has it', async () => {
    /** Where each is made: on a plot, from its menu, a y axis command on its axis. */
    const at = (c) => ({ on: 'a', from: 'menu', how: c.needs?.includes('axis') ? 'shared' : undefined });
    for (const need of HOSTED) {
      const needing = COMMANDS.filter((c) => c.needs?.includes(need));
      expect(needing.length, `commands that need ${need}`).toBeGreaterThan(0);
      const log = [], full = page(['time', 'spectrum'], ['a'], 'a', []), p = page(['time', 'spectrum'], ['a'], 'a', log, null, ['s'], ['s'], [need]);
      for (const c of COMMANDS) {
        const seen = [];
        spy(c, seen);
        const lacks = needing.includes(c);
        const taken = full.cmds.run(c.id, at(c)), ran = seen.length;
        expect(p.cmds.run(c.id, at(c)), `${c.id} taken without ${need}`).toBe(lacks ? false : taken);
        expect(seen.length - ran, `${c.id} run without ${need}`).toBe(lacks ? 0 : ran);
        if (c.label) expect(p.cmds.row(c.id, at(c)).offered, `${c.id}'s row without ${need}`).toBe(!lacks && full.cmds.row(c.id, at(c)).offered);
        if (c.check && lacks) expect(await p.cmds.check(c.id, at(c)), `${c.id}'s check without ${need}`).toBe(null);
      }
      if (need === 'workspace') expect(log.filter((l) => l.startsWith('copyCheck')), 'asked of the page without a workspace').toEqual([]);
      expect(p.cmds.offered().map((o) => o.id), `the palette without ${need}`).toEqual(full.cmds.offered().map((o) => o.id).filter((id) => !needing.some((c) => c.id === id)));
    }
  });

  it('that needs signals acts on those its origin says, on the plots selected, and with no signal does not run', () => {
    const needing = COMMANDS.filter((c) => c.needs?.includes('signals'));
    expect(needing.length).toBeGreaterThan(0);
    fc.assert(
      fc.property(fc.constantFrom(...needing), fc.subarray(['x', 'y', 'z']), fc.constantFrom('x', 'y', 'z'), fc.subarray(IDS), fc.constantFrom('row', 'palette', 'page'), (c, signals, key, picked, origin) => {
        const seen = [];
        runs.push([c, c.run]);
        c.run = (ctx) => void seen.push({ signals: ctx.signals, plots: ctx.plots.map((t) => t.id) });
        const p = page(['time', 'time', 'spectrum', 'time'], picked, null, [], null, signals);
        // The oracle, from the rules alone.
        const want = origin === 'row' ? (signals.includes(key) ? signals : [key]) : origin === 'palette' ? [key] : signals;
        const taken = p.cmds.run(c.id, { ...(origin === 'row' && { signal: key }), ...(origin === 'palette' && { signals: [key] }), from: origin === 'page' ? 'key' : origin });
        expect(taken, `${c.id} taken`).toBe(want.length > 0 && (picked.length > 0 || !!c.empty));
        expect(seen, `${c.id} run on`).toEqual(taken ? [{ signals: want, plots: picked }] : []);
        // The palette's own list of commands has none that needs a signal: it names signals itself.
        expect(p.cmds.offered().map((o) => o.id)).not.toContain(c.id);
      }),
    );
  });

  it('that puts signals on plots has the row its menu shows: Remove where every plot it acts on shows every signal, else Add, each counting the signals and the plots it changes', () => {
    const log = [];
    // Every plot shows signal 's' alone.
    const row = (id, picked, signals) => page(['time', 'time', 'time'], picked, null, log, null, signals).cmds.row(id);
    expect(row('removeFromPlots', ['a', 'b'], ['s'])).toMatchObject({ offered: true, value: '2 plots', destructive: true, show: ['Delete'] });
    expect(row('addToPlots', ['a', 'b'], ['s']).offered).toBe(false);
    expect(row('removeFromPlots', ['a'], ['s'])).toMatchObject({ offered: true, value: null });
    expect(row('addToPlots', ['a', 'b'], ['s', 't'])).toMatchObject({ offered: true, value: '2 signals to 2 plots', disabled: false, show: ['Enter'] });
    expect(row('removeFromPlots', ['a', 'b'], ['s', 't']).offered).toBe(false);
    // Plots that show both signals.
    expect(page(['time', 'time', 'time'], ['a', 'b'], null, log, null, ['s', 't'], ['s', 't']).cmds.row('removeFromPlots')).toMatchObject({ offered: true, value: '2 signals from 2 plots' });
    expect(row('addToPlots', ['a'], ['t', 'u', 'v'])).toMatchObject({ offered: true, value: '3 signals' });
    expect(row('addToPlots', [], ['t'])).toMatchObject({ offered: true, value: 'No plot selected', disabled: true });
    expect(row('removeFromPlots', [], ['s']).offered).toBe(false);
    expect(row('newPlot', [], ['s', 't'])).toMatchObject({ offered: true, value: '2 signals', show: ['Shift', 'Enter'] });
    expect(row('newPlot', ['a'], ['s']).value).toBe('Time · Overlay');
  });
});

/**
 * A signal of `kind` of 20 slots of 10 points, with a peak in each of
 * `slots`, as {xs, ys, at}, `at` the x of each peak in order: a triangle
 * `tall` high, 1 to 3, on a floor of 0, and in each other slot a bump 0.1
 * high, which stands out by less than the rule asks. A time series's points
 * are a tenth of a second apart from 0, its y axis 0 to 3, so the rule asks
 * 0.3, and a capture's values are the same along its own x; a spectrum's
 * bins 1 Hz apart from 1 Hz, its amplitude 10 to the
 * power of half that, so its peaks stand 10 to 30 dB and its bumps 1 dB.
 */
function bumps(kind, slots, tall) {
  const lv = Array.from({ length: 200 }, (_, i) => {
    const k = Math.floor(i / 10), h = slots.includes(k) ? tall[slots.indexOf(k)] : 0.1;
    return h * Math.max(0, 1 - Math.abs((i % 10) - 5) / 3);
  });
  const xs = lv.map((_, i) => (kind === 'spectrum' ? i + 1 : i / 10));
  return { xs, ys: kind === 'spectrum' ? lv.map((v) => 10 ** (v / 2)) : lv, at: [...slots].sort((a, b) => a - b).map((k) => xs[k * 10 + 5]) };
}
const peakSlots = fc.uniqueArray(fc.nat(19), { maxLength: 6 }).chain((slots) => fc.tuple(fc.constant(slots), fc.array(fc.double({ min: 1, max: 3, noNaN: true }), { minLength: slots.length, maxLength: slots.length })));

/**
 * A page of plots `specs`, each {id, kind, g, view}, `g` a signal `bumps`
 * made and `view` what its x axis shows, with `selected` selected: each a
 * PlotView as the peak commands read it, drawing what its view shows (a
 * time plot's sources holding every point, a spectrum's every bin), its
 * cursors written by a CursorEdits. `log` records each edit and each view
 * step, and each view a command moves.
 */
function peakPage(specs, selected) {
  const log = { edits: [], steps: 0, views: [] };
  const plots = Object.fromEntries(specs.map(({ id, kind }) => [id, { id, kind, items: [{ key: `${id}.s` }], cursors: undefined }]));
  const pvs = {};
  const ws = {
    plots,
    setPlot: (id, patch) => void (plots[id] = { ...plots[id], ...patch }),
    setView: (id, patch) => void (log.views.push([id, 'fRange', ...patch.fRange]), (pvs[id].xd = patch.fRange)),
  };
  const cursors = new CursorEdits(ws, { group: (what, fn) => (log.edits.push(what), fn()) });
  const views = {};
  for (const { id, kind, g, view } of specs) {
    const t = { key: `${id}.s` }, src = { absStart: 0, absEnd: g.xs.length, xAt: (k) => g.xs[k], yAt: (k) => g.ys[k] };
    const pv = Object.assign(Object.create(PlotView.prototype), {
      plotId: id, config: () => ({ kind }), spectrum: kind === 'spectrum', slice: kind === 'time' ? null : {}, logX: false, xd: view, cursors,
      fExtent: [g.xs[0], g.xs.at(-1)], hoverTime: { t: null }, specHover: null, yAxes: [{ plans: [{ t }], range: [0, 3] }],
      xKind: () => (kind === 'spectrum' ? 'hz' : kind === 'capture' ? { unit: 's' } : 'zero'),
    });
    Object.defineProperty(pv, 'drawn', {
      get() {
        if (kind !== 'time') return { plans: [{ t, s: { f: g.xs, a: g.ys } }] };
        const first = g.xs.findIndex((x) => x >= pv.xd[0]), past = g.xs.findIndex((x) => x >= pv.xd[1]);
        return { plans: [{ t, src, a0: Math.max(0, first - 1), a1: past < 0 ? g.xs.length : Math.min(g.xs.length, past + 1) }] };
      },
    });
    pvs[id] = pv;
    views[id] = {
      show: (lo, hi) => void (log.views.push([id, 'show', lo, hi]), (pv.xd = [lo, hi])),
      pause: () => void log.views.push([id, 'pause']),
    };
  }
  const selection = new Selection((id) => !!plots[id], () => Object.keys(plots));
  selection.only(selected, null);
  const cmds = commands({
    has: () => true,
    targets: (on) => selection.targets(on),
    pageTargets: (scope) => selection.pageTargets(scope),
    signalTargets: () => [],
    plotOf: (id) => ({ id, plot: plots[id], view: views[id], pv: pvs[id] }),
    last: () => selection.last,
    ws, selection, cursors,
    viewHistory: { step: (fn) => (log.steps++, fn()) },
  });
  /** X cursor `name` of plot `id`. */
  const x = (id, name) => xOf(plots[id].cursors, plots[id].kind)[name];
  return { cmds, cursors, log, pvs, x };
}

/** The x of slot boundary `k` of a signal `bumps` made, on its floor. */
const edge = (g, k) => g.xs[Math.min(k * 10, g.xs.length - 1)];

describe('a peak command', () => {
  it('puts X1 on the peak of the view nearest it, or the middle, then steps right through every peak, past the view’s edge as one view step each, and back left, then does nothing, its row saying so', () => {
    fc.assert(
      fc.property(fc.constantFrom('time', 'spectrum', 'capture'), peakSlots, fc.integer({ min: 0, max: 20 }), fc.integer({ min: 0, max: 20 }), fc.option(fc.double({ min: 0, max: 1, noNaN: true })), (kind, [slots, tall], i, j, start) => {
        const g = bumps(kind, slots, tall), view = [edge(g, Math.min(i, j)), edge(g, Math.max(i, j))];
        fc.pre(view[1] > view[0]);
        const p = peakPage([{ id: 'a', kind, g, view }], ['a']);
        const from = start === null ? (view[0] + view[1]) / 2 : view[0] + start * (view[1] - view[0]);
        if (start !== null) p.cursors.set('Prepare', new Map([['a', { [{ time: 'x', spectrum: 'f', capture: 'c' }[kind]]: { a: from, b: null } }]]));
        const inView = g.at.filter((x) => x > view[0] && x < view[1]);
        const nearest = inView.reduce((best, x) => (best === null || Math.abs(x - from) < Math.abs(best - from) ? x : best), null);
        /** Runs command `id` from the key, and checks it made one edit and one view step, or with nowhere to go, nothing, its row disabled saying `why`. */
        const run = (id, why) => {
          const edits = p.log.edits.length, steps = p.log.steps, views = p.log.views.length, row = p.cmds.row(id, { on: 'a' });
          const taken = p.cmds.run(id, { from: 'key' });
          if (why) {
            expect(row, id).toMatchObject({ disabled: true, value: why });
            expect([taken, p.log.edits.length, p.log.steps, p.log.views.length], id).toEqual([false, edits, steps, views]);
            return;
          }
          expect(row.disabled, id).toBe(false);
          expect([taken, p.log.edits.length, p.log.steps], id).toEqual([true, edits + 1, steps + 1]);
        };
        run('x1ToPeak', nearest === null && 'No peak in view');
        if (nearest === null) return;
        expect(p.x('a', 'a')).toBe(nearest);
        // A time plot is paused as its X1 is placed.
        if (kind === 'time') expect(p.log.views).toContainEqual(['a', 'pause']);
        const span = view[1] - view[0];
        for (const [id, dir] of [['peakRight', 1], ['peakLeft', -1]]) {
          const ahead = g.at.filter((x) => dir * (x - p.x('a', 'a')) > 0);
          for (const want of dir > 0 ? ahead : ahead.reverse()) {
            const shown = p.pvs.a.xd;
            run(id);
            expect(p.x('a', 'a'), `${id} to ${want}`).toBe(want);
            // The view moves only for a peak past its edge, keeps its span, and shows the peak.
            const now = p.pvs.a.xd;
            if (want > shown[0] && want < shown[1]) expect(now).toBe(shown);
            else {
              expect(now[1] - now[0]).toBeCloseTo(span, 9);
              expect(want > now[0] && want < now[1], `${want} shown in ${now}`).toBe(true);
              // A slice's view stays within the x it has values at.
              if (kind !== 'time') expect(now[0] >= g.xs[0] - 1e-9 && now[1] <= g.xs.at(-1) + 1e-9, `${now} within ${[g.xs[0], g.xs.at(-1)]}`).toBe(true);
            }
          }
          run(id, `No peak to the ${dir > 0 ? 'right' : 'left'}`);
        }
      }),
    );
  });

  it('moves the X cursor placed or moved last, X1 where none is, on each selected plot of the lead plot’s kind to a peak of its own, as one edit and one view step', () => {
    const g = (slots) => bumps('time', slots, slots.map(() => 2));
    const [ga, gb] = [g([3, 8]), g([5, 12])];
    const view = [0, 19.9];
    const p = peakPage([{ id: 'a', kind: 'time', g: ga, view }, { id: 'b', kind: 'time', g: gb, view }, { id: 'c', kind: 'spectrum', g: bumps('spectrum', [4], [2]), view: [1, 200] }], ['a', 'b', 'c']);
    p.cmds.run('x1ToPeak', { on: 'a', from: 'menu' });
    // From the middle, 9.95 s: a's nearest peak is at 8.5 s, b's at 12.5 s; the spectrum is left as it was.
    expect([p.x('a', 'a'), p.x('b', 'a'), p.x('c', 'a')]).toEqual([8.5, 12.5, null]);
    expect(p.log.edits).toEqual(['X1 to peak (2 plots)']);
    expect(p.log.steps).toBe(1);
    // X2 placed last: Next peak right moves X2, each plot from where its own X2 is.
    p.cursors.set('Prepare', new Map([['a', { x: { a: 8.5, b: 1 } }], ['b', { x: { a: 12.5, b: 1 } }]]));
    expect(p.cursors.lastX('a')).toBe('b');
    expect(p.cmds.row('peakRight', { on: 'a' }).value).toBe('X2 Mixed');
    p.cmds.run('peakRight', { on: 'a', from: 'menu' });
    expect([p.x('a', 'b'), p.x('b', 'b'), p.x('a', 'a')]).toEqual([3.5, 5.5, 8.5]);
    expect(p.log.edits.at(-1)).toBe('Next peak right (2 plots)');
    // X2 removed: the one left, X1, is the one that steps.
    p.cursors.set('Prepare', new Map([['a', { x: { a: 8.5, b: null } }]]));
    expect(p.cursors.lastX('a')).toBe('a');
    // Made on the spectrum, it moves the spectrum alone: X1, not placed, from the view's left edge to its first peak, at 46 Hz.
    p.cmds.run('peakRight', { on: 'c', from: 'menu' });
    expect([p.x('c', 'a'), p.x('a', 'a')]).toEqual([46, 8.5]);
    // A plot with no peak to go to is left as it was while another moves.
    const q = peakPage([{ id: 'a', kind: 'time', g: g([2]), view }, { id: 'b', kind: 'time', g: g([]), view }], ['a', 'b']);
    q.cmds.run('x1ToPeak', { on: 'b', from: 'menu' });
    expect([q.x('a', 'a'), q.x('b', 'a'), q.log.edits]).toEqual([2.5, null, ['X1 to peak']]);
  });
});

// View history against a model of its rules (round 2, View history #2 to
// #8, round 1 #29), each stated here on its own. Random runs of drags,
// wheel and arrow-key bursts with gaps around IDLE_MS, the window's slider,
// y zooms, commands (follow live, pause, cursors placed and cleared, y set
// and fitted, a spectrum's frequencies), gestures on several plots, tab
// switches, and idle time, on a hub that has just started and holds two
// minutes, so for a while it has dropped nothing and later it drops what
// old views showed. The page draws between actions as it does each frame:
// a time plot fits its y range once, and a live spectrum takes the newest
// estimate. The model is what each tab shows after each gesture, the
// gestures grouped by its own rule (a drag, a command, or wheel turns or
// keys of one kind on the same plots each less than IDLE_MS apart); a group
// that ends where it began leaves no entry and takes nothing from Forward.
// Back shows the tab's entry before, Forward the one after; a paused time
// plot whose start the hub has since dropped starts at the oldest time it
// holds, never past the newest samples. Then examples of what the model
// leaves out: plots closed, moved to another tab or hidden behind a
// maximized one, and tabs gone for good.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { GONE, IDLE_MS, ViewHistory } from './viewhistory.svelte.js';
import { Views } from './views.js';
import { Workspace } from './workspace.svelte.js';
import { History } from './history.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { presets } from './layout.js';
import { makeTab, newPlot } from './tabs.js';

/** How far back the hub holds, in seconds; it started at device time START. */
const HELD_S = 120, START = 1000;

/**
 * Tab A of three time plots and tab B of two spectra, A open, each plot
 * with its view, on a clock the test moves, and a hub that holds HELD_S
 * seconds from START on: before START it never had anything.
 */
function setup() {
  const tab = (name, kind, n) => {
    const ps = Array.from({ length: n }, (_, i) => newPlot(kind, 'overlay', [`${name}.${i}`]));
    return makeTab(name, { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: presets.rows(ps.map((p) => p.id)) });
  };
  const tabs = [tab('A', 'time', 3), tab('B', 'spectrum', 2)];
  const ws = new Workspace({ tabs, activeId: tabs[0].id });
  const t = { ms: 0, notices: [] };
  const hub = { now: (perf) => START + perf / 1000, liveSeconds: () => 600, historySeconds: () => HELD_S };
  const oldest = () => Math.max(START, hub.now(t.ms) - HELD_S);
  const dropped = () => (oldest() > START ? oldest() : null);
  const views = new Views(hub);
  for (const id of ws.allPlotIds()) views.of(id);
  const vh = new ViewHistory(ws, views, { now: () => t.ms, dropped, notice: (text) => t.notices.push(text) });
  // Each signal's newest spectrum estimate, a new one as time passes.
  const estimate = new Map();
  const spectrumOf = (key) => {
    const at = Math.floor(t.ms / 5000);
    if (estimate.get(key)?.at !== at) estimate.set(key, { at, f: [1, 2], a: [at, at] });
    return estimate.get(key);
  };
  /**
   * What the page draws each frame does to the open tab's views: a time
   * plot fits y once, and paused keeps within what it captured; a live
   * spectrum takes its newest estimate.
   */
  const frame = () => {
    for (const id of Object.keys(ws.plots)) {
      const p = ws.plots[id], v = views.get(id), kind = p.kind === 'time' ? 'time' : 'spectrum:log';
      if (p.kind === 'time') v.limits = v.live ? null : [oldest(), v.pausedAt];
      if (!v.yByKind.has(kind)) v.yByKind.set(kind, new Map());
      const st = v.yByKind.get(kind).get('shared') ?? { range: null, auto: true };
      v.yByKind.get(kind).set('shared', st);
      if (p.kind === 'time' && st.auto) Object.assign(st, { range: [-1.2, 1.2], auto: false });
      if (p.kind === 'spectrum' && st.auto) st.range = [0, Math.floor(t.ms / 5000) + 1];
      if (p.kind === 'spectrum' && (v.live || !v.spectra.has(p.items[0].key))) v.spectra.set(p.items[0].key, spectrumOf(p.items[0].key));
    }
  };
  return { ws, views, vh, hub, t, dropped, frame };
}

/** What plot `p`'s view `v` shows, read from the view itself, in one string; spectra by `ids` of them. */
function showing(p, v, ids) {
  const placed = (m) => JSON.stringify([...m].sort().map(([k, byKey]) => [k, [...byKey].sort().filter(([, c]) => c.a !== null || c.b !== null)]).filter(([, e]) => e.length));
  const held = JSON.stringify([...v.yByKind].sort().map(([k, byKey]) => [k, [...byKey].sort().filter(([, st]) => !st.auto).map(([key, st]) => [key, st.range])]).filter(([, e]) => e.length));
  const paused = !v.live;
  return JSON.stringify({
    live: v.live, window: v.window, span: v.span, right: paused ? v.right : null, pausedAt: paused ? v.pausedAt : null,
    x: v.cursors, f: v.specCursors, y: placed(v.yCursorsByKind), ranges: held, fRange: p.fRange ?? null,
    spectra: paused ? [...v.spectra].map(([k, s]) => [k, ids(s)]) : null,
  });
}

const fraction = fc.double({ min: 0, max: 1, noNaN: true });
/** A gap between two wheel turns or key presses, in ms: anywhere up to a second, and often right about IDLE_MS. */
const gap = fc.oneof(fc.integer({ min: 0, max: 1000 }), fc.integer({ min: IDLE_MS - 20, max: IDLE_MS + 20 }), fc.constant(IDLE_MS));
/** The plots of the open tab a gesture acts on, as a mask: one or several, often the first or all, so gestures follow on the same plots. */
const mask = fc.oneof(fc.constantFrom(1, 7), fc.integer({ min: 1, max: 7 }));
/** A wheel turn's zoom: a small one, or one big enough that a plot soon stops at its narrowest or widest while another goes on. */
const factor = fc.oneof(fc.double({ min: 0.7, max: 1.4, noNaN: true }), fc.constantFrom(0.05, 3));
const move = fc.oneof(
  fc.record({ t: fc.constant('pan'), f: fc.double({ min: -0.5, max: 0.5, noNaN: true }) }),
  fc.record({ t: fc.constant('zoom'), f: factor, anchor: fraction }),
  fc.record({ t: fc.constant('cursor'), name: fc.constantFrom('a', 'b'), at: fc.option(fraction) }),
  fc.record({ t: fc.constant('ycursor'), name: fc.constantFrom('a', 'b'), v: fc.option(fc.double({ min: -1, max: 1, noNaN: true })) }),
  fc.record({ t: fc.constant('offsetY'), d: fc.double({ min: -1, max: 1, noNaN: true }) }),
  fc.record({ t: fc.constant('frequencies'), lo: fc.integer({ min: 1, max: 50 }) }),
);
const action = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('drag'), mask, moves: fc.array(fc.record({ gap, move }), { minLength: 1, maxLength: 5 }) }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('wheel'), mask, turns: fc.array(fc.record({ gap, f: factor, anchor: fraction }), { minLength: 1, maxLength: 6 }) }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('keys'), mask, presses: fc.array(fc.record({ gap, kind: fc.constantFrom('pan', 'zoom'), back: fc.boolean() }), { minLength: 1, maxLength: 5 }) }) },
  // A zoom in and straight back out, as ArrowUp then ArrowDown: a burst that ends where it began.
  { weight: 3, arbitrary: fc.record({ t: fc.constant('keys'), mask, presses: fc.constant([{ gap: 50, kind: 'zoom', back: false }, { gap: 50, kind: 'zoom', back: true }]) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('wheelY'), at: fc.nat(), turns: fc.array(fc.record({ gap, f: factor }), { minLength: 1, maxLength: 4 }) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('slider'), mask, held: fc.boolean(), spans: fc.array(fc.record({ gap, s: fc.constantFrom(1, 2, 5, 10, 20, 60) }), { minLength: 1, maxLength: 4 }) }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('command'), mask, what: fc.constantFrom('follow', 'follow', 'addX', 'addX', 'pause', 'clearX', 'addY', 'clearCursors', 'setY', 'fitY', 'frequencies', 'everyFrequency') }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('idle'), ms: fc.oneof(fc.integer({ min: 0, max: 2000 }), fc.integer({ min: 20_000, max: 200_000 })) }) },
  { weight: 1, arbitrary: fc.constant({ t: 'tab' }) },
  { weight: 4, arbitrary: fc.constant({ t: 'back' }) },
  { weight: 2, arbitrary: fc.constant({ t: 'forward' }) },
);

describe('view history, against a model of its rules', () => {
  it('makes one entry of each drag, command and burst of a tab, and Back and Forward show the tab as it was at the entry before and after', () => {
    // A run is quick, so there are always enough for each rule to be reached.
    const numRuns = Math.max(100, fc.readConfigureGlobal().numRuns);
    const count = { entries: 0, joined: 0, cameBack: 0, keptForward: 0, rested: 0, otherKind: 0, oneStopped: 0, several: 0, shown: 0, startup: 0, gone: 0, spectra: 0, frequencies: 0, yRanges: 0, cursors: 0 };
    fc.assert(
      fc.property(fc.array(action, { minLength: 10, maxLength: 40 }), (actions) => {
        const { ws, views, vh, hub, t, dropped, frame } = setup();
        const spectrumIds = new Map();
        const ids = (s) => (spectrumIds.has(s) || spectrumIds.set(s, spectrumIds.size), spectrumIds.get(s));
        const tabIds = () => Object.keys(ws.plots);
        const show = (id) => showing(ws.tabs.find((tab) => tab.plots[id]).plots[id], views.get(id), ids);
        const tabShows = () => new Map(tabIds().map((id) => [id, show(id)]));
        const same = (a, b) => [...a].every(([id, s]) => b.get(id) === s);
        frame();
        // The model: each tab's entries, each what the tab showed after its gesture and the plots that gesture moved, and which it shows.
        const model = new Map(ws.tabs.map((tab) => [tab.id, { line: [{ shows: new Map(Object.keys(tab.plots).map((id) => [id, show(id)])), moved: [] }], at: 0 }]));
        const known = new Map(ws.allPlotIds().map((id) => [id, show(id)]));
        let group = null; // the burst open to more: {tab, kind, targets, last, at, base, forwards}
        /** A group's entry, as the tab shows now: none, Forward as it was, when it shows what it did as the group began. */
        const settle = (g) => {
          const m = model.get(g.tab), now = tabShows(), none = same(now, g.base);
          if (none && g.made && g.forwards.length) count.keptForward++;
          g.made ||= !none;
          const moved = [...now.keys()].filter((id) => now.get(id) !== g.base.get(id));
          m.line = none ? [...m.line.slice(0, g.at + 1), ...g.forwards] : [...m.line.slice(0, g.at + 1), { shows: now, moved }];
          m.at = none ? g.at : g.at + 1;
          for (const [id, s] of now) known.set(id, s);
        };
        const begin = (kind, targets) => {
          const m = model.get(ws.activeId);
          return { tab: ws.activeId, kind, targets, last: t.ms, at: m.at, base: tabShows(), forwards: m.line.slice(m.at + 1) };
        };
        /** What showing entry `s` of plot `id` shows: a paused time plot whose start the hub dropped starts at the oldest it holds, never past the newest samples. */
        const restored = (id, s) => {
          const v = JSON.parse(s), p = ws.plots[id], d = dropped(), newest = hub.now(t.ms) - views.get(id).delay;
          if (v.live || p.kind !== 'time') return [s, false];
          let { right, pausedAt } = v, moved = false;
          if (d !== null && right - v.span < d) (moved = Math.max(right, d + v.span) > right), (right = Math.max(right, d + v.span)), (pausedAt = Math.max(pausedAt, right));
          return [JSON.stringify({ ...v, right: Math.min(right, newest), pausedAt: Math.min(pausedAt, newest) }), moved];
        };
        /** Back or Forward in the model: the plots the gesture of the entry gone past moved, as the entry `by` along shows them. */
        const go = (by) => {
          group = null;
          const m = model.get(ws.activeId), from = m.line[m.at].shows, changed = m.line[by < 0 ? m.at : m.at + 1].moved;
          m.at += by;
          const to = m.line[m.at].shows;
          let moved = false;
          for (const id of changed) {
            const [r, lost] = restored(id, to.get(id));
            moved ||= lost;
            known.set(id, r);
          }
          const differs = (field) => changed.some((id) => JSON.stringify(JSON.parse(to.get(id))[field]) !== JSON.stringify(JSON.parse(from.get(id))[field]));
          if (changed.length > 1) count.several++;
          if (changed.some((id) => JSON.parse(to.get(id)).spectra)) count.spectra++;
          if (differs('fRange')) count.frequencies++;
          if (differs('ranges')) count.yRanges++;
          if (differs('y') || differs('f')) count.cursors++;
          if (dropped() === null && changed.some((id) => ws.plots[id].kind === 'time' && !JSON.parse(to.get(id)).live && JSON.parse(to.get(id)).right - JSON.parse(to.get(id)).span < START)) count.startup++;
          count.shown++;
          return moved;
        };
        const kindOf = (id) => ws.plots[id].kind;
        const yOf = (id) => views.get(id).yByKind.get(kindOf(id) === 'time' ? 'time' : 'spectrum:log')?.get('shared');
        const setY = (v, name, val) => {
          if (!v.yCursorsByKind.has('time')) v.yCursorsByKind.set('time', new Map());
          const c = v.yCursorsByKind.get('time').get('shared') ?? { a: null, b: null };
          v.yCursorsByKind.get('time').set('shared', { ...c, [name]: val });
        };
        const moveOn = (id, m) => {
          const v = views.get(id);
          // Time pans and zooms on time plots: a spectrum's drag moves its frequencies.
          if (m.t === 'pan' && kindOf(id) === 'time') v.pan(m.f, t.ms);
          else if (m.t === 'zoom' && kindOf(id) === 'time') v.zoom(m.f, m.anchor, t.ms);
          else if (m.t === 'cursor') {
            v.pause(t.ms);
            const [x0, x1] = v.range(hub.now(t.ms));
            if (kindOf(id) === 'time') v.cursors[m.name] = m.at === null ? null : x0 + m.at * (x1 - x0);
            else v.specCursors[m.name] = m.at === null ? null : 1 + m.at;
          } else if (m.t === 'ycursor') setY(v, m.name, m.v);
          else if (m.t === 'offsetY') {
            const st = yOf(id);
            if (st?.range) Object.assign(st, { range: st.range.map((r) => r + m.d), auto: false });
          } else if (m.t === 'frequencies' && kindOf(id) === 'spectrum') ws.setView(id, { fRange: [m.lo, m.lo * 10] });
        };
        const command = (id, what) => {
          const v = views.get(id), st = yOf(id);
          if (what === 'follow') v.follow(true);
          else if (what === 'pause') v.pause(t.ms);
          else if (what === 'addX') {
            v.pause(t.ms);
            const [x0, x1] = v.range(hub.now(t.ms));
            if (kindOf(id) === 'time') v.cursors = { a: x0 + (x1 - x0) / 3, b: x0 + (2 * (x1 - x0)) / 3 };
            else v.specCursors = { a: 2, b: 3 };
          } else if (what === 'clearX') v.cursors = { a: null, b: null };
          else if (what === 'addY') (setY(v, 'a', -0.5), setY(v, 'b', 0.5));
          else if (what === 'clearCursors') v.clearCursors();
          else if (what === 'setY' && st) Object.assign(st, { range: [5, 6], auto: false });
          // As PlotView `refit` does: a time plot's at once to what it showed, a spectrum's to follow.
          else if (what === 'fitY' && st) Object.assign(st, kindOf(id) === 'time' ? { range: [-1.2, 1.2], auto: false } : { auto: true });
          else if (what === 'frequencies' && kindOf(id) === 'spectrum') ws.setView(id, { fRange: [2, 20] });
          else if (what === 'everyFrequency' && kindOf(id) === 'spectrum') ws.setView(id, { fRange: null });
        };
        const act = (a) => {
          const here = tabIds(), targets = here.filter((_, i) => a.mask & (1 << i)), vs = targets.map((id) => views.get(id));
          if (a.t === 'drag' || a.t === 'command' || (a.t === 'slider' && a.held)) {
            // A drag, the slider dragged, or a command: one group, whatever it passes through.
            group = null;
            const g = begin(null, targets);
            if (a.t === 'command') vh.step(() => targets.forEach((id) => command(id, a.what)));
            else {
              vh.begin();
              for (const p of a.moves ?? a.spans) {
                t.ms += p.gap;
                if (a.t === 'drag') targets.forEach((id) => moveOn(id, p.move));
                else vh.burst('window', vs, () => vh.step(() => targets.forEach((id) => kindOf(id) === 'time' && views.get(id).setSpan(p.s))));
              }
              vh.end();
            }
            frame();
            settle(g);
          } else if (a.t === 'wheel' || a.t === 'keys' || a.t === 'wheelY' || a.t === 'slider') {
            // A burst: each turn or press joins the group before it when of the same kind on the same plots less than IDLE_MS on.
            const on = a.t === 'wheelY' ? [here[a.at % here.length]] : targets;
            for (const p of a.turns ?? a.presses ?? a.spans) {
              t.ms += p.gap;
              const kind = { wheel: 'zoom', wheelY: 'zoomY', slider: 'window' }[a.t] ?? p.kind;
              const near = group && group.tab === ws.activeId && group.targets.join() === on.join();
              if (near && group.kind === kind && t.ms - group.last >= IDLE_MS) count.rested++;
              if (near && group.kind !== kind && t.ms - group.last < IDLE_MS) count.otherKind++;
              const joins = near && group.kind === kind && t.ms - group.last < IDLE_MS;
              if (joins) count.joined++;
              else group = begin(kind, on);
              group.last = t.ms;
              const was = new Map(on.map((id) => [id, show(id)]));
              vh.burst(kind, on.map((id) => views.get(id)), () => {
                for (const id of on) {
                  const v = views.get(id), st = yOf(id);
                  // The wheel, the keys and the slider move time on time plots only, as the page does.
                  const time = kindOf(id) === 'time';
                  if (a.t === 'wheel' && time) v.zoom(p.f, p.anchor, t.ms);
                  else if (a.t === 'wheelY' && st?.range) Object.assign(st, { range: st.range.map((r) => r * p.f), auto: false });
                  else if (a.t === 'slider' && time) vh.step(() => v.setSpan(p.s));
                  else if (kind === 'pan' && time) v.pan(p.back ? -0.1 : 0.1, t.ms);
                  else if (kind === 'zoom' && time) v.zoom(p.back ? 1.25 : 0.8, v.live ? 1 : 0.5, t.ms);
                }
              });
              if (joins && on.some((id) => show(id) === was.get(id)) && on.some((id) => show(id) !== was.get(id))) count.oneStopped++;
              frame();
              const entries = model.get(group.tab).line.length;
              settle(group);
              if (joins && same(tabShows(), group.base)) count.cameBack++;
              if (model.get(group.tab).line.length > entries) count.entries++;
            }
          } else if (a.t === 'idle') (t.ms += a.ms), frame();
          else if (a.t === 'tab') (ws.selectTab(ws.tabs.find((tab) => tab.id !== ws.activeId).id), frame());
          else if (a.t === 'back' || a.t === 'forward') {
            const notices = t.notices.length, by = a.t === 'back' ? -1 : 1, m = model.get(ws.activeId);
            const can = m.at + by >= 0 && m.at + by < m.line.length;
            expect(vh[a.t](), `${a.t}: went`).toBe(can);
            group = null;
            const moved = can && go(by);
            frame();
            // A notice comes with a view moved to start at the oldest time held, never while the hub has dropped nothing.
            if (t.notices.length > notices) {
              expect(t.notices.slice(notices)).toEqual([GONE]);
              expect(dropped(), 'a notice while nothing was dropped').not.toBeNull();
              expect(moved, 'a notice for views the model keeps where they were').toBe(true);
              count.gone++;
            }
          }
          // Every plot shows what the model says, in every tab, and Back and Forward can be pressed just when there is an entry to go to.
          for (const [id, s] of known) expect(show(id), `${a.t}: ${id}`).toBe(s);
          const m = model.get(ws.activeId);
          expect(vh.canBack, `${a.t}: can go back`).toBe(m.at > 0);
          expect(vh.canForward, `${a.t}: can go forward`).toBe(m.at < m.line.length - 1);
        };
        actions.forEach(act);
        // Then all the way back and forward again in each tab, so every entry made is checked.
        for (const tab of ws.tabs) {
          if (ws.activeId !== tab.id) act({ t: 'tab' });
          while (model.get(tab.id).at > 0) act({ t: 'back' });
          while (model.get(tab.id).at < model.get(tab.id).line.length - 1) act({ t: 'forward' });
        }
      }),
      { numRuns },
    );
    // Each rule had cases that reached it, a few in every hundred runs.
    for (const [rule, n] of Object.entries(count)) expect(n, rule).toBeGreaterThanOrEqual(numRuns / 25);
  }, 120_000);
});

describe('view history', () => {
  /** Tab A of three time plots, tab B of two spectra, and their views, with A open and `ids` its plots and `b` B's. */
  const page = () => {
    const p = setup();
    return { ...p, ids: Object.keys(p.ws.plots), b: Object.keys(p.ws.tabs[1].plots) };
  };

  it('shows a view that starts before the hub had anything as it was, with no notice, while the hub has dropped nothing', () => {
    // The hub's oldest time is its first sample, which is no drop.
    const { ws, views, t, hub } = page(), id = Object.keys(ws.plots)[0], v = views.get(id);
    const vh = new ViewHistory(ws, views, { now: () => t.ms, oldest: () => START + 3, dropped: () => null, notice: (text) => t.notices.push(text) });
    t.ms = 2000;
    vh.step(() => v.pause(t.ms));
    const paused = v.state();
    vh.burst('zoom', [v], () => v.zoom(0.5, 0.5, t.ms));
    vh.back();
    expect(t.notices).toEqual([]);
    expect(v.right).toBe(paused.right);
    expect(v.right).toBeLessThanOrEqual(hub.now(t.ms));
  });

  it('never shows a paused view past the newest samples, even moved later to start where the hub has dropped', () => {
    const { ws, views, vh, t, hub } = page(), v = views.get(Object.keys(ws.plots)[0]);
    vh.step(() => v.pause(t.ms));
    // As wide as the history: started at the oldest time held, it would end past the newest samples.
    vh.step(() => (v.span = HELD_S));
    vh.step(() => v.follow());
    t.ms = (HELD_S + 30) * 1000;
    vh.back();
    expect(v.right).toBeLessThanOrEqual(hub.now(t.ms) - v.delay);
    expect(t.notices).toEqual([GONE]);
  });

  it('can go back only to steps that change what shows, and leaves those of closed plots where they are', () => {
    const { ws, views, vh, t, ids } = page();
    vh.step(() => views.get(ids[0]).pause(t.ms));
    ws.closePlot(ids[0]);
    expect(vh.canBack).toBe(false);
    expect(vh.back()).toBe(false);
    expect(vh.canForward).toBe(false);
  });

  it('keeps Forward when a burst ends where it began', () => {
    const { ws, views, vh, t, ids } = page(), v = views.get(ids[0]);
    vh.step(() => v.pause(t.ms));
    vh.back();
    expect(vh.canForward).toBe(true);
    vh.burst('zoom', [v], () => v.zoom(0.8, 1, t.ms));
    vh.burst('zoom', [v], () => v.zoom(1.25, 1, t.ms));
    expect(vh.canForward).toBe(true);
    expect(vh.canBack).toBe(false);
  });

  it('makes one step of a burst on several plots when one stops at its narrowest as the others go on', () => {
    const { ws, views, vh, t, ids } = page(), [a, b] = [views.get(ids[0]), views.get(ids[1])];
    vh.step(() => (a.pause(t.ms), b.pause(t.ms), (a.span = 0.003)));
    const was = [a.state(), b.state()];
    for (let i = 0; i < 4; i++) (t.ms += 50), vh.burst('zoom', [a, b], () => [a, b].forEach((v) => v.zoom(0.5, 0.5, t.ms)));
    vh.back();
    expect([a.state().span, b.state().span]).toEqual([was[0].span, was[1].span]);
  });

  it('holds a spectrum’s frequencies, y ranges, cursors and the spectrum it held paused, and never moves a spectrum for what the hub dropped', () => {
    const { ws, views, vh, t, b, frame } = page();
    ws.selectTab(ws.tabs[1].id);
    frame();
    const v = views.get(b[0]), key = ws.plots[b[0]].items[0].key;
    vh.step(() => v.pause(t.ms));
    const held = v.spectra.get(key), paused = v.state();
    t.ms += 10_000;
    vh.step(() => v.follow());
    frame();
    expect(v.spectra.get(key)).not.toBe(held);
    vh.step(() => (ws.setView(b[0], { fRange: [2, 20] }), (v.specCursors = { a: 3, b: 4 }), v.yByKind.set('spectrum:log', new Map([['shared', { range: [0, 9], auto: false }]]))));
    t.ms += (HELD_S + 60) * 1000;
    vh.back();
    expect([ws.plots[b[0]].fRange, v.specCursors, v.yByKind.get('spectrum:log')?.get('shared')]).toEqual([null, { a: null, b: null }, undefined]);
    vh.back();
    expect([v.live, v.right, v.spectra.get(key)]).toEqual([false, paused.right, held]);
    expect(t.notices).toEqual([]);
  });

  it('while plots are maximized, goes back past the steps of plots hidden, and takes them up once they show again', () => {
    const { ws, views, vh, t, ids } = page(), [a, b] = [views.get(ids[0]), views.get(ids[1])];
    vh.step(() => a.pause(t.ms));
    vh.step(() => b.pause(t.ms));
    ws.maximized = [ids[0]];
    expect(vh.back()).toBe(true);
    expect([a.live, b.live]).toEqual([true, false]);
    expect(vh.canBack).toBe(false);
    ws.maximized = null;
    expect(vh.back()).toBe(true);
    expect(b.live).toBe(true);
  });

  it('leaves the steps of a plot moved to another tab in the tab they were made in, which goes back past them', () => {
    const { ws, views, vh, t, ids } = page(), a = views.get(ids[0]);
    vh.step(() => a.pause(t.ms));
    ws.movePlotToTab(ids[0], ws.tabs[1].id);
    expect(vh.canBack).toBe(false);
    ws.selectTab(ws.tabs[1].id);
    expect(vh.canBack).toBe(false);
    ws.movePlotToTab(ids[0], ws.tabs[0].id);
    ws.selectTab(ws.tabs[0].id);
    expect(vh.back()).toBe(true);
    expect(a.live).toBe(true);
  });

  it('lets go of a tab’s steps once neither the tabs nor an undo or a redo hold the tab', () => {
    const { ws, views, vh, t, ids } = page();
    const history = new History(ws, new Favorites(null));
    vh.step(() => views.get(ids[0]).pause(t.ms));
    const a = ws.activeId;
    history.group('Close tab', () => ws.closeTab(ws.tabs[0]));
    vh.prune(history.held().tabs);
    expect(vh.holds(a)).toBe(true);
    // Edits enough that the close falls off the end of what undo keeps.
    for (let i = 0; i < 200; i++) history.group('Rename', () => ws.renameTab(ws.tabs[0].id, `B${i}`));
    vh.prune(history.held().tabs);
    expect(vh.holds(a)).toBe(false);
  });
});

describe('a plot closed and brought back by undo', () => {
  /** The page as `setup` makes it, with its edit history, which keeps the views of the plots it holds as the page does. */
  const page = () => {
    const p = setup(), history = new History(p.ws, new Favorites(null));
    const edit = (fn) => (fn(), p.views.prune(history.held().plots));
    return { ...p, history, edit, ids: Object.keys(p.ws.plots) };
  };

  it('shows its view as it was, and Back goes on through its steps', () => {
    const { ws, views, vh, t, history, edit, ids } = page(), v = views.get(ids[0]);
    vh.step(() => (v.pause(t.ms), (v.cursors = { a: 999, b: null })));
    const was = v.state();
    edit(() => history.group('Close plot', () => ws.closePlot(ids[0])));
    expect(views.get(ids[0]), 'kept while an undo can bring it back').toBe(v);
    expect(vh.canBack).toBe(false);
    edit(() => history.undo());
    vh.returned([ids[0]]);
    expect(views.get(ids[0]).state()).toEqual(was);
    expect(vh.back()).toBe(true);
    expect(v.live).toBe(true);
    expect(t.notices).toEqual([]);
  });

  it('starts at the oldest time held, with the notice, when the hub has dropped where it started meanwhile', () => {
    const { ws, views, vh, t, hub, history, edit, ids } = page(), v = views.get(ids[0]);
    vh.step(() => v.pause(t.ms));
    edit(() => history.group('Close plot', () => ws.closePlot(ids[0])));
    t.ms = (HELD_S + 60) * 1000;
    edit(() => history.undo());
    vh.returned([ids[0]]);
    expect(v.right - v.span).toBeCloseTo(hub.now(t.ms) - HELD_S);
    expect(t.notices).toEqual([GONE]);
  });

  it('lets go of its view once no undo or redo can bring it back', () => {
    const { ws, views, history, edit, ids } = page();
    edit(() => history.group('Close plot', () => ws.closePlot(ids[0])));
    for (let i = 0; i < 200; i++) edit(() => history.group('Rename', () => ws.renameTab(ws.tabs[0].id, `A${i}`)));
    expect(views.get(ids[0])).toBeUndefined();
  });
});

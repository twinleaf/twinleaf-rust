// View history against a model of its rules (round 2, View history #2 to
// #8, round 1 #29), each stated here on its own. Random runs of drags,
// wheel and arrow-key bursts with gaps around IDLE_MS, the window field's,
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
// holds, never past the newest samples. Cursors are edits, in the tab: the
// run places, moves and removes them in its gestures; following live takes
// a plot's time X cursors, and Back or Forward takes those of a plot it
// shows live and puts back, on one live it shows paused, those it had as
// it last left that paused view to go live: as the entry's gesture began,
// Back, or ended, Forward, unless Back or Forward has since made it live
// from there, which takes the ones it has then. Nothing else changes one,
// and no plot that follows live has time X cursors. Then examples of what the model
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
import { CursorEdits, hasTimeX, withX, withXPair, withY, withoutX, xOf } from './cursors.js';

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
  const hub = { now: (perf) => START + perf / 1000, liveSeconds: () => 600 };
  const oldest = () => Math.max(START, hub.now(t.ms) - HELD_S);
  const dropped = () => (oldest() > START ? oldest() : null);
  const views = new Views(hub);
  // What the hub holds of every plot's signals: each view reads the oldest time its plot can show from it.
  views.held = () => ({ oldest: oldest(), dropped: dropped() !== null, history: null });
  for (const id of ws.allPlotIds()) views.of(id);
  const history = new History(ws, new Favorites(null)), cursors = new CursorEdits(ws, history);
  const vh = new ViewHistory(ws, views, { now: () => t.ms, notice: (text) => t.notices.push(text), edit: (label, fn) => history.group(label, fn), cursors });
  // Each signal's newest spectrum estimate, a new one as time passes.
  const estimate = new Map();
  const spectrumOf = (key) => {
    const at = Math.floor(t.ms / 5000);
    if (estimate.get(key)?.at !== at) estimate.set(key, { at, f: [1, 2], a: [at, at] });
    return estimate.get(key);
  };
  /**
   * What the page draws each frame does to the open tab's views: a time
   * plot fits y once, and a live spectrum takes its newest estimate.
   */
  const frame = () => {
    for (const id of Object.keys(ws.plots)) {
      const p = ws.plots[id], v = views.get(id), kind = p.kind === 'time' ? 'time' : 'spectrum:log';
      if (!v.yByKind.has(kind)) v.yByKind.set(kind, new Map());
      const st = v.yByKind.get(kind).get('shared') ?? { range: null, auto: true };
      v.yByKind.get(kind).set('shared', st);
      if (p.kind === 'time' && st.auto) Object.assign(st, { range: [-1.2, 1.2], auto: false });
      if (p.kind === 'spectrum' && st.auto) st.range = [0, Math.floor(t.ms / 5000) + 1];
      if (p.kind === 'spectrum' && (v.live || !v.spectra.has(p.items[0].key))) v.spectra.set(p.items[0].key, spectrumOf(p.items[0].key));
    }
  };
  return { ws, views, vh, hub, t, dropped, frame, history, cursors };
}

/** Whether no plot of any tab whose view follows live has time X cursors. */
const noLiveX = (ws, views) => ws.tabs.every((tab) => Object.entries(tab.plots).every(([id, p]) => !views.get(id)?.live || !hasTimeX(p.cursors)));

/** What plot `p`'s view `v` shows, read from the view itself, in one string; spectra by `ids` of them. */
function showing(p, v, ids) {
  const held = JSON.stringify([...v.yByKind].sort().map(([k, byKey]) => [k, [...byKey].sort().filter(([, st]) => !st.auto).map(([key, st]) => [key, st.range])]).filter(([, e]) => e.length));
  const paused = !v.live;
  return JSON.stringify({
    live: v.live, window: v.window, span: v.span, right: paused ? v.right : null, pausedAt: paused ? v.pausedAt : null,
    ranges: held, fRange: p.fRange ?? null,
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
  { weight: 1, arbitrary: fc.record({ t: fc.constant('window'), mask, spans: fc.array(fc.record({ gap, s: fc.constantFrom(1, 2, 5, 10, 20, 60) }), { minLength: 1, maxLength: 4 }) }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('command'), mask, what: fc.constantFrom('follow', 'follow', 'addX', 'addX', 'pause', 'clearX', 'addY', 'clearCursors', 'setY', 'fitY', 'frequencies', 'everyFrequency') }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('idle'), ms: fc.oneof(fc.integer({ min: 0, max: 2000 }), fc.integer({ min: 20_000, max: 200_000 })) }) },
  { weight: 1, arbitrary: fc.constant({ t: 'tab' }) },
  { weight: 4, arbitrary: fc.constant({ t: 'back' }) },
  { weight: 2, arbitrary: fc.constant({ t: 'forward' }) },
);

/** `n` wheel turns `gap` ms apart on the plots of `mask`, each zooming by `f` about the middle. */
const turns = (mask, n, f, gap) => ({ t: 'wheel', mask, turns: Array.from({ length: n }, () => ({ gap, f, anchor: 0.5 })) });
/** Ten seconds' rest, which ends any burst. */
const rest = { t: 'idle', ms: 10_000 };
/**
 * Runs that reach the burst on several plots one of which stops at its
 * narrowest or widest while another goes on, whatever the random runs do:
 * one plot zoomed to its narrowest or widest alone, then a burst on it and
 * the next, which goes on while the first stays.
 */
const STOPS = [0.05, 3].flatMap((f) => [1, 2].map((first) => [turns(first, 6, f, 0), rest, turns(first | (first << 1), 6, f, 50), rest, rest, rest, rest, rest, { t: 'back' }, { t: 'back' }]));
const runs = fc.array(action, { minLength: 10, maxLength: 40 });
/**
 * The runs tried first, every time: those of STOPS, and runs drawn once
 * from the generator by a seed of their own. Each rule is counted over
 * these alone, so how often each is reached is the same on every run of
 * the test, whatever the random runs after them do.
 */
const EXAMPLES = [...STOPS, ...fc.sample(runs, { seed: 1, numRuns: 100 })].map((actions) => [actions]);
/** The least number of the examples that reach each rule. */
const REACHED = 4;

describe('view history, against a model of its rules', () => {
  it('makes one entry of each drag, command and burst of a tab, and Back and Forward show the tab as it was at the entry before and after', () => {
    // A run is quick, so the random runs come on top of the examples, a hundred at the least.
    const numRuns = Math.max(100, fc.readConfigureGlobal().numRuns);
    const reached = { entries: 0, joined: 0, cameBack: 0, keptForward: 0, rested: 0, otherKind: 0, oneStopped: 0, several: 0, shown: 0, startup: 0, gone: 0, spectra: 0, frequencies: 0, yRanges: 0, cursorsKept: 0, xTaken: 0, xBack: 0 };
    let tried = 0;
    fc.assert(
      fc.property(runs, (actions) => {
        // The examples' counts go to `reached`; a random run's go nowhere.
        const count = tried++ < EXAMPLES.length ? reached : { ...reached };
        const { ws, views, vh, hub, t, dropped, frame, cursors: edits } = setup();
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
        // The cursors each plot has, as the run's cursor edits, following
        // live, and Back and Forward across a pause left them: nothing else
        // changes them.
        const cursors = new Map();
        const editCursors = (id, change) => {
          edits.set('Edit', new Map([[id, change(ws.plots[id].cursors)]]));
          cursors.set(id, ws.plots[id].cursors);
        };
        /** Following live, as every Follow live, Pause and Live of the page does: the time X cursors go. */
        const follow = (ids) => {
          vh.follow(ids, true);
          for (const id of ids) cursors.set(id, withoutX(cursors.get(id), 'time'));
        };
        /** The time X cursors of the open tab's plots, by id. */
        const tabXs = () => new Map(tabIds().map((id) => [id, xOf(ws.plots[id].cursors, 'time')]));
        let group = null; // the burst open to more: {tab, kind, targets, last, at, base, xs, forwards}
        /**
         * A group's entry, as the tab shows now: none, Forward as it was, when
         * it shows what it did as the group began. It holds the time X cursors
         * of the plots it moved as the group began and as it ended.
         */
        const settle = (g) => {
          const m = model.get(g.tab), now = tabShows(), none = same(now, g.base);
          if (none && g.made && g.forwards.length) count.keptForward++;
          g.made ||= !none;
          const moved = [...now.keys()].filter((id) => now.get(id) !== g.base.get(id));
          m.line = none ? [...m.line.slice(0, g.at + 1), ...g.forwards] : [...m.line.slice(0, g.at + 1), { shows: now, moved, xs: { before: g.xs, after: tabXs() } }];
          m.at = none ? g.at : g.at + 1;
          for (const [id, s] of now) known.set(id, s);
        };
        const begin = (kind, targets) => {
          const m = model.get(ws.activeId);
          return { tab: ws.activeId, kind, targets, last: t.ms, at: m.at, base: tabShows(), xs: tabXs(), forwards: m.line.slice(m.at + 1) };
        };
        /** What showing entry `s` of plot `id` shows: a paused time plot whose start the hub dropped starts at the oldest it holds, no wider than what is held, never past the newest samples. */
        const restored = (id, s) => {
          const v = JSON.parse(s), p = ws.plots[id], d = dropped(), newest = hub.now(t.ms) - views.get(id).delay;
          if (v.live || p.kind !== 'time') return [s, false];
          let { span, right, pausedAt } = v, moved = false;
          right = Math.min(right, newest);
          pausedAt = Math.min(pausedAt, newest);
          if (d !== null && right - span < d) {
            const fits = Math.min(span, newest - d), to = Math.min(newest, Math.max(right, d + fits));
            moved = fits !== span || to !== right;
            if (moved) [span, right, pausedAt] = [fits, to, Math.max(pausedAt, to)];
          }
          return [JSON.stringify({ ...v, span, right, pausedAt }), moved];
        };
        /**
         * Back or Forward in the model: the plots the gesture of the entry
         * gone past moved, as the entry `by` along shows them. A plot shown
         * live loses its time X cursors, which the entry's other side then
         * holds when it was paused; one live shown paused gets back those
         * that side holds, as the gesture began, Back, or ended, Forward.
         */
        const go = (by) => {
          group = null;
          const m = model.get(ws.activeId), from = m.line[m.at].shows, past = m.line[by < 0 ? m.at : m.at + 1], changed = past.moved;
          m.at += by;
          const to = m.line[m.at].shows;
          let moved = false;
          for (const id of changed) {
            const [r, lost] = restored(id, to.get(id));
            moved ||= lost;
            known.set(id, r);
            const x = (by < 0 ? past.xs.before : past.xs.after).get(id);
            if (JSON.parse(to.get(id)).live) {
              if (!JSON.parse(from.get(id)).live) (by < 0 ? past.xs.after : past.xs.before).set(id, xOf(cursors.get(id), 'time'));
              if (hasTimeX(cursors.get(id))) count.xTaken++;
              cursors.set(id, withoutX(cursors.get(id), 'time'));
            } else if (JSON.parse(from.get(id)).live && hasTimeX({ x })) {
              cursors.set(id, withXPair(cursors.get(id), 'time', x));
              count.xBack++;
            }
          }
          const differs = (field) => changed.some((id) => JSON.stringify(JSON.parse(to.get(id))[field]) !== JSON.stringify(JSON.parse(from.get(id))[field]));
          if (changed.length > 1) count.several++;
          if (changed.some((id) => JSON.parse(to.get(id)).spectra)) count.spectra++;
          if (differs('fRange')) count.frequencies++;
          if (differs('ranges')) count.yRanges++;
          if (changed.some((id) => cursors.get(id))) count.cursorsKept++;
          if (dropped() === null && changed.some((id) => ws.plots[id].kind === 'time' && !JSON.parse(to.get(id)).live && JSON.parse(to.get(id)).right - JSON.parse(to.get(id)).span < START)) count.startup++;
          count.shown++;
          return moved;
        };
        const kindOf = (id) => ws.plots[id].kind;
        const yOf = (id) => views.get(id).yByKind.get(kindOf(id) === 'time' ? 'time' : 'spectrum:log')?.get('shared');
        const moveOn = (id, m) => {
          const v = views.get(id);
          // Time pans and zooms on time plots: a spectrum's drag moves its frequencies.
          if (m.t === 'pan' && kindOf(id) === 'time') v.pan(m.f, t.ms);
          else if (m.t === 'zoom' && kindOf(id) === 'time') v.zoom(m.f, m.anchor, t.ms);
          else if (m.t === 'cursor') {
            v.pause(t.ms);
            const [x0, x1] = v.range(hub.now(t.ms));
            editCursors(id, (c) => withX(c, kindOf(id), m.name, m.at === null ? null : kindOf(id) === 'time' ? x0 + m.at * (x1 - x0) : 1 + m.at));
          } else if (m.t === 'ycursor') editCursors(id, (c) => withY(c, kindOf(id), 'shared', m.name, m.v));
          else if (m.t === 'offsetY') {
            const st = yOf(id);
            if (st?.range) Object.assign(st, { range: st.range.map((r) => r + m.d), auto: false });
          } else if (m.t === 'frequencies' && kindOf(id) === 'spectrum') ws.setView(id, { fRange: [m.lo, m.lo * 10] });
        };
        const command = (id, what) => {
          const v = views.get(id), st = yOf(id);
          if (what === 'pause') v.pause(t.ms);
          else if (what === 'addX') {
            v.pause(t.ms);
            const [x0, x1] = v.range(hub.now(t.ms));
            editCursors(id, (c) => withX(withX(c, kindOf(id), 'a', kindOf(id) === 'time' ? x0 + (x1 - x0) / 3 : 2), kindOf(id), 'b', kindOf(id) === 'time' ? x0 + (2 * (x1 - x0)) / 3 : 3));
          } else if (what === 'clearX') editCursors(id, (c) => withoutX(c, kindOf(id)));
          else if (what === 'addY') editCursors(id, (c) => withY(withY(c, kindOf(id), 'shared', 'a', -0.5), kindOf(id), 'shared', 'b', 0.5));
          else if (what === 'clearCursors') editCursors(id, () => undefined);
          else if (what === 'setY' && st) Object.assign(st, { range: [5, 6], auto: false });
          // As PlotView `refit` does: a time plot's at once to what it showed, a spectrum's to follow.
          else if (what === 'fitY' && st) Object.assign(st, kindOf(id) === 'time' ? { range: [-1.2, 1.2], auto: false } : { auto: true });
          else if (what === 'frequencies' && kindOf(id) === 'spectrum') ws.setView(id, { fRange: [2, 20] });
          else if (what === 'everyFrequency' && kindOf(id) === 'spectrum') ws.setView(id, { fRange: null });
        };
        const act = (a) => {
          const here = tabIds(), targets = here.filter((_, i) => a.mask & (1 << i));
          if (a.t === 'drag' || a.t === 'command') {
            // A drag or a command: one group, whatever it passes through.
            group = null;
            const g = begin(null, targets);
            if (a.t === 'command') vh.step(() => (a.what === 'follow' ? follow(targets) : targets.forEach((id) => command(id, a.what))));
            else {
              vh.begin();
              for (const p of a.moves) {
                t.ms += p.gap;
                targets.forEach((id) => moveOn(id, p.move));
              }
              vh.end();
            }
            frame();
            settle(g);
          } else if (a.t === 'wheel' || a.t === 'keys' || a.t === 'wheelY' || a.t === 'window') {
            // A burst: each turn or press joins the group before it when of the same kind on the same plots less than IDLE_MS on.
            const on = a.t === 'wheelY' ? [here[a.at % here.length]] : targets;
            for (const p of a.turns ?? a.presses ?? a.spans) {
              t.ms += p.gap;
              const kind = { wheel: 'zoom', wheelY: 'zoomY', window: 'window' }[a.t] ?? p.kind;
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
                  // The wheel, the keys and the window field move time on time plots only, as the page does.
                  const time = kindOf(id) === 'time';
                  if (a.t === 'wheel' && time) v.zoom(p.f, p.anchor, t.ms);
                  else if (a.t === 'wheelY' && st?.range) Object.assign(st, { range: st.range.map((r) => r * p.f), auto: false });
                  else if (a.t === 'window' && time) v.setSpan(p.s);
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
          for (const tab of ws.tabs) for (const [id, plot] of Object.entries(tab.plots)) expect(plot.cursors, `${a.t}: the cursors of ${id}`).toEqual(cursors.get(id));
          expect(noLiveX(ws, views), `${a.t}: a plot that follows live has time X cursors`).toBe(true);
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
      { examples: EXAMPLES, numRuns: EXAMPLES.length + numRuns },
    );
    for (const [rule, n] of Object.entries(reached)) expect(n, rule).toBeGreaterThanOrEqual(REACHED);
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
    const { ws, views, vh, t, hub } = page(), id = Object.keys(ws.plots)[0], v = views.get(id);
    views.held = () => ({ oldest: START + 3, dropped: false, history: null });
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
    const { ws, views, vh, t, ids, history } = page();
    vh.step(() => views.get(ids[0]).pause(t.ms));
    history.group('Close', () => ws.closePlot(ids[0]));
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

  it('holds a spectrum’s frequencies, y ranges and the spectrum it held paused, and never moves a spectrum for what the hub dropped', () => {
    const { ws, views, vh, t, b, frame, history } = page();
    ws.selectTab(ws.tabs[1].id);
    frame();
    const v = views.get(b[0]), key = ws.plots[b[0]].items[0].key;
    vh.step(() => v.pause(t.ms));
    const held = v.spectra.get(key), paused = v.state();
    t.ms += 10_000;
    vh.step(() => v.follow());
    frame();
    expect(v.spectra.get(key)).not.toBe(held);
    history.group('Cursors', () => ws.setPlot(b[0], { cursors: { f: { a: 3, b: 4 } } }));
    vh.step(() => (ws.setView(b[0], { fRange: [2, 20] }), v.yByKind.set('spectrum:log', new Map([['shared', { range: [0, 9], auto: false }]]))));
    t.ms += (HELD_S + 60) * 1000;
    vh.back();
    expect([ws.plots[b[0]].fRange, v.yByKind.get('spectrum:log')?.get('shared')]).toEqual([null, undefined]);
    expect(ws.plots[b[0]].cursors, 'its cursors are no step').toEqual({ f: { a: 3, b: 4 } });
    vh.back();
    expect([v.live, v.right, v.spectra.get(key)]).toEqual([false, paused.right, held]);
    expect(t.notices).toEqual([]);
  });

  it('while plots are maximized, goes back past the steps of plots hidden, and takes them up once they show again', () => {
    const { ws, views, vh, t, ids } = page(), [a, b] = [views.get(ids[0]), views.get(ids[1])];
    vh.step(() => a.pause(t.ms));
    vh.step(() => b.pause(t.ms));
    ws.maximize([ids[0]]);
    expect(vh.back()).toBe(true);
    expect([a.live, b.live]).toEqual([true, false]);
    expect(vh.canBack).toBe(false);
    ws.restoreLayout();
    expect(vh.back()).toBe(true);
    expect(b.live).toBe(true);
  });

  it('leaves the steps of a plot moved to another tab in the tab they were made in, which goes back past them', () => {
    const { ws, views, vh, t, ids, history } = page(), a = views.get(ids[0]);
    vh.step(() => a.pause(t.ms));
    history.group('Move', () => ws.movePlotToTab(ids[0], ws.tabs[1].id));
    expect(vh.canBack).toBe(false);
    ws.selectTab(ws.tabs[1].id);
    expect(vh.canBack).toBe(false);
    history.group('Move', () => ws.movePlotToTab(ids[0], ws.tabs[0].id));
    ws.selectTab(ws.tabs[0].id);
    expect(vh.back()).toBe(true);
    expect(a.live).toBe(true);
  });

  it('lets go of a tab’s steps once neither the tabs nor an undo or a redo hold the tab', () => {
    const { ws, views, vh, t, ids, history } = page();
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
    const p = setup();
    const edit = (fn) => (fn(), p.views.prune(p.history.held().plots));
    return { ...p, edit, ids: Object.keys(p.ws.plots) };
  };

  it('shows its view as it was, and Back goes on through its steps', () => {
    const { ws, views, vh, t, history, edit, ids } = page(), v = views.get(ids[0]);
    vh.step(() => v.pause(t.ms));
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

// A kind switch is an edit, which Ctrl+Z undoes, and a view step, which Back
// crosses: random runs of switches, pans, another edit, X cursors placed and
// cleared, pauses, following live, Back, Forward, undo and redo on one plot.
// The model is the list of what the plot showed, its kind and its view,
// after each change of either, however made: by the command, by an undo or
// by a redo, each with the time X cursors the plot had as it began and as
// it ended. Back shows the entry before and Forward the one after, and the
// two histories never disagree: the plot's kind is always the edit
// history's, and always the entry's the model is at; and a plot that
// follows live never has time X cursors. Following live takes them; Back or
// Forward that shows a live plot paused puts back those it had as it last
// left that paused view to go live, by any route, and no others; an undo
// or a redo changes them just as the edit history has them, and one that
// brings them back to a live plot shows the paused view it left as it went
// live; a redo of an edit that made the plot live makes it live again as
// that edit left it, and no other redo makes it live; nothing else changes them.
describe('a kind switch, and time X cursors across going live', () => {
  const act = fc.constantFrom('switch', 'switch', 'pan', 'legend', 'place', 'place', 'clear', 'pause', 'follow', 'follow', 'back', 'back', 'forward', 'undo', 'undo', 'redo');
  const times = (n, acts) => Array.from({ length: n }, () => acts).flat();
  /**
   * Runs that reach every count the test asks of its cases, whatever the
   * random ones do: a switch undone and redone, four times; a pan then a
   * switch, gone back across the switch and on past the pan, four times; X
   * cursors placed, live, then Back and Forward, four times; and X cursors
   * placed, live, then undone, four times, and cleared first, four times;
   * X cursors placed, live, Back, then cleared, so Forward and Back again
   * bring none back; and X cursors placed on a paused plot, Back to live,
   * then Forward, which brings back those Back took.
   */
  const EXAMPLES = [
    times(4, ['switch', 'undo', 'redo']),
    times(4, ['pan', 'switch', 'back', 'back']),
    times(4, ['place', 'follow', 'back', 'forward', 'back']),
    times(4, ['place', 'follow', 'undo', 'redo']),
    times(4, ['place', 'clear', 'follow', 'undo']),
    ['place', 'follow', 'back', 'clear', 'forward', 'back'],
    ['pause', 'place', 'back', 'forward'],
  ];
  it('is a step Back and Forward cross, going on through the steps before it, whatever undo and redo do between, and a live plot has no time X cursors', () => {
    const count = { crossed: 0, wentOn: 0, undone: 0, redone: 0, xTaken: 0, xBack: 0, xUndone: 0, redoneLive: 0 };
    fc.assert(
      fc.property(fc.array(act, { minLength: 4, maxLength: 24 }), (acts) => {
        const { ws, views, hub, t, history, cursors, vh } = setup(), id = Object.keys(ws.plots)[0], v = views.get(id);
        const seen = () => JSON.stringify([ws.plots[id].kind, v.live, v.live ? null : v.right, v.span]);
        const x = () => xOf(ws.plots[id].cursors, 'time');
        const line = [{ shows: seen(), xs: null }];
        let at = 0, placed = 0;
        // The time X cursors the plot should have, and the paused view it left when it last went live.
        let want = x(), left = null;
        /** A change of what the plot shows is an entry after the one it is at, in place of those Forward had, with the time X cursors as the change began (`was`) and ended. */
        const changed = (was) => {
          if (seen() === line[at].shows) return false;
          line.splice(at + 1, Infinity, { shows: seen(), xs: { before: was, after: x() } });
          at++;
          return true;
        };
        let crossedLast = false;
        // What the plot showed just after each edit that made it live: a redo of one shows that again.
        const wentTo = new Map();
        const run = (a) => {
          t.ms += 1000;
          const was = x(), before = seen(), wasLive = v.live, top = history.undos.at(-1);
          if (a === 'switch') vh.step(() => history.group('Kind', () => ws.setPlot(id, { kind: ws.plots[id].kind === 'time' ? 'spectrum' : 'time' })));
          else if (a === 'pan') vh.step(() => v.pan(-0.1, t.ms));
          else if (a === 'legend') history.group('Legend', () => ws.setPlot(id, { legend: !ws.plots[id].legend }));
          else if (a === 'place') {
            // As Add X cursors: a time plot pauses, as one step with the edit.
            const kind = ws.plots[id].kind;
            vh.step(() => {
              if (kind === 'time') v.pause(t.ms);
              cursors.set('Add X cursors', new Map([[id, withX(ws.plots[id].cursors, kind, 'a', ++placed)]]));
            });
            if (kind === 'time') want = { ...want, a: placed };
          } else if (a === 'clear') (cursors.clear([id]), (want = { a: null, b: null }));
          else if (a === 'pause') vh.step(() => v.pause(t.ms));
          else if (a === 'follow') (vh.follow([id], true), (want = { a: null, b: null }));
          else if (a === 'undo' || a === 'redo') {
            const e = a === 'undo' ? history.undos.at(-1) : history.redos.at(-1);
            vh.edited(() => history[a]());
            // An undo or a redo changes the cursors as the edit history has them, and no further.
            if (e) want = xOf(e[a === 'undo' ? 'before' : 'after'].tabs[0].plots[id]?.cursors, 'time');
            if (wasLive && hasTimeX({ x: want })) {
              expect(v.live, `${a}: cursors back on a live plot pause it`).toBe(false);
              if (left) expect(seen().slice(seen().indexOf(',')), `${a}: the paused view it left`).toBe(left.slice(left.indexOf(',')));
              count.xUndone++;
            }
            // A redo of an edit that made the plot live makes it live again, as that edit left it; no other redo makes it live.
            if (a === 'redo' && wentTo.has(e)) (expect(seen(), 'redo: live again, as it went').toBe(wentTo.get(e)), count.redoneLive++);
            else if (a === 'redo') expect(!wasLive && v.live, 'redo: live').toBe(false);
          }
          if (a === 'back' || a === 'forward') {
            const by = a === 'back' ? -1 : 1, can = at + by >= 0 && at + by < line.length;
            const from = JSON.parse(line[at].shows), past = line[by < 0 ? at : at + 1];
            expect(vh[a](), a).toBe(can);
            if (can) at += by;
            const to = JSON.parse(line[at].shows);
            const crossed = can && to[0] !== from[0];
            if (crossed) count.crossed++;
            // A Back past a step of the view alone, straight after one across a switch.
            else if (can && a === 'back' && crossedLast) count.wentOn++;
            crossedLast = crossed && a === 'back';
            // Shown live, the plot loses its time X cursors, which the paused view it leaves then holds;
            // live shown paused, it gets back those that view holds.
            if (can && to[1] && !from[1]) past.xs[by < 0 ? 'after' : 'before'] = want;
            if (can && to[1] && hasTimeX({ x: want })) (count.xTaken++, (want = { a: null, b: null }));
            else if (can && !to[1] && from[1] && past && hasTimeX({ x: past.xs[by < 0 ? 'before' : 'after'] })) (count.xBack++, (want = past.xs[by < 0 ? 'before' : 'after']));
          } else if (changed(was) && (a === 'undo' || a === 'redo')) count[a === 'undo' ? 'undone' : 'redone']++;
          if (['follow', 'back', 'forward'].includes(a) && !wasLive && v.live && history.undos.at(-1) !== top) wentTo.set(history.undos.at(-1), seen());
          if (!wasLive && v.live) left = before;
          expect(seen(), `${a}: what the plot shows`).toBe(line[at].shows);
          expect([vh.canBack, vh.canForward], `${a}: Back and Forward`).toEqual([at > 0, at < line.length - 1]);
          expect(x(), `${a}: the time X cursors`).toEqual(want);
          expect(noLiveX(ws, views), `${a}: a live plot with time X cursors`).toBe(true);
          // Never past the newest samples, whatever was restored.
          if (!v.live) expect(v.right).toBeLessThanOrEqual(hub.now(t.ms));
        };
        acts.forEach(run);
        // Then all the way back, through every step made.
        while (at > 0) run('back');
      }),
      // The examples come on top of the random runs.
      { examples: EXAMPLES.map((acts) => [acts]), numRuns: EXAMPLES.length + fc.readConfigureGlobal().numRuns },
    );
    for (const [rule, n] of Object.entries(count)) expect(n, rule).toBeGreaterThan(3);
  });
});

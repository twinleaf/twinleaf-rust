// The whole page played by random runs of what a person does with the real
// mouse and keys, its invariants checked after every step. A failure prints
// the seed and path that replay its shortest run with FC_SEED and FC_PATH;
// FC_RUNS scales the runs.
//
// The steps:
// | step    | what it does                                                        |
// |---------|---------------------------------------------------------------------|
// | click   | a click, plain, with Shift or with Ctrl, on any kind of control,     |
// |         | a plot's Back and Forward among them                                 |
// | right   | a right-click on a plot, its header or chips, a tab, a dock header,  |
// |         | a signal or a property row; a later click may pick from its menu     |
// | key     | Escape, Delete, Backspace, F, 1 to 4, Ctrl+A, Ctrl+Z, Ctrl+Y, L,     |
// |         | Space, |, _, K, arrows, Enter, Tab, ]                                |
// | move    | a plot picked up by its grip or header onto a cell of another,       |
// |         | let go there or put away with Escape                                 |
// | toTab   | a plot, or the plots selected with it, dragged onto a tab            |
// | gap     | a gap between plots dragged                                          |
// | chip    | a signal's chip dragged onto another plot's chips, body or side      |
// | tabDrag | a tab dragged past the next                                          |
// | box     | a box from empty dock space across the tiles, with Shift or not,     |
// |         | let go or put away with Escape                                       |
// | wheel   | the wheel over a plot, with Shift or not                             |
// | across  | the pointer over the top bar's menu buttons, resting a moment or     |
// |         | long enough for a tooltip, the first pressed or not                  |
// | quick   | two presses with no frame between                                    |
//
// The invariants, after every step:
// | invariant                                                                 |
// |---------------------------------------------------------------------------|
// | the plots selected are plots the open tab shows                            |
// | plots and property rows are never chosen together                          |
// | the step made at most one undo step                                        |
// | undo then redo gives the same tabs; after a step that made one, the same   |
// | open tab and selection too, but after a quick, whose second press may      |
// | choose again once its first has made the step                              |
// | the maximized plots are of the tab, and never none: null instead           |
// | the tiles showing are the maximized plots, or every plot, each where the   |
// | layout puts it                                                             |
// | a plot move that lands lands where its preview showed                      |
// | the docks keep their widths, but for a menu's command, which can hide one  |
// | at most one popup layer is open, and no tooltip shows over one but for a   |
// | control inside it                                                          |
// | nothing threw, no promise was left rejected, Svelte reported nothing       |
//
// At most one popup layer (menu, popover, dialog or select) is open, and no
// tooltip shows over one (class 9): an invariant of the runs above too, and
// of runs of its own, of what opens and closes popup layers.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, dropped, emptySpots, feed, focusPage, frames, glide, headerSpot, holding, leafOf, mountPage, pressKey, quietApi, rested, showing, until, withExamples } from './harness/page.js';
import { GUTTER, plotIds, rects, restrict } from '../src/lib/layout.js';
import { STEP } from '../src/lib/moves.js';

const KEYS = [['sine.sine', 'sine.cosine'], ['aux.triangle'], ['sine.cosine'], ['aux.sawtooth']];
const SIGNALS = ['vector.x', 'vector.y'];
const RPCS = [
  { name: 'gain', channel: 'setting' },
  { name: 'offset', channel: 'setting' },
  { name: 'temp', channel: 'report' },
];
const EDGES = ['center', 'left', 'right', 'top', 'bottom'];
/** Menu items a run never picks: they open a file chooser or download a file. */
const OFF_LIMITS = /^(Export every|Import)/;

const visible = (el) => {
  const r = el.getBoundingClientRect();
  return r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== 'hidden' && !el.closest('[inert]');
};
const all = (selector) => [...document.querySelectorAll(selector)].filter(visible);
const nth = (list, i) => (list.length ? list[i % list.length] : null);
const topButton = (text) => [...document.querySelectorAll('.top-bar button')].find((b) => b.textContent.trim().startsWith(text));

/** What a click or right-click can land on, each giving the element of index `i`. */
const TARGETS = {
  plot: (i) => nth(all('.tiles .leaf .plot-body'), i),
  header: (i) => nth(all('.tiles .leaf .plot-head'), i),
  plotButton: (i) => nth(all('.tiles .leaf .plot-head button'), i),
  // Back and Forward through the tab's views, while there is one to go to.
  viewStep: (i) => nth(all('.tiles .leaf .plot-head button.view-step:not([disabled])'), i),
  chip: (i) => nth(all('.tiles .leaf [id^="chip-"]'), i),
  gap: (i) => nth(all('.tiles .separator'), i),
  tab: (i) => nth(all('[role=tablist] [data-tab] span'), i),
  dockHead: (i) => nth(all('.dock-head'), i),
  signal: (i) => nth(all('.palette [data-key]'), i),
  row: (i) => nth(all('tr[id^=prop-]'), i),
  live: () => topButton('Live') ?? topButton('Follow live'),
  layout: () => topButton('Layout'),
  file: () => topButton('File'),
  settings: () => document.querySelector('.top-bar button[aria-label="Display settings"]'),
  window: () => document.querySelector('.top-bar input[aria-label="Time window"]'),
  eraser: () => document.querySelector('.top-bar button[aria-label="Clear the cursors"]'),
  status: () => document.querySelector('.status-bar'),
  menuItem: (i) => nth(all('[role^=menuitem]:not([data-disabled])').filter((m) => !OFF_LIMITS.test(m.textContent.trim())), i),
};
const TRIGGERS = ['layout', 'file', 'settings'];
/** How long the pointer rests on each menu button: a moment, or past a tooltip's delay. */
const rest = fc.constantFrom(60, 400);

const key = fc.constantFrom('Escape', 'Delete', 'Backspace', 'f', 'F', '1', '2', '3', '4', 'Control+a', 'Control+z', 'Control+y', 'l', 'Space', '|', '_', 'k', 'ArrowLeft', 'ArrowUp', 'Enter', 'Tab', ']');
const step = fc.oneof(
  { weight: 6, arbitrary: fc.record({ t: fc.constant('click'), at: fc.constantFrom(...Object.keys(TARGETS)), i: fc.nat(), mod: fc.constantFrom(null, 'shift', 'ctrl') }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('right'), at: fc.constantFrom('plot', 'header', 'chip', 'tab', 'dockHead', 'signal', 'row'), i: fc.nat() }) },
  { weight: 5, arbitrary: fc.record({ t: fc.constant('key'), key }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('move'), a: fc.nat(), b: fc.nat(), cell: fc.constantFrom(...EDGES), by: fc.constantFrom('grip', 'header'), escape: fc.boolean() }) },
  fc.record({ t: fc.constant('toTab'), a: fc.nat(), b: fc.nat() }),
  fc.record({ t: fc.constant('gap'), i: fc.nat(), by: fc.integer({ min: -120, max: 120 }) }),
  fc.record({ t: fc.constant('chip'), a: fc.nat(), b: fc.nat(), onto: fc.constantFrom('chips', 'body', 'left', 'bottom') }),
  fc.record({ t: fc.constant('tabDrag'), a: fc.nat() }),
  { weight: 2, arbitrary: fc.record({ t: fc.constant('box'), k: fc.nat(), fx: fc.double({ min: 0, max: 1, noNaN: true }), fy: fc.double({ min: 0, max: 1, noNaN: true }), shift: fc.boolean(), escape: fc.boolean() }) },
  fc.record({ t: fc.constant('wheel'), i: fc.nat(), dy: fc.constantFrom(-240, -120, 120, 240), shift: fc.boolean() }),
  { weight: 2, arbitrary: fc.record({ t: fc.constant('across'), path: fc.array(fc.constantFrom(...TRIGGERS), { minLength: 2, maxLength: 4 }), press: fc.boolean(), rest }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('quick'), a: fc.constantFrom(...TRIGGERS, 'plot', 'header'), b: fc.constantFrom(...TRIGGERS, 'plot', 'header'), i: fc.nat() }) },
);

/** How often each kind of step ran on something, over every run: none would leave the property vacuous. */
const ran = {};
/** How many steps made an undo step, and how many plot moves landed where their preview showed, over every run. */
let stepped = 0, previewed = 0;

/** The box around the tiles of plots `ids`. */
function around(ids) {
  const rs = ids.map((id) => leafOf(id).getBoundingClientRect());
  const x = Math.min(...rs.map((r) => r.left)), y = Math.min(...rs.map((r) => r.top));
  return { x, y, w: Math.max(...rs.map((r) => r.right)) - x, h: Math.max(...rs.map((r) => r.bottom)) - y };
}

/** Plays step `s` on `page`; says whether it found what it acts on. */
async function play(s, page) {
  const { ws, history } = page;
  if (s.t === 'click' || s.t === 'right') {
    const el = TARGETS[s.at](s.i);
    if (!el) return false;
    const [x, y] = s.at === 'header' ? [el.getBoundingClientRect().left + 12, centre(el)[1]] : centre(el);
    const button = s.t === 'right' ? 'right' : 'left';
    await commands.mouse(holding(s.mod ? [{ shift: 'Shift', ctrl: 'Control' }[s.mod]] : [], [['move', x, y], ['down', button], ['up', button]]));
  } else if (s.t === 'key') await pressKey(s.key);
  else if (s.t === 'move' || s.t === 'toTab') {
    const shown = ws.shownIds();
    if (shown.length < 2 && s.t === 'move') return false;
    const from = nth(shown, s.a);
    if (!from) return false;
    await commands.mouse([['move', ...centre(leafOf(from))]]);
    const start = s.by === 'header' ? headerSpot(leafOf(from)) : centre(leafOf(from).querySelector('.grip'));
    if (!start) return false;
    let to;
    if (s.t === 'toTab') {
      const tab = nth(all('[role=tablist] [data-tab]'), s.b);
      if (!tab) return false;
      to = centre(tab);
    } else {
      const onto = shown.filter((id) => id !== from)[s.b % (shown.length - 1)];
      const [cx, cy] = centre(leafOf(onto));
      const [dx, dy] = { center: [0, 0], left: [-1, 0], right: [1, 0], top: [0, -1], bottom: [0, 1] }[s.cell];
      to = [cx + dx * STEP, cy + dy * STEP];
    }
    // What a move carries: the selection when the plot is one of several selected, else the plot.
    const group = ws.selected.has(from) && ws.selected.size > 1 ? [...ws.selected] : [from];
    await commands.mouse([['move', ...start], ['down'], ...glide(start, to, 5)]);
    await until(() => document.querySelector('.drop-preview, [data-tab].target'), 1000);
    // Where the preview is set, not where its slide there has got to.
    const set = document.querySelector('.drop-preview')?.style;
    const preview = set && { left: parseFloat(set.left), top: parseFloat(set.top), width: parseFloat(set.width), height: parseFloat(set.height) };
    if (s.escape) await pressKey('Escape');
    const steps = history.undos.length, layout = ws.layout;
    await commands.mouse([['up']]);
    flushSync();
    await frames();
    // A move that landed by its preview lands where the preview showed.
    if (preview && history.undos.length > steps && ws.layout !== layout && group.every((id) => ws.plots[id])) {
      const got = around(group);
      expect(Math.max(Math.abs(preview.left - got.x), Math.abs(preview.top - got.y), Math.abs(preview.width - got.w), Math.abs(preview.height - got.h)), `landed where the preview showed, ${JSON.stringify(s)}`).toBeLessThan(1.5);
      previewed++;
    }
  } else if (s.t === 'gap') {
    const g = nth(all('.tiles .separator'), s.i);
    if (!g) return false;
    const [x, y] = centre(g), r = g.getBoundingClientRect();
    const to = r.height > r.width ? [x + s.by, y] : [x, y + s.by];
    await commands.mouse([['move', x, y], ['down'], ...glide([x, y], to, 5), ['up']]);
  } else if (s.t === 'chip') {
    const el = nth(all('.tiles .leaf [id^="chip-"]'), s.a), onto = nth(all('.tiles .leaf'), s.b);
    if (!el || !onto) return false;
    const zone = onto.querySelector('.trace-zone'), [cx, cy] = centre(onto);
    const to = { chips: zone && centre(zone), body: centre(onto.querySelector('.plot-body') ?? onto), left: [cx - STEP, cy], bottom: [cx, cy + STEP] }[s.onto];
    if (!to) return false;
    const from = centre(el), via = zone ? centre(zone) : to;
    // Resting on the way and at the end, as the drag library needs to see where it is.
    await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8], ...glide(from, via, 3), ['wait', 150], ...glide(via, to, 3), ['wait', 150], ['up']]);
    await dropped();
  } else if (s.t === 'tabDrag') {
    const tabs = all('[role=tablist] [data-tab]');
    if (tabs.length < 2) return false;
    const a = nth(tabs, s.a), b = tabs[(tabs.indexOf(a) + 1) % tabs.length];
    const from = centre(a.querySelector('span') ?? a), [bx, by] = centre(b), r = b.getBoundingClientRect();
    const to = [bx + (r.width / 3) * Math.sign(bx - from[0]), by];
    await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 2], ...glide(from, [bx, by], 3), ['wait', 300], ...glide([bx, by], to, 2), ['wait', 200], ['up']]);
    await dropped();
  } else if (s.t === 'box') {
    const from = nth(emptySpots(), s.k);
    if (!from) return false;
    const r = document.querySelector('.tiles').getBoundingClientRect();
    const to = [r.left + 2 + s.fx * (r.width - 4), r.top + 2 + s.fy * (r.height - 4)];
    if (s.shift) await commands.mouse([['hold', 'Shift']]);
    await commands.mouse([['move', ...from], ['down'], ...glide(from, to, 5)]);
    if (s.escape) await pressKey('Escape');
    await commands.mouse([['up'], ...(s.shift ? [['let go', 'Shift']] : [])]);
  } else if (s.t === 'wheel') {
    const body = TARGETS.plot(s.i);
    if (!body) return false;
    const [x, y] = centre(body);
    await commands.mouse([['move', x, y]]);
    (body.querySelector('canvas') ?? body).dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY: s.dy, clientX: x, clientY: y, shiftKey: s.shift }));
  } else if (s.t === 'across') {
    // The pointer passes over the top bar's menu buttons, pressing the first when asked.
    const els = s.path.map((n) => TARGETS[n]()).filter(Boolean);
    if (els.length < 2) return false;
    const moves = [];
    els.forEach((el, k) => {
      moves.push(['move', ...centre(el)], ['wait', s.rest]);
      if (k === 0 && s.press) moves.push(['down'], ['up'], ['wait', s.rest]);
    });
    await commands.mouse(moves);
  } else if (s.t === 'quick') {
    // Two presses with no frame between, as a quick double press or a press on one menu then another.
    const a = TARGETS[s.a](s.i), b = TARGETS[s.b](s.i);
    if (!a || !b) return false;
    await commands.mouse([['move', ...centre(a)], ['down'], ['up'], ['move', ...centre(b)], ['down'], ['up']]);
  }
  ran[s.t] = (ran[s.t] ?? 0) + 1;
  return true;
}

/** Every plot shown whose tile is more than a pixel from the box the layout gives it, among the plots shown. */
function misplaced(ws) {
  const box = document.querySelector('.tile-box')?.getBoundingClientRect();
  if (!box) return [];
  const tree = ws.maximized ? restrict(ws.layout, new Set(ws.maximized)) : ws.layout;
  const out = [];
  for (const [id, w] of rects(tree, { x: box.left, y: box.top, w: box.width, h: box.height }, GUTTER)) {
    const got = leafOf(id)?.getBoundingClientRect();
    if (!got || Math.max(Math.abs(got.left - w.x), Math.abs(got.top - w.y), Math.abs(got.width - w.w), Math.abs(got.height - w.h)) > 1) out.push(id);
  }
  return out;
}

/** The popup layers open, a submenu as part of its menu and a select in a popover as part of the popover; and whether a tooltip shows for a control outside them. */
function layers() {
  const open = (slot) => [...document.querySelectorAll(`[data-slot=${slot}][data-state=open]`)];
  const popovers = open('popover-content');
  const all = [...open('dropdown-menu-content'), ...open('context-menu-content'), ...popovers, ...open('dialog-content'), ...open('alert-dialog-content'), ...(popovers.length ? [] : open('select-content'))];
  // A tooltip of a control inside the open layer is part of it.
  const tooltip = [...document.querySelectorAll('[data-slot=tooltip-content]')].some((t) => {
    const of = document.querySelector(`[aria-describedby="${CSS.escape(t.id)}"]`);
    return /open/.test(t.dataset.state ?? '') && !all.some((l) => of && l.contains(of));
  });
  return { n: all.length, tooltip };
}

/** The page's errors since `listen()`: thrown, rejected, or reported by Svelte in the console. */
function listen() {
  const errors = [];
  const onError = (e) => errors.push(e.error?.stack ?? e.message ?? String(e.reason?.stack ?? e.reason));
  window.addEventListener('error', onError);
  window.addEventListener('unhandledrejection', onError);
  const svelte = (...args) => {
    const text = args.map(String).join(' ');
    if (/svelte|state_|effect_|https:\/\/svelte\.dev\/e\//i.test(text)) errors.push(text);
  };
  const warn = vi.spyOn(console, 'warn').mockImplementation(svelte);
  const error = vi.spyOn(console, 'error').mockImplementation((...args) => errors.push(args.map(String).join(' ')));
  return {
    errors,
    stop() {
      window.removeEventListener('error', onError);
      window.removeEventListener('unhandledrejection', onError);
      warn.mockRestore();
      error.mockRestore();
    },
  };
}

/** What a step leaves of the selection that undo and redo answer for: the open tab, the plots selected and the rows picked. */
const chosen = (ws) => JSON.stringify({ tab: ws.activeId, plots: [...ws.selected].sort(), rows: [...ws.selection.rows].sort() });
/** Each dock frame's width, in px. */
const dockWidths = () => [...document.querySelectorAll('.dock-frame')].map((f) => Math.round(f.getBoundingClientRect().width));
/** Whether step `s` may hide or show a dock, by a menu's command: a dock slides as it does. */
const movesDocks = (s) => (s.t === 'click' && s.at === 'menuItem') || (s.t === 'key' && s.key === 'Enter');

/** Checks the invariants after step `s`, which began with `steps` undo steps and the docks `widths` wide. */
function check(page, { steps, widths }, s) {
  const { ws, history } = page;
  const shown = ws.shownIds(), at = JSON.stringify(s);
  expect([...ws.selected].filter((id) => !shown.includes(id)), `selected but not shown, after ${at}`).toEqual([]);
  expect(ws.selected.size > 0 && ws.selection.rows.size > 0, `plots and rows chosen together, after ${at}`).toBe(false);
  expect(history.undos.length - steps, `undo steps made by ${at}`).toBeLessThanOrEqual(1);
  if (ws.maximized) expect(ws.maximized.length > 0 && ws.maximized.every((id) => ws.plots[id]), `maximized plots of the tab, after ${at}`).toBe(true);
  expect(showing().sort(), `the plots showing, after ${at}`).toEqual([...shown].sort());
  expect(misplaced(ws), `tiles away from the layout, after ${at}`).toEqual([]);
  oneLayer(page, null, s);
  if (!movesDocks(s)) expect(dockWidths(), `dock widths, after ${at}`).toEqual(widths);
  // Undo then redo gives back the tabs; after a step that made one, what the step left of the selection too.
  // A quick is two presses: its second may deselect after its first made the step, as a chip's Remove then Layout.
  if (history.undos.length) {
    const made = history.undos.length > steps;
    if (made) stepped++;
    const tabs = JSON.stringify(ws.tabs), left = chosen(ws);
    const kept = { tab: ws.activeId, maximized: ws.maximized, selection: ws.selection.snapshot() };
    history.undo();
    history.redo();
    flushSync();
    expect(JSON.stringify(ws.tabs), `undo then redo, after ${at}`).toBe(tabs);
    if (made && s.t !== 'quick') expect(chosen(ws), `the selection after undo then redo, after ${at}`).toBe(left);
    // The run goes on from what the step left, not from what the redo chose: its tab, its maximized plots and its selection.
    if (ws.activeId !== kept.tab) ws.selectTab(kept.tab);
    ws.maximize(kept.maximized);
    ws.selection.restore(kept.selection);
    flushSync();
  }
}

/** Plays `steps` on a fresh page, `after(page, before, s)` checked after every step, `before` as the step found the page. */
async function run(steps, after) {
  const page = await mountPage(KEYS, 'grid', 1, SIGNALS, RPCS);
  const seen = listen();
  try {
    feed(page.hub);
    await focusPage();
    for (const s of steps) {
      const before = { steps: page.history.undos.length, widths: dockWidths() };
      await play(s, page);
      flushSync();
      await frames(2);
      expect(seen.errors, `errors after ${JSON.stringify(s)}`).toEqual([]);
      after(page, before, s);
      // The next step starts with the docks where a slide brings them to rest.
      if (movesDocks(s)) await rested(() => dockWidths().join());
    }
  } finally {
    seen.stop();
    // Whatever a run leaves held goes before the next.
    await commands.mouse([['up'], ['let go', 'Shift'], ['let go', 'Control']]);
    page.dispose();
  }
}

beforeEach(() => {
  quietApi();
  // A run that reaches File's export or import by the keys saves no file and opens no chooser.
  const click = HTMLElement.prototype.click;
  vi.spyOn(HTMLElement.prototype, 'click').mockImplementation(function () {
    if (!(this instanceof HTMLAnchorElement && this.download) && !(this instanceof HTMLInputElement && this.type === 'file')) click.call(this);
  });
});
afterEach(() => vi.restoreAllMocks());


describe('the page, played by random runs of gestures and keys', () => {
  it('holds every invariant after every step', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 3, maxLength: 12 }), (steps) => run(steps, check)),
      withExamples([
          [[{ t: 'key', key: 'Control+a' }, { t: 'move', a: 0, b: 1, cell: 'left', by: 'grip', escape: false }, { t: 'key', key: 'Control+z' }, { t: 'box', k: 0, fx: 0.7, fy: 0.7, shift: false, escape: true }]],
          [[{ t: 'click', at: 'plot', i: 1, mod: null }, { t: 'click', at: 'plot', i: 2, mod: 'shift' }, { t: 'key', key: 'f' }, { t: 'key', key: 'Delete' }, { t: 'key', key: 'Control+z' }]],
          [[{ t: 'click', at: 'row', i: 0, mod: null }, { t: 'click', at: 'row', i: 1, mod: 'ctrl' }, { t: 'right', at: 'plot', i: 0 }, { t: 'click', at: 'menuItem', i: 3, mod: null }, { t: 'chip', a: 0, b: 2, onto: 'bottom' }]],
          [[{ t: 'toTab', a: 0, b: 1 }, { t: 'gap', i: 0, by: 60 }, { t: 'tabDrag', a: 0 }, { t: 'wheel', i: 0, dy: 120, shift: false }, { t: 'key', key: 'Control+y' }]],
          // Plots maximized, then the keys and a box that select: only what shows.
          [[{ t: 'click', at: 'plot', i: 0, mod: null }, { t: 'key', key: 'f' }, { t: 'key', key: 'Control+a' }, { t: 'key', key: '3' }, { t: 'box', k: 0, fx: 0.9, fy: 0.9, shift: false, escape: false }]],
          // A plot closed, another maximized, and the close taken back: the plot it brings back stays hidden, and is not selected.
          [[{ t: 'click', at: 'plot', i: 1, mod: null }, { t: 'key', key: 'Delete' }, { t: 'click', at: 'plot', i: 0, mod: null }, { t: 'key', key: 'f' }, { t: 'key', key: 'Control+z' }]],
          // A maximized plot's header, pressed at its middle, on its chip's Remove, then Layout pressed, which deselects.
          [[{ t: 'move', a: 1, b: 0, cell: 'center', by: 'grip', escape: false }, { t: 'key', key: 'f' }, { t: 'quick', a: 'layout', b: 'layout', i: 0 }, { t: 'quick', a: 'header', b: 'layout', i: 0 }]],
          // A plot docked beside another, by its grip and by its header.
          [[{ t: 'move', a: 0, b: 0, cell: 'bottom', by: 'grip', escape: false }, { t: 'move', a: 1, b: 1, cell: 'right', by: 'header', escape: false }]],
          // The left dock hidden from its header's menu, which slides it to its rail, then a box from the dock space left.
          [[{ t: 'right', at: 'dockHead', i: 0 }, { t: 'click', at: 'menuItem', i: 523181528, mod: null }, { t: 'box', k: 0, fx: 0, fy: 0, shift: false, escape: false }, { t: 'move', a: 0, b: 0, cell: 'center', by: 'grip', escape: false }, { t: 'chip', a: 0, b: 0, onto: 'chips' }]],
        ]),
    );
    // A replay of one failure runs that case alone.
    if (fc.readConfigureGlobal().path !== undefined) return;
    for (const kind of ['click', 'right', 'key', 'move', 'toTab', 'gap', 'chip', 'tabDrag', 'box', 'wheel', 'across', 'quick']) expect(ran[kind], kind).toBeGreaterThan(0);
    expect(stepped).toBeGreaterThan(5);
    expect(previewed).toBeGreaterThan(0);
  }, 600_000);
});

/** What opens and closes popup layers: presses on their triggers and items, right-clicks, the pointer resting on triggers, quick double presses and keys. */
const overlayStep = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('click'), at: fc.constantFrom(...TRIGGERS, 'plotButton', 'menuItem', 'plot', 'tab'), i: fc.nat(), mod: fc.constant(null) }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('right'), at: fc.constantFrom('plot', 'header', 'chip', 'tab', 'dockHead', 'signal', 'row'), i: fc.nat() }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('across'), path: fc.array(fc.constantFrom(...TRIGGERS), { minLength: 2, maxLength: 4 }), press: fc.boolean(), rest }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('quick'), a: fc.constantFrom(...TRIGGERS, 'plot', 'header', 'plotButton'), b: fc.constantFrom(...TRIGGERS, 'plot', 'header', 'plotButton'), i: fc.nat() }) },
  fc.record({ t: fc.constant('key'), key: fc.constantFrom('Escape', 'Enter', 'Tab', 'ArrowDown', 'k', 'a') }),
);

/** The invariant of class 9 after step `s`: at most one popup layer open, and no tooltip over one. */
function oneLayer(page, before, s) {
  const { n, tooltip } = layers();
  expect(n, `popup layers open after ${JSON.stringify(s)}`).toBeLessThanOrEqual(1);
  expect(n > 0 && tooltip, `a tooltip over a popup after ${JSON.stringify(s)}`).toBe(false);
}

describe('the page’s popup layers, played by random runs of menu presses, hovers and keys', () => {
  it('keep at most one popup layer open, and no tooltip over one (class 9)', async () => {
    await fc.assert(fc.asyncProperty(fc.array(overlayStep, { minLength: 2, maxLength: 8 }), (steps) => run(steps, oneLayer)), withExamples([
        // The pointer resting on Display settings while Layout's menu is open.
        [[{ t: 'across', path: ['layout', 'settings'], press: true, rest: 400 }]],
        // Layout pressed, the pointer over File and back, then File pressed.
        [[{ t: 'across', path: ['layout', 'file', 'layout'], press: true, rest: 60 }, { t: 'click', at: 'file', i: 0, mod: null }]],
      ]));
  }, 600_000);
});

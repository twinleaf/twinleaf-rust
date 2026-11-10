// Undo steps on the whole page (round 1 #24 to #26). A command on any
// selection, from a key, a plot's menu, the Layout menu or the Signals list,
// is at most one undo step; so is each drag, press to release, of a plot, a
// gap, a signal's chip, a tab, a legend or a cursor: one pulled out of its
// tab, moved, dragged off its plot, which removes it, or, out of view,
// dragged back in by the tag that names it at the plot's edge. Undo puts back the
// tabs as they were and the selection as it was just before the step, less
// the plots that are gone, the property rows picked too, less those whose
// property went; redo puts back both as they were just after it.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, drag, dropped, editCursors, feed, focusPage, frames, glide, leafOf, mountPage, pickItem, plotMenu, pressKey, settle, until, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { xOf, yOf } from '../src/lib/cursors.js';
import { plotIds } from '../src/lib/layout.js';
import { STEP } from '../src/lib/moves.js';

const KEYS = [['sine.sine', 'sine.cosine'], ['aux.triangle'], ['sine.cosine'], ['aux.sawtooth']];
const SIGNALS = ['vector.x', 'vector.y'];
const RPCS = [
  { name: 'gain', channel: 'setting' },
  { name: 'offset', channel: 'setting' },
  { name: 'temp', channel: 'report' },
];

/** What undo and redo put back of the selection: the plots, sorted, and the rows picked, sorted. */
const chosen = (ws) => ({ plots: [...ws.selected].sort(), rows: [...ws.selection.rows].sort() });

/** The act that picks the plot menu's items `labels`, once a right-click on plot `id` has opened it. */
const fromMenu = async (id, ...labels) => (await plotMenu(id), () => pickItem(...labels));

/**
 * The commands, each `prepare(page, id)` making ready what comes before the
 * step, as a right-click opening a menu does, and returning the act that
 * makes it.
 */
const COMMANDS = {
  Delete: () => () => pressKey('Delete'),
  l: () => () => pressKey('l'),
  '|': () => () => pressKey('|'),
  _: () => () => pressKey('_'),
  f: () => () => pressKey('f'),
  'Split right': (page, id) => fromMenu(id, 'Split right'),
  'Close plot': (page, id) => fromMenu(id, 'Close plot'),
  'Kind Spectrum': (page, id) => fromMenu(id, 'Kind', 'Spectrum'),
  'Move to tab': (page, id) => fromMenu(id, 'Move to tab', 'Layout 2'),
  'Arrange as columns': async () => {
    await commands.mouse([['move', ...centre([...document.querySelectorAll('.top-bar button')].find((b) => b.textContent.trim() === 'Layout'))], ['down'], ['up']]);
    return () => pickItem('Columns');
  },
  'a signal clicked': () => () => document.querySelector('.palette [data-key="vector.x"]').click(),
  // Commands on the cursors, which are edits; every plot has some placed first.
  'the eraser': (page) => (cursorsEverywhere(page), () => document.querySelector('button[aria-label="Clear the cursors"]').click()),
  'End, which follows live': (page) => (cursorsEverywhere(page), () => pressKey('End')),
  'Clear cursors': (page, id) => (cursorsEverywhere(page), fromMenu(id, 'Clear cursors')),
};
/** Places X1, X2 and a Y1 on every plot, as steps made before the case. */
function cursorsEverywhere(page) {
  for (const id of plotIds(page.ws.layout)) editCursors(page, id, () => ({ x: { a: 3, b: 4 }, y: { time: { shared: { a: 0.5, b: null } } } }));
  flushSync();
}

describe('a command', () => {
  it('on any selection, from any origin, is at most one step, which undo and redo take back and do again, the selection with it', async () => {
    const made = {};
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2, 3]), fc.nat(3), fc.constantFrom(...Object.keys(COMMANDS)), async (picks, k, name) => {
        const page = await mountPage(KEYS, 'grid', 1, SIGNALS);
        try {
          const ids = plotIds(page.ws.layout);
          await focusPage();
          page.ws.selection.only(picks.map((i) => ids[i]));
          flushSync();
          document.activeElement?.blur();
          const act = await COMMANDS[name](page, ids[k]);
          flushSync();
          // What the step starts from: the tabs and the selection once the menu is open.
          const tabs = page.ws.tabs, before = chosen(page.ws), steps = page.history.undos.length;
          await act();
          flushSync();
          await frames();
          const added = page.history.undos.length - steps;
          expect(added, name).toBeLessThanOrEqual(1);
          if (!added) return void expect(page.ws.tabs, `${name} made no step, so changed nothing`).toBe(tabs);
          made[name] = (made[name] ?? 0) + 1;
          const after = page.ws.tabs, then = chosen(page.ws);
          page.history.undo();
          flushSync();
          expect(page.ws.tabs, `${name} undone`).toBe(tabs);
          const exists = (id) => !!page.ws.plots[id];
          expect(chosen(page.ws), `${name} undone`).toEqual({ plots: before.plots.filter(exists), rows: before.rows });
          page.history.redo();
          flushSync();
          expect(page.ws.tabs, `${name} done again`).toBe(after);
          expect(chosen(page.ws), `${name} done again`).toEqual(then);
        } finally {
          page.dispose();
        }
      }),
      // Each command first on two selected plots, the first of them its plot; then random runs.
      withExamples(Object.keys(COMMANDS).map((name) => [[0, 1], 0, name])),
    );
    for (const name of ['Delete', 'l', '|', 'Split right', 'Close plot', 'Kind Spectrum', 'Move to tab', 'Arrange as columns', 'a signal clicked', 'the eraser', 'End, which follows live', 'Clear cursors']) expect(made[name], name).toBeGreaterThan(0);
  }, 600_000);
});

/** Picks plot `from` up by its grip and holds it over `to`, until the drop it would make shows or a while has passed. */
async function lift(from, to) {
  await commands.mouse([['move', ...centre(leafOf(from))]]);
  const at = centre(leafOf(from).querySelector('.grip'));
  await commands.mouse([['move', ...at], ['down'], ...glide(at, to)]);
  await until(() => document.querySelector('.drop-preview, [data-tab].target'), 1000);
}

/** How near the pointer, in px, a cursor let go lands: the real mouse moves by whole device pixels. */
const PX = 1.5;
/** Presses at `from`, moves to `to` by way of the point between, and lets go. */
const pull = (from, to) => commands.mouse([['move', ...from], ['down'], ['move', (from[0] + to[0]) / 2, (from[1] + to[1]) / 2], ['move', ...to], ['up']]);
/** Waits until plot `id`'s body holds still on the page: a page just mounted, or a plot given its first cursor, is still laying itself out. */
async function still(id) {
  const at = () => JSON.stringify(leafOf(id).querySelector('.plot-body').getBoundingClientRect());
  for (let was = ''; was !== at(); await frames(15)) was = at();
  await settle();
}
/** Plot `a` of the page, fed and paused, so it has axes and holds still: {id, pv, P} with P its plot area. */
async function heldPlot(page, a) {
  feed(page.hub);
  const id = plotIds(page.ws.layout)[a % 4];
  page.views.get(id).pause(clock.now());
  await still(id);
  const pv = page.pv(id);
  return { id, pv, P: pv.geo.plot };
}

/**
 * The drags, each run on the page with the real mouse; a few rest along the
 * way, as the drag library needs to see where they are. One that makes a
 * step of its own first, to have something to drag, returns how many.
 */
const DRAGS = {
  async plot(page, a, b) {
    const ids = plotIds(page.ws.layout), from = ids[a % ids.length], onto = ids.filter((id) => id !== from)[b % (ids.length - 1)];
    const [cx, cy] = centre(leafOf(onto));
    await lift(from, [[cx, cy], [cx - STEP, cy], [cx, cy + STEP]][b % 3]);
    await commands.mouse([['up']]);
  },
  async 'plots to a tab'(page, a) {
    const ids = plotIds(page.ws.layout);
    page.ws.selection.only([ids[a % ids.length], ids[(a + 1) % ids.length]]);
    flushSync();
    await lift(ids[a % ids.length], centre(document.querySelectorAll('[role=tablist] [data-tab]')[1]));
    await commands.mouse([['up']]);
  },
  async gap(page, a, b) {
    const gaps = [...document.querySelectorAll('.tiles .separator')], g = gaps[a % gaps.length], [x, y] = centre(g), r = g.getBoundingClientRect();
    const by = ((b % 7) - 3) * 30 || 40;
    await drag([x, y], r.height > r.width ? [x + by, y] : [x, y + by]);
  },
  async chip(page, a, b) {
    const ids = plotIds(page.ws.layout).filter((id) => page.ws.plots[id].items.length);
    const from = ids[a % ids.length], item = page.ws.plots[from].items[0];
    const onto = leafOf(plotIds(page.ws.layout)[b % 4]);
    const to = b % 2 ? centre(onto.querySelector('.trace-zone')) : centre(onto.querySelector('.plot-body'));
    // By its left end: in a narrow plot the Add button lies over the rest of a long chip.
    const chip = document.getElementById(`chip-${item.id}`).getBoundingClientRect();
    await drag([chip.left + 10, chip.top + chip.height / 2], to, [centre(onto.querySelector('.trace-zone'))], 200);
    await dropped();
  },
  async tab(page, a) {
    const tabs = [...document.querySelectorAll('[role=tablist] [data-tab]')];
    const from = tabs[a % tabs.length], onto = tabs[(a + 1) % tabs.length];
    const [bx, by] = centre(onto), r = onto.getBoundingClientRect(), start = centre(from.querySelector('span'));
    await drag(start, [bx + (r.width / 3) * Math.sign(bx - start[0]), by], [[bx, by]], 500);
    await dropped();
  },
  async legend(page, a, b) {
    const id = plotIds(page.ws.layout)[a % 4];
    page.history.group('Legend', () => page.ws.setPlot(id, { legend: true }));
    const legend = () => leafOf(id).querySelector('.pinned-legend');
    if (!(await until(legend, 2000))) return;
    const [x, y] = centre(legend());
    await commands.mouse([['move', x, y], ['down'], ['move', x - 20 * (b % 4), y + 15], ['move', x - 40 * (b % 4), y + 30 * (b % 3)], ['up']]);
    return 1;
  },
  async 'an X cursor out of its tab'(page, a, b) {
    const { id, pv, P } = await heldPlot(page, a), name = b % 2 ? 'b' : 'a';
    const tab = pv.pressable.find((t) => t.part === 'tab' && t.kind === 'x' && t.name === name), to = [P.x + P.w * (0.3 + 0.1 * (b % 5)), P.y + P.h / 2];
    await pull([tab.x + tab.w / 2, tab.y + tab.h / 2], to);
    // It lands under the pointer.
    expect(Math.abs(pv.pxOf(xOf(page.ws.plots[id].cursors, 'time')[name]) - to[0])).toBeLessThan(PX);
  },
  async 'a Y cursor out of its tab'(page, a, b) {
    const { id, pv } = await heldPlot(page, a);
    // A tab the pointer reaches: a small plot's header, shown over its top while the pointer is on it, covers the first.
    await commands.mouse([['move', ...centre(leafOf(id))]]);
    const reached = (t) => document.elementFromPoint(t.x + t.w / 2, t.y + t.h / 2) === pv.overlay;
    const tabs = pv.pressable.filter((t) => t.part === 'tab' && t.kind === 'y' && reached(t)), tab = tabs[b % tabs.length], name = tab.name, lane = tab.axis.lane, to = [lane.x + lane.w / 2, lane.y + lane.h * (0.3 + 0.1 * (b % 5))];
    await pull([tab.x + tab.w / 2, tab.y + tab.h / 2], to);
    // Under the pointer on the axis as it is drawn now: a first cursor placed makes room at the top of the plot.
    const axis = pv.yAxes.find((x) => x.key === tab.key);
    expect(Math.abs(pv.axisScale(axis)(yOf(page.ws.plots[id].cursors, 'time', tab.key)[name]) - to[1])).toBeLessThan(PX);
  },
  async 'an X cursor moved, or off its plot'(page, a, b) {
    const { id, pv, P } = await heldPlot(page, a), off = b % 3 === 2;
    editCursors(page, id, () => ({ x: { a: pv.dataAt(P.x + P.w / 2), b: null } }));
    await still(id);
    const y = P.y + P.h / 2, to = [off ? P.x + P.w + 20 : P.x + P.w * (0.2 + 0.1 * (b % 3)), y];
    await pull([P.x + P.w / 2, y], to);
    const at = xOf(page.ws.plots[id].cursors, 'time').a;
    if (off) expect(at, 'let go beside the plot, it is removed').toBeNull();
    else expect(Math.abs(pv.pxOf(at) - to[0])).toBeLessThan(PX);
    return 1;
  },
  async 'a Y cursor moved, or off its lane'(page, a, b) {
    const { id, pv } = await heldPlot(page, a), off = b % 3 === 2, key = pv.yAxes[0].key, mid = (pv.yBounds(key)[0] + pv.yBounds(key)[1]) / 2;
    editCursors(page, id, () => ({ y: { time: { [key]: { a: mid, b: null } } } }));
    // The plot as it is drawn with the cursor on it.
    await still(id);
    const axis = pv.yAxes[0], lane = axis.lane;
    const x = lane.x + lane.w / 2, to = [x, off ? lane.y - 12 : lane.y + lane.h * (0.2 + 0.1 * (b % 3))];
    await pull([x, pv.axisScale(axis)(mid)], to);
    const at = yOf(page.ws.plots[id].cursors, 'time', axis.key).a;
    if (off) expect(at, 'let go above its lane, it is removed').toBeNull();
    else expect(Math.abs(pv.axisScale(axis)(at) - to[1])).toBeLessThan(PX);
    return 1;
  },
  async 'an X cursor out of view, by its tag at the edge'(page, a, b) {
    const { id, pv, P } = await heldPlot(page, a), name = b % 2 ? 'b' : 'a', [x0, x1] = pv.xd;
    // Before the view's start, or after its end.
    editCursors(page, id, () => ({ x: { a: null, b: null, [name]: b % 4 < 2 ? x0 - 5 : x1 + 5 } }));
    await still(id);
    const tag = pv.pressable.find((t) => t.edge && t.kind === 'x' && t.name === name), to = [P.x + P.w * (0.3 + 0.1 * (b % 5)), P.y + P.h / 2];
    expect(tag, 'its tag at the edge').toBeTruthy();
    await pull([tag.x + tag.w / 2, tag.y + tag.h / 2], to);
    expect(Math.abs(pv.pxOf(xOf(page.ws.plots[id].cursors, 'time')[name]) - to[0]), 'it lands under the pointer').toBeLessThan(PX);
    return 1;
  },
  async 'a Y cursor out of view, by its tag at the edge'(page, a, b) {
    const { id, pv } = await heldPlot(page, a), name = b % 2 ? 'b' : 'a', key = pv.yAxes[0].key, [lo, hi] = pv.yBounds(key);
    await commands.mouse([['move', ...centre(leafOf(id))]]);
    const reached = (t) => !!t && document.elementFromPoint(t.x + t.w / 2, t.y + t.h / 2) === pv.overlay;
    let tag;
    // Above what the axis shows, or below it: below where a small plot's header covers the tag above.
    for (const v of b % 4 < 2 ? [hi + 1, lo - 1] : [lo - 1]) {
      if (reached(tag)) break;
      editCursors(page, id, () => ({ y: { time: { [key]: { a: null, b: null, [name]: v } } } }));
      await still(id);
      tag = pv.pressable.find((t) => t.edge && t.kind === 'y' && t.name === name);
    }
    expect(reached(tag), 'its tag at the edge').toBe(true);
    const steps = page.history.undos.length, lane = pv.yAxes[0].lane, to = [lane.x + lane.w / 2, lane.y + lane.h * (0.3 + 0.1 * (b % 5))];
    await pull([tag.x + tag.w / 2, tag.y + tag.h / 2], to);
    expect(Math.abs(pv.axisScale(pv.yAxes[0])(yOf(page.ws.plots[id].cursors, 'time', key)[name]) - to[1]), 'it lands under the pointer').toBeLessThan(PX);
    return steps;
  },
};

describe('a drag, press to release,', () => {
  it('of a plot, plots to a tab, a gap, a chip, a tab, a legend or a cursor is at most one step, and undo takes it back', async () => {
    const made = {};
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(...Object.keys(DRAGS)), fc.nat(), fc.nat(), async (name, a, b) => {
        const page = await mountPage(KEYS, 'grid', 2, [...new Set(KEYS.flat())]);
        try {
          // The steps a drag made first, to have its legend or its cursor, come before it.
          const base = (await DRAGS[name](page, a, b)) ?? 0;
          flushSync();
          await frames();
          const added = page.history.undos.length - base;
          expect(added, name).toBeLessThanOrEqual(1);
          if (!added) return;
          made[name] = (made[name] ?? 0) + 1;
          const before = page.history.undos.at(-1).before.tabs;
          const after = page.ws.tabs;
          page.history.undo();
          flushSync();
          expect(page.ws.tabs, `${name} undone`).toBe(before);
          page.history.redo();
          expect(page.ws.tabs, `${name} done again`).toBe(after);
        } finally {
          page.dispose();
        }
      }),
      withExamples(Object.keys(DRAGS).flatMap((name) => [[name, 0, 1], [name, 1, 2]])),
    );
    for (const name of Object.keys(DRAGS)) expect(made[name], name).toBeGreaterThan(0);
  }, 600_000);
});

describe('a pin of property rows', () => {
  it('undone, picks again the rows picked before it, but those whose property went, and done again, those picked after it', async () => {
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2], { minLength: 1 }), fc.nat(2), fc.option(fc.nat(2)), async (picks, star, gone) => {
        const page = await mountPage(KEYS, 'grid', 0, [], RPCS);
        try {
          // Each property's row, in whichever panel shows it: each name is one panel's here.
          const rows = RPCS.map(({ name }) => [...document.querySelectorAll('tr[id^=prop-]')].find((tr) => tr.textContent.includes(name)));
          const click = (el, init = {}) => {
            el.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, cancelable: true, button: 0, ...init }));
            el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, button: 0, ...init }));
          };
          picks.forEach((i, n) => click(rows[i], { ctrlKey: n > 0 }));
          flushSync();
          const before = chosen(page.ws), steps = page.history.undos.length;
          expect(before.rows.length).toBe(picks.length);
          rows[star].querySelector('button.star').click();
          flushSync();
          expect(page.history.undos.length).toBe(steps + 1);
          const then = chosen(page.ws);
          // Something else picked meanwhile: a plot.
          page.ws.selection.only([plotIds(page.ws.layout)[0]]);
          // And a property gone from the device, as a new table of it says.
          const left = RPCS.filter((_, i) => i !== gone);
          if (gone !== null) page.hub.text({ type: 'table', seq: page.hub.seq + 1, route: '/', rpcs: left.map(({ name, channel }) => ({ name, channel, type: 'f32', access: ['read', 'write'] })) });
          const kept = (rows) => rows.filter((k) => left.some((p) => k.endsWith(`:${p.name}`)));
          flushSync();
          page.history.undo();
          flushSync();
          expect(chosen(page.ws)).toEqual({ plots: [], rows: kept(before.rows) });
          page.history.redo();
          flushSync();
          expect(chosen(page.ws)).toEqual({ plots: [], rows: kept(then.rows) });
        } finally {
          page.dispose();
        }
      }),
      withExamples([[[0, 2], 0, 0]]),
    );
  }, 600_000);
});

// Undo steps on the whole page (round 1 #24 to #26). A command on any
// selection, from a key, a plot's menu, the Layout menu or the Signals list,
// is at most one undo step; so is each drag, press to release, of a plot, a
// gap, a signal's chip, a tab or a legend. Undo puts back the tabs as they
// were and the selection as it was just before the step, less the plots
// that are gone, the property rows picked too, less those whose property
// went; redo puts back both as they were just after it.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, drag, dropped, focusPage, frames, glide, leafOf, mountPage, pickItem, plotMenu, until, withExamples } from './harness/page.js';
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
  Delete: () => () => commands.key('Delete'),
  l: () => () => commands.key('l'),
  '|': () => () => commands.key('|'),
  _: () => () => commands.key('_'),
  f: () => () => commands.key('f'),
  'Split right': (page, id) => fromMenu(id, 'Split right'),
  'Close plot': (page, id) => fromMenu(id, 'Close plot'),
  'Kind Spectrum': (page, id) => fromMenu(id, 'Kind', 'Spectrum'),
  'Move to tab': (page, id) => fromMenu(id, 'Move to tab', 'Layout 2'),
  'Arrange as columns': async () => {
    await commands.mouse([['move', ...centre([...document.querySelectorAll('.top-bar button')].find((b) => b.textContent.trim() === 'Layout'))], ['down'], ['up']]);
    return () => pickItem('Columns');
  },
  'a signal clicked': () => () => document.querySelector('.palette [data-key="vector.x"]').click(),
};

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
    for (const name of ['Delete', 'l', '|', 'Split right', 'Close plot', 'Kind Spectrum', 'Move to tab', 'Arrange as columns', 'a signal clicked']) expect(made[name], name).toBeGreaterThan(0);
  }, 600_000);
});

/** Picks plot `from` up by its grip and holds it over `to`, until the drop it would make shows or a while has passed. */
async function lift(from, to) {
  await commands.mouse([['move', ...centre(leafOf(from))]]);
  const at = centre(leafOf(from).querySelector('.grip'));
  await commands.mouse([['move', ...at], ['down'], ...glide(at, to)]);
  await until(() => document.querySelector('.drop-preview, [data-tab].target'), 1000);
}

/** The drags, each run on the page with the real mouse; a few rest along the way, as the drag library needs to see where they are. */
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
  },
};

describe('a drag, press to release,', () => {
  it('of a plot, plots to a tab, a gap, a chip, a tab or a legend is at most one step, and undo takes it back', async () => {
    const made = {};
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(...Object.keys(DRAGS)), fc.nat(), fc.nat(), async (name, a, b) => {
        const page = await mountPage(KEYS, 'grid', 2);
        try {
          await DRAGS[name](page, a, b);
          flushSync();
          await frames();
          // The legend's own step comes before the drag.
          const base = name === 'legend' ? 1 : 0, added = page.history.undos.length - base;
          expect(added, name).toBeLessThanOrEqual(1);
          if (!added) return;
          made[name] = (made[name] ?? 0) + 1;
          const before = page.history.undos.at(-1).before.tabs;
          page.history.undo();
          flushSync();
          expect(page.ws.tabs, `${name} undone`).toBe(before);
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

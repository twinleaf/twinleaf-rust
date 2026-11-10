// A plot's own commands act on the selection it is in, or on it alone
// (round 1 #7, round 2 Selection #5 and #6), on the whole page: for any
// selection and any plot, Maximize shows those plots alone, laid out among
// themselves as the layout has them, and Split puts an empty plot beside
// each, as one step, and selects the new plots. A key acts on the
// selection, and with none selected, a view key on every plot and a plot
// key on none (round 2 Selection #3), wherever focus is. Through closes,
// moves to another tab, undos and redos, what shows is the maximized plots
// still in the tab, or every plot once none is left (#6). A paused plot's
// page, taken down, asks the hub for no more windows. A plot menu's copy
// count is the count of the window shown now, whichever count the hub
// answers last (class 7).
import { describe, expect, it, vi } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, feed, focusPage, frames, leafOf, mountPage, pickItem, plotMenu, quietApi, showing, until, wait } from './harness/page.js';
import { GUTTER, plotIds, rects, restrict } from '../src/lib/layout.js';
import { api } from '../src/lib/protocol.js';

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine'], ['aux.sawtooth']];

/** The plots a command on plot `id` acts on, with `picked` selected: the selection if `id` is in it, else `id` alone. */
const targets = (picked, id) => (picked.includes(id) ? picked : [id]);

/** Presses the button labelled `label` on plot `id`, the pointer brought over the plot first so its header shows it. */
async function pressButton(id, label) {
  const leaf = document.querySelector(`.leaf[data-plot="${id}"]`);
  await commands.mouse([['move', ...centre(leaf)]]);
  const button = leaf.querySelector(`button[aria-label^="${label}"]`);
  const [x, y] = centre(button);
  await commands.mouse([['move', x, y], ['down'], ['up']]);
  flushSync();
  await frames();
}


describe('a plot’s own command', () => {
  it('Maximize shows the plots it acts on alone, each where the layout puts it among them', async () => {
    const page = await mountPage(KEYS, 'grid');
    let checked = 0;
    try {
      await fc.assert(
        fc.asyncProperty(fc.subarray(plotIds(page.ws.layout)), fc.nat(), async (picked, k) => {
          const ids = plotIds(page.ws.layout), id = ids[k % ids.length];
          page.ws.maximized = null;
          page.ws.selected.clear();
          for (const p of picked) page.ws.selected.add(p);
          flushSync();
          await frames();
          await pressButton(id, 'Maximize');
          const want = targets(picked, id);
          expect([...page.ws.maximized].sort()).toEqual([...want].sort());
          const box = document.querySelector('.tile-box').getBoundingClientRect();
          const at = rects(restrict(page.ws.layout, new Set(want)), { x: box.left, y: box.top, w: box.width, h: box.height }, GUTTER);
          expect(showing().sort()).toEqual([...want].sort());
          for (const p of want) {
            const r = leafOf(p).getBoundingClientRect(), w = at.get(p);
            expect(Math.max(Math.abs(r.left - w.x), Math.abs(r.top - w.y), Math.abs(r.width - w.w), Math.abs(r.height - w.h)), p).toBeLessThan(1.5);
          }
          checked++;
        })
      );
      expect(checked).toBeGreaterThan(5);
    } finally {
      page.dispose();
    }
  }, 600_000);

  it('Split, from its menu, puts an empty plot beside each plot it acts on, as one step, and selects the new plots', async () => {
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2, 3]), fc.nat(), fc.constantFrom('right', 'down'), async (picks, k, way) => {
        const page = await mountPage(KEYS, 'columns');
        try {
          const ids = plotIds(page.ws.layout), picked = picks.map((i) => ids[i]), id = ids[k % ids.length];
          page.ws.selected.clear();
          for (const p of picked) page.ws.selected.add(p);
          flushSync();
          const before = page.ws.tabs;
          await plotMenu(id);
          await pickItem(`Split ${way}`);
          const want = targets(picked, id);
          const made = plotIds(page.ws.layout).filter((p) => !ids.includes(p));
          expect(made.length).toBe(want.length);
          for (const p of made) expect(page.ws.plots[p].items).toEqual([]);
          expect([...page.ws.selected].sort()).toEqual([...made].sort());
          expect(page.history.undos.length).toBe(1);
          page.history.undo();
          flushSync();
          expect(page.ws.tabs).toBe(before);
        } finally {
          page.dispose();
        }
      })
    );
  }, 600_000);
});

/**
 * Keys of each scope, each with `acted(page, before, lead)`: the plots it
 * acted on, as the page shows them, `before` the plots before it and
 * `lead` the plot a picker should open on. Split's are counted instead.
 */
const KEYED = {
  Delete: { scope: 'plot', acted: (page, before) => before.filter((id) => !page.ws.plots[id]) },
  f: { scope: 'plot' },
  '|': { scope: 'plot' },
  _: { scope: 'plot' },
  l: { scope: 'plot', acted: (page, before) => before.filter((id) => page.ws.plots[id].legend) },
  k: { scope: 'plot', acted: () => picking() },
  a: { scope: 'plot', acted: () => picking() },
  ArrowLeft: { scope: 'view', acted: (page, before) => before.filter((id) => paused(id) && page.ws.plots[id].kind === 'time') },
  ' ': { scope: 'view', acted: (page, before) => before.filter(paused) },
};
/** Whether plot `id` is paused, as its header's button says. */
const paused = (id) => !!document.querySelector(`.leaf[data-plot="${id}"] button[aria-label="Follow live"]`);
/** The plots with a picker open, the kind picker or the signals menu. */
const picking = () => [...document.querySelectorAll('.leaf[data-plot]')].filter((l) => l.querySelector('.kind-select[data-state=open], .trace-zone[data-state=open]')).map((l) => l.dataset.plot);

describe('a key', () => {
  it('acts on the selection, or with none, a view key on every plot shown and a plot key on none, wherever focus is', async () => {
    const seen = new Set();
    await fc.assert(
      fc.asyncProperty(
        fc.option(fc.nat(3)),
        fc.option(fc.subarray([0, 1, 2, 3], { minLength: 1 })),
        fc.subarray([0, 1, 2, 3]),
        fc.constantFrom(...Object.keys(KEYED)),
        fc.option(fc.nat(3)),
        async (spectrum, max, picks, key, focus) => {
          const page = await mountPage(KEYS, 'grid');
          try {
            const before = plotIds(page.ws.layout);
            if (spectrum !== null) page.history.group('Kind', () => page.ws.setPlot(before[spectrum], { kind: 'spectrum' }));
            // Some plots maximized, or none; what is selected and focused is shown, as only a shown plot can be.
            const was = max && max.map((i) => before[i]);
            page.ws.maximized = was;
            const shown = page.ws.shownIds();
            const picked = picks.map((i) => before[i]).filter((id) => shown.includes(id));
            const into = focus !== null && shown.includes(before[focus]) ? before[focus] : null;
            page.ws.selection.only(picked);
            // Focus on the page, or in a plot, which makes it the one clicked last and selects nothing.
            if (into) document.querySelector(`.leaf[data-plot="${into}"] .plot-card`).focus();
            else document.activeElement?.blur();
            flushSync();
            await frames();
            const undos = page.history.undos.length;
            await commands.key(key === ' ' ? 'Space' : key);
            flushSync();
            await frames();
            const { scope, acted } = KEYED[key];
            const want = picked.length ? picked : scope === 'view' ? shown : [];
            const last = into ?? picked[0];
            const lead = want.includes(last) ? last : want[0];
            const made = plotIds(page.ws.layout).filter((id) => !before.includes(id));
            if (key === '|' || key === '_') {
              // An empty plot beside each, selected to fill next.
              expect(made.length).toBe(want.length);
              if (want.length) expect([...page.ws.selected].sort()).toEqual([...made].sort());
            } else if (key === 'f') {
              // F leaves a maximized view whatever is selected, else maximizes what it acts on.
              expect(page.ws.maximized).toEqual(was ? null : want.length ? want : null);
            }
            else if (key === 'k' || key === 'a') expect(acted(), `${key} with ${picked.length} selected`).toEqual(want.length ? [lead] : []);
            else expect([...acted(page, before)].sort(), `${key} with ${picked.length} selected`).toEqual([...want].filter((id) => key !== 'ArrowLeft' || page.ws.plots[id].kind === 'time').sort());
            if (!want.length) expect(page.history.undos.length).toBe(undos);
            seen.add(`${scope}:${picked.length ? 'some' : 'none'}:${!into ? 'page' : picked.includes(into) ? 'in' : 'out'}`);
          } finally {
            page.dispose();
          }
        },
      ),
      {
        // Each scope with plots selected or none, and focus on the page, on a selected plot or on another; then random runs.
        examples: ['Delete', ' '].flatMap((key) => [[[1, 2], null], [[1, 2], 1], [[1, 2], 0], [[], null], [[], 0]].map(([picks, focus]) => [null, null, picks, key, focus])),
      },
    );
    expect(seen.size).toBe(10);
  }, 600_000);
});

describe('maximized plots', () => {
  it('show alone, the maximized ones still in the tab, through any run of closes, moves to another tab, undos and redos', async () => {
    const plotStep = fc.oneof(
      fc.record({ t: fc.constant('close'), k: fc.nat() }),
      fc.record({ t: fc.constant('toTab'), k: fc.nat() }),
      fc.record({ t: fc.constant('undo') }),
      fc.record({ t: fc.constant('redo') }),
    );
    let pruned = 0;
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2, 3], { minLength: 1 }), fc.array(plotStep, { minLength: 1, maxLength: 6 }), async (max, steps) => {
        const page = await mountPage(KEYS, 'grid', 1);
        try {
          const ids = plotIds(page.ws.layout);
          await focusPage();
          page.ws.selection.only(max.map((i) => ids[i]));
          flushSync();
          document.activeElement?.blur();
          await commands.key('f');
          flushSync();
          expect([...page.ws.maximized].sort()).toEqual(max.map((i) => ids[i]).sort());
          for (const s of steps) {
            const shown = page.ws.shownIds();
            if (s.t === 'close' && shown.length) {
              page.ws.selection.only([shown[s.k % shown.length]]);
              flushSync();
              await commands.key('Delete');
            } else if (s.t === 'toTab' && shown.length) {
              await plotMenu(shown[s.k % shown.length]);
              await pickItem('Move to tab', 'Layout 2');
            } else if (s.t === 'undo') await commands.key('Control+z');
            else if (s.t === 'redo') await commands.key('Control+y');
            flushSync();
            await frames();
            // What shows is the maximized plots of the tab while any are, else every plot.
            const all = plotIds(page.ws.layout), max = page.ws.maximized;
            if (max) expect(max.length && max.every((id) => all.includes(id))).toBe(true);
            expect(showing().sort()).toEqual([...(max ?? all)].sort());
            // A close from the keys moves on to a plot that shows, as the plot clicked last.
            if (s.t === 'close' && page.ws.focusedId) expect(page.ws.shownIds()).toContain(page.ws.focusedId);
            if (max && max.length < shown.length) pruned++;
          }
        } finally {
          page.dispose();
        }
      }),
      { examples: [[[0, 1], [{ t: 'close', k: 0 }, { t: 'undo' }, { t: 'redo' }, { t: 'toTab', k: 0 }]]] },
    );
    expect(pruned).toBeGreaterThan(0);
  }, 600_000);
});


describe('a paused plot', () => {
  it('asks the hub for no window once its page is taken down', async () => {
    quietApi();
    const page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
    try {
      feed(page.hub);
      await focusPage();
      await commands.key('Space');
      const [id] = plotIds(page.ws.layout), body = leafOf(id).querySelector('.plot-body'), [x, y] = centre(body);
      // Two zooms at once: the second's read waits its turn.
      for (const deltaY of [120, -120]) body.querySelector('canvas').dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY, clientX: x, clientY: y }));
    } finally {
      page.dispose();
    }
    const asked = api.window.mock.calls.length;
    // Longer than any read waits its turn.
    await wait(300);
    expect(api.window.mock.calls.length).toBe(asked);
    vi.restoreAllMocks();
  });
});

/** The rows the hub counts in a window: one a millisecond, so each window has a count of its own. */
const countOf = ({ t0, t1 }) => Math.round((t1 - t0) * 1000);
/** The plot menu open, if one is. */
const openPlotMenu = () => document.querySelector('[data-slot=context-menu-content][data-state=open]');

describe('the count a plot’s menu shows for a copy', () => {
  // Class 7, stale async: the hub's counts are held back and handed out in
  // any order while the menu opens, closes and the plot zooms under it.
  it('is, once every count is back, the count of the window shown now, whatever order they came back in', async () => {
    const step = fc.oneof(
      fc.record({ t: fc.constant('open') }),
      fc.record({ t: fc.constant('close') }),
      fc.record({ t: fc.constant('zoom'), out: fc.boolean() }),
      { weight: 3, arbitrary: fc.record({ t: fc.constant('answer'), k: fc.nat() }) },
    );
    let checked = 0;
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 10 }), fc.array(fc.nat(), { maxLength: 10 }), async (steps, order) => {
        const pending = [];
        quietApi((q) => new Promise((done) => pending.push(() => done({ count: countOf(q), oldest: q.t0 - 1, x: [], y: [] }))));
        const page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
        try {
          feed(page.hub);
          await focusPage();
          // Paused, so the window holds still but for the zooms.
          await commands.key('Space');
          const [id] = plotIds(page.ws.layout), body = leafOf(id).querySelector('.plot-body');
          const asked = () => api.rows.mock.calls.length;
          const answer = async (i) => {
            pending.splice(i % pending.length, 1)[0]();
            await until(() => true);
          };
          for (const s of steps) {
            if (s.t === 'open') await plotMenu(id);
            else if (s.t === 'close') await commands.key('Escape');
            else if (s.t === 'zoom') {
              const [x, y] = centre(body), was = asked(), open = openPlotMenu();
              body.querySelector('canvas').dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY: s.out ? 120 : -120, clientX: x, clientY: y }));
              // An open menu counts again once the view rests.
              if (open) await until(() => asked() > was, 3000);
            } else if (pending.length) await answer(s.k);
            await frames();
          }
          // Every count that is still out comes back, in any order.
          for (const k of order) if (pending.length) await answer(k);
          while (pending.length) await answer(0);
          await frames();
          // A menu left open shows the count; a shut one shows none.
          const item = [...(openPlotMenu()?.querySelectorAll('[role=menuitem]') ?? [])].find((m) => m.textContent.trim().startsWith('Copy data'));
          if (!item) return;
          const shown = item.querySelector('.menu-value')?.textContent.trim();
          // The window shown now, as the hub is asked it afresh.
          const was = asked();
          await commands.key('Escape');
          await until(() => !openPlotMenu());
          await plotMenu(id);
          expect(await until(() => asked() > was)).toBe(true);
          expect(shown).toBe(`${countOf(api.rows.mock.calls.at(-1)[0]).toLocaleString()} rows`);
          checked++;
        } finally {
          page.dispose();
          vi.restoreAllMocks();
        }
      }),
      {
        // The menu opens, the plot zooms under it, and the first count comes back after the second.
        examples: [[[{ t: 'open' }, { t: 'zoom', out: true }, { t: 'answer', k: 1 }, { t: 'answer', k: 0 }], []]],
      },
    );
    expect(checked).toBeGreaterThan(3);
  }, 600_000);
});

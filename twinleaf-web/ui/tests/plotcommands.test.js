// A plot's own commands act on the selection it is in, or on it alone
// (round 1 #7, round 2 Selection #5 and #6), on the whole page: for any
// selection and any plot, Maximize shows those plots alone, laid out among
// themselves as the layout has them, and Split puts an empty plot beside
// each, as one step, and selects the new plots. A key acts on the
// selection, and with none selected, a view key on every plot and a plot
// key on none (round 2 Selection #3), wherever focus is. Maximize, and
// Restore, leave the same plots maximized and selected from every origin:
// the key, the palette, a plot's menu, its button and a double-click on its
// header. Through those, Escape, splits, tabs opened, closes, moves to
// another tab, undos and redos, what shows is the maximized plots still in
// the tab, or every plot once none is left (#6) or an undo or a redo brings
// a plot into the tab, and what is selected shows (workspace.svelte.js). A paused plot's
// page, taken down, asks the hub for no more windows. A plot menu's copy
// count is the count of the window shown now, whichever count the hub
// answers last (class 7).
import { describe, expect, it, vi } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { OPEN, centre, clearNotices, feed, focusPage, frames, headerSpot, leafOf, mountPage, pickItem, plotMenu, pressKey, quietApi, settle, showing, until, wait, withExamples } from './harness/page.js';
import { GUTTER, plotIds, rects, restrict } from '../src/lib/layout.js';
import { api } from '../src/lib/protocol.js';
import { COMMANDS } from '../src/lib/commands.js';

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
          page.ws.restoreLayout();
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
 * Keys of each scope, as the command table gives it, each with
 * `acted(page, before, lead)`: the plots it acted on, as the page shows
 * them, `before` the plots before it and `lead` the plot a picker should
 * open on. Split's are counted instead.
 */
const KEYED = {
  Delete: { acted: (page, before) => before.filter((id) => !page.ws.plots[id]) },
  f: {},
  '|': {},
  _: {},
  l: { acted: (page, before) => before.filter((id) => page.ws.plots[id].legend) },
  k: { acted: () => picking() },
  a: { acted: () => picking() },
  ArrowLeft: { acted: (page, before) => before.filter((id) => paused(id) && page.ws.plots[id].kind === 'time') },
  ' ': { acted: (page, before) => before.filter(paused) },
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
            page.ws.maximize(was);
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
            await pressKey(key === ' ' ? 'Space' : key);
            flushSync();
            await frames();
            const { acted } = KEYED[key], { scope } = COMMANDS.find((c) => c.keys?.includes(key));
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
        // Each scope with plots selected or none, and focus on the page, on a selected plot or on another; then random runs.
        withExamples(['Delete', ' '].flatMap((key) => [[[1, 2], null], [[1, 2], 1], [[1, 2], 0], [[], null], [[], 0]].map(([picks, focus]) => [null, null, picks, key, focus]))),
    );
    expect(seen.size).toBe(10);
  }, 600_000);
});

/**
 * The origins of Maximize, which is Restore while plots are maximized: each
 * makes the command, on plot `id` where the origin is a plot's, and returns
 * false where it does not offer it.
 */
const MAXIMIZE = {
  key: () => pressKey('f'),
  palette: async () => {
    await pressKey('Control+k');
    await settle();
    const item = document.querySelector('[data-slot=command-item][data-command="maximize"]');
    if (item) item.click();
    else await pressKey('Escape');
    // The palette, closed, fades out over the page, and takes every press until it has gone.
    expect(await until(() => !document.querySelector('[data-slot=dialog-overlay]'), 2000), 'the palette gone').toBe(true);
    return !!item;
  },
  menu: async (id) => {
    await plotMenu(id);
    const row = document.querySelector('[role^=menuitem][data-command=maximize]');
    expect(row, `the menu of plot ${id} open, with the command’s row`).toBeTruthy();
    row.click();
  },
  button: async (id) => {
    const button = leafOf(id).querySelector('.plot-head [data-command=maximize]');
    // A narrow plot's header has no such button.
    if (!button.getBoundingClientRect().width) return false;
    await commands.mouse([['move', ...centre(button)], ['down'], ['up']]);
  },
  header: async (id) => {
    const spot = headerSpot(leafOf(id));
    if (!spot) return false;
    await commands.mouse([['dblclick', ...spot]]);
  },
};
/** The origins made on a plot, which is selected first unless it is in the selection. */
const ON_A_PLOT = ['menu', 'button', 'header'];

describe('maximized plots', () => {
  it('are what the last Maximize from any origin left of the tab’s plots, show alone with the selection among them, and go with Restore, Escape, a split, an undo or a redo that brings a plot back, or a tab opened, through any run of those, closes, moves to another tab, undos and redos', async () => {
    const plotStep = fc.oneof(
      { weight: 4, arbitrary: fc.record({ t: fc.constant('max'), by: fc.constantFrom(...Object.keys(MAXIMIZE)), k: fc.nat() }) },
      { weight: 2, arbitrary: fc.record({ t: fc.constant('select'), ks: fc.subarray([0, 1, 2, 3]) }) },
      fc.record({ t: fc.constant('escape') }),
      fc.record({ t: fc.constant('close'), k: fc.nat() }),
      fc.record({ t: fc.constant('toTab'), k: fc.nat() }),
      fc.record({ t: fc.constant('split'), k: fc.nat() }),
      fc.record({ t: fc.constant('tab') }),
      fc.record({ t: fc.constant('undo') }),
      fc.record({ t: fc.constant('redo') }),
    );
    let pruned = 0;
    const seen = new Set();
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2, 3], { minLength: 1 }), fc.array(plotStep, { minLength: 1, maxLength: 6 }), async (max, steps) => {
        const page = await mountPage(KEYS, 'grid', 1);
        try {
          const ids = plotIds(page.ws.layout);
          await focusPage();
          page.ws.selection.only(max.map((i) => ids[i]));
          flushSync();
          document.activeElement?.blur();
          await pressKey('f');
          flushSync();
          expect([...page.ws.maximized].sort()).toEqual(max.map((i) => ids[i]).sort());
          for (const s of steps) {
            await clearNotices();
            const shown = page.ws.shownIds(), picked = [...page.ws.selected], was = page.ws.maximized, tab = page.ws.activeId, at = JSON.stringify(s);
            const had = new Set(Object.keys(page.ws.plots));
            const on = shown.length ? shown[(s.k ?? 0) % shown.length] : null;
            // The plots the step leaves maximized, were none to go, and the selection it leaves where the rules say which.
            let left = was, selected;
            if (s.t === 'max' && (on || !ON_A_PLOT.includes(s.by))) {
              const acts = ON_A_PLOT.includes(s.by) ? targets(picked, on) : picked;
              const made = (await MAXIMIZE[s.by](on)) !== false;
              // The palette offers it with plots to act on, or a view to leave.
              if (s.by === 'palette') expect(made, `offered, ${at}`).toBe(acts.length > 0 || !!was);
              if (made) {
                left = was ? null : acts;
                selected = acts;
                if (was || acts.length) seen.add(`${s.by}:${was ? 'restore' : 'maximize'}`);
              }
            } else if (s.t === 'select') page.ws.selection.only(shown.filter((_, i) => s.ks.includes(i)));
            else if (s.t === 'escape') {
              // Wherever focus is but a field, as in a plot's signals once a press on its chip row leaves it there, Escape leaves a
              // maximized view with the selection kept, or with none deselects (keys.test.js).
              await pressKey('Escape');
              left = null;
              selected = was ? picked : [];
            } else if (s.t === 'close' && on) {
              page.ws.selection.only([on]);
              flushSync();
              await pressKey('Delete');
            } else if (s.t === 'toTab' && on && page.ws.tab.name === 'A') {
              await plotMenu(on);
              await pickItem('Move to tab', 'Layout 2');
            } else if (s.t === 'split' && on) {
              page.ws.selection.only([on]);
              flushSync();
              await pressKey('|');
              left = null;
            } else if (s.t === 'tab') await pressKey(']');
            else if (s.t === 'undo') await pressKey('Control+z');
            else if (s.t === 'redo') await pressKey('Control+y');
            await until(() => !document.querySelector(OPEN), 2000);
            flushSync();
            await frames();
            // An undo or a redo that brings a plot into the tab leaves the view, so the plot shows.
            if ((s.t === 'undo' || s.t === 'redo') && Object.keys(page.ws.plots).some((id) => !had.has(id))) left = null;
            // A tab opened shows every plot; a plot that has left the tab is maximized no more, and with none left every plot shows.
            const kept = page.ws.activeId === tab ? (left ?? []).filter((id) => page.ws.plots[id]) : [];
            const all = plotIds(page.ws.layout), now = page.ws.maximized;
            expect(now && [...now].sort(), `maximized after ${at}`).toEqual(kept.length ? kept.sort() : null);
            expect(showing().sort(), `showing after ${at}`).toEqual([...(now ?? all)].sort());
            // What is selected, and the plot clicked last, show.
            const shows = page.ws.shownIds();
            expect([...page.ws.selected].filter((id) => !shows.includes(id)), `selected but hidden after ${at}`).toEqual([]);
            if (page.ws.focusedId) expect(shows, `the plot clicked last after ${at}`).toContain(page.ws.focusedId);
            if (selected) expect([...page.ws.selected].sort(), `selected after ${at}`).toEqual([...selected].sort());
            if (now && was && now.length < was.length) pruned++;
          }
        } finally {
          page.dispose();
        }
      }),
      withExamples([
        [[0, 1], [{ t: 'close', k: 0 }, { t: 'undo' }, { t: 'redo' }, { t: 'toTab', k: 0 }]],
        // Each origin restores, then maximizes.
        [[0, 1], [{ t: 'max', by: 'key', k: 0 }, { t: 'max', by: 'key', k: 0 }, { t: 'max', by: 'palette', k: 0 }, { t: 'max', by: 'palette', k: 0 }, { t: 'max', by: 'menu', k: 1 }, { t: 'max', by: 'menu', k: 1 }]],
        [[2], [{ t: 'max', by: 'button', k: 0 }, { t: 'max', by: 'button', k: 3 }, { t: 'max', by: 'header', k: 0 }, { t: 'max', by: 'header', k: 1 }, { t: 'select', ks: [] }, { t: 'max', by: 'palette', k: 0 }]],
        // A plot closed before the view was entered comes back, and the view is left so it shows.
        [[0], [{ t: 'escape' }, { t: 'close', k: 1 }, { t: 'select', ks: [0] }, { t: 'max', by: 'key', k: 0 }, { t: 'undo' }]],
        // A split leaves the view; taken back, the view entered again, and made again, the view is left again and every plot shows.
        [[0], [{ t: 'split', k: 0 }, { t: 'undo' }, { t: 'select', ks: [0] }, { t: 'max', by: 'button', k: 0 }, { t: 'redo' }, { t: 'tab' }, { t: 'tab' }]],
        // Restored by the key and maximized from a header, focus left in the plot's signals: Escape leaves the view.
        [[3], [{ t: 'max', by: 'key', k: 0 }, { t: 'max', by: 'header', k: 0 }, { t: 'escape' }]],
        // Restored first from a plot's menu, which a right-click opens where the plot is drawn at its maximized size.
        [[0, 1], [{ t: 'max', by: 'menu', k: 0 }, { t: 'escape' }]],
      ]),
    );
    // A replay of one failure runs that case alone.
    if (fc.readConfigureGlobal().path !== undefined) return;
    expect(pruned).toBeGreaterThan(0);
    expect([...seen].sort()).toEqual(Object.keys(MAXIMIZE).flatMap((by) => [`${by}:maximize`, `${by}:restore`]).sort());
  }, 600_000);
});


describe('a paused plot', () => {
  it('asks the hub for no window once its page is taken down', async () => {
    quietApi();
    const page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
    try {
      feed(page.hub);
      await focusPage();
      await pressKey('Space');
      const [id] = plotIds(page.ws.layout), body = leafOf(id).querySelector('.plot-body'), [x, y] = centre(body);
      // Two zooms at once, eased over the frames after: the second frame's read waits its turn.
      for (const deltaY of [120, -120]) body.querySelector('canvas').dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY, clientX: x, clientY: y }));
      await frames(2);
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
          await pressKey('Space');
          const [id] = plotIds(page.ws.layout), body = leafOf(id).querySelector('.plot-body');
          const asked = () => api.rows.mock.calls.length;
          const answer = async (i) => {
            pending.splice(i % pending.length, 1)[0]();
            await until(() => true);
          };
          for (const s of steps) {
            if (s.t === 'open') await plotMenu(id);
            else if (s.t === 'close') await pressKey('Escape');
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
          await pressKey('Escape');
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
        // The menu opens, the plot zooms under it, and the first count comes back after the second.
        withExamples([[[{ t: 'open' }, { t: 'zoom', out: true }, { t: 'answer', k: 1 }, { t: 'answer', k: 0 }], []]]),
    );
    expect(checked).toBeGreaterThan(3);
  }, 600_000);
});

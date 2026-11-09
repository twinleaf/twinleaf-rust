// A plot's own commands act on the selection it is in, or on it alone
// (round 1 #7, round 2 Selection #5 and #6), on the whole page: for any
// selection and any plot, Maximize shows those plots alone, laid out among
// themselves as the layout has them, and Split puts an empty plot beside
// each, as one step, and selects the new plots. A key acts on the
// selection, and with none selected, a view key on every plot and a plot
// key on none (round 2 Selection #3), wherever focus is.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, frames, mountPage } from './browser/page.js';
import { GUTTER, plotIds, rects, restrict } from '../src/lib/layout.js';

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

/** Right-clicks plot `id`'s body and picks the menu item named `label`. */
async function pickFromMenu(id, label) {
  const body = document.querySelector(`.leaf[data-plot="${id}"] .plot-body`);
  const [x, y] = centre(body);
  body.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, button: 2, clientX: x, clientY: y }));
  flushSync();
  await frames();
  const item = [...document.querySelectorAll('[role=menuitem]')].find((m) => m.textContent.trim().startsWith(label));
  item.click();
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
          for (const p of ids) {
            const el = document.querySelector(`.leaf[data-plot="${p}"]`);
            expect(getComputedStyle(el).visibility === 'hidden', `${p} hidden`).toBe(!want.includes(p));
            if (!want.includes(p)) continue;
            const r = el.getBoundingClientRect(), w = at.get(p);
            expect(Math.max(Math.abs(r.left - w.x), Math.abs(r.top - w.y), Math.abs(r.width - w.w), Math.abs(r.height - w.h)), p).toBeLessThan(1.5);
          }
          checked++;
        }),
        { numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 5) },
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
          await pickFromMenu(id, `Split ${way}`);
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
      }),
      { numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 10) },
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
            } else if (key === 'f') expect(page.ws.maximized).toEqual(!want.length ? was : was ? null : want);
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
        numRuns: Math.max(60, Math.ceil(fc.readConfigureGlobal().numRuns / 2)),
        // Each scope with plots selected or none, and focus on the page, on a selected plot or on another; then random runs.
        examples: ['Delete', ' '].flatMap((key) => [[[1, 2], null], [[1, 2], 1], [[1, 2], 0], [[], null], [[], 0]].map(([picks, focus]) => [null, null, picks, key, focus])),
      },
    );
    expect(seen.size).toBe(10);
  }, 600_000);
});

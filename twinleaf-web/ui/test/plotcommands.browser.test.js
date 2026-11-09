// A plot's own commands act on the selection it is in, or on it alone
// (round 1 #7, round 2 Selection #5 and #6), on the whole page: for any
// selection and any plot, Maximize shows those plots alone, laid out among
// themselves as the layout has them, and Split puts an empty plot beside
// each, as one step, and selects the new plots.
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

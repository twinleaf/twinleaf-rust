// The real-time smoke set. Every other page test steps a virtual clock; a
// page that only ever ran on one could stop following the browser's time
// and no test would see. Here the page is mounted on the real clock and
// nothing steps it: it draws frames by itself, a wheel burst ends by a real
// rest, a click on one of several selected plots narrows the selection a
// real moment later, and a menu closed by a press outside is gone when its
// fade is. Each waits real time, so there are few, and they check that
// time passes for the page, not how long it takes.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import { centre, feed, leafOf, mountPage, plotMenu, pressKey, turnWheel, turnWheelToRest, wait } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { IDLE_MS } from '../src/lib/viewhistory.svelte.js';
import { DOUBLE_CLICK_MS } from '../src/lib/selection.svelte.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** The page on the real clock: three plots in rows, fed. `wait` then waits real time, and `frames` the browser's frames. */
async function realPage() {
  const page = await mountPage([SIGNALS, SIGNALS, SIGNALS], 'rows', 0, SIGNALS, [], { realClock: true });
  feed(page.hub);
  return { ...page, ids: plotIds(page.ws.layout) };
}
/** Plot `id`'s header button whose label starts `label`. */
const button = (id, label) => leafOf(id).querySelector(`.plot-head button[aria-label^="${label}"]`);

describe('real-time smoke: the page on the browser’s clock', () => {
  it('draws frames and moves a live view on by itself', async () => {
    const page = await realPage();
    try {
      const view = page.views.get(page.ids[0]), edge = () => view.range(page.hub.now(clock.now()))[1];
      const start = edge(), at = clock.now();
      // The frame rate shows once the page has drawn for half a second.
      await wait(1200);
      flushSync();
      expect(clock.now() - at, 'the page’s time is the browser’s').toBeGreaterThanOrEqual(1100);
      expect(edge(), 'the live edge moved on').toBeGreaterThan(start + 1);
      const fps = [...document.querySelectorAll('.status-bar .item')].map((el) => el.textContent.match(/^(\d+) fps$/)?.[1]).find(Boolean);
      expect(Number(fps), 'frames drawn a second').toBeGreaterThan(0);
      expect(page.pv(page.ids[0]).geo?.plot, 'the plot is drawn').toBeTruthy();
    } finally {
      page.dispose();
    }
  });

  it('makes wheel turns one step until they rest for real, and the next a step of its own', async () => {
    const page = await realPage();
    try {
      const id = page.ids[0], view = page.views.get(id);
      await wait(100);
      turnWheel(page, id, -120);
      await wait(40);
      await turnWheelToRest(page, id, -120);
      const burst = view.span;
      await wait(IDLE_MS + 150);
      await turnWheelToRest(page, id, -120);
      const later = view.span;
      expect(later).toBeLessThan(burst);
      button(id, 'Back').click();
      flushSync();
      expect(view.span, 'Back past the turn after the rest').toBe(burst);
      button(id, 'Back').click();
      flushSync();
      expect(view.live, 'Back past the burst, both turns at once').toBe(true);
      expect(button(id, 'Back').disabled).toBe(true);
    } finally {
      page.dispose();
    }
  });

  it('narrows the selection to a clicked plot a real moment after the click', async () => {
    const page = await realPage();
    try {
      document.body.focus();
      await pressKey('Control+a');
      flushSync();
      expect(page.ws.selected.size).toBe(3);
      const [x, y] = centre(leafOf(page.ids[1]).querySelector('.plot-body'));
      await commands.mouse([['move', x, y], ['down'], ['up']]);
      await wait(DOUBLE_CLICK_MS + 200);
      flushSync();
      expect([...page.ws.selected]).toEqual([page.ids[1]]);
    } finally {
      page.dispose();
    }
  });

  it('has a menu closed by a press outside gone once its fade is', async () => {
    const page = await realPage();
    try {
      await wait(100);
      await plotMenu(page.ids[0]);
      await commands.mouse([['move', ...centre(document.querySelector('.status-bar'))], ['down'], ['up']]);
      // Longer than the fade and the longest its copy is kept.
      await wait(900);
      flushSync();
      expect(document.querySelector('[data-slot$=-menu-content]'), 'no menu, and no copy of one').toBeNull();
    } finally {
      page.dispose();
    }
  });
});

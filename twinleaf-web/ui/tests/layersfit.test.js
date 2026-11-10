// Every popup layer fits in the window, however short the window is: a
// menu, a popover or the command palette that is taller than the room it has
// stays inside the window and scrolls within itself. The layers are opened
// as a person opens them, at several window heights.
import { afterAll, describe, expect, it } from 'vitest';
import { page as browser } from 'vitest/browser';
import { MENUS, click, menuPage, settle } from './harness/menus.js';
import { OPEN, centre, closeAll, pressKey, until } from './harness/page.js';

const HEIGHTS = [900, 420, 300];
const button = (label) => document.querySelector(`button[aria-label="${label}"]`);
/** Each layer, `open(page)` opening it. */
const LAYERS = {
  'the plot menu': (page) => MENUS['the plot menu'](page, page.ids[0]),
  'the Layout dropdown': (page) => MENUS['the Layout dropdown'](page),
  'the File dropdown': (page) => MENUS['the File dropdown'](page),
  'the display settings': () => click(centre(button('Display settings'))),
  'the command palette': () => pressKey('Control+k'),
};
const layers = () => [...document.querySelectorAll(`${OPEN}, [role=dialog]`)].filter((el) => el.dataset.state !== 'closed');

afterAll(() => browser.viewport(1200, 900));

describe('a popup layer', () => {
  it('stays inside the window at any height, and scrolls within itself when it is the taller', async () => {
    let scrolled = 0;
    for (const height of HEIGHTS) {
      await browser.viewport(1200, height);
      const page = await menuPage();
      try {
        for (const [name, open] of Object.entries(LAYERS)) {
          await open(page);
          expect(await until(() => layers().length > 0, 2000), `${name} opens at ${height} px`).toBe(true);
          await settle();
          for (const el of layers()) {
            const r = el.getBoundingClientRect();
            expect(r.top, `${name} at ${height} px: its top`).toBeGreaterThanOrEqual(-1);
            expect(r.bottom, `${name} at ${height} px: its bottom`).toBeLessThanOrEqual(window.innerHeight + 1);
            // What does not fit can be reached: the layer, or a list in it, scrolls.
            const scrollers = [el, ...el.querySelectorAll('*')].filter((e) => e.scrollHeight > e.clientHeight + 1 && /auto|scroll/.test(getComputedStyle(e).overflowY));
            const clipped = [el, ...el.querySelectorAll('*')].some((e) => e.scrollHeight > e.clientHeight + 1 && getComputedStyle(e).overflowY === 'hidden' && e.clientHeight > 40);
            if (clipped) expect(scrollers.length, `${name} at ${height} px scrolls`).toBeGreaterThan(0);
            if (scrollers.length) scrolled++;
          }
          if (name === 'the command palette') await pressKey('Escape');
          else await closeAll();
          await until(() => !layers().length, 2000);
        }
      } finally {
        page.dispose();
      }
    }
    expect(scrolled).toBeGreaterThan(0);
  }, 300_000);
});

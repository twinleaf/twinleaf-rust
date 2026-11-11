// Every popup layer fits in the window, however short the window is: a
// menu, a popover or the command palette that is taller than the room it has
// stays inside the window and scrolls within itself. The layers are opened
// as a person opens them, at several window heights. A tooltip stays inside
// the window wherever its button is, at several window sizes, and wraps to
// fit, no narrower than it must.
import { afterAll, describe, expect, it } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import fc from 'fast-check';
import { mount, unmount } from 'svelte';
import { MENUS, click, menuPage, settle } from './harness/menus.js';
import { OPEN, centre, clickOn, closeAll, mountPage, pressKey, rested, until, withExamples } from './harness/page.js';
import TipStage from './harness/TipStage.svelte';
import { TIP_EDGE } from '../src/lib/components/ui/tooltip/tooltip-content.svelte';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

const HEIGHTS = [900, 420, 300];
/** The window sizes a tooltip is tried at: the page's, a small window's, and two phones'. */
const SIZES = [[1200, 900], [700, 500], [360, 640], [320, 568]];
/** The least width a tooltip of more than one line fills, in px, where the window has the room. */
const TIP_LEAST = 200;
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

  it('as a tooltip, stays inside the window wherever its button is and however long it reads, and wraps no narrower than it must', async () => {
    const words = fc.array(fc.constantFrom('211', 'kB/s', 'coming', 'in.', 'Holding', '758.9', 'MB,', '60', 'min', 'of', 'each', 'stream.', '/vmr0/ch1/stream/x-axis-field'), { minLength: 1, maxLength: 40 });
    const place = fc.record({ x: fc.double({ min: 0, max: 1, noNaN: true }), y: fc.double({ min: 0, max: 1, noNaN: true }), text: words.map((w) => w.join(' ')) });
    const INFO = '211 kB/s coming in. Holding 758.9 MB, 60 min of each stream.';
    // At each edge and corner, the History popover's tooltip and the longest the cases make.
    const EXAMPLES = [[1, 0.5, INFO], [0, 0.5, INFO], [0.5, 0, INFO], [0.5, 1, INFO], [1, 1, INFO], [0, 0, INFO], [1, 0, Array(40).fill('stream.').join(' ')], [0, 1, '/vmr0/ch1/stream/x-axis-field']].map(([x, y, text]) => [{ x, y, text }]);
    for (const [width, height] of SIZES) {
      await browser.viewport(width, height);
      await fc.assert(
        fc.asyncProperty(place, async ({ x, y, text }) => {
          const where = `${text.length} characters, its button at ${x.toFixed(2)}, ${y.toFixed(2)} of a ${width} × ${height} window`;
          const stage = mount(TipStage, { target: document.body, props: { text, left: x * (width - 16), top: y * (height - 16) } });
          try {
            await settle();
            await commands.mouse([['move', ...centre(document.querySelector('.tip-stage'))]]);
            const tip = () => document.querySelector('[data-slot=tooltip-content]');
            expect(await until(tip, 2000), `${where}: the tooltip opens`).toBe(true);
            expect(await rested(() => JSON.stringify(tip().getBoundingClientRect())), `${where}: the tooltip comes to rest`).toBe(true);
            const r = tip().getBoundingClientRect();
            expect([r.left, r.top], `${where}: its top left`).toEqual([Math.max(0, r.left), Math.max(0, r.top)]);
            expect([r.right, r.bottom], `${where}: its bottom right`).toEqual([Math.min(width, r.right), Math.min(height, r.bottom)]);
            // A tooltip of more than one line is as wide as its widest, or the window allows.
            const style = getComputedStyle(tip());
            const lines = Math.round((r.height - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom)) / parseFloat(style.lineHeight));
            if (lines > 1) expect(r.width, `${where}: wrapped`).toBeGreaterThanOrEqual(Math.min(TIP_LEAST, width - 2 * TIP_EDGE));
          } finally {
            unmount(stage);
            await commands.mouse([['move', 0, 0]]);
            await until(() => !document.querySelector('[data-slot=tooltip-content]'), 2000);
          }
        }),
        withExamples(EXAMPLES),
      );
    }
  }, 300_000);

  it('as the History popover’s tooltip, stays inside the window at any width while what it says changes', async () => {
    for (const [width, height] of SIZES) {
      await browser.viewport(width, height);
      const page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
      try {
        const memory = (rate) => page.hub.text({ type: 'memory', bytes: 758.9e6, budget: 1e9, seconds: 3600, max_age: 3600, rate, oldest: { 0: 0 } });
        memory(211e3);
        await settle();
        await clickOn(document.querySelector('.status-bar .history'));
        const info = () => document.querySelector('[data-slot=popover-content] .info');
        expect(await until(info, 2000), `${width} px: the popover opens`).toBe(true);
        await commands.mouse([['move', ...centre(info())]]);
        const tip = () => document.querySelector('[data-slot=tooltip-content]');
        expect(await until(tip, 2000), `${width} px: the tooltip opens`).toBe(true);
        for (const rate of [211e3, 9.9e9, 1, 21.1e6]) {
          memory(rate);
          await settle();
          expect(await rested(() => JSON.stringify(tip().getBoundingClientRect())), 'the tooltip comes to rest').toBe(true);
          const r = tip().getBoundingClientRect(), where = `${width} × ${height}, “${tip().textContent}”`;
          expect([r.left, r.top], `${where}: its top left`).toEqual([Math.max(0, r.left), Math.max(0, r.top)]);
          expect([r.right, r.bottom], `${where}: its bottom right`).toEqual([Math.min(width, r.right), Math.min(height, r.bottom)]);
        }
        await shot(`tooltip-history-${width}`);
      } finally {
        page.dispose();
      }
    }
  }, 120_000);

  it('brings the page no scroll bars while it reaches past an edge of the window, as it may for a frame while it is placed', async () => {
    await browser.viewport(1200, 900);
    const page = await menuPage();
    const sizes = () => JSON.stringify({ page: [document.documentElement.clientWidth, document.documentElement.clientHeight], docks: [...document.querySelectorAll('.dock-frame')].map((f) => f.getBoundingClientRect().width) });
    const past = document.createElement('div');
    try {
      const was = sizes();
      expect(JSON.parse(was).page, 'the page fills the window').toEqual([window.innerWidth, window.innerHeight]);
      document.body.append(past);
      // Past the right edge, then past the bottom one, by a px or by much.
      for (const [left, top] of [[window.innerWidth - 19, 40], [40, window.innerHeight - 19], [window.innerWidth + 300, window.innerHeight + 300]]) {
        past.style.cssText = `position: absolute; left: ${left}px; top: ${top}px; width: 20px; height: 20px;`;
        await settle();
        expect(sizes(), `the page and its docks with a layer at ${left}, ${top}`).toBe(was);
      }
    } finally {
      past.remove();
      page.dispose();
    }
  });
});

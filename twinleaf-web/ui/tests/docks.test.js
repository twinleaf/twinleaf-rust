// The docks' panels are lists inside a list. The subjects are found on the
// page: every dock, every open panel in it and every list in a panel.
import { afterAll, describe, expect, it } from 'vitest';
import { page as browser, commands } from 'vitest/browser';
import { centre, mountPage, settle, wait } from './harness/page.js';

const LIST = '[data-slot=scroll-area-viewport]';
const BAR = '[data-slot=scroll-area-scrollbar]';
const panels = () => [...document.querySelectorAll('.dock-side:not(.rail) .dock-panel.open')];
const titleOf = (panel) => panel.getAttribute('aria-label');
/** Whether the list of viewport `list` draws a scroll bar. */
const barOf = (list) => [...list.parentElement.querySelectorAll(`:scope > ${BAR}`)].some((bar) => bar.getBoundingClientRect().height > 0);
const rpcs = (n, channel, extra = {}) => Array.from({ length: n }, (_, i) => ({ name: `${channel ?? 'other'}.p${i}`, channel, value: i, ...extra }));

afterAll(() => browser.viewport(1200, 900));

describe('a list in a dock', () => {
  it('whose rows fit does not scroll and draws no scroll bar', async () => {
    await browser.viewport(1200, 900);
    for (const routes of [['/'], ['/', '/0'], ['/', '/0', '/1']]) {
      const page = await mountPage([['sine.sine']], 'columns', 0, ['sine.sine', 'sine.cos'], [...rpcs(2, 'setting'), ...rpcs(2, 'report', { access: ['read'] })], { routes });
      try {
        await settle(6);
        const lists = panels().flatMap((panel) => [...panel.querySelectorAll(LIST)].map((list) => [titleOf(panel), list]));
        expect(lists.map(([title]) => title)).toEqual(expect.arrayContaining(['Devices', 'Signals', 'Settings']));
        for (const [title, list] of lists) {
          // A list shows its scroll bar under the pointer at the latest.
          await commands.mouse([['move', ...centre(list)]]);
          await wait(100);
          await settle();
          const said = `${title} with ${routes.length} device${routes.length === 1 ? '' : 's'}`;
          expect(list.scrollHeight - list.clientHeight, `${said}: px of rows out of view`).toBe(0);
          expect(barOf(list), `${said}: a scroll bar`).toBe(false);
        }
      } finally {
        page.dispose();
      }
    }
  }, 120_000);
});

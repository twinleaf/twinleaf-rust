// A menu shows what it showed when it opened until it is gone. A press
// outside a menu can change what its rows say, as when it deselects the
// plots a row counted; the menu it closes must not show the new words on
// its way out. Every menu of the page is held to it.
// A menu opened again before it has gone works as any does.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { MENUS, menuPage, settle } from './harness/menus.js';
import { centre, frames } from './harness/page.js';

describe('a menu closed by a press outside it', () => {
  it('says what it said until it is gone, whatever the press changed', async () => {
    const page = await menuPage();
    let checked = 0;
    try {
      for (const [name, open] of Object.entries(MENUS)) {
        // Two plots selected, so the press outside, which deselects them, changes what rows count.
        page.ws.selection.only(page.ids.slice(0, 2));
        await settle();
        const menu = await open(page, page.ids[1]);
        expect(menu, `${name} opens`).toBeTruthy();
        const said = menu.textContent;
        /** What the menu shows: itself, or the copy fading in its place. */
        const shown = () => (getComputedStyle(menu).visibility === 'hidden' ? menu.nextElementSibling : menu);
        // The status bar is outside every menu, and a press on it deselects.
        await commands.mouse([['move', ...centre(document.querySelector('.status-bar'))], ['down']]);
        for (let i = 0; i < 40 && menu.isConnected; i++) {
          expect(shown()?.textContent ?? said, `${name}, closing`).toBe(said);
          await frames(1);
        }
        await commands.mouse([['up']]);
        expect(menu.isConnected, `${name} gone`).toBe(false);
        await settle();
        checked++;
      }
    } finally {
      page.dispose();
    }
    expect(checked).toBe(Object.keys(MENUS).length);
  }, 300_000);
});

describe('a menu opened again while it fades', () => {
  it('shows, and its rows work', async () => {
    const page = await menuPage();
    try {
      const id = page.ids[1];
      for (let i = 0; i < 3; i++) {
        await MENUS['the plot menu'](page, id);
        // A press outside, and at once the menu again.
        await commands.mouse([['move', ...centre(document.querySelector('.status-bar'))], ['down'], ['up']]);
        const menu = await MENUS['the plot menu'](page, id);
        expect(menu, 'the menu opens again').toBeTruthy();
        expect(getComputedStyle(menu).visibility, 'and shows').toBe('visible');
      }
      // Its rows are the page's own: one of them still does what it says.
      const before = page.ws.maximized;
      await commands.key('f');
      await settle();
      expect(page.ws.maximized, 'Maximize ran').not.toEqual(before);
    } finally {
      page.dispose();
    }
  }, 120_000);
});

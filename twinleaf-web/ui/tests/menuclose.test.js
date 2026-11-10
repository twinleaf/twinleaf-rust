// A menu shows what it showed when it opened until it is gone. A press
// outside a menu can change what its rows say, as when it deselects the
// plots a row counted; the menu it closes must not show the new words on
// its way out. Every menu of the page is held to it.
//
// The page does not do this yet: the plot menu loses its count of plots as
// it fades. The test is kept failing until it does.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { MENUS, menuPage, settle } from './harness/menus.js';
import { centre, frames } from './harness/page.js';

describe('a menu closed by a press outside it', () => {
  it.fails('says what it said until it is gone, whatever the press changed', async () => {
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
        // The status bar is outside every menu, and a press on it deselects.
        await commands.mouse([['move', ...centre(document.querySelector('.status-bar'))], ['down']]);
        for (let i = 0; i < 40 && menu.isConnected; i++) {
          expect(menu.textContent, `${name}, closing`).toBe(said);
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

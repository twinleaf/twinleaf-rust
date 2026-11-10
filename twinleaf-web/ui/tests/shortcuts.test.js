// A key a menu row shows does what the row does. For every row of every
// menu of the page that shows a key, the page after the key is pressed,
// with the menu open or once it is closed, is the page after the row is
// clicked: the same tabs,
// selection, maximized plots and stars, and the same thing open or being
// typed in. The menus are found on the page, so a row added to one is held
// to this without being named here.
import { describe, expect, it } from 'vitest';
import { MENUS, click, closeAll, menuPage, settle } from './harness/menus.js';
import { OPEN, centre, pressKey, until } from './harness/page.js';

/** The keys a row's `<kbd>`s name, as the keyboard command takes them. */
const NAMES = { '⇧': 'Shift', Ctrl: 'Control', '|': '|', _: '_' };
const keysOf = (row) => [...row.querySelectorAll('kbd')].map((k) => NAMES[k.textContent.trim()] ?? k.textContent.trim()).map((k) => (k.length === 1 ? k.toLowerCase() : k)).join('+');
/** A row's name: its text less its keys and its value. */
function labelOf(row) {
  const copy = row.cloneNode(true);
  for (const el of copy.querySelectorAll('kbd, .menu-value')) el.remove();
  return copy.textContent.trim();
}
const shown = (el) => el.getClientRects().length > 0;

/** What the page shows that a command can change, ids put as the order they first come in. */
function snapshot(page) {
  const ids = new Map();
  const plain = (v) => JSON.stringify(v).replace(/\b[a-z]+-\d+-[a-z0-9]{1,4}\b/g, (id) => ids.get(id) ?? (ids.set(id, `#${ids.size}`), ids.get(id)));
  return plain({
    tabs: page.ws.tabs,
    open: page.ws.activeId,
    selected: [...page.ws.selected],
    maximized: page.ws.maximized,
    stars: [...page.history.favorites.names].sort(),
    layers: document.querySelectorAll(`${OPEN}, [role=dialog]`).length,
    typing: document.activeElement?.tagName === 'INPUT',
  });
}

/** The rows of each menu that show a key, as [menu, label, keys]. */
async function rowsWithKeys() {
  const page = await menuPage();
  const found = [];
  try {
    for (const [name, open] of Object.entries(MENUS)) {
      const menu = await open(page, page.ids[1]);
      expect(menu, `${name} opens`).toBeTruthy();
      for (const row of menu.querySelectorAll('[role^=menuitem]:not([data-disabled])')) {
        if ([...row.querySelectorAll('kbd')].some(shown)) found.push([name, labelOf(row), keysOf(row)]);
      }
      await closeAll();
    }
  } finally {
    page.dispose();
  }
  return found;
}

/** The page after `menu`'s row `label` is clicked (`keys` null), or after `keys` are pressed with the menu open or, `closed`, once it is closed. */
async function after(menu, label, keys, closed = false) {
  const page = await menuPage();
  try {
    const m = await MENUS[menu](page, page.ids[1]);
    const row = [...m.querySelectorAll('[role^=menuitem]')].find((r) => labelOf(r) === label);
    if (keys === null) {
      await click(centre(row));
      // The menu the row was in goes; what the row opened stays.
      await until(() => !m.isConnected);
    }
    if (keys !== null) {
      if (closed) {
        // One Escape closes the menu; a second would deselect.
        await pressKey('Escape');
        expect(await until(() => !document.querySelector(OPEN)), 'the menu closed').toBe(true);
      }
      await pressKey(keys);
      if (!closed) await until(() => !m.isConnected);
    }
    await settle(6);
    return snapshot(page);
  } finally {
    page.dispose();
  }
}

describe('a key a menu row shows', () => {
  it('does what the row does, with the menu open and once it is closed', async () => {
    const rows = await rowsWithKeys();
    // The plot's, a chip's, the Signals list's and a tab's menus each show keys.
    expect(new Set(rows.map(([menu]) => menu)).size).toBeGreaterThanOrEqual(4);
    const wrong = [];
    for (const [menu, label, keys] of rows) {
      // Copying leaves the page as it was either way: what it puts on the clipboard is copy.js's to test.
      if (keys === 'Control+c') continue;
      const byRow = await after(menu, label, null);
      if ((await after(menu, label, keys)) !== byRow) wrong.push(`${menu} › ${label} (${keys}), menu open`);
      if ((await after(menu, label, keys, true)) !== byRow) wrong.push(`${menu} › ${label} (${keys}), menu closed`);
    }
    expect(wrong.join('; ')).toBe('');
  }, 600_000);
});

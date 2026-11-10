// A key a menu row shows does what the row does. For every row of every
// menu of the page that shows a key, the page after the key is pressed,
// with the menu open or once it is closed, is the page after the row is
// clicked: the same tabs,
// selection, maximized plots and stars, and the same thing open or being
// typed in. A key runs a row only as the row shows it: with Ctrl held where
// the row shows Ctrl and not where it does not. And Enter is the
// highlighted row's while one is highlighted, whatever row shows Enter.
// The menus are found on the page, so a row added to one is held to these
// without being named here.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { MENUS, click, closeAll, menuPage, settle } from './harness/menus.js';
import { OPEN, centre, pressKey, until, withExamples } from './harness/page.js';
import { lastKey } from '../src/lib/keyrouter.js';

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

/** A menu's rows that can be chosen. */
const ROWS = '[role^=menuitem]:not([data-disabled])';
/** Runs `during`, and gives the menu rows clicked meanwhile, by the mouse, a key or the menu itself. */
async function clicked(during) {
  const rows = [];
  const note = (e) => e.target.closest?.('[role^=menuitem]') && rows.push(e.target.closest('[role^=menuitem]'));
  window.addEventListener('click', note, true);
  try {
    await during();
    await settle();
  } finally {
    window.removeEventListener('click', note, true);
  }
  return rows;
}
/** `keys` with Ctrl held where they are without it, and without it where they have it. */
const otherCtrl = (keys) => (keys.split('+').includes('Control') ? keys.split('+').filter((k) => k !== 'Control').join('+') : `Control+${keys}`);

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

  it('runs its row only with the Ctrl the row shows: the plain key runs no row that shows Ctrl with it, and Ctrl with the key none that shows it plain', async () => {
    const rows = await rowsWithKeys();
    const seen = { plain: 0, ctrl: 0 };
    await fc.assert(
      fc.asyncProperty(fc.nat(rows.length - 1), fc.subarray([0, 1, 2]), async (i, picks) => {
        const [menu, label, keys] = rows[i], pressed = otherCtrl(keys);
        const page = await menuPage();
        try {
          page.ws.selection.only(picks.map((p) => page.ids[p]));
          await settle();
          const m = await MENUS[menu](page, page.ids[1]);
          // Every row of the open menus, by the keys it shows now.
          const shows = new Map([...document.querySelectorAll(ROWS)].map((r) => [r, keysOf(r)]));
          const row = [...m.querySelectorAll(ROWS)].find((r) => labelOf(r) === label && shows.get(r) === keys);
          // The selection decides which rows a menu has: one it does not have now shows no key to press.
          if (!row) return;
          const ran = await clicked(() => pressKey(pressed));
          const said = `${pressed} in ${menu}, where ${label} shows ${keys}`;
          expect(ran.filter((r) => shows.get(r) !== pressed).map(labelOf), said).toEqual([]);
          if (![...shows.values()].includes(pressed)) expect(lastKey().by, said).not.toBe('menu');
          seen[pressed.includes('Control') ? 'ctrl' : 'plain']++;
        } finally {
          await closeAll();
          page.dispose();
        }
      }),
      withExamples(rows.map((_, i) => [i, [1]])),
    );
    // A row that shows Ctrl with its key was pressed plain, and one that shows a plain key with Ctrl.
    expect(seen.plain).toBeGreaterThan(0);
    expect(seen.ctrl).toBeGreaterThan(0);
  }, 600_000);
});

describe('Enter in an open menu', () => {
  it('runs the highlighted row and no other, in every menu with a row that shows Enter, however the row was highlighted', async () => {
    const menus = [...new Set((await rowsWithKeys()).filter(([, , keys]) => keys.split('+').at(-1) === 'Enter').map(([menu]) => menu))];
    expect(menus.length).toBeGreaterThan(0);
    let beside = 0;
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(...menus), fc.nat(), fc.constantFrom('pointer', 'arrows'), fc.subarray([0, 1, 2]), async (menu, at, how, picks) => {
        const page = await menuPage();
        try {
          page.ws.selection.only(picks.map((p) => page.ids[p]));
          await settle();
          const m = await MENUS[menu](page, page.ids[1]);
          const rows = [...m.querySelectorAll(ROWS)], row = rows[at % rows.length];
          if (how === 'pointer') await commands.mouse([['move', ...centre(row)]]);
          else for (let i = 0; i < rows.length && !row.hasAttribute('data-highlighted'); i++) await pressKey('ArrowDown');
          await settle();
          const said = `${labelOf(row)} of ${menu}, highlighted by the ${how}`;
          expect(m.querySelector('[data-highlighted]'), said).toBe(row);
          // Whether another row of the menu shows plain Enter now, and could take it.
          const other = rows.some((r) => r !== row && keysOf(r) === 'Enter');
          const ran = await clicked(() => pressKey('Enter'));
          expect(ran.filter((r) => r !== row).map(labelOf), said).toEqual([]);
          // A row that opens a submenu opens it; any other is run, once.
          if (!row.hasAttribute('aria-haspopup')) expect(ran, said).toEqual([row]);
          else expect(row.getAttribute('aria-expanded'), said).toBe('true');
          if (other) beside++;
        } finally {
          await closeAll();
          page.dispose();
        }
      }),
      // The Signals list's first signal is in the first two plots and not the third: with the third selected, Add to plot shows Enter.
      withExamples(menus.flatMap((menu) => [[menu, 1, 'pointer', [2]], [menu, 1, 'arrows', [2]], [menu, 2, 'pointer', [0, 2]], [menu, 0, 'arrows', [2]]])),
    );
    // The highlighted row had a row beside it that shows Enter.
    expect(beside).toBeGreaterThan(0);
  }, 600_000);
});

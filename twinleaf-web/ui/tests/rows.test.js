// Property rows on the whole page, as the user's verdicts have them (round
// 1, #8 to #11, #13 and #27; round 2, Selection #13 and #15). A click picks
// a row alone, Ctrl drops or adds one, Shift picks the run from the row
// clicked last in page order, across panels and docks and past folded
// panels, and a right-click on a row not picked picks it alone. Add to and
// Remove from Favorites, from the menu or a picked row's star, act on every picked row as
// one undo step. In a table, the table's keys act on its rows, the page's
// keys act as anywhere, and the plots' keys do nothing. Every press is the
// real mouse's.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { clickOn, frames, mountPage, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { KEY as DOCKS_KEY, PANELS } from '../src/lib/docks.svelte.js';

const KEYS = [['sine.sine'], ['aux.triangle']];
const RPCS = [
  { name: 'gain', channel: 'setting' },
  { name: 'offset', channel: 'setting' },
  { name: 'enable', channel: 'setting', type: 'bool' },
  { name: 'sync', channel: 'setting', type: 'bool' },
  { name: 'temp', channel: 'report', access: ['read'] },
  { name: 'volts', channel: 'report', access: ['read'] },
  { name: 'errors', channel: 'diagnostic', type: 'u32', access: ['read'] },
  { name: 'reboot', channel: 'action', type: null, access: [] },
  { name: 'save', channel: 'action', type: null, access: [] },
  { name: 'misc', channel: null, type: 'u8' },
  { name: 'note', channel: null, type: 'u8' },
];
/** Panels of rows in both docks, one folded between two open ones. */
const DOCKS = {
  left: [{ id: 'devices', open: true }, { id: 'signals', open: false }, { id: 'settings', open: true }],
  right: [{ id: 'favorites', open: true }, { id: 'reports', open: true }, { id: 'diagnostics', open: false }, { id: 'actions', open: true }, { id: 'other', open: true }],
  hidden: { left: false, right: false },
};
const mount = (more = 0) => mountPage(KEYS, 'columns', more, [], RPCS, { saved: { [DOCKS_KEY]: DOCKS } });

/** The panel id of the dock panel `el` is in. */
const panelOf = (el) => Object.entries(PANELS).find(([, p]) => p.title === el.closest('.dock-panel')?.getAttribute('aria-label'))?.[0];
/** The rows and action buttons the page shows, each {key, el}, the key `${panel}:${name}`, in page order. */
const rowEls = () =>
  [...document.querySelectorAll('.dock-panel tr[id^=prop-], .dock-panel .action-grid button[data-name]')].map((el) => ({
    key: `${panelOf(el)}:${el.dataset.name ?? el.querySelector('.pname').firstChild.textContent.trim()}`,
    el,
  }));
/** The rows the page shows, as keys, in page order: an action button is one. */
const shown = () => rowEls().map((r) => r.key);
const rowEl = (key) => rowEls().find((r) => r.key === key)?.el;
/** Where a click on row `key` lands: a cell of a table's row, or the button. */
const target = (key) => (rowEl(key).matches('tr') ? rowEl(key).querySelector('td:last-child') : rowEl(key));
/** The rows the page marks picked, as keys. */
const marked = () => rowEls().filter(({ el }) => (el.getAttribute('aria-selected') ?? el.getAttribute('aria-pressed')) === 'true').map((r) => r.key);
const nameOf = (key) => key.slice(key.indexOf(':') + 1);
/** Whether row `key` is a table's row, not an action button. */
const isRow = (key) => rowEl(key)?.matches('tr');
const panelHead = (id) => [...document.querySelectorAll('.dock-panel')].find((s) => s.getAttribute('aria-label') === PANELS[id].title)?.querySelector('.dock-head');
const isOpen = (id) => panelHead(id).getAttribute('aria-expanded') === 'true';

/** A click on `el` with the real mouse, Ctrl or Shift held as `init` says. */
const click = (el, init = {}) => clickOn(el, { ctrl: !!init.ctrlKey, shift: !!init.shiftKey });
/** Opens the row menu on `el` with a right-click of the real mouse, and waits for it. */
async function rightClick(el) {
  await clickOn(el, { right: true });
  flushSync();
  for (let i = 0; i < 30 && !document.querySelector('[role=menu]'); i++) await frames();
}
/** Closes an open menu with Escape, and waits until it has gone. */
async function closeMenu() {
  if (!document.querySelector('[role=menu]')) return;
  await commands.key('Escape');
  for (let i = 0; i < 20 && document.querySelector('[role=menu]'); i++) await frames();
}
const menuItems = () => [...document.querySelectorAll('[role=menu] [role=menuitem]')];

describe('a property row', () => {
  it('is picked by a click, Ctrl-click, Shift-click or right-click as a sheet picks, across panels and docks in page order, past folded panels, and only while it shows', async () => {
    const page = await mount();
    const folds = ['settings', 'reports', 'other', 'diagnostics', 'favorites'];
    const initial = Object.fromEntries(folds.map((id) => [id, isOpen(id)]));
    const favorites = page.history.favorites;
    const seen = { across: 0, docks: 0, pastFolded: 0, dropped: 0, rightUnpicked: 0, rightPicked: 0, rightButton: 0, buttons: 0, hidden: 0, starredAway: 0, anchorHidden: 0, copy: 0, bar: 0 };
    const hidden = () => !!document.querySelector('.dock-side.right.rail');
    /** Clicks `el` with Shift held, which keeps the selection, and waits for the dock to slide. */
    async function shiftPress(el) {
      await clickOn(el, { shift: true });
      for (let i = 0; i < 90 && hidden() !== el.getAttribute('aria-label').startsWith('Hide'); i++) await frames();
      await frames(2);
    }
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.oneof(
              { weight: 6, arbitrary: fc.record({ t: fc.constantFrom('click', 'ctrl', 'ctrl', 'shift', 'shift', 'right'), at: fc.nat() }) },
              fc.record({ t: fc.constantFrom('fold', 'keyfold'), panel: fc.constantFrom(...folds) }),
              fc.record({ t: fc.constantFrom('hide', 'show') }),
              fc.record({ t: fc.constant('star'), at: fc.nat() }),
            ),
            { minLength: 1, maxLength: 10 },
          ),
          async (acts) => {
            if (hidden()) await shiftPress(document.querySelector('button[aria-label="Show the right dock"]'));
            for (const id of folds) if (isOpen(id) !== initial[id]) await click(panelHead(id));
            page.history.group('Seed', () => favorites.set(['gain', 'temp']));
            page.ws.selection.clear();
            flushSync();
            await frames();
            // The model: the rows picked, in the order picked, whether they show or not; the row a Shift pick runs from; the row picked last.
            let picks = [], anchor = null, at = page.ws.selection.rowAt;
            const shownPicks = () => picks.filter((k) => shown().includes(k));
            for (const a of acts) {
              if ((a.t === 'fold' || a.t === 'keyfold') && !panelHead(a.panel)) continue;
              if (a.t === 'fold') {
                // A press on a dock header lets go of the rows, as of the plots.
                await click(panelHead(a.panel));
                flushSync();
                await frames();
                picks = [];
                anchor = null;
              } else if (a.t === 'keyfold') {
                // Folding from the keys presses nothing: the picks in the panel only stop showing.
                // Focus arrives from the keys, as the header had none: it is the header's Space then.
                document.activeElement?.blur();
                await commands.key('Shift');
                panelHead(a.panel).focus();
                await commands.key(' ');
                flushSync();
                await frames();
                if (picks.some((k) => k.startsWith(`${a.panel}:`))) seen.hidden++;
              } else if (a.t === 'hide' || a.t === 'show') {
                const button = document.querySelector(`button[aria-label="${a.t === 'hide' ? 'Hide' : 'Show'} the right dock"]`);
                if (!button) continue;
                await shiftPress(button);
                if (a.t === 'hide' && picks.some((k) => !k.startsWith('settings:'))) seen.hidden++;
              } else if (a.t === 'star') {
                const rows = shown().filter(isRow);
                if (!rows.length) continue;
                const key = rows[a.at % rows.length], name = nameOf(key);
                const on = !favorites.has(name), before = [...favorites.names];
                const names = shownPicks().includes(key) ? [...new Set(shownPicks().map(nameOf))] : [name];
                await clickOn(rowEl(key).querySelector('button.star'));
                flushSync();
                await frames();
                expect([...favorites.names].sort()).toEqual((on ? [...new Set([...before, ...names])] : before.filter((n) => !names.includes(n))).sort());
                if (!on && picks.some((k) => k.startsWith('favorites:') && names.includes(nameOf(k)))) seen.starredAway++;
              } else {
                const order = shown();
                if (!order.length) continue;
                const key = order[a.at % order.length];
                const cell = target(key);
                if (a.t === 'right') {
                  // An action button has no row menu, and a right-click there picks nothing.
                  const button = cell.matches('button');
                  if (button) seen.rightButton++;
                  else if (shownPicks().includes(key)) seen.rightPicked++;
                  else (seen.rightUnpicked++, (picks = [key]), (anchor = key), (at = key));
                  await rightClick(cell);
                  if (!button) {
                    // Copy counts what it copies: every picked row that shows, each property once.
                    const n = new Set(shownPicks().map(nameOf)).size;
                    expect(menuItems().find((m) => m.textContent.includes('Copy')).textContent.trim()).toMatch(n > 1 ? `Copy ${n} rows` : /^Copy row/);
                    seen.copy++;
                  }
                  await closeMenu();
                } else {
                  if (cell.matches('button')) seen.buttons++;
                  await click(cell, { ctrlKey: a.t === 'ctrl', shiftKey: a.t === 'shift' });
                  flushSync();
                  at = key;
                  if (a.t === 'shift' && anchor && !order.includes(anchor)) seen.anchorHidden++;
                  if (a.t === 'click' || (a.t === 'shift' && !order.includes(anchor))) (picks = [key]), (anchor = key);
                  else if (a.t === 'ctrl') {
                    const base = shownPicks();
                    if (base.includes(key)) (picks = base.filter((k) => k !== key)), seen.dropped++;
                    else picks = [...base, key];
                    anchor = key;
                  } else {
                    const [i, j] = [order.indexOf(anchor), order.indexOf(key)].sort((x, y) => x - y);
                    picks = order.slice(i, j + 1);
                    const panels = new Set(picks.map((k) => k.split(':')[0]));
                    if (panels.size > 1) seen.across++;
                    if (panels.has('settings') && [...panels].some((p) => p !== 'settings')) seen.docks++;
                    if (panels.has('reports') && panels.has('other') && !isOpen('diagnostics')) seen.pastFolded++;
                  }
                }
              }
              flushSync();
              await frames();
              // The rows picked are those of the picks that show, in the order picked, and the page marks just those.
              expect([...page.ws.selection.rows], JSON.stringify(a)).toEqual(shownPicks());
              expect(marked().sort()).toEqual(shownPicks().sort());
              expect(page.ws.selected.size).toBe(0);
              // The edit bar shows only under the row picked last, while it shows, and counts the rows it edits.
              const bars = [...document.querySelectorAll('.write-bar')];
              if (!shown().includes(at)) expect(bars.length, 'no bar for a row that does not show').toBe(0);
              else if (bars.length) {
                expect(bars.length).toBe(1);
                expect(panelOf(bars[0])).toBe(at.split(':')[0]);
                const names = new Set(shownPicks().map(nameOf));
                if (names.size > 1) {
                  const label = bars[0].querySelector('.wname').textContent;
                  expect(label.split(', ').reduce((n, part) => n + parseInt(part, 10), 0), label).toBe(names.size);
                  seen.bar++;
                }
              }
            }
          },
        ),
          // Each case counted below, whatever the seed: the rows run settings (0 to 3), favorites (4, 5), reports (6, 7), the action buttons (8, 9) and other (10, 11).
          withExamples([
            [[{ t: 'click', at: 0 }, { t: 'shift', at: 11 }, { t: 'ctrl', at: 3 }, { t: 'ctrl', at: 3 }, { t: 'right', at: 5 }, { t: 'click', at: 6 }, { t: 'shift', at: 10 }, { t: 'right', at: 0 }, { t: 'ctrl', at: 8 }, { t: 'right', at: 9 }]],
            [[{ t: 'ctrl', at: 4 }, { t: 'ctrl', at: 2 }, { t: 'ctrl', at: 3 }, { t: 'hide' }, { t: 'right', at: 2 }, { t: 'show' }, { t: 'right', at: 2 }]],
            [[{ t: 'click', at: 4 }, { t: 'star', at: 4 }, { t: 'click', at: 6 }, { t: 'keyfold', panel: 'reports' }, { t: 'shift', at: 8 }]],
          ]),
      );
      for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
    } finally {
      page.dispose();
    }
  }, 600_000);

  it('menu offers Add to and Remove from Favorites for the picked rows it changes, the star on a picked row sets every picked row, and either is one step undo takes back', async () => {
    const page = await mount();
    const seen = { both: 0, menu: 0, starPicked: 0, starOther: 0 };
    const favorites = page.history.favorites;
    try {
      await fc.assert(
        fc.asyncProperty(fc.array(fc.nat(), { minLength: 1, maxLength: 5 }), fc.subarray(RPCS.map((r) => r.name)), fc.constantFrom('menu', 'menu', 'starPicked', 'starOther'), fc.nat(), async (at, pinned, how, k) => {
          await closeMenu();
          page.history.group('Seed', () => favorites.set(pinned));
          flushSync();
          await frames();
          const order = shown();
          const keys = [...new Set(at.map((i) => order[i % order.length]))];
          page.ws.selection.pickRows(keys);
          flushSync();
          const names = [...new Set(keys.map(nameOf))];
          const before = [...favorites.names].sort();
          // The step undo would take back, as History keeps only the newest ones.
          const last = page.history.undos.at(-1);
          let want;
          if (how === 'menu') {
            // The menu opens on a picked row of a table, as an action button has none.
            const tables = keys.filter(isRow);
            if (!tables.length) return;
            await rightClick(target(tables[k % tables.length]));
            expect([...page.ws.selection.rows].sort(), 'a right-click on a picked row keeps the pick').toEqual([...keys].sort());
            const toPin = names.filter((n) => !pinned.includes(n)), toUnpin = names.filter((n) => pinned.includes(n));
            const count = (list) => ((toPin.length && toUnpin.length) || list.length > 1 ? `${list.length} ` : '');
            // An item's label, less its value (the Favorites' count).
            const label = (m) => [...m.childNodes].filter((n) => !n.classList?.contains('menu-value')).map((n) => n.textContent).join('').trim();
            const items = menuItems().map(label).filter((t) => /^(Add|Remove) .*Favorites/.test(t));
            expect(items).toEqual([...(toPin.length ? [`Add ${count(toPin)}to Favorites`] : []), ...(toUnpin.length ? [`Remove ${count(toUnpin)}from Favorites`] : [])]);
            if (toPin.length && toUnpin.length) seen.both++;
            const pin = k % 2 === 0 ? toPin.length > 0 : !toUnpin.length;
            await clickOn(menuItems().find((m) => m.textContent.trim().startsWith(pin ? 'Add' : 'Remove')));
            want = pin ? [...new Set([...pinned, ...names])] : pinned.filter((n) => !names.includes(n));
            seen.menu++;
          } else {
            const picked = how === 'starPicked';
            const choices = (picked ? keys : order.filter((key) => !keys.includes(key))).filter(isRow);
            if (!choices.length) return;
            const key = choices[k % choices.length];
            const on = !pinned.includes(nameOf(key));
            await clickOn(rowEl(key).querySelector('button.star'));
            const set = picked ? names : [nameOf(key)];
            want = on ? [...new Set([...pinned, ...set])] : pinned.filter((n) => !set.includes(n));
            seen[how]++;
          }
          flushSync();
          await closeMenu();
          expect([...favorites.names].sort()).toEqual(want.sort());
          // A star on a row not picked leaves the pick, but for a picked row it took out of Favorites, which no longer shows.
          if (how === 'starOther') expect([...page.ws.selection.rows].sort(), 'a star on a row not picked leaves the pick').toEqual(keys.filter((k) => shown().includes(k)).sort());
          if (want.sort().join() !== before.join()) {
            expect(page.history.undos.at(-1)).not.toBe(last);
            page.history.undo();
            flushSync();
            expect([...favorites.names].sort()).toEqual(before);
          } else expect(page.history.undos.at(-1)).toBe(last);
        }),
        // Each case counted below, whatever the seed.
        withExamples([[[0, 1], ['gain'], 'menu', 0], [[0, 1], [], 'starPicked', 0], [[0], [], 'starOther', 1]]),
      );
      for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
    } finally {
      page.dispose();
    }
  }, 600_000);
});

describe('a key pressed in a table', () => {
  const TABLE = ['ArrowDown', 'ArrowUp', 'Home', 'End', 'Control+a'];
  const PAGE = [' ', '1', '2', ']', '?', 'Escape', 'Control+z'];
  const PLOT = ['l', 'y', 'k', 'f', 'Delete', '|', '_', 'a'];
  it('acts on its rows when it is the table’s, as anywhere when it is the page’s, and not at all when it is a plot’s', async () => {
    const seen = { table: 0, page: 0, plot: 0 };
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(...TABLE, ...PAGE, ...PLOT), fc.nat(), async (key, at) => {
        const page = await mount(1);
        try {
          page.history.group('Pin', () => page.history.favorites.pin(['offset'], true));
          flushSync();
          await frames();
          const ids = plotIds(page.ws.layout);
          const order = shown();
          const rowsShown = order.filter(isRow);
          const start = rowsShown[at % rowsShown.length];
          const panel = start.split(':')[0];
          const rows = order.filter((k) => k.startsWith(`${panel}:`));
          await click(rowEl(start).querySelector('td:last-child'));
          flushSync();
          rowEl(start).closest('[role=grid]').focus();
          const tab = page.ws.activeId, tabs = page.ws.tabs, steps = page.history.undos.length;
          const live = () => [...document.querySelectorAll('button')].find((b) => /^(Live|Follow live)$/.test(b.textContent.trim())).textContent.trim();
          const wasLive = live();
          await commands.key(key);
          flushSync();
          await frames();
          const picks = [...page.ws.selection.rows];
          if (TABLE.includes(key)) {
            seen.table++;
            const i = rows.indexOf(start);
            const to = { ArrowDown: Math.min(i + 1, rows.length - 1), ArrowUp: Math.max(i - 1, 0), Home: 0, End: rows.length - 1 }[key];
            expect(picks.sort()).toEqual(key === 'Control+a' ? [...rows].sort() : [rows[to]]);
          } else if (PAGE.includes(key)) {
            seen.page++;
            if (key === ' ') expect(live()).not.toBe(wasLive);
            if (key === '1' || key === '2') expect([...page.ws.selected]).toEqual([ids[+key - 1]]);
            if (key === '1' || key === '2' || key === 'Escape') expect(picks).toEqual([]);
            if (key === ']') expect(page.ws.activeId).not.toBe(tab);
            if (key === '?') expect(document.querySelector('[role=dialog]')).toBeTruthy();
            if (key === 'Control+z') expect([...page.history.favorites.names]).toEqual([]);
          } else {
            seen.plot++;
            expect(page.ws.tabs, key).toBe(tabs);
            expect(page.ws.maximized).toBe(null);
            expect(page.history.undos.length).toBe(steps);
            expect(picks).toEqual([start]);
            expect(document.querySelector('[role=dialog], [role=menu]')).toBe(null);
          }
        } finally {
          page.dispose();
        }
      }),
      withExamples([...TABLE, ...PAGE, ...PLOT].map((k) => [k, 3])),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 600_000);
});

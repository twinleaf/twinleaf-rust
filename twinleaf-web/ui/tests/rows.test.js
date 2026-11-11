// Property rows on the whole page, as the user's verdicts have them (round
// 1, #8 to #11, #13 and #27; round 2, Selection #13 and #15). A click picks
// a row alone, Ctrl drops or adds one, Shift picks the run from the row
// clicked last in page order, across panels and docks and past folded
// panels and past the Signals list, whose signals are rows of another kind
// that no pick of property rows takes in, and a right-click on a row not
// picked picks it alone. Add to and
// Remove from Favorites, from the menu or a picked row's star, act on every picked row as
// one undo step, and so does dragging a row or an action onto Favorites,
// which carries the picked rows with it when it is one of them; a drop
// anywhere else, or Escape, changes nothing, and a panel's header still
// drags the panel. In a table, the table's keys act on its rows, the page's
// keys act as anywhere, and the plots' keys do nothing. Every press is the
// real mouse's.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { commands } from 'vitest/browser';
import { centre, clickOn, dropped, frames, glide, mountPage, pressKey, settle, until, wait, withExamples } from './harness/page.js';
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
/** Panels of rows in both docks, one folded between two open ones, and the Signals list open after the left dock's rows, so a run from them to the right dock's passes it. */
const SIGNALS = ['vector.x', 'vector.y'];
const DOCKS = {
  left: [{ id: 'devices', open: true }, { id: 'settings', open: true }, { id: 'signals', open: true }],
  right: [{ id: 'favorites', open: true }, { id: 'reports', open: true }, { id: 'diagnostics', open: false }, { id: 'actions', open: true }, { id: 'other', open: true }],
  hidden: { left: false, right: false },
};
const mount = (more = 0) => mountPage(KEYS, 'columns', more, SIGNALS, RPCS, { saved: { [DOCKS_KEY]: DOCKS } });

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
/** Right-clicks `el` with the real mouse, and waits for the row menu that opens, or with `opens` false, as on what has none, a few frames in which none does. */
async function rightClick(el, opens = true) {
  await clickOn(el, { right: true });
  flushSync();
  if (opens) expect(await until(() => document.querySelector('[role=menu]')), 'the row menu open').toBe(true);
  else {
    await frames(4);
    expect(document.querySelector('[role=menu]'), 'no menu').toBe(null);
  }
}
/** Closes the menu, if one is open, with Escape, and waits until none is left on the page, one that was closing too. */
async function closeMenu() {
  if (document.querySelector('[role=menu][data-state=open]')) await pressKey('Escape');
  expect(await until(() => !document.querySelector('[role=menu]')), 'the menu gone').toBe(true);
}
const menuItems = () => [...document.querySelectorAll('[role=menu] [role=menuitem]')];

describe('a property row', () => {
  it('is picked by a click, Ctrl-click, Shift-click or right-click as a sheet picks, across panels and docks in page order, past folded panels, and only while it shows', async () => {
    const folds = ['settings', 'reports', 'other', 'diagnostics', 'favorites'];
    const seen = { across: 0, docks: 0, pastFolded: 0, dropped: 0, rightUnpicked: 0, rightPicked: 0, rightButton: 0, buttons: 0, hidden: 0, starredAway: 0, anchorHidden: 0, copy: 0, bar: 0 };
    const hidden = () => !!document.querySelector('.dock-side.right.rail');
    /** Clicks the dock's button `el` with Shift held, which keeps the selection, and waits for the dock to slide as the button said it would. */
    async function shiftPress(el) {
      const hides = el.getAttribute('aria-label').startsWith('Hide');
      await clickOn(el, { shift: true });
      expect(await until(() => hidden() === hides), hides ? 'the dock hidden' : 'the dock shown').toBe(true);
      await frames(2);
    }
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
          // A page of its own for each case: the page keeps the row picked last while it does not show,
          // which nothing on it tells, so a case that began on the page another left would not replay alone.
          const page = await mount(), favorites = page.history.favorites;
          try {
            page.history.group('Seed', () => favorites.set(['gain', 'temp']));
            flushSync();
            await frames();
            // The model: the rows picked, in the order picked, whether they show or not; the row a Shift pick runs from; the row picked last.
            let picks = [], anchor = null, at = null;
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
                await pressKey('Shift');
                panelHead(a.panel).focus();
                await pressKey(' ');
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
                  await rightClick(cell, !button);
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
              // The signals on the page are no part of a pick of property rows, a run past them too.
              expect(document.querySelectorAll('.palette .chip').length).toBe(SIGNALS.length);
              expect([page.ws.selection.signals.size, document.querySelectorAll('.palette .chip[aria-selected=true]').length]).toEqual([0, 0]);
              // The page keeps the row picked last, and says it while it shows.
              expect(page.ws.selection.rowAt, 'the row picked last, while it shows').toBe(shown().includes(at) ? at : null);
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
          } finally {
            page.dispose();
          }
        },
      ),
        // Each case counted below, whatever the seed: the rows run settings (0 to 3), favorites (4, 5), reports (6, 7), the action buttons (8, 9) and other (10, 11).
        withExamples([
          [[{ t: 'click', at: 0 }, { t: 'shift', at: 11 }, { t: 'ctrl', at: 3 }, { t: 'ctrl', at: 3 }, { t: 'right', at: 5 }, { t: 'click', at: 6 }, { t: 'shift', at: 10 }, { t: 'right', at: 0 }, { t: 'ctrl', at: 8 }, { t: 'right', at: 9 }]],
          [[{ t: 'ctrl', at: 4 }, { t: 'ctrl', at: 2 }, { t: 'ctrl', at: 3 }, { t: 'hide' }, { t: 'right', at: 2 }, { t: 'show' }, { t: 'right', at: 2 }]],
          [[{ t: 'click', at: 4 }, { t: 'star', at: 4 }, { t: 'click', at: 6 }, { t: 'keyfold', panel: 'reports' }, { t: 'shift', at: 8 }]],
          // The row picked last, in Favorites, starred away and its pick let go by a press on a header, then starred
          // again from Settings: it shows again as the row picked last, the edit bar under it, until its dock hides.
          [[{ t: 'click', at: 4 }, { t: 'star', at: 4 }, { t: 'fold', panel: 'other' }, { t: 'star', at: 0 }, { t: 'hide' }]],
        ]),
    );
    // A replay of one failure runs that case alone.
    if (fc.readConfigureGlobal().path !== undefined) return;
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
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

describe('a row or an action dragged', () => {
  /** The Favorites panel, on `side`, open or folded, after one panel there or first, among panels of rows in both docks. */
  const docksWith = (side, open, first) => {
    const fav = { id: 'favorites', open };
    const left = [{ id: 'devices', open: false }, { id: 'settings', open: true }, { id: 'actions', open: true }];
    const right = [{ id: 'reports', open: true }, { id: 'diagnostics', open: false }, { id: 'other', open: true }];
    const at = (list) => (first ? [fav, ...list] : [list[0], fav, ...list.slice(1)]);
    return { left: side === 'left' ? at(left) : left, right: side === 'right' ? at(right) : right, hidden: { left: false, right: false } };
  };
  const favoritesPanel = () => document.querySelector('.dock-panel[data-takes=rows]');
  /** The panels as the docks show them, in order, each id and whether it is open. */
  const arrangement = () => [...document.querySelectorAll('.dock-side')].map((d) => [...d.querySelectorAll('.dock-panel')].map((s) => `${s.getAttribute('aria-label')}:${s.querySelector('.dock-head').getAttribute('aria-expanded')}`).join(','));
  /** A spot on the Favorites panel, at fractions `fx`, `fy` of what shows of it: its header alone while it is folded. */
  function onFavorites(fx, fy) {
    const panel = favoritesPanel();
    panel.scrollIntoView({ block: 'nearest' });
    const open = panel.querySelector('.dock-head').getAttribute('aria-expanded') === 'true';
    const r = (open ? panel : panel.querySelector('.dock-head')).getBoundingClientRect(), frame = panel.closest('.dock-frame').getBoundingClientRect();
    const top = Math.max(r.top, frame.top) + 2, bottom = Math.min(r.bottom, frame.bottom) - 2;
    return [r.left + 2 + fx * (r.width - 4), top + fy * (bottom - top)];
  }
  /** Spots where a drop does nothing: a plot, the status bar, another panel's header, or back on the row carried. */
  const elsewhere = (from) => ({
    plot: () => centre(document.querySelector('.tiles .leaf .plot-body')),
    status: () => centre(document.querySelector('.status-bar')),
    header: () => centre([...document.querySelectorAll('.dock-panel:not([data-takes]) .dock-head')].at(-1)),
    self: () => from,
  });

  it('stars what it carries, the picked rows with it when it is one of them, when let go on Favorites, open or folded, in either dock, as the star does, as one step; anywhere else, or after Escape, nothing changes', async () => {
    const seen = { favorites: 0, folded: 0, left: 0, right: 0, several: 0, action: 0, fromFavorites: 0, equalsStar: 0, cancel: 0, plot: 0, status: 0, header: 0, self: 0, nothing: 0 };
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          side: fc.constantFrom('left', 'right'),
          open: fc.boolean(),
          first: fc.boolean(),
          pinned: fc.subarray(RPCS.map((r) => r.name)),
          picks: fc.array(fc.nat(), { maxLength: 4 }),
          from: fc.nat(),
          end: fc.constantFrom('favorites', 'favorites', 'cancel', 'plot', 'status', 'header', 'self'),
          fx: fc.double({ min: 0, max: 1, noNaN: true }),
          fy: fc.double({ min: 0, max: 1, noNaN: true }),
        }),
        async (c) => {
          const page = await mountPage(KEYS, 'columns', 0, SIGNALS, RPCS, { saved: { [DOCKS_KEY]: docksWith(c.side, c.open, c.first) } });
          try {
            const favorites = page.history.favorites;
            page.history.group('Seed', () => favorites.set(c.pinned));
            flushSync();
            await frames();
            const order = shown();
            const keys = [...new Set(c.picks.map((i) => order[i % order.length]))];
            page.ws.selection.pickRows(keys);
            flushSync();
            await frames();
            const key = order[c.from % order.length];
            const el = rowEl(key);
            el.scrollIntoView({ block: 'nearest' });
            await frames();
            const from = centre(el.matches('tr') ? el.querySelector('td:last-child') : el);
            // What it carries: every picked row when it is one, each name once, else it alone.
            const carried = keys.includes(key) ? [...new Set(keys.map(nameOf))] : [nameOf(key)];
            const before = [...favorites.names].sort(), last = page.history.undos.at(-1), calls = page.hub.calls.length, docksBefore = arrangement();
            const to = c.end === 'favorites' || c.end === 'cancel' ? onFavorites(c.fx, c.fy) : elsewhere(from)[c.end]();
            // Whether that spot is on Favorites: a row of Favorites let go on itself is.
            const over = !!document.elementFromPoint(...to)?.closest('.dock-panel')?.matches('[data-takes=rows]');
            await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8], ...glide([from[0] + 8, from[1] + 8], to, 5)]);
            flushSync();
            // Mid-drag: Favorites shows it takes the rows, the more while the pointer is on it, and no panel is being dragged.
            expect(favoritesPanel().classList.contains('awaiting'), 'Favorites shows it takes rows').toBe(true);
            if (c.end === 'favorites' || c.end === 'cancel') expect(over, 'the spot is on Favorites').toBe(true);
            expect(favoritesPanel().classList.contains('drop-target'), `Favorites marked under the pointer exactly when it is, ${c.end}`).toBe(over);
            expect(document.getElementById('dnd-action-dragged-el'), 'no panel drag').toBe(null);
            if (c.end === 'cancel') await pressKey('Escape');
            await commands.mouse([['up']]);
            flushSync();
            await frames();
            expect(favoritesPanel().classList.contains('awaiting'), 'the look goes with the drag').toBe(false);
            expect(arrangement(), 'the docks as they were').toEqual(docksBefore);
            expect(page.hub.calls.length, 'no action ran').toBe(calls);
            // The picks as they were: the drop is no click.
            expect([...page.ws.selection.rows].sort(), 'the picks as they were').toEqual(keys.filter((k) => shown().includes(k)).sort());
            const lands = over && c.end !== 'cancel';
            const want = lands ? [...new Set([...before, ...carried])].sort() : before;
            expect([...favorites.names].sort(), `${c.end}: the stars`).toEqual(want);
            if (want.join() === before.join()) {
              expect(page.history.undos.at(-1), 'no step').toBe(last);
              seen.nothing++;
            } else {
              // One step, named as the star's and the menu's, which undo takes back.
              const added = want.length - before.length;
              expect(page.history.undoLabel).toBe(`Add ${added > 1 ? `${added} ` : ''}to Favorites`);
              page.history.undo();
              flushSync();
              await frames();
              expect([...favorites.names].sort(), 'undo takes it back').toEqual(before);
              // The star of a row not starred, clicked instead, makes the same stars and the same step.
              const star = rowEl(key)?.querySelector('button.star');
              if (star && !before.includes(nameOf(key))) {
                page.ws.selection.pickRows(keys);
                flushSync();
                await clickOn(star);
                flushSync();
                expect([...favorites.names].sort(), 'as the star does').toEqual(want);
                expect(page.history.undoLabel).toBe(`Add ${added > 1 ? `${added} ` : ''}to Favorites`);
                seen.equalsStar++;
              }
            }
            seen[c.end]++;
            if (lands) (seen[c.side]++, c.open || seen.folded++);
            if (carried.length > 1) seen.several++;
            if (!el.matches('tr')) seen.action++;
            if (key.startsWith('favorites:')) seen.fromFavorites++;
          } finally {
            await commands.mouse([['up']]);
            page.dispose();
          }
        },
      ),
      withExamples([
        // One row onto open Favorites on the right, below a panel.
        [{ side: 'right', open: true, first: false, pinned: [], picks: [], from: 1, end: 'favorites', fx: 0.5, fy: 0.5 }],
        // Several picked rows, one of them dragged, onto folded Favorites on the left.
        [{ side: 'left', open: false, first: true, pinned: ['gain'], picks: [1, 2, 6], from: 2, end: 'favorites', fx: 0.3, fy: 0.5 }],
        // An action onto Favorites, then each place a drop does nothing.
        [{ side: 'left', open: true, first: false, pinned: [], picks: [], from: 3, end: 'favorites', fx: 0.9, fy: 0.9 }],
        [{ side: 'right', open: true, first: true, pinned: ['temp'], picks: [0, 1], from: 0, end: 'cancel', fx: 0.5, fy: 0.2 }],
        [{ side: 'right', open: false, first: false, pinned: [], picks: [], from: 4, end: 'plot', fx: 0, fy: 0 }],
        [{ side: 'left', open: true, first: true, pinned: ['gain', 'reboot'], picks: [], from: 0, end: 'status', fx: 0, fy: 0 }],
        [{ side: 'left', open: true, first: false, pinned: ['gain'], picks: [], from: 0, end: 'header', fx: 0, fy: 0 }],
        [{ side: 'right', open: true, first: false, pinned: [], picks: [3, 4], from: 3, end: 'self', fx: 0, fy: 0 }],
      ]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 600_000);

  it('leaves a panel’s header to drag the panel, after a row was carried to Favorites', async () => {
    const page = await mountPage(KEYS, 'columns', 0, SIGNALS, RPCS, { saved: { [DOCKS_KEY]: docksWith('right', true, true) } });
    try {
      const row = centre(rowEl('settings:gain').querySelector('td:last-child')), fav = onFavorites(0.5, 0.5);
      await commands.mouse([['move', ...row], ['down'], ['move', row[0] + 8, row[1] + 8], ...glide([row[0] + 8, row[1] + 8], fav, 5), ['up']]);
      flushSync();
      expect([...page.history.favorites.names]).toEqual(['gain']);
      // Favorites' header, picked up, carried to the left dock and let go there.
      const a = centre(favoritesPanel().querySelector('.dock-title'));
      await commands.mouse([['move', ...a], ['down'], ['move', a[0] + 6, a[1] + 6], ['wait', 60], ['move', a[0] + 12, a[1] + 12]]);
      await wait(500);
      const r = document.querySelector('.dock-side.left .dock').getBoundingClientRect(), b = [r.left + r.width / 2, r.top + 40];
      await commands.mouse([...glide([a[0] + 12, a[1] + 12], b, 5), ['wait', 120], ['move', b[0] + 2, b[1] + 3], ['wait', 120], ['up']]);
      await dropped();
      await settle(4);
      expect(favoritesPanel().closest('.dock-side').classList.contains('left'), 'Favorites moved to the left dock').toBe(true);
      expect([...page.history.favorites.names], 'a panel drag stars nothing').toEqual(['gain']);
    } finally {
      await commands.mouse([['up']]);
      page.dispose();
    }
  });
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
          await pressKey(key);
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

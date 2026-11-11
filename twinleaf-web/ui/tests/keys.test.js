// Who gets a key, on the whole page, with the real keys and mouse: the key
// router's order (keyrouter.js ORDER), from every place on the page a key
// can be pressed, found on the page.
// - Escape belongs first to a drag or a resize under way, which it cancels;
//   then to an open menu, popover or dialog, which closes, or to a field
//   something is typed in, which keeps it to put back what it showed, focus
//   kept; then it leaves a maximized view, the selection kept;
//   then it deselects; then it goes out a level. That is its order wherever
//   focus is: a list in the page, a plot's signals or the Signals list,
//   keeps the other keys pressed in it and not Escape, and no part of the
//   page takes Escape before the maximized view does but a drag and what is
//   open over it. It never changes a cursor, a layout or the undo history,
//   and a tooltip takes it from none of them (the user's verdicts, round 2
//   Selection #17 and #18, and the follow-up of 2026-11-09).
// - What is typed in a field is the field's: no claimant takes it.
// - A Space the page takes, to pause, presses nothing as it is let go,
//   whatever button a click left focus on; a Space on a button the keyboard
//   moved focus to is the button's, and presses it once.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { OPEN, centre, clickOn, closeAll, editCursors, emptySpots, feed, focusPage, frames, headerSpot, leafOf, mountPage, plotMenu, pressKey, quietApi, settle, until, withExamples, withYPair } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { cursorOf, out as outOf, page as tree } from '../src/lib/focus.js';
import { ORDER, lastKey } from '../src/lib/keyrouter.js';
import { actionFor } from '../src/lib/keymap.js';

const KEYS = [['sine.sine', 'sine.cosine'], ['aux.triangle'], ['aux.sawtooth']];
const SIGNALS = ['vector.x', 'vector.y'];
const RPCS = [
  { name: 'gain', channel: 'setting' },
  { name: 'enable', channel: 'setting', type: 'bool' },
  { name: 'temp', channel: 'report', access: ['read'] },
  { name: 'reboot', channel: 'action', type: null, access: [] },
];

beforeEach(() => quietApi());
afterEach(() => vi.restoreAllMocks());

/** What can take focus, by the keys or a press. */
const FOCUSABLE = 'button, input, select, textarea, a[href], [tabindex]';
/** An open menu, popover, list or dialog, whose keys are its own. */
const POPUP = '[role=menu], [role=dialog], [role=alertdialog], [data-slot$=-content]';
/** What keeps the keys typed into it, Escape among them. */
const FIELD = 'input, textarea, select, [contenteditable]';

/** Every place on the page a key can be pressed now: each element that can take focus and shows. */
function places() {
  return [...document.querySelectorAll(FOCUSABLE)].filter((el) => !el.disabled && !el.closest('[inert], [aria-hidden=true], [data-sonner-toaster]') && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden');
}
/** A short name for place `el`, for a failure to say where the key was pressed. */
const named = (el) => `${el.tagName.toLowerCase()}${el.className && typeof el.className === 'string' ? `.${el.className.trim().split(/\s+/)[0]}` : ''}[${el.getAttribute('aria-label') ?? el.getAttribute('role') ?? el.textContent.trim().slice(0, 20)}]`;
/** What the page holds that Escape may never change: each plot's cursors, the layout, the tabs and the undo history's length. */
const kept = (page) => JSON.stringify([Object.values(page.ws.plots).map((p) => p.cursors), page.ws.layout, page.ws.tabs.map((t) => t.id), page.history.undos.length]);
/** A drag under way, as the page draws it. */
const dragging = () => !!document.querySelector('.move-ghost, .marquee, .separator.active');

/**
 * Who Escape belongs to now, worked out here from what the page shows: the
 * id of its claimant in the router's order, or null for the element with
 * focus, or no one. Going out a level is 'navigation'.
 */
function owner(page, drag) {
  const el = document.activeElement;
  if (drag) return 'drag';
  if (el.closest(POPUP)) return null;
  const n = cursorOf(el), out = n && outOf(n) ? 'navigation' : null;
  if (el.closest(FIELD)) return out;
  if (page.ws.maximized) return 'restore';
  if (page.ws.selected.size || page.ws.selection.rows.size) return 'deselect';
  return out;
}

/** Starts drag `kind` with the real mouse and leaves the button down; returns whether one is under way. */
async function startDrag(page, kind) {
  let from = null;
  if (kind === 'move') from = headerSpot(leafOf(page.ws.shownIds()[0]));
  else if (kind === 'box') from = emptySpots()[0] ?? null;
  else if (kind === 'gap') {
    const gap = document.querySelector('.separator');
    from = gap && getComputedStyle(gap).visibility !== 'hidden' && gap.getClientRects().length ? centre(gap) : null;
  }
  if (!from) return false;
  await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 12, from[1] + 12], ['move', from[0] + 30, from[1] + 30]]);
  flushSync();
  await frames();
  return true;
}

describe('Escape', () => {
  it('belongs to a drag, then an open menu or a field, then the maximized view, then the selection, then the level above, from every place on the page', async () => {
    const seen = new Set(), where = new Set();
    await fc.assert(
      fc.asyncProperty(
        fc.option(fc.subarray([0, 1, 2], { minLength: 1 })),
        fc.constantFrom('none', 'plots', 'rows'),
        fc.nat(),
        fc.constantFrom('keys', 'press'),
        fc.constantFrom('none', 'none', 'menu', 'move', 'box', 'gap', 'tip'),
        async (max, picks, at, how, over) => {
          const page = await mountPage(KEYS, 'columns', 1, [...KEYS.flat(), ...SIGNALS], RPCS);
          let held = false;
          try {
            feed(page.hub);
            await focusPage();
            const ids = plotIds(page.ws.layout);
            editCursors(page, ids[0], (c) => withYPair(c, 'time', 'shared', -0.5, 0.5));
            page.ws.maximize(max && max.map((i) => ids[i]));
            if (picks === 'plots') page.ws.selection.only(page.ws.shownIds().slice(0, 2));
            await settle();
            if (picks === 'rows') await clickOn(document.querySelector('.dock-panel tr[id^=prop-] td:last-child'));
            await settle();
            // The place the key is pressed: one of every place found, reached by the keys or by a press.
            const found = places();
            expect(found.length).toBeGreaterThan(20);
            const el = found[at % found.length], name = named(el);
            if (how === 'keys') await pressKey('Shift');
            el.focus();
            await settle();
            // What is over the page: a menu, a drag under way, or a tooltip. A plot move shows a maximized view whole while it is held.
            const maximized = page.ws.maximized;
            if (over === 'menu') await plotMenu(page.ws.shownIds()[0]);
            else if (over === 'tip') {
              await commands.mouse([['move', ...centre(document.querySelector('.top-bar [data-slot=tooltip-trigger]'))]]);
              await until(() => document.querySelector('[data-slot=tooltip-content]'), 1500);
            } else if (over !== 'none') held = await startDrag(page, over);
            await settle();
            for (let press = 0; press < 5; press++) {
              const drag = held && dragging();
              const want = owner(page, drag), before = { max: page.ws.maximized, plots: [...page.ws.selected], rows: [...page.ws.selection.rows], kept: kept(page), focus: document.activeElement };
              const popup = document.activeElement.closest(POPUP);
              await pressKey('Escape');
              flushSync();
              await frames();
              const by = lastKey().by;
              const said = `Escape ${press + 1} on ${name}, ${how}, over ${over}`;
              expect(by, said).toBe(want);
              expect(kept(page), `${said}: cursors, layout, tabs and history`).toBe(before.kept);
              const now = { max: page.ws.maximized, plots: [...page.ws.selected], rows: [...page.ws.selection.rows] };
              if (want === 'drag') {
                expect(dragging(), `${said}: the drag`).toBe(false);
                // A plot move and a box deselect as they are put away; a resize does not.
                expect(now, said).toEqual({ max: maximized, plots: over === 'gap' ? before.plots : [], rows: over === 'gap' ? before.rows : [] });
              } else if (want === 'restore') expect(now, said).toEqual({ max: null, plots: before.plots, rows: before.rows });
              else if (want === 'deselect') expect(now, said).toEqual({ max: null, plots: [], rows: [] });
              else expect(now, said).toEqual({ max: before.max, plots: before.plots, rows: before.rows });
              if (want === 'navigation') expect(cursorOf(document.activeElement), `${said}: focus`).toBe(outOf(cursorOf(before.focus)));
              if (popup) await until(() => !popup.isConnected || popup.dataset.state === 'closed', 2000);
              seen.add(popup ? 'popup' : (want ?? 'no one'));
              where.add(name);
            }
          } finally {
            if (held) await commands.mouse([['up']]);
            page.dispose();
          }
        },
      ),
      withExamples([
        [null, 'plots', 0, 'keys', 'menu'],
        [[0], 'plots', 3, 'press', 'move'],
        [[0, 1], 'plots', 5, 'keys', 'box'],
        [null, 'rows', 8, 'keys', 'gap'],
        [[1], 'plots', 11, 'keys', 'tip'],
        [null, 'rows', 14, 'press', 'tip'],
        [[2], 'none', 17, 'keys', 'none'],
        [null, 'plots', 21, 'press', 'none'],
        // Focus in the edit bar's field, a property row picked, nothing typed: out a level.
        [null, 'rows', 2147483636, 'keys', 'none'],
      ]),
    );
    // Each owner in Escape's order was met, from several places.
    for (const who of ['drag', 'popup', 'restore', 'deselect', 'navigation']) expect([...seen], who).toContain(who);
    expect(where.size).toBeGreaterThan(5);
  }, 600_000);

  it('leaves the maximized view, then deselects, then goes out a level, with focus in a list of the page: a plot’s signals, the Signals list', async () => {
    const LISTS = { 'a plot’s signals': (id) => leafOf(id).querySelector('.trace-zone'), 'the Signals list': () => document.querySelector('.dock-panel .palette') };
    for (const [name, list] of Object.entries(LISTS)) {
      const page = await mountPage(KEYS, 'columns', 1, [...KEYS.flat(), ...SIGNALS], RPCS);
      try {
        feed(page.hub);
        await focusPage();
        const id = plotIds(page.ws.layout)[0], el = list(id);
        page.ws.selection.only([id]);
        page.ws.maximize([id]);
        await settle();
        el.focus();
        await settle();
        expect(document.activeElement, `${name} has focus`).toBe(el);
        const steps = [];
        for (let press = 0; press < 3; press++) {
          await pressKey('Escape');
          flushSync();
          await frames();
          steps.push({ by: lastKey().by, maximized: !!page.ws.maximized, selected: page.ws.selected.size, focus: document.activeElement === el ? 'the list' : document.activeElement === outOf(el) ? 'the level above' : named(document.activeElement) });
        }
        expect(steps, `three Escapes in ${name}, in a maximized view of a selected plot`).toEqual([
          { by: 'restore', maximized: false, selected: 1, focus: 'the list' },
          { by: 'deselect', maximized: false, selected: 0, focus: 'the list' },
          { by: 'navigation', maximized: false, selected: 0, focus: 'the level above' },
        ]);
      } finally {
        page.dispose();
      }
    }
  }, 60_000);

  it('in a field something is typed in puts back what the field showed, focus kept, and is the field’s; the next goes out a level: in every field on the page', async () => {
    /** The page with a property row picked, so the edit bar shows its field. */
    const mounted = async () => {
      const page = await mountPage(KEYS, 'columns', 1, [...KEYS.flat(), ...SIGNALS], RPCS);
      feed(page.hub);
      await focusPage();
      await clickOn(document.querySelector('.dock-panel tr[id^=prop-] td:last-child'));
      await settle();
      return page;
    };
    const fieldsOf = () => places().filter((el) => el.matches('input:not([type=checkbox]):not([type=radio]), textarea'));
    let page = await mounted();
    const names = fieldsOf().map(named);
    page.dispose();
    expect(names.some((n) => n.includes('New value of')), `the edit bar's field among ${names}`).toBe(true);
    const steps = {};
    for (const [i, name] of names.entries()) {
      page = await mounted();
      try {
        const field = fieldsOf()[i];
        field.focus();
        await settle();
        const was = field.value, parent = outOf(cursorOf(field));
        await pressKey('x');
        await settle();
        const typed = field.value !== was;
        const at = [];
        for (let press = 0; press < 2; press++) {
          await pressKey('Escape');
          flushSync();
          await frames();
          at.push({ by: lastKey().by, shows: field.value === was ? 'as it was' : field.value, focus: document.activeElement === field ? 'the field' : cursorOf(document.activeElement) === parent ? 'the level above' : named(document.activeElement) });
        }
        // Out of the edit bar's field is the table of the row it edits; out of any other, the node around it.
        const edited = parent?.matches('[role=grid]') && parent.querySelector(`#${parent.getAttribute('aria-activedescendant')}`)?.getAttribute('aria-selected') === 'true';
        steps[name] = { typed, at, out: edited ? 'the table, on the row edited' : parent === tree.parent(cursorOf(field)) ? 'the node around it' : named(parent) };
      } finally {
        page.dispose();
      }
    }
    const want = (n) => ({
      typed: true,
      at: [{ by: null, shows: 'as it was', focus: 'the field' }, { by: 'navigation', shows: 'as it was', focus: 'the level above' }],
      out: n.includes('New value of') ? 'the table, on the row edited' : 'the node around it',
    });
    expect(steps).toEqual(Object.fromEntries(names.map((n) => [n, want(n)])));
  }, 120_000);

  it('has its owners in that order in the router, and none before the element but a drag and an open menu', () => {
    const at = (id) => ORDER.findIndex((o) => o.id === id);
    const order = ['drag', 'menu', 'element', 'restore', 'deselect', 'navigation'];
    expect(order.map(at)).toEqual([...order.map(at)].sort((a, b) => a - b));
    expect(order.map(at).every((i) => i >= 0)).toBe(true);
    // A claimant with a place is offered Escape only where the keymap gives that place the key.
    const offered = ORDER.slice(0, at('element')).filter((o) => !o.place || actionFor(o.place, { key: 'Escape' }));
    expect(offered.map((o) => o.id)).toEqual(['drag', 'menu']);
  });
});

describe('a key typed in a field', () => {
  const TYPED = ['a', 'l', 'f', 'y', 'k', '1', ']', '?', ' ', 'Delete', 'Backspace', 'ArrowLeft', 'Home', 'Control+a', 'Control+z', 'Control+c'];
  it('is the field’s, in every field on the page: no claimant takes it, and nothing else changes', async () => {
    const page = await mountPage(KEYS, 'columns', 1, [...KEYS.flat(), ...SIGNALS], RPCS);
    try {
      feed(page.hub);
      await focusPage();
      page.ws.selection.only(plotIds(page.ws.layout).slice(0, 2));
      await settle();
      const fields = places().filter((el) => el.matches('input:not([type=checkbox]):not([type=radio]), textarea'));
      expect(fields.length).toBeGreaterThan(0);
      await fc.assert(
        fc.asyncProperty(fc.nat(fields.length - 1), fc.constantFrom(...TYPED), async (i, key) => {
          const field = fields[i];
          field.focus();
          await settle();
          const before = { kept: kept(page), plots: [...page.ws.selected], max: page.ws.maximized, tab: page.ws.activeId };
          await pressKey(key);
          await settle();
          expect(lastKey().by, `${key} in ${named(field)}`).toBe(null);
          expect({ kept: kept(page), plots: [...page.ws.selected], max: page.ws.maximized, tab: page.ws.activeId }, `${key} in ${named(field)}`).toEqual(before);
          expect(document.querySelector(`${OPEN}, [role=dialog], [role=alertdialog]`), `${key} in ${named(field)} opens nothing`).toBe(null);
        }),
        withExamples(fields.flatMap((f, i) => [[i, ' '], [i, 'Delete'], [i, 'Control+z']])),
      );
    } finally {
      page.dispose();
    }
  }, 600_000);
});

describe('Space on a button', () => {
  it('a click left focus on pauses and presses nothing as it is let go, on every button of the page; on one the keyboard moved focus to, presses it once', async () => {
    const seen = { pause: 0, button: 0 }, where = new Set();
    await fc.assert(
      fc.asyncProperty(fc.nat(), fc.constantFrom('press', 'keys'), fc.boolean(), async (at, how, picked) => {
        const page = await mountPage(KEYS, 'columns', 1, [...KEYS.flat(), ...SIGNALS], RPCS);
        const clicks = [], note = (e) => clicks.push(e.target);
        try {
          feed(page.hub);
          await focusPage();
          const ids = plotIds(page.ws.layout);
          if (picked) page.ws.selection.only(ids.slice(0, 2));
          await settle();
          // Every button of the page a key can be pressed on, as a click leaves focus on it; and each plot's Pause, as the keyboard does.
          const all = places().filter((el) => el.matches('button'));
          expect(all.length).toBeGreaterThan(10);
          const buttons = how === 'press' ? all : all.filter((el) => el.matches('.plot-head [data-command=pause]'));
          expect(buttons.length).toBeGreaterThan(0);
          const el = buttons[at % buttons.length], said = `Space on ${named(el)}, focused by ${how}`;
          if (how === 'keys') await pressKey('Shift');
          el.focus();
          await settle();
          const live = () => ids.map((id) => page.views.of(id).live);
          const before = live();
          window.addEventListener('click', note, true);
          await pressKey(' ');
          flushSync();
          await frames();
          const by = lastKey().by;
          if (how === 'keys') {
            expect(by, `${said}: the button keeps it`).toBe(null);
            expect(clicks, `${said}: pressed once`).toEqual([el]);
            const own = ids.indexOf(el.closest('.leaf').dataset.plot);
            expect(live()[own], `${said}: its plot paused`).toBe(!before[own]);
            seen.button++;
          } else if (by === 'pause') {
            expect(clicks, `${said}: the page took it, and letting it go presses nothing`).toEqual([]);
            expect(live(), `${said}: paused`).not.toEqual(before);
            seen.pause++;
            where.add(named(el));
          }
        } finally {
          window.removeEventListener('click', note, true);
          page.dispose();
        }
      }),
      withExamples([[0, 'press', false], [3, 'press', true], [7, 'keys', false], [12, 'press', false], [19, 'keys', true], [25, 'press', true], [31, 'press', false]]),
    );
    // The page took Space from buttons of several kinds, and a button kept it.
    expect(seen.pause).toBeGreaterThan(3);
    expect(where.size).toBeGreaterThan(3);
    expect(seen.button).toBeGreaterThan(0);
  }, 600_000);

  it('on a dropdown’s trigger, Space or Enter after a click is the page’s and opens nothing; after the keyboard moved focus to it, opens it and is not the page’s', async () => {
    const page = await mountPage(KEYS, 'columns', 1, [...KEYS.flat(), ...SIGNALS], RPCS);
    const triggers = () => places().filter((el) => el.matches('[aria-haspopup]'));
    const tried = new Set();
    try {
      feed(page.hub);
      await focusPage();
      await settle();
      const count = triggers().length;
      // Each plot's kind and layout, and the page's own.
      expect(count).toBeGreaterThanOrEqual(2 * KEYS.length);
      const ids = plotIds(page.ws.layout);
      const live = () => JSON.stringify(ids.map((id) => page.views.of(id).live));
      for (let i = 0; i < count; i++) {
        for (const how of ['press', 'keys']) {
          for (const key of [' ', 'Enter']) {
            await focusPage();
            await settle();
            const el = triggers()[i], said = `${key === ' ' ? 'Space' : key} on ${named(el)}, focused by ${how}`;
            tried.add(el);
            // A plot's trigger is in the plot selected, as a click on it selects it.
            const plot = el.closest('.leaf')?.dataset.plot;
            if (plot) page.ws.selection.only([plot]);
            if (how === 'keys') await pressKey('Shift');
            el.focus();
            await settle();
            const before = live(), steps = page.history.undos.length;
            await pressKey(key);
            await settle();
            const by = lastKey().by, opened = el.getAttribute('aria-expanded') === 'true';
            if (el.closest('[role=listbox]')) {
              // A list in the page, a plot's signals, keeps every key pressed in it but Escape, however focus came: its keys are its own.
              expect(lastKey().page ? by : null, `${said}: the page did nothing`).toBe(null);
              expect(live(), `${said}: nothing paused`).toBe(before);
            } else if (how === 'keys') {
              expect(opened, `${said}: opened`).toBe(true);
              expect(by, `${said}: the page did nothing`).toBe(null);
              expect(live(), `${said}: nothing paused`).toBe(before);
            } else {
              expect(opened, `${said}: opened nothing`).toBe(false);
              // Space pauses; Enter in a plot goes into its signals.
              if (key === ' ') expect([by, live() !== before], `${said}: paused`).toEqual(['pause', true]);
              else if (plot) expect(by, `${said}: into its signals`).toBe('plot');
            }
            expect(page.history.undos.length, `${said}: no edit`).toBe(steps);
            if (document.querySelector(OPEN)) await closeAll();
          }
        }
      }
      expect(tried.size).toBe(count);
    } finally {
      page.dispose();
    }
  }, 300_000);
});

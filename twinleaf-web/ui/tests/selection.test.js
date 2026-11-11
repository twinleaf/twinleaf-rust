// What keeps the plots selected and what clears them, on the whole page, as
// the user's verdicts list it (round 2, Selection #1, #2, #16, #17). A
// press on each control of every kind the list names, with any plots
// selected, clears the selection or keeps it as the list says; a press on a
// plot's own control selects that plot first; keyboard focus selects;
// Escape leaves a maximized view first, the selection kept, then deselects;
// and the plot keys act on no plot from a property table.
// Signals are picked as rows are, with the plots selected and never with
// property rows (round 2, late notes); a click adds nothing, and Enter, a
// double-click, the menu and a drag put the signals picked on the plots
// selected, Delete taking them off (plan, "Signals selected like rows"). A
// double-click on one of several picked puts them all on; the arrows move
// the list's cursor and pick nothing but with Shift, Space and Enter pick
// the signal under it; the menu's Colour and Copy name act on the signals
// picked; and an undo puts back the plots selected, not the signals picked.
import { describe, expect, it } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, clickOn, closeAll, drag, dropped, focusPage, frames, leafOf, mountPage, pickItem, plotMenu, pressKey, settle, until, wait, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { DOUBLE_CLICK_MS } from '../src/lib/selection.svelte.js';
import { colors } from '../src/lib/colors.svelte.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine']];

/** The controls round 2 #16 names, each a selector for its elements on the page and whether a press there keeps the plots selected. */
const LIST = [
  ['the Live button', () => [...document.querySelectorAll('button')].filter((b) => /^(Live|Paused|Pause)/.test(b.textContent.trim())), false],
  ['the Layout menu', () => [...document.querySelectorAll('button')].filter((b) => b.textContent.trim() === 'Layout'), false],
  ['display settings', () => [...document.querySelectorAll('button[aria-label="Display settings"]')], false],
  // The open tab among them: pressed, it changes nothing else (tabs.test.js).
  ['the tab strip', () => [...document.querySelectorAll('[role=tablist] [data-tab]')], false],
  ['dock headers', () => [...document.querySelectorAll('.dock-head')], false],
  ['the status bar', () => [...document.querySelectorAll('.status-bar')], false],
  ['the gaps between plots', () => [...document.querySelectorAll('.tiles .separator')], false],
  ['the plots: their bodies and headers', () => [...document.querySelectorAll('.tiles .leaf .plot-body, .tiles .leaf .plot-head')], true],
  ['the window field', () => [...document.querySelectorAll('.top-bar .window-field, .top-bar input[aria-label="Time window"]')], true],
  ['the eraser', () => [...document.querySelectorAll('button[aria-label="Clear cursors"]')], true],
  ['File', () => [...document.querySelectorAll('button')].filter((b) => b.textContent.trim() === 'File'), true],
  ['Commands', () => [...document.querySelectorAll('button.search')], true],
  ['the Signals panel: its search, its signals and its list', () => [...document.querySelectorAll('[data-keeps-selection] input[type=search], .palette .chip, .palette')], true],
];

/** A press and its release on `el`, as a click's are, so whatever it starts ends. */
function press(el, init = {}) {
  const at = { bubbles: true, cancelable: true, button: 0, pointerType: 'mouse', ...init };
  el.dispatchEvent(new PointerEvent('pointerdown', at));
  el.dispatchEvent(new PointerEvent('pointerup', at));
}

describe('a press', () => {
  it('on each control the list names keeps the plots selected or clears them, as it says', async () => {
    const page = await mountPage(KEYS, 'columns', 1, ['vector.x', 'vector.y']);
    try {
      for (const [name, find, keeps] of LIST) {
        const els = find();
        expect(els.length, `${name} on the page`).toBeGreaterThan(0);
        for (const el of els) {
          const ids = plotIds(page.ws.layout);
          page.ws.selected.clear();
          for (const id of ids.slice(0, 2)) page.ws.selected.add(id);
          // A plot pressed is selected alone unless it was in the selection; the rest of the list keeps it as it was.
          const inPlot = el.closest('.leaf')?.dataset.plot;
          press(el);
          flushSync();
          if (!keeps) expect(page.ws.selected.size, `${name}: clears`).toBe(0);
          else if (!inPlot || ids.slice(0, 2).includes(inPlot)) expect([...page.ws.selected], `${name}: keeps`).toEqual(ids.slice(0, 2));
        }
      }
    } finally {
      page.dispose();
    }
  });

  it('with Shift, or the wheel anywhere, never clears the selection', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b] = plotIds(page.ws.layout);
      for (const [, find] of LIST) {
        for (const el of find()) {
          page.ws.selected.clear();
          page.ws.selected.add(a);
          page.ws.selected.add(b);
          press(el, { shiftKey: true });
          el.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: 40 }));
          flushSync();
          expect(page.ws.selected.has(a) && page.ws.selected.has(b)).toBe(true);
        }
      }
    } finally {
      page.dispose();
    }
  });

  it('on a plot’s own control selects that plot first, or keeps the selection it is in', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      let checked = 0;
      fc.assert(
        fc.property(fc.subarray(ids), fc.nat(), fc.nat(), (picked, k, j) => {
          const id = ids[k % ids.length];
          const controls = [...document.querySelectorAll(`.leaf[data-plot="${id}"] button, .leaf[data-plot="${id}"] [role=combobox]`)];
          if (!controls.length) return;
          page.ws.selected.clear();
          for (const p of picked) page.ws.selected.add(p);
          press(controls[j % controls.length]);
          flushSync();
          expect([...page.ws.selected].sort()).toEqual(picked.includes(id) ? [...picked].sort() : [id]);
          expect(page.ws.focusedId).toBe(id);
          checked++;
        }),
      );
      expect(checked).toBeGreaterThan(0);
    } finally {
      page.dispose();
    }
  });
});

describe('Escape', () => {
  it('leaves a maximized view first, the selection kept, then deselects, then changes nothing, wherever focus is', async () => {
    await fc.assert(
      fc.asyncProperty(fc.option(fc.subarray([0, 1, 2], { minLength: 1 })), fc.subarray([0, 1, 2]), fc.option(fc.nat(2)), async (max, picks, focus) => {
        const page = await mountPage(KEYS, 'columns');
        try {
          // The page has the keys, however long the browser took to give them.
          await focusPage();
          const ids = plotIds(page.ws.layout);
          page.ws.maximize(max && max.map((i) => ids[i]));
          const shown = page.ws.shownIds();
          const picked = picks.map((i) => ids[i]).filter((id) => shown.includes(id));
          page.ws.selection.only(picked);
          const into = focus !== null && shown.includes(ids[focus]) ? ids[focus] : null;
          if (into) document.querySelector(`.leaf[data-plot="${into}"] .plot-card`).focus();
          else document.activeElement?.blur();
          flushSync();
          await frames();
          // What each Escape leaves: the layout restored, the selection kept; then nothing selected.
          const steps = [];
          if (max) steps.push({ maximized: null, selected: picked });
          if (picked.length) steps.push({ maximized: null, selected: [] });
          steps.push({ maximized: null, selected: [] });
          for (const want of steps) {
            await pressKey('Escape');
            flushSync();
            expect(page.ws.maximized).toEqual(want.maximized);
            expect([...page.ws.selected].sort()).toEqual([...want.selected].sort());
          }
          // Deselecting lets go of the plot clicked last too.
          if (picked.length) expect(page.ws.focusedId).toBe(null);
          expect(page.history.undos.length).toBe(0);
        } finally {
          page.dispose();
        }
      }),
      withExamples([[[0], [0, 1], null], [[0, 1], [1], 1], [null, [2], 2], [[1], [], null]]),
    );
  }, 600_000);
});

describe('keyboard focus', () => {
  it('on a plot selects it alone', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      flushSync();
      await pressKey('2');
      flushSync();
      await frames();
      expect([...page.ws.selected]).toEqual([b]);
    } finally {
      page.dispose();
    }
  });

  it('is not needed for a plot key to act on the selection', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      document.body.focus();
      await pressKey('Control+a');
      flushSync();
      expect(page.ws.selected.size).toBe(ids.length);
      expect(page.ws.focusedId).toBe(null);
      await pressKey('l');
      flushSync();
      for (const id of ids) expect(page.ws.plots[id].legend, id).toBe(true);
    } finally {
      page.dispose();
    }
  });
});

describe('signals in the Signals list', () => {
  /** The list's signals, as the page shows them, in its order. */
  const chips = () => [...document.querySelectorAll('.palette .chip[data-key]')];
  const chip = (key) => chips().find((c) => c.dataset.key === key);
  const marked = () => chips().filter((c) => c.getAttribute('aria-selected') === 'true').map((c) => c.dataset.key);
  const shows = (page, ids) => Object.fromEntries(ids.map((id) => [id, page.ws.plots[id]?.items.map((i) => i.key) ?? null]));
  /** The menu's row that starts with `label`, and what its value says. */
  const menuRow = (label) => [...document.querySelectorAll('[data-slot=context-menu-content][data-state=open] [role=menuitem]')].find((m) => m.textContent.trim().startsWith(label));
  const valueOf = (row) => row.querySelector('.menu-value')?.textContent.trim() ?? null;
  const counted = (n, one) => (n > 1 ? `${n} ${one}s` : null);

  /** How signals are picked, and what then puts them on plots or takes them off: its key, a double-click, its menu's row or a drag, made on signal `on`. */
  const pickStep = fc.record({ how: fc.constantFrom('click', 'click', 'ctrl', 'ctrl', 'shift'), at: fc.nat() });
  const ORIGINS = ['Enter', 'Shift+Enter', 'Delete', 'Backspace', 'double-click', 'menu', 'drag'];

  it('are picked by a click, Ctrl-click and Shift-click as rows are, which adds nothing; Enter, a double-click, the menu and a drag put the picked signals on the selected plots, Shift+Enter in a new one, and Delete takes them off, each as one step', async () => {
    const seen = Object.fromEntries([...ORIGINS, 'several', 'withPlots', 'menuRemove', 'menuAdd', 'dragSeveral', 'rowAlone', 'narrowed', 'doubleSeveral', 'keyPicks', 'undoKeeps', 'bothCounted'].map((k) => [k, 0]));
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2]), fc.array(pickStep, { minLength: 1, maxLength: 4 }), fc.constantFrom(...ORIGINS), fc.nat(), fc.nat(2), async (plotPicks, picksMade, origin, onAt, toAt) => {
        // Each plot shows one of the list's signals; two of the list's are on no plot.
        const page = await mountPage(KEYS, 'columns', 0, [...KEYS.flat(), 'vector.x', 'vector.y']);
        try {
          const ids = plotIds(page.ws.layout), selected = plotPicks.map((i) => ids[i]);
          await focusPage();
          page.ws.selection.only(selected);
          await settle();
          const order = chips().map((c) => c.dataset.key);
          expect([...order].sort()).toEqual([...KEYS.flat(), 'vector.x', 'vector.y'].sort());
          const tabs = page.ws.tabs;
          // The model, as a sheet picks rows: the signals picked in the order picked, the one a Shift pick runs from, the one clicked last.
          let picks = [], anchor = null, at = null;
          for (const p of picksMade) {
            const key = order[p.at % order.length];
            const narrows = p.how === 'click' && picks.length > 1 && picks.includes(key);
            await clickOn(chip(key), { ctrl: p.how === 'ctrl', shift: p.how === 'shift' });
            flushSync();
            at = key;
            if (narrows) {
              // A click on one of several signals picked narrows the pick to it once the double-click time has passed.
              expect([...page.ws.selection.signals], `${key} clicked among ${picks}`).toEqual(picks);
              await wait(DOUBLE_CLICK_MS + 10);
              seen.narrowed++;
            }
            if (p.how === 'shift' && anchor) {
              const [i, j] = [order.indexOf(anchor), order.indexOf(key)].sort((x, y) => x - y);
              picks = order.slice(i, j + 1);
            } else {
              if (p.how === 'ctrl') picks = picks.includes(key) ? picks.filter((k) => k !== key) : [...picks, key];
              else picks = [key];
              anchor = key;
            }
            expect([...page.ws.selection.signals], JSON.stringify(p)).toEqual(picks);
            expect(marked().sort(), 'the page marks the signals picked').toEqual([...picks].sort());
            // The plots stay selected, no property row is picked with a signal, and a click puts no signal anywhere.
            expect([...page.ws.selected].sort()).toEqual([...selected].sort());
            expect(page.ws.selection.rows.size).toBe(0);
            expect(page.ws.tabs, 'a click adds nothing').toBe(tabs);
          }
          if (picks.length > 1) seen.several++;
          if (picks.length && selected.length) seen.withPlots++;
          const on = order[onAt % order.length], to = ids[toAt];
          const before = shows(page, ids), steps = page.history.undos.length;
          // What it acts on: made on a row, the signals picked when the row is one of them, or else that signal alone.
          let made = on, acts = selected, takes = false;
          if (origin === 'double-click') {
            // On one of several signals picked, it keeps them picked and puts them all on the plots; on another, its clicks pick it alone.
            await commands.mouse([['dblclick', ...centre(chip(on).querySelector('.sig-key'))]]);
            if (!picks.includes(on)) picks = [on];
            else if (picks.length > 1) seen.doubleSeveral++;
            await wait(DOUBLE_CLICK_MS + 10);
            expect([...page.ws.selection.signals], 'a double-click leaves the signals picked').toEqual(picks);
          } else if (origin === 'menu') {
            await clickOn(chip(on), { right: true });
            expect(await until(() => menuRow('Colour'), 2000), 'the menu opens').toBe(true);
            // A right-click on a signal not picked picks it alone.
            if (!picks.includes(on)) picks = [on];
            expect([...page.ws.selection.signals]).toEqual(picks);
            const lacking = selected.filter((id) => picks.some((k) => !before[id].includes(k)));
            takes = selected.length > 0 && !lacking.length;
            const add = menuRow('Add to plot'), remove = menuRow('Remove from plot');
            expect([!!add, !!remove], 'one of Add to plot and Remove from plot').toEqual([!takes, takes]);
            if (!selected.length) {
              expect(valueOf(add)).toBe('No plot selected');
              expect(add.hasAttribute('data-disabled')).toBe(true);
            } else {
              // The row counts the signals it acts on and the plots it changes, each when more than one: "2 signals to 2 plots", "2 signals from 2 plots".
              const changed = takes ? selected : lacking;
              expect(valueOf(takes ? remove : add)).toBe([counted(picks.length, 'signal'), counted(changed.length, 'plot')].filter(Boolean).join(takes ? ' from ' : ' to ') || null);
              if (!takes && picks.length > 1 && changed.length > 1) seen.bothCounted++;
              // Remove from plot shows the key that does it.
              if (takes) expect([...remove.querySelectorAll('kbd')].map((k) => k.textContent)).toEqual(['Delete']);
              seen[takes ? 'menuRemove' : 'menuAdd']++;
            }
            if (selected.length) await clickOn(takes ? remove : add);
            else await pressKey('Escape');
          } else if (origin === 'drag') {
            const from = centre(chip(on).querySelector('.sig-key')), body = centre(leafOf(to).querySelector('.plot-body'));
            await drag(from, body, [], 60);
            await dropped();
            acts = [to];
            if (picks.includes(on) && picks.length > 1) seen.dragSeveral++;
          } else {
            // The list's keys are made on the signal clicked last, which they pick first unless it is picked.
            made = at;
            await pressKey(origin);
            takes = origin === 'Delete' || origin === 'Backspace';
            if (!picks.includes(made)) {
              picks = [made];
              seen.keyPicks++;
            }
            expect([...page.ws.selection.signals], `${origin} picks the signal it is made on`).toEqual(picks);
          }
          await settle();
          const signals = origin === 'double-click' ? picks : picks.includes(made) ? picks : [made];
          if (!picks.includes(made)) seen.rowAlone++;
          const after = shows(page, [...ids, ...plotIds(page.ws.layout).filter((id) => !ids.includes(id))]);
          const said = `${origin} on ${made} with ${picks} picked and plots ${plotPicks} selected`;
          if (origin === 'Shift+Enter') {
            // A new plot of the signals, in the order picked; the others as they were.
            const fresh = plotIds(page.ws.layout).filter((id) => !ids.includes(id));
            expect(fresh.length, said).toBe(1);
            expect(after[fresh[0]], said).toEqual(signals);
            for (const id of ids) expect(after[id], said).toEqual(before[id]);
          } else {
            expect(plotIds(page.ws.layout), said).toEqual(ids);
            for (const id of ids) {
              const want = !acts.includes(id) ? before[id] : takes ? before[id].filter((k) => !signals.includes(k)) : [...before[id], ...signals.filter((k) => !before[id].includes(k))];
              // A drag puts the signal held first, then those picked with it.
              if (origin === 'drag') expect([...after[id]].sort(), said).toEqual([...want].sort());
              else expect(after[id], said).toEqual(want);
            }
          }
          const changed = Object.keys(after).some((id) => JSON.stringify(after[id]) !== JSON.stringify(before[id] ?? null));
          expect(page.history.undos.length - steps, `${said}: one step where it changed a plot, none where not`).toBe(changed ? 1 : 0);
          if (changed) {
            // A signal picked after the step, which the undo leaves picked: it puts back the plots only.
            const other = chips().find((c) => !signals.includes(c.dataset.key));
            if (other) {
              await clickOn(other);
              await wait(DOUBLE_CLICK_MS + 10);
              seen.undoKeeps++;
            }
            const pickedNow = [...page.ws.selection.signals];
            page.history.undo();
            flushSync();
            expect(shows(page, ids), `${said}, undone`).toEqual(before);
            expect(plotIds(page.ws.layout)).toEqual(ids);
            expect([...page.ws.selection.signals], `${said}: the undo leaves the signals picked`).toEqual(pickedNow);
          }
          seen[origin]++;
        } finally {
          page.dispose();
        }
      }),
      // Each origin on two signals picked with two plots selected, made on one of them; a signal alone; Remove from the menu; a row not picked.
      withExamples([
        ...ORIGINS.map((origin) => [[0, 1], [{ how: 'click', at: 3 }, { how: 'ctrl', at: 4 }], origin, 4, 2]),
        [[0], [{ how: 'click', at: 0 }, { how: 'shift', at: 2 }, { how: 'ctrl', at: 1 }], 'Enter', 0, 0],
        [[0, 2], [{ how: 'click', at: 3 }], 'Enter', 3, 0],
        [[0, 2], [{ how: 'click', at: 3 }], 'menu', 3, 0],
        [[], [{ how: 'click', at: 3 }], 'menu', 3, 0],
        [[1], [{ how: 'click', at: 3 }, { how: 'ctrl', at: 4 }], 'menu', 0, 0],
        [[1], [{ how: 'click', at: 3 }, { how: 'ctrl', at: 4 }], 'drag', 0, 1],
        [[1], [{ how: 'click', at: 3 }, { how: 'ctrl', at: 3 }], 'Enter', 0, 1],
        // A click on one of two signals picked narrows to it, then a double-click on another.
        [[0, 1], [{ how: 'click', at: 3 }, { how: 'ctrl', at: 4 }, { how: 'click', at: 4 }], 'double-click', 2, 0],
      ]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 900_000);

  /** A key pressed in the list: the arrows, Home and End, with Shift or not, Space, with Shift or not, and Enter. */
  const listKey = fc.constantFrom('ArrowDown', 'ArrowDown', 'ArrowUp', 'Home', 'End', 'Shift+ArrowDown', 'Shift+ArrowUp', ' ', 'Shift+ ', 'Enter');

  it('move the list’s cursor by the arrows, which pick nothing but with Shift the run to it; Space picks the signal under the cursor, and Enter picks it unless it is picked and puts the signals picked on the plot selected', async () => {
    const seen = { moved: 0, apart: 0, run: 0, runFromCursor: 0, space: 0, enterPicks: 0, enterKeeps: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(listKey, { minLength: 1, maxLength: 10 }), async (keys) => {
        const page = await mountPage(KEYS, 'columns', 0, [...KEYS.flat(), 'vector.x', 'vector.y']);
        try {
          const ids = plotIds(page.ws.layout);
          await focusPage();
          page.ws.selection.only([ids[0]]);
          await settle();
          const order = chips().map((c) => c.dataset.key), list = document.querySelector('.palette');
          await pressKey('Shift');
          list.focus();
          // The model: the cursor's row, the signals picked in the order picked, and the signal a Shift pick runs from.
          let at = 0, picks = [], anchor = null;
          const run = (to) => {
            const [i, j] = [order.indexOf(anchor), to].sort((x, y) => x - y);
            picks = order.slice(i, j + 1);
          };
          for (const k of keys) {
            await pressKey(k);
            await settle();
            const shift = k.startsWith('Shift+'), base = shift ? k.slice('Shift+'.length) : k;
            const to = { ArrowDown: Math.min(order.length - 1, at + 1), ArrowUp: Math.max(0, at - 1), Home: 0, End: order.length - 1 }[base];
            if (to !== undefined) {
              if (shift) {
                if (!picks.length) ((picks = [order[at]]), (anchor = order[at]), seen.runFromCursor++);
                run(to);
                seen.run++;
              } else {
                seen.moved++;
                if (picks.length && !picks.includes(order[to])) seen.apart++;
              }
              at = to;
            } else if (base === ' ') {
              if (shift && anchor) run(at);
              else (picks = [order[at]]), (anchor = order[at]);
              seen.space++;
            } else {
              if (!picks.includes(order[at])) ((picks = [order[at]]), (anchor = order[at]), seen.enterPicks++);
              else seen.enterKeeps++;
              const shown = page.ws.plots[ids[0]].items.map((i) => i.key);
              expect(picks.filter((p) => !shown.includes(p)), `Enter puts ${picks} on the plot selected`).toEqual([]);
            }
            const said = `${k} in ${JSON.stringify(keys)}`;
            expect([...page.ws.selection.signals], `the signals picked after ${said}`).toEqual(picks);
            expect(marked().sort(), `the page marks the signals picked after ${said}`).toEqual([...picks].sort());
            expect(document.getElementById(list.getAttribute('aria-activedescendant'))?.dataset.key, `the cursor after ${said}`).toBe(order[at]);
          }
        } finally {
          page.dispose();
        }
      }),
      withExamples([
        // The arrows move off the signal picked and pick nothing; Space picks the one they are on; Shift+Down runs from it.
        [[' ', 'ArrowDown', 'ArrowDown', ' ', 'Shift+ArrowDown', 'Shift+ArrowUp']],
        // With none picked, Shift+Down runs from the cursor's row; Enter on a signal picked keeps the run, and on another picks it.
        [['ArrowDown', 'Shift+ArrowDown', 'Enter', 'End', 'Enter', 'Home', 'Shift+ ']],
      ]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 900_000);

  it('show the cursor apart from the signals picked: the focus ring around the cursor’s row, the picked look on those picked', async () => {
    const page = await mountPage(KEYS, 'columns', 0, [...KEYS.flat(), 'vector.x', 'vector.y']);
    try {
      await focusPage();
      await settle();
      const order = chips().map((c) => c.dataset.key);
      await clickOn(chip(order[0]));
      await clickOn(chip(order[1]), { ctrl: true });
      // From the signal clicked last, two rows down.
      for (let i = 0; i < 2; i++) await pressKey('ArrowDown');
      expect(await until(() => document.querySelector('.focus-ring')), 'a ring').toBe(true);
      await settle(6);
      const cursor = chip(order[3]), ring = [...document.querySelectorAll('.focus-ring')].map((r) => r.getBoundingClientRect());
      expect(marked().sort(), 'the arrows pick nothing').toEqual([order[0], order[1]].sort());
      expect(cursor.classList.contains('picked'), 'the cursor’s row has no picked look').toBe(false);
      const box = cursor.getBoundingClientRect();
      expect(ring.length, 'one ring').toBe(1);
      expect(Math.abs(ring[0].top - box.top) < 6 && Math.abs(ring[0].bottom - box.bottom) < 6, 'the ring is around the cursor’s row').toBe(true);
      // The picked look differs from a row with neither.
      const look = (el) => [getComputedStyle(el).backgroundColor, getComputedStyle(el).borderTopColor].join(' ');
      expect(look(chip(order[0])), 'the picked look').not.toBe(look(chip(order[4])));
      if (SHOTS) await browser.screenshot({ path: `${SHOTS}/signals-cursor.png` });
    } finally {
      page.dispose();
    }
  });

  it('the menu’s Colour and Copy name act on the signals picked when the menu’s signal is one of them, else on it alone, the names one a line', async () => {
    const seen = { several: 0, alone: 0, colour: 0, copy: 0 };
    const keys = [...KEYS.flat(), 'vector.x', 'vector.y'];
    await fc.assert(
      fc.asyncProperty(fc.array(fc.record({ ctrl: fc.boolean(), at: fc.nat() }), { minLength: 1, maxLength: 4 }), fc.nat(), fc.constantFrom('Colour', 'Copy name'), fc.nat(7), async (clicks, onAt, row, colour) => {
        const page = await mountPage(KEYS, 'columns', 0, keys);
        const copied = [];
        const write = navigator.clipboard.writeText;
        navigator.clipboard.writeText = (text) => (copied.push(text), Promise.resolve());
        try {
          await focusPage();
          await settle();
          const order = chips().map((c) => c.dataset.key);
          for (const c of clicks) {
            await clickOn(chip(order[c.at % order.length]), { ctrl: c.ctrl });
            await wait(DOUBLE_CLICK_MS + 10);
          }
          const picks = [...page.ws.selection.signals], on = order[onAt % order.length];
          const targets = picks.includes(on) ? picks : [on];
          seen[targets.length > 1 ? 'several' : 'alone']++;
          const was = Object.fromEntries(order.map((k) => [k, colors.of(k)]));
          await clickOn(chip(on), { right: true });
          expect(await until(() => menuRow(row), 2000), 'the menu opens').toBe(true);
          const said = `${row} on ${on} with ${picks} picked`;
          if (row === 'Colour') {
            await clickOn(menuRow('Colour'));
            const swatch = () => [...document.querySelectorAll('.swatch-choice')][colour];
            expect(await until(swatch, 2000), 'the colours show').toBe(true);
            await clickOn(swatch());
            await settle();
            for (const k of order) expect(colors.of(k), `${said}: ${k}`).toBe(targets.includes(k) ? colour : was[k]);
            seen.colour++;
          } else {
            await clickOn(menuRow('Copy name'));
            await settle();
            expect(copied, said).toEqual([targets.join('\n')]);
            seen.copy++;
          }
        } finally {
          navigator.clipboard.writeText = write;
          for (const k of keys) colors.pick(k, null);
          await closeAll();
          page.dispose();
        }
      }),
      withExamples([
        // Two signals picked, the menu on one of them, then on one not picked.
        [[{ ctrl: false, at: 0 }, { ctrl: true, at: 2 }], 2, 'Colour', 3],
        [[{ ctrl: false, at: 0 }, { ctrl: true, at: 2 }, { ctrl: true, at: 4 }], 0, 'Copy name', 0],
        [[{ ctrl: false, at: 0 }, { ctrl: true, at: 2 }], 1, 'Colour', 5],
        [[{ ctrl: false, at: 0 }, { ctrl: true, at: 2 }], 1, 'Copy name', 0],
      ]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 900_000);

  it('with plots selected, Delete on a signal none of them shows changes nothing, Ctrl+A picks every signal the search shows, and Escape deselects them with the plots', async () => {
    const page = await mountPage(KEYS, 'columns', 0, [...KEYS.flat(), 'vector.x', 'vector.y']);
    try {
      const ids = plotIds(page.ws.layout);
      await focusPage();
      page.ws.selection.only(ids.slice(0, 2));
      await settle();
      // A signal no selected plot shows: Delete has nothing to take off, and closes no plot.
      await clickOn(chip('vector.x'));
      await pressKey('Delete');
      await settle();
      expect(plotIds(page.ws.layout), 'no plot closed').toEqual(ids);
      expect(page.history.undos.length).toBe(0);
      await pressKey('Control+a');
      await settle();
      expect([...page.ws.selection.signals].sort()).toEqual(chips().map((c) => c.dataset.key).sort());
      expect([...page.ws.selected], 'the plots stay selected').toEqual(ids.slice(0, 2));
      // The search leaves signals out: those are picked no more, and Ctrl+A in the field is the field's.
      const search = document.querySelector('.dock-panel input[type=search]');
      search.focus();
      for (const ch of 'vector') await pressKey(ch);
      await settle();
      expect([...page.ws.selection.signals].sort()).toEqual(['vector.x', 'vector.y']);
      // Escape clears the search first, then deselects the signals with the plots.
      await pressKey('Escape');
      await settle();
      expect(search.value).toBe('');
      document.querySelector('.palette').focus();
      await pressKey('Escape');
      await settle();
      expect(page.ws.selection.signals.size + page.ws.selected.size).toBe(0);
    } finally {
      page.dispose();
    }
  });

  it('are never picked with property rows: picking either lets go of the other', async () => {
    const page = await mountPage(KEYS, 'columns', 0, ['vector.x', 'vector.y'], [{ name: 'gain', channel: 'setting' }, { name: 'offset', channel: 'setting' }]);
    try {
      const row = () => document.querySelector('tr[id^=prop-] td:last-child');
      expect(row(), 'a property row on the page').toBeTruthy();
      await clickOn(chip('vector.x'));
      await clickOn(chip('vector.y'), { ctrl: true });
      flushSync();
      expect(page.ws.selection.signals.size).toBe(2);
      await clickOn(row());
      flushSync();
      expect([page.ws.selection.signals.size, page.ws.selection.rows.size], 'a row picked lets go of the signals').toEqual([0, 1]);
      expect(marked()).toEqual([]);
      await clickOn(chip('vector.y'));
      flushSync();
      expect([page.ws.selection.signals.size, page.ws.selection.rows.size], 'a signal picked lets go of the rows').toEqual([1, 0]);
      expect(document.querySelectorAll('tr[id^=prop-][aria-selected=true]').length).toBe(0);
    } finally {
      page.dispose();
    }
  });
});

describe('Escape, pressed again', () => {
  it('with focus on a plot’s control goes out a level, and selects nothing', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      flushSync();
      await pressKey('Shift');
      document.querySelector(`.leaf[data-plot="${a}"] .plot-head button`).focus();
      for (let i = 0; i < 3; i++) {
        await pressKey('Escape');
        flushSync();
        expect(page.ws.selected.size, `after Escape ${i + 1}`).toBe(0);
      }
    } finally {
      page.dispose();
    }
  });
});

describe('F from the keys', () => {
  it('maximizes the selection, and again restores, whichever plot was clicked last', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      page.ws.selection.point(b);
      flushSync();
      document.body.focus();
      await pressKey('f');
      flushSync();
      expect([...page.ws.maximized].sort()).toEqual([a, c].sort());
      await pressKey('f');
      flushSync();
      expect(page.ws.maximized).toBe(null);
    } finally {
      page.dispose();
    }
  });
});

describe('a split', () => {
  it('undone and done again selects the new plots again', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      page.ws.selected.add(ids[0]);
      flushSync();
      await plotMenu(ids[0]);
      await pickItem('Split right');
      const made = plotIds(page.ws.layout).filter((p) => !ids.includes(p));
      expect([...page.ws.selected]).toEqual(made);
      page.history.undo();
      page.history.redo();
      flushSync();
      expect([...page.ws.selected]).toEqual(made);
    } finally {
      page.dispose();
    }
  });
});

describe('a click on one of several selected plots', () => {
  it('narrows the selection to it a moment later, unless a deselect comes first', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      document.body.focus();
      await pressKey('Control+a');
      const [x, y] = centre(document.querySelector(`.leaf[data-plot="${ids[0]}"] .plot-body`));
      await commands.mouse([['move', x, y], ['down'], ['up']]);
      await pressKey('Escape');
      await wait(400);
      flushSync();
      expect(page.ws.selected.size).toBe(0);
    } finally {
      page.dispose();
    }
  });
});

describe('a digit', () => {
  it('selects its plot alone, the plot that has focus already among them', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a] = plotIds(page.ws.layout);
      await pressKey('1');
      flushSync();
      await frames();
      await pressKey('Control+a');
      await pressKey('1');
      flushSync();
      expect([...page.ws.selected]).toEqual([a]);
    } finally {
      page.dispose();
    }
  });
});

describe('property rows', () => {
  const RPCS = [
    { name: 'gain', channel: 'setting' },
    { name: 'offset', channel: 'setting' },
    { name: 'temp', channel: 'report' },
  ];
  /** The row of property `name`, in whichever panel shows it: each name is one panel's here. */
  const row = (name) => [...document.querySelectorAll('tr[id^=prop-]')].find((tr) => tr.textContent.includes(name));
  const click = (el, init = {}) => {
    el.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, cancelable: true, button: 0, ...init }));
    el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, button: 0, ...init }));
  };
  const picked = () => [...document.querySelectorAll('tr[id^=prop-][aria-selected=true]')].map((tr) => tr.textContent.match(/gain|offset|temp/)?.[0]).sort();

  it('and plots are never chosen together, and a row pick spans panels', async () => {
    const page = await mountPage(KEYS, 'columns', 0, [], RPCS);
    try {
      const [a] = plotIds(page.ws.layout);
      expect(row('gain'), 'the Settings panel shows its rows').toBeTruthy();
      page.ws.selected.add(a);
      flushSync();
      click(row('gain'));
      flushSync();
      expect(page.ws.selected.size).toBe(0);
      expect(picked()).toEqual(['gain']);
      click(row('temp'), { ctrlKey: true });
      flushSync();
      expect(picked()).toEqual(['gain', 'temp']);
      click(row('offset'));
      flushSync();
      expect(picked()).toEqual(['offset']);
      const [x, y] = centre(document.querySelector(`.leaf[data-plot="${a}"] .plot-body`));
      await commands.mouse([['move', x, y], ['down'], ['up']]);
      flushSync();
      expect([...page.ws.selected]).toEqual([a]);
      expect(picked()).toEqual([]);
    } finally {
      page.dispose();
    }
  });

  it('keep the plot keys from acting on any plot, rows picked or plots still selected as the keys reach the table', async () => {
    // A row clicked, or every plot selected and the table reached from the keys, which picks no row.
    for (const reach of [() => click(row('gain')), (page) => page.ws.selection.only(plotIds(page.ws.layout))]) {
      const page = await mountPage(KEYS, 'columns', 0, [], RPCS);
      try {
        const ids = plotIds(page.ws.layout);
        reach(page);
        flushSync();
        await pressKey('Shift');
        row('gain').closest('[role=grid]').focus();
        for (const key of ['l', 'y', 'k', 'f', 'Delete']) await pressKey(key);
        flushSync();
        expect(plotIds(page.ws.layout)).toEqual(ids);
        for (const id of ids) expect(page.ws.plots[id].legend, id).toBe(false);
        expect(page.ws.maximized).toBe(null);
      } finally {
        page.dispose();
      }
    }
  });
});

describe('Space after a click on a dock header', () => {
  it('pauses the plots, as anywhere, and starts no keyboard drag of the panel', async () => {
    const page = await mountPage(KEYS, 'columns', 0, [], [{ name: 'gain', channel: 'setting' }, { name: 'temp', channel: 'report', access: ['read'] }]);
    try {
      const live = () => [...document.querySelectorAll('button')].find((b) => /^(Live|Follow live)$/.test(b.textContent.trim())).textContent.trim();
      const head = (title) => document.querySelector(`.dock-panel[aria-label="${title}"] .dock-head`);
      const was = live();
      // A click folds the Settings panel and focuses its header.
      await commands.mouse([['move', ...centre(head('Settings'))], ['down'], ['up']]);
      flushSync();
      await frames();
      expect(document.activeElement).toBe(head('Settings'));
      await pressKey(' ');
      flushSync();
      await frames();
      expect(live(), 'Space pauses').not.toBe(was);
      expect(head('Settings').tabIndex, 'the header stays in the Tab order').toBe(0);
      // The page is not in a drag: a click on a row picks it.
      const row = document.querySelector('.dock-panel[aria-label="Reports"] tr[id^=prop-] td:last-child');
      await commands.mouse([['move', ...centre(row)], ['down'], ['up']]);
      flushSync();
      expect([...page.ws.selection.rows]).toEqual(['reports:temp']);
    } finally {
      page.dispose();
    }
  });
});

describe('a split from the keys', () => {
  it('with several plots selected keeps every new plot selected, though it moves focus to one', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      await pressKey('1');
      flushSync();
      page.ws.selection.only([ids[0], ids[2]]);
      flushSync();
      await pressKey('|');
      flushSync();
      await frames();
      const made = plotIds(page.ws.layout).filter((p) => !ids.includes(p));
      expect(made.length).toBe(2);
      expect([...page.ws.selected].sort()).toEqual([...made].sort());
      expect(made).toContain(document.activeElement.closest('.leaf')?.dataset.plot);
    } finally {
      page.dispose();
    }
  });
});

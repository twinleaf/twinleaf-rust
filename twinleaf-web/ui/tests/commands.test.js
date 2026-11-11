// Every plot command of the table (commands.js) does the same thing from
// each origin that offers it: its row in a menu, a plot's button or an
// axis's popover, its key, the toolbar and the command palette. The origins
// are found on the page by the command each names (data-command), so a
// command added to the table is held to this by its entry alone. Each
// command's run is replaced by a record of the plots it was run on, and for
// any selection the record is what the rules say: made on a plot, the
// selection it is in or it alone; from the page, the selection, or with
// none every plot for a view command and none for a plot command; less the
// plots of kinds the command does not take. Then every origin that runs the
// command on the same plots does the same thing, as it runs the one entry.
// A command made on a signal is made from the page as far as plots go: its
// origins are the Signals list's menu, its keys pressed in the list, and the
// palette's rows that name a signal. A tab command acts on no plot but on
// a tab: made on a tab, from its menu, its buttons or its keys, on that
// tab, here one that is not open; from the tab strip, on the open one.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { MENUS, axisPopover, closeAll, openMenu } from './harness/menus.js';
import { centre, click, clickOn, editCursors, feed, focusPage, leafOf, mountPage, pressKey, rightClick, settle, typeInto, until, wait, withExamples, withYPair } from './harness/page.js';
import { COMMANDS, PLOTLESS } from '../src/lib/commands.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];

/**
 * Four plots in rows, each of both signals with its legend and cursors
 * placed, the first independent, with an axis a signal, and the last a
 * spectrum, another tab, and a signal in the Signals list that no plot
 * shows: every origin of every command shows on its first or its last plot.
 */
async function commandPage() {
  const page = await mountPage([SIGNALS, SIGNALS, SIGNALS, SIGNALS], 'rows', 1, [...SIGNALS, 'mag.x']);
  feed(page.hub);
  // Spectra with peaks either side of X1 and X2, so each peak command has somewhere to go on every plot.
  const session = page.hub.session, perf = clock.now(), f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  session.apply({ kind: 'spectra', spectra: [...session.traces.values()].map((t) => ({ id: t.meta.id, f, a: f.map((v) => ([2, 5, 20].includes(v) ? 1 : 0.1)), floor: 0.02 })) }, perf);
  session.tick(perf);
  const ids = plotIds(page.ws.layout);
  page.history.group('Prepare', () => ids.forEach((id, i) => page.ws.setPlot(id, { legend: true, ...(i === 0 && { mode: 'independent' }), ...(i === 3 && { kind: 'spectrum' }) })));
  await wait(300);
  await settle();
  for (const id of ids) {
    const kind = page.ws.plots[id].kind;
    // A time plot paused over the samples, as its X cursors mark moments of them, so they have peaks in view.
    if (kind === 'time') page.views.get(id).show(10, 20, clock.now());
    editCursors(page, id, (c) => withYPair({ ...c, ...(kind === 'time' ? { x: { a: 14, b: 16 } } : { f: { a: 3, b: 7 } }) }, kind, page.pv(id).yAxes[0].key, 0.1, 0.5));
  }
  // The legend and the readout show what a frame drew a few times a second.
  await wait(200);
  await settle();
  return { ...page, ids };
}

/** The rows, buttons and items naming command `id` in `el`. */
const naming = (el, id) => [...(el?.querySelectorAll(`[data-command="${id}"]`) ?? [])];
/** Brings the pointer over plot `id`, so its header shows its buttons. */
const hover = (id) => commands.mouse([['move', ...centre(leafOf(id))]]);

/**
 * The origins made on plot `id`: each opens what holds its control and
 * returns it. A plot's menus, its header and its axes' popovers.
 */
const ON_PLOT = {
  'the plot menu': (page, id) => MENUS['the plot menu'](page, id),
  // A menu the keys open on the legend itself, not one of its rows, is the legend's own.
  'the legend’s menu': async (page, id) => {
    const el = leafOf(id).querySelector('.pinned-legend'), [x, y] = centre(el);
    el.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, button: 2, clientX: x, clientY: y }));
    await settle();
    return openMenu();
  },
  'the readout’s menu': async (page, id) => (await rightClick(centre(leafOf(id).querySelector('.cursor-readout'))), openMenu()),
  'a chip’s menu': (page, id) => MENUS['a chip’s menu'](page, id),
  'the add-signal menu': (page, id) => MENUS['the add-signal menu'](page, id),
  'the header': async (page, id) => (await hover(id), leafOf(id).querySelector('.plot-head')),
  'the x axis': (page, id) => axisPopover(page, id, 'x'),
  'the y axis': (page, id) => axisPopover(page, id, 'y'),
};
const listSignals = () => [...document.querySelectorAll('.palette .chip[data-key]')];
/** The last tab of the strip, which is not the open one. */
const lastTab = () => [...document.querySelectorAll('[role=tab]')].at(-1);
/** The origins made on a tab, the last: each opens what holds its control and returns it. A tab's menu and its buttons. */
const ON_TAB = {
  'a tab’s menu': async () => (await rightClick(centre(lastTab())), openMenu()),
  'a tab’s buttons': async () => lastTab(),
};
const palette = () => document.querySelector('[data-slot=dialog-overlay]');
/** Closes the command palette, if it is open, and waits until it has gone: it takes every press until then. */
async function closePalette() {
  if (document.querySelector('[data-slot=command-item]')) await pressKey('Escape');
  return until(() => !palette(), 2000);
}
/**
 * The origins made from the page: each opens what holds its control and
 * returns it. One that takes the command's id opens what holds that
 * command's control, where what it holds differs by what it is opened on.
 */
const ON_PAGE = {
  'the toolbar': async () => document.querySelector('.top-bar'),
  'the File dropdown': () => MENUS['the File dropdown'](),
  'the Layout dropdown': () => MENUS['the Layout dropdown'](),
  'the tab strip': async () => document.querySelector('[data-focus=tabs]'),
  // A signal's menu has Add to plot or Remove from plot by what the selected plots show: the first signal whose menu names the command, or else the last.
  'the Signals list’s menu': async (page, on, id) => {
    let menu = null;
    for (const signal of listSignals()) {
      await closeAll();
      await rightClick(centre(signal.querySelector('.sig-key')));
      menu = openMenu();
      if (naming(menu, id).length) break;
    }
    return menu;
  },
  'the palette’s rows': async () => (await pressKey('Control+k'), await settle(), document.querySelector('[data-slot=command-list]')),
};

/** Every origin a control is found at, by name. */
const ORIGINS = { ...ON_PLOT, ...ON_TAB, ...ON_PAGE };

/** The key that runs command `c`, as the keyboard command names it, or null. */
function keyOf(c) {
  return c.keys ? (c.ctrl ? 'Control+' : '') + (c.shift ? 'Shift+' : '') + ({ ' ': 'Space' }[c.keys[0]] ?? c.keys[0]) : null;
}

/** A list of values a control opens: a row's submenu, or a dropdown's list. */
const VALUES = '[data-slot$=-menu-sub-content], [data-slot=select-content]';
/** What a field with no value of its own takes: a window, a frequency, or the low end of a range, of which a field takes one. */
const TYPED = ['5', '0.001'];
/**
 * Makes the gesture of `control`, which names a command, until `ran()`: a
 * click; on a control that opens a list of values, a click then on the
 * first value there that is not the one chosen; in a field, or in the
 * first field of a group of them, a value typed and Enter.
 */
async function make(control, ran) {
  control.scrollIntoView({ block: 'nearest' });
  const field = control.matches('input') ? control : control.querySelector('input');
  if (field) {
    for (const text of TYPED) {
      if (ran()) return;
      await typeInto(field, text);
      await pressKey('Enter');
      await settle();
    }
    return;
  }
  const open = [...document.querySelectorAll(VALUES)];
  await click(centre(control));
  const list = [...document.querySelectorAll(VALUES)].find((l) => !open.includes(l));
  const value = list?.querySelector('[role=menuitemradio][aria-checked=false], [role=menuitem], [role=option][aria-selected=false]');
  if (value) await click(centre(value));
}

/** Has every command record what it is run on, its plots and for a tab command its tab, into `calls`, in place of running; returns what puts them back. */
function record(calls) {
  const runs = COMMANDS.map((c) => [c, c.run]);
  for (const c of COMMANDS) c.run = (ctx) => void calls.push({ id: c.id, plots: ctx.plots.map((t) => t.id).sort(), ...(ctx.tab && { tab: ctx.tab.id }) });
  return () => runs.forEach(([c, run]) => (c.run = run));
}

describe('a plot command', () => {
  it('runs on what its origin and scope say from each origin that offers it, so does the same from each', async () => {
    const page = await commandPage();
    const calls = [], restore = record(calls);
    // Which origin offers which command, found on the page.
    const offers = new Map(COMMANDS.map((c) => [c.id, []]));
    // The plots, of the first and the last, on which an origin of each command was first found.
    const foundOn = new Map(COMMANDS.map((c) => [c.id, new Set([0])]));
    const reached = new Map(COMMANDS.map((c) => [c.id, new Set()]));
    try {
      const named = new Set();
      // One plot selected, so a signal's menu has both of its rows, each on a signal.
      page.ws.selection.only([page.ids[0]]);
      await settle();
      // An origin made on a plot is opened on the first plot and on the last, the spectrum, as some controls are a time plot's or a spectrum's alone.
      for (const [name, open] of Object.entries(ORIGINS)) {
        for (const k of ON_PLOT[name] ? [0, 3] : [0]) {
          // An origin that opens by command is opened for each.
          for (const c of open.length > 2 ? COMMANDS : [null]) {
            await closeAll();
            expect(await closePalette(), 'the palette gone').toBe(true);
            const el = await open(page, page.ids[k], c?.id);
            expect(el, `${name} opens`).toBeTruthy();
            for (const row of el.querySelectorAll('[data-command]')) named.add(row.dataset.command);
            for (const x of c ? [c] : COMMANDS) if (naming(el, x.id).length && !offers.get(x.id).includes(name)) (offers.get(x.id).push(name), foundOn.get(x.id).add(k));
          }
        }
      }
      await closeAll();
      await closePalette();
      // Every control that names a command names one of the table.
      expect([...named].filter((id) => !offers.has(id))).toEqual([]);

      await fc.assert(
        fc.asyncProperty(fc.constantFrom(...COMMANDS.map((c) => c.id)), fc.subarray([0, 1, 2, 3]), fc.nat(3), async (id, picks, k) => {
          const c = COMMANDS.find((x) => x.id === id), ids = page.ids, picked = picks.map((i) => ids[i]), on = ids[k];
          const kinds = (list) => list.filter((p) => !c.kinds || c.kinds.includes(page.ws.plots[p].kind)).sort();
          // The rules, worked out here from the selection alone.
          const fromPlot = kinds(picked.includes(on) ? picked : [on]);
          const fromPage = PLOTLESS.includes(c.scope) ? [] : kinds(picked.length ? picked : c.scope === 'view' ? ids : []);
          /** Makes an origin's gesture, `act`, on the selection, and checks that the command ran once on `want`, a tab command on the open tab or with `onLast` on the last, or with none and not `empty`, not at all. */
          const check = async (origin, want, act, onLast = false) => {
            await closeAll();
            // The palette, left open or closed, fades out over the page, and takes every press until it has gone.
            expect(await closePalette(), `the palette gone before ${id} from ${origin}`).toBe(true);
            await focusPage();
            page.ws.selection.only(picked);
            document.activeElement?.blur();
            await settle();
            calls.length = 0;
            const made = await act();
            if (made === false) return;
            await until(() => calls.length, 300);
            await settle();
            const runsWith = want.length || c.empty ? [{ id, plots: want, ...(c.scope === 'tab' && { tab: onLast ? page.ws.tabs.at(-1).id : page.ws.activeId }) }] : [];
            expect(calls, `${id} from ${origin}, made on ${k} with ${picks} selected`).toEqual(runsWith);
            reached.get(id).add(origin);
          };
          for (const name of offers.get(id)) {
            await check(name, ON_PLOT[name] ? fromPlot : fromPage, async () => {
              const el = await ORIGINS[name](page, on, id);
              // A control that is disabled takes no gesture, as the sample line's − at Auto: the first that is not.
              const control = naming(el, id).find((c) => !c.disabled);
              // A control the plot shows only for some plots, as a time axis's pause, is not this plot's; nor is a signal's row the menu has only for some selections.
              if (!control) return false;
              await make(control, () => calls.length > 0);
            }, !!ON_TAB[name]);
          }
          const key = keyOf(c);
          // A key of the Signals list is pressed there, on a signal a click picked; a tab's, with focus on the last tab.
          const before = { list: () => clickOn(listSignals()[0]), tabs: () => lastTab().focus() }[c.place];
          if (key) await check('its key', fromPage, async () => (await before?.(), pressKey(key)), c.place === 'tabs');
          if (c.palette) {
            await check('the palette', fromPage, async () => {
              await pressKey('Control+k');
              await settle();
              const item = document.querySelector(`[data-slot=command-item][data-command="${id}"]`);
              // A command on plots with none to act on now is not offered; one of a tab or the page always is.
              expect(!!item, `${id} offered with ${picks} selected`).toBe(PLOTLESS.includes(c.scope) || fromPage.length > 0);
              // The palette that offers nothing to pick is closed as it was opened, by a key.
              if (!item) {
                await pressKey('Escape');
                return false;
              }
              item.click();
              await settle();
            });
          }
        }),
        // A command the palette does not offer, then one whose first origin is a plot's header; each command on each plot an origin of it was found on, in a selection of the two; then random cases.
        withExamples([['splitRight', [], 0], ['pause', [3], 0], ...COMMANDS.flatMap((c) => [...foundOn.get(c.id)].map((k) => [c.id, [0, 3], k]))]),
      );
    } finally {
      restore();
      page.dispose();
    }
    // Each command is reached from somewhere, and from every origin that offers it.
    for (const c of COMMANDS) {
      const want = [...offers.get(c.id), ...(keyOf(c) ? ['its key'] : []), ...(c.palette ? ['the palette'] : [])];
      expect(want.length, `${c.id} has an origin on the page`).toBeGreaterThan(0);
      expect([...reached.get(c.id)].sort(), c.id).toEqual(want.sort());
    }
  }, 600_000);
});

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
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { MENUS, closeAll, openMenu } from './harness/menus.js';
import { centre, click, editCursors, feed, focusPage, leafOf, mountPage, pressKey, rightClick, settle, until, wait, withExamples, withYPair } from './harness/page.js';
import { COMMANDS } from '../src/lib/commands.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];

/**
 * Four plots in rows, each of both signals with its legend and cursors
 * placed, the last a spectrum, and another tab: every origin of every
 * command shows on it.
 */
async function commandPage() {
  const page = await mountPage([SIGNALS, SIGNALS, SIGNALS, SIGNALS], 'rows', 1, SIGNALS);
  feed(page.hub);
  const ids = plotIds(page.ws.layout);
  page.history.group('Prepare', () => ids.forEach((id, i) => page.ws.setPlot(id, { legend: true, ...(i === 3 && { kind: 'spectrum' }) })));
  await wait(300);
  await settle();
  for (const id of ids) {
    const kind = page.ws.plots[id].kind;
    editCursors(page, id, (c) => withYPair({ ...c, ...(kind === 'time' ? { x: { a: 14, b: 16 } } : { f: { a: 3, b: 7 } }) }, kind, 'shared', 0.1, 0.5));
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
/** Right-clicks plot `id`'s axis `which` ('x', or 'y' for its first y axis), which opens its popover. */
async function axisPopover(page, id, which) {
  const pv = page.pv(id), P = pv.geo.plot, band = pv.axisBands[0];
  // The y axis low down, clear of the readout in the plot's top left corner.
  await rightClick(which === 'x' ? [P.x + P.w / 2, P.y + P.h + 8] : [(band.x0 + band.x1) / 2, P.y + P.h * 0.9]);
  const open = () => document.querySelector('[data-slot=popover-content][data-state=open]');
  await until(open, 2000);
  return open();
}

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
  'the header': async (page, id) => (await hover(id), leafOf(id).querySelector('.plot-head')),
  'the x axis': (page, id) => axisPopover(page, id, 'x'),
  'the y axis': (page, id) => axisPopover(page, id, 'y'),
};
/** The origins made from the page: each opens what holds its control and returns it. */
const ON_PAGE = {
  'the toolbar': async () => document.querySelector('.top-bar'),
  'the File dropdown': () => MENUS['the File dropdown'](),
};

/** The key that runs command `c`, as the keyboard command names it, or null. */
function keyOf(c) {
  if (c.keys) return { ' ': 'Space' }[c.keys[0]] ?? c.keys[0];
  return c.show?.includes('Ctrl') ? c.show.map((k) => (k === 'Ctrl' ? 'Control' : k.toLowerCase())).join('+') : null;
}

/** Has every command record what it is run on into `calls`, in place of running; returns what puts them back. */
function record(calls) {
  const runs = COMMANDS.map((c) => [c, c.run]);
  for (const c of COMMANDS) c.run = (ctx) => void calls.push({ id: c.id, plots: ctx.plots.map((t) => t.id).sort() });
  return () => runs.forEach(([c, run]) => (c.run = run));
}

describe('a plot command', () => {
  it('runs on what its origin and scope say from each origin that offers it, so does the same from each', async () => {
    const page = await commandPage();
    const calls = [], restore = record(calls);
    // Which origin offers which command, found on the page.
    const offers = new Map(COMMANDS.map((c) => [c.id, []]));
    const reached = new Map(COMMANDS.map((c) => [c.id, new Set()]));
    try {
      const named = new Set();
      for (const [name, open] of [...Object.entries(ON_PLOT), ...Object.entries(ON_PAGE)]) {
        await closeAll();
        const el = await open(page, page.ids[0]);
        expect(el, `${name} opens`).toBeTruthy();
        for (const row of el.querySelectorAll('[data-command]')) named.add(row.dataset.command);
        for (const c of COMMANDS) if (naming(el, c.id).length) offers.get(c.id).push(name);
      }
      await closeAll();
      // Every control that names a command names one of the table.
      expect([...named].filter((id) => !offers.has(id))).toEqual([]);

      await fc.assert(
        fc.asyncProperty(fc.constantFrom(...COMMANDS.map((c) => c.id)), fc.subarray([0, 1, 2, 3]), fc.nat(3), async (id, picks, k) => {
          const c = COMMANDS.find((x) => x.id === id), ids = page.ids, picked = picks.map((i) => ids[i]), on = ids[k];
          const kinds = (list) => list.filter((p) => !c.kinds || c.kinds.includes(page.ws.plots[p].kind)).sort();
          // The rules, worked out here from the selection alone.
          const fromPlot = kinds(picked.includes(on) ? picked : [on]);
          const fromPage = kinds(picked.length ? picked : c.scope === 'view' ? ids : []);
          /** Makes an origin's gesture, `act`, on the selection, and checks that the command ran once on `want`, or with none and not `empty`, not at all. */
          const check = async (origin, want, act) => {
            await closeAll();
            // The palette, closed, fades out over the page, and takes every press until it has gone.
            expect(await until(() => !document.querySelector('[data-slot=dialog-overlay]'), 2000), `the palette gone before ${id} from ${origin}`).toBe(true);
            await focusPage();
            page.ws.selection.only(picked);
            document.activeElement?.blur();
            await settle();
            calls.length = 0;
            const made = await act();
            if (made === false) return;
            await until(() => calls.length, 300);
            await settle();
            const runsWith = want.length || c.empty ? [{ id, plots: want }] : [];
            expect(calls, `${id} from ${origin}, made on ${k} with ${picks} selected`).toEqual(runsWith);
            reached.get(id).add(origin);
          };
          for (const name of offers.get(id)) {
            const open = ON_PLOT[name] ?? ON_PAGE[name];
            await check(name, ON_PLOT[name] ? fromPlot : fromPage, async () => {
              const el = await open(page, on);
              const control = naming(el, id)[0];
              // A control the plot shows only for some plots, as a time axis's pause, is not this plot's.
              if (!control) return false;
              await click(centre(control));
            });
          }
          const key = keyOf(c);
          if (key) await check('its key', fromPage, () => pressKey(key));
          if (c.label && !c.needs?.includes('axis')) {
            await check('the palette', fromPage, async () => {
              await pressKey('Control+k');
              await settle();
              const item = document.querySelector(`[data-slot=command-item][data-value^="command ${id} "]`);
              // A command with no plots to act on now is not offered.
              expect(!!item, `${id} offered with ${picks} selected`).toBe(fromPage.length > 0);
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
        // A command the palette does not offer, then one whose first origin is a plot's header; each command on a plot in a selection of two; then random cases.
        withExamples([['splitRight', [], 0], ['pause', [3], 0], ...COMMANDS.map((c) => [c.id, [0, 3], 0])]),
      );
    } finally {
      restore();
      page.dispose();
    }
    // Each command is reached, and from every origin that offers it.
    for (const c of COMMANDS) {
      const want = [...offers.get(c.id), ...(keyOf(c) ? ['its key'] : []), ...(c.label && !c.needs?.includes('axis') ? ['the palette'] : [])];
      expect([...reached.get(c.id)].sort(), c.id).toEqual(want.sort());
    }
  }, 600_000);
});

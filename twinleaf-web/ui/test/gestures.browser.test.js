// What a box, a press, Escape and Space do on the whole page, and who owns a
// key, with the real mouse and keys, as the user's verdicts say (round 1
// #17, #21 to #23; round 2 Selection #14, #16, #18, #19).
// - A box drawn from empty dock or panel space selects every plot it
//   touches, and none starts on a row.
// - Scrollbars and the dock-width handles deselect, but the Signals panel's.
// - Escape during a box or a plot move puts it away, puts nothing down,
//   records nothing and deselects everything; Escape never changes a cursor.
// - A key pressed in an open menu stays there.
// - The Live button pauses every plot, and Space after it has them follow.
// - Space on a dock header the keyboard moved to folds its panel, and a
//   header dragged moves its panel.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, drag, dropped, emptySpots, feed, focusPage, frames, glide, headerSpot, holding, leafOf, mountPage, pickItem, plotMenu, quietApi, until } from './browser/page.js';
import { plotIds } from '../src/lib/layout.js';
import { STEP } from '../src/lib/moves.js';

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine'], ['aux.sawtooth']];
/** Whether plot `id` is paused, as its header's button says. */
const paused = (id) => !!leafOf(id).querySelector('button[aria-label="Follow live"]');

beforeEach(() => quietApi());
afterEach(() => vi.restoreAllMocks());

/** The plots shown whose tiles the box from `a` to `b` touches. */
function touched(a, b) {
  const [x0, x1] = [Math.min(a[0], b[0]), Math.max(a[0], b[0])], [y0, y1] = [Math.min(a[1], b[1]), Math.max(a[1], b[1])];
  return [...document.querySelectorAll('.tiles .leaf[data-plot]')]
    .filter((el) => getComputedStyle(el).visibility !== 'hidden')
    .filter((el) => {
      const r = el.getBoundingClientRect();
      return r.left < x1 && r.right > x0 && r.top < y1 && r.bottom > y0;
    })
    .map((el) => el.dataset.plot);
}

/** A point inside the tiles, `fx` and `fy` the fractions across them. */
function inTiles(fx, fy) {
  const r = document.querySelector('.tiles').getBoundingClientRect();
  return [r.left + 2 + fx * (r.width - 4), r.top + 2 + fy * (r.height - 4)];
}

/** A press at `from`, then the moves to `to`. */
const pressTo = (from, to) => [['move', ...from], ['down'], ...glide(from, to, 6)];

describe('a box', () => {
  it('drawn from empty dock or panel space selects every plot it touches, with Shift those too, and none starts on a row', async () => {
    const page = await mountPage(KEYS, 'grid', 0, ['vector.x'], [{ name: 'gain', channel: 'setting' }]);
    let boxes = 0;
    try {
      const starts = emptySpots();
      expect(starts.length, 'empty space in the docks').toBeGreaterThan(0);
      await fc.assert(
        fc.asyncProperty(fc.nat(), fc.double({ min: 0, max: 1, noNaN: true }), fc.double({ min: 0, max: 1, noNaN: true }), fc.subarray([0, 1, 2, 3]), fc.boolean(), async (k, fx, fy, picks, shift) => {
          const ids = plotIds(page.ws.layout), base = picks.map((i) => ids[i]);
          page.ws.selection.only(base);
          flushSync();
          const from = starts[k % starts.length], to = inTiles(fx, fy);
          await commands.mouse(holding(shift ? ['Shift'] : [], [...pressTo(from, to), ['up']]));
          flushSync();
          const want = new Set(touched(from, to));
          if (shift) for (const id of base) want.add(id);
          expect([...page.ws.selected].sort()).toEqual([...want].sort());
          expect(document.querySelector('.marquee')).toBe(null);
          boxes++;
        }),
        { numRuns: Math.max(20, Math.ceil(fc.readConfigureGlobal().numRuns / 4)) },
      );
      // A drag that starts on a property row picks rows, and draws no box.
      const row = document.querySelector('tr[id^=prop-]');
      page.ws.selection.only([plotIds(page.ws.layout)[0]]);
      flushSync();
      await commands.mouse(pressTo(centre(row), inTiles(0.9, 0.9)));
      await frames();
      expect(document.querySelector('.marquee')).toBe(null);
      await commands.mouse([['up']]);
      flushSync();
      expect(page.ws.selected.size).toBe(0);
    } finally {
      page.dispose();
    }
    expect(boxes).toBeGreaterThan(10);
  }, 600_000);
});

describe('a press', () => {
  it('on a scrollbar outside the Signals panel, or a dock-width handle, deselects; on the Signals panel’s scrollbar it keeps the selection', async () => {
    const rpcs = Array.from({ length: 40 }, (_, i) => ({ name: `gain${i}`, channel: 'setting' }));
    const signals = Array.from({ length: 40 }, (_, i) => `vector.c${i}`);
    const page = await mountPage(KEYS, 'grid', 0, signals, rpcs);
    try {
      const ids = plotIds(page.ws.layout);
      const seen = { keeps: 0, clears: 0 };
      // Each scroll area shows its bar while the pointer is over it.
      for (const area of document.querySelectorAll('[data-slot=scroll-area]')) {
        await commands.mouse([['move', ...centre(area)]]);
        const bars = () => [...area.querySelectorAll('[data-slot=scroll-area-scrollbar]')].filter((b) => b.getBoundingClientRect().height);
        await until(() => bars().length, 1000);
        for (const bar of bars()) {
          page.ws.selection.only(ids.slice(0, 2));
          flushSync();
          await commands.mouse([['move', ...centre(bar)], ['down'], ['up']]);
          flushSync();
          const keeps = !!bar.closest('[data-keeps-selection]');
          expect([...page.ws.selected], keeps ? 'the Signals panel’s scrollbar keeps' : 'a scrollbar deselects').toEqual(keeps ? ids.slice(0, 2) : []);
          seen[keeps ? 'keeps' : 'clears']++;
        }
      }
      for (const handle of document.querySelectorAll('[data-slot=resizable-handle]')) {
        if (!handle.getBoundingClientRect().height) continue;
        page.ws.selection.only(ids.slice(0, 2));
        flushSync();
        await commands.mouse([['move', ...centre(handle)], ['down'], ['up']]);
        flushSync();
        expect(page.ws.selected.size, 'a dock-width handle deselects').toBe(0);
        seen.clears++;
      }
      expect(seen.keeps, 'the Signals panel’s scrollbar').toBeGreaterThan(0);
      expect(seen.clears, 'other scrollbars and the handles').toBeGreaterThan(2);
    } finally {
      page.dispose();
    }
  });
});

describe('Escape during a gesture', () => {
  it('during a box or a plot move puts it away, puts nothing down, records nothing and deselects everything', async () => {
    const kinds = new Set();
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom('box', 'grip', 'header'),
        fc.subarray([0, 1, 2, 3]),
        fc.nat(),
        fc.nat(),
        fc.constantFrom('center', 'left', 'right', 'top', 'bottom'),
        fc.boolean(),
        async (kind, picks, a, b, cell, maximize) => {
          const page = await mountPage(KEYS, 'grid');
          try {
            const all = plotIds(page.ws.layout);
            // Three plots maximized, or none: a move lifts them out of it, and one put down nowhere goes back.
            const max = maximize ? all.slice(0, 3) : null;
            page.ws.maximized = max;
            const ids = page.ws.shownIds(), moved = ids[a % ids.length], onto = ids.filter((id) => id !== moved)[b % (ids.length - 1)];
            page.ws.selection.only(picks.map((i) => all[i]).filter((id) => ids.includes(id)));
            flushSync();
            await frames();
            const layout = page.ws.layout, steps = page.history.undos.length;
            let from;
            if (kind === 'box') from = emptySpots()[a % emptySpots().length];
            else {
              await commands.mouse([['move', ...centre(leafOf(moved))]]);
              from = kind === 'grip' ? centre(leafOf(moved).querySelector('.grip')) : headerSpot(leafOf(moved));
            }
            const [cx, cy] = centre(leafOf(onto));
            const [dx, dy] = { center: [0, 0], left: [-1, 0], right: [1, 0], top: [0, -1], bottom: [0, 1] }[cell];
            const to = [cx + dx * STEP, cy + dy * STEP];
            await commands.mouse(pressTo(from, to));
            // Under way: the box drawn, or the plot lifted.
            await until(() => document.querySelector(kind === 'box' ? '.marquee' : '.move-ghost'), 2000);
            await commands.key('Escape');
            // What comes after Escape, the pointer still down, starts nothing again.
            await commands.mouse([['move', to[0] + 20, to[1] + 10], ['move', to[0] - 10, to[1] + 30], ['up']]);
            flushSync();
            await frames();
            expect(page.ws.layout).toBe(layout);
            expect(page.ws.maximized).toEqual(max);
            expect(page.history.undos.length).toBe(steps);
            expect(page.ws.selected.size).toBe(0);
            expect(page.ws.selection.rows.size).toBe(0);
            expect(document.querySelector('.drop-preview, .marquee, .move-ghost')).toBe(null);
            kinds.add(kind);
          } finally {
            page.dispose();
          }
        },
      ),
      { numRuns: Math.max(15, Math.ceil(fc.readConfigureGlobal().numRuns / 5)), examples: [['box', [0], 0, 0, 'left', false], ['grip', [0, 1], 0, 0, 'top', true], ['header', [], 1, 1, 'center', false]] },
    );
    expect(kinds.size).toBe(3);
  }, 600_000);
});

describe('Escape', () => {
  /** The plots that show a cursors' readout. */
  const withCursors = () => [...document.querySelectorAll('.leaf[data-plot]')].filter((l) => l.querySelector('.cursor-readout')).map((l) => l.dataset.plot).sort();

  it('never changes a cursor, whatever is maximized, selected or focused, however often it is pressed', async () => {
    let placed = 0;
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2, 3], { minLength: 1 }), fc.option(fc.subarray([0, 1, 2, 3], { minLength: 1 })), fc.subarray([0, 1, 2, 3]), fc.option(fc.nat(3)), fc.integer({ min: 1, max: 4 }), async (cursed, max, picks, focus, presses) => {
        const page = await mountPage(KEYS, 'grid', 0, KEYS.flat());
        try {
          feed(page.hub);
          await focusPage();
          const ids = plotIds(page.ws.layout), want = cursed.map((i) => ids[i]).sort();
          // X cursors on some plots, from the menu of the first, with them selected.
          page.ws.selection.only(cursed.map((i) => ids[i]));
          flushSync();
          await plotMenu(ids[cursed[0]]);
          await pickItem('Add X cursors');
          // The readout shows once the plots have drawn their cursors.
          await until(() => withCursors().length === want.length);
          expect(withCursors()).toEqual(want);
          page.ws.maximized = max && max.map((i) => ids[i]);
          const shown = page.ws.shownIds();
          page.ws.selection.only(picks.map((i) => ids[i]).filter((id) => shown.includes(id)));
          if (focus !== null && shown.includes(ids[focus])) leafOf(ids[focus]).querySelector('.plot-card').focus();
          else document.activeElement?.blur();
          flushSync();
          for (let i = 0; i < presses; i++) await commands.key('Escape');
          // Every plot shown again, so each draws its readout.
          page.ws.maximized = null;
          await until(() => withCursors().length >= want.length, 3000);
          expect(withCursors()).toEqual(want);
          placed++;
        } finally {
          page.dispose();
        }
      }),
      { numRuns: Math.max(15, Math.ceil(fc.readConfigureGlobal().numRuns / 5)) },
    );
    expect(placed).toBeGreaterThan(10);
  }, 600_000);
});

describe('a key', () => {
  it('pressed in an open menu stays in it, and acts on no plot', async () => {
    const page = await mountPage(KEYS, 'grid');
    try {
      const ids = plotIds(page.ws.layout);
      page.ws.selection.only(ids.slice(0, 2));
      flushSync();
      await plotMenu(ids[0]);
      for (const key of ['l', 'f', '|', 'Delete', 'Backspace']) await commands.key(key);
      flushSync();
      expect(plotIds(page.ws.layout)).toEqual(ids);
      for (const id of ids) expect(page.ws.plots[id].legend, id).toBe(false);
      expect(page.ws.maximized).toBe(null);
      expect(page.history.undos.length).toBe(0);
    } finally {
      page.dispose();
    }
  });
});

describe('the Live button', () => {
  it('pauses every plot of the tab, and Space after a click on it has them all follow live', async () => {
    const page = await mountPage(KEYS, 'grid', 0, KEYS.flat());
    try {
      feed(page.hub);
      await frames(3);
      const ids = plotIds(page.ws.layout);
      const live = [...document.querySelectorAll('.top-bar button')].find((b) => b.textContent.trim() === 'Live');
      await commands.mouse([['move', ...centre(live)], ['down'], ['up']]);
      await frames();
      for (const id of ids) expect(paused(id), id).toBe(true);
      await commands.key('Space');
      flushSync();
      await frames();
      for (const id of ids) expect(paused(id), id).toBe(false);
    } finally {
      page.dispose();
    }
  });
});

describe('Space', () => {
  const open = (head) => head.getAttribute('aria-expanded') === 'true';

  it('on a dock header the keyboard moved to folds or unfolds its panel, and pauses nothing', async () => {
    const page = await mountPage(KEYS, 'grid', 0, KEYS.flat());
    try {
      feed(page.hub);
      await focusPage();
      const ids = plotIds(page.ws.layout);
      for (const head of document.querySelectorAll('.dock-head')) {
        const was = open(head);
        // A key pressed first, so the focus that follows is the keyboard's.
        await commands.key('Shift');
        head.focus();
        await commands.key('Space');
        flushSync();
        await frames();
        expect(open(head), head.textContent).toBe(!was);
        for (const id of ids) expect(paused(id), id).toBe(false);
      }
    } finally {
      page.dispose();
    }
  });
});

describe('a dock header', () => {
  it('dragged moves its panel to where it is let go, as the drag library hears the pointer on it', async () => {
    const page = await mountPage(KEYS, 'grid', 0, ['vector.x']);
    try {
      const head = [...document.querySelectorAll('.dock-head')].find((h) => h.textContent.includes('Signals'));
      const r = document.querySelector('.dock-frame.right .dock').getBoundingClientRect();
      await drag(centre(head), [r.left + r.width / 2, r.bottom - 20], [], 200);
      await dropped();
      const there = () => [...document.querySelectorAll('.dock-frame.right .dock-head')].some((h) => h.textContent.includes('Signals'));
      await until(there, 2000);
      expect(there()).toBe(true);
    } finally {
      page.dispose();
    }
  });
});

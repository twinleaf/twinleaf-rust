// What a box, a press, Escape and Space do on the whole page, and who owns a
// key, with the real mouse and keys, as the user's verdicts say (round 1
// #17, #21 to #23; round 2 Selection #14, #16, #18, #19).
// - A box drawn from empty dock or panel space selects every plot it
//   touches, and none starts on a row.
// - Scrollbars and the dock-width handles deselect, but the Signals panel's.
// - Escape during a box or a plot move puts it away, puts nothing down,
//   records nothing and deselects everything; Escape never changes a cursor.
// - A maximized plot closed while it is moved leaves the maximized plots,
//   and the rest fill the tab again once the move is put down.
// - A key pressed in an open menu that none of its rows shows stays there.
// - The Live button pauses every plot, and Space after it has them follow.
// - Space on a dock header the keyboard moved to folds its panel, and a
//   header dragged moves its panel.
// - Space or Enter on an item of a drag zone (a tab, a plot's chip, a signal
//   of the Signals list, a dock's panel header) starts no keyboard drag of
//   the drag library's, and after a click on the item the page hears the
//   key (class 2).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { EMPTY, centre, drag, dropped, emptySpots, feed, focusPage, frames, glide, headerSpot, holding, leafOf, mountPage, pickItem, plotMenu, pressKey, quietApi, until, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { STEP } from '../src/lib/moves.js';
import { lastKey } from '../src/lib/keyrouter.js';

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
        // The two cases that, with spots taken without room around them, started 1 px below the Signals list's row, which grows onto it as its font loads; then random runs.
        withExamples([
          [1687448287, 0, 0, [], false],
          [495722447, 0, 0, [], false],
        ]),
      );
      // The starts, taken as the page mounted, are empty space still once it has laid itself out.
      for (const at of starts) expect(document.elementFromPoint(...at).matches(EMPTY), `a start at ${at} is still empty space`).toBe(true);
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
            page.ws.maximize(max);
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
            await pressKey('Escape');
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
      withExamples([['box', [0], 0, 0, 'left', false], ['grip', [0, 1], 0, 0, 'top', true], ['header', [], 1, 1, 'center', false]]),
    );
    expect(kinds.size).toBe(3);
  }, 600_000);
});

describe('a maximized plot closed while it is moved', () => {
  it('leaves the maximized plots, and the rest fill the tab again once the move is put down', async () => {
    const page = await mountPage(KEYS, 'grid');
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      await focusPage();
      page.ws.maximize([a, b, c]);
      flushSync();
      await frames();
      const from = centre(leafOf(a).querySelector('.grip')), to = centre(leafOf(b));
      await commands.mouse(pressTo(from, to));
      expect(await until(() => document.querySelector('.move-ghost'), 2000)).toBe(true);
      // Lifted, the plots show whole; Delete closes the plot held, which the press selected.
      expect(page.ws.maximized).toBe(null);
      await pressKey('Delete');
      await commands.mouse([['move', to[0] + 20, to[1] + 10], ['up']]);
      flushSync();
      await frames();
      expect(page.ws.plots[a]).toBeUndefined();
      expect(document.querySelector('.move-ghost')).toBe(null);
      expect(page.ws.maximized).toEqual([b, c]);
    } finally {
      page.dispose();
    }
  });
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
          page.ws.maximize(max && max.map((i) => ids[i]));
          const shown = page.ws.shownIds();
          page.ws.selection.only(picks.map((i) => ids[i]).filter((id) => shown.includes(id)));
          if (focus !== null && shown.includes(ids[focus])) leafOf(ids[focus]).querySelector('.plot-card').focus();
          else document.activeElement?.blur();
          flushSync();
          for (let i = 0; i < presses; i++) await pressKey('Escape');
          // Every plot shown again, so each draws its readout.
          page.ws.restoreLayout();
          await until(() => withCursors().length >= want.length, 3000);
          expect(withCursors()).toEqual(want);
          placed++;
        } finally {
          page.dispose();
        }
      })
    );
    expect(placed).toBeGreaterThan(10);
  }, 600_000);
});

describe('a key', () => {
  it('pressed in an open menu, and shown by none of its rows, stays in it and acts on no plot', async () => {
    const page = await mountPage(KEYS, 'grid');
    try {
      const ids = plotIds(page.ws.layout);
      page.ws.selection.only(ids.slice(0, 2));
      flushSync();
      await plotMenu(ids[0]);
      for (const key of ['l', 'y', 'Backspace']) await pressKey(key);
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
      await pressKey('Space');
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
        await pressKey('Shift');
        head.focus();
        await pressKey('Space');
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

/** Each drag zone's items, what a click lands on, and where the keys reach for one: the item, or the zone it is an option of. `own` is the claimant that takes a key after a click there: a click on a signal picks it, and Enter then puts it on the selected plots. */
const ZONES = {
  tab: { items: () => [...document.querySelectorAll('[role=tablist] [data-tab]')], click: (el) => el.querySelector('span'), reach: (el) => el },
  chip: { items: () => [...document.querySelectorAll('.tiles .trace-zone .trace-chip')], click: (el) => el.querySelector('.name') ?? el, reach: (el) => el.closest('.trace-zone') },
  signal: { items: () => [...document.querySelectorAll('.palette [data-key]')], click: (el) => el.querySelector('.sig-key') ?? el, reach: (el) => el.closest('.palette'), own: { Enter: 'list' } },
  'dock header': { items: () => [...document.querySelectorAll('.dock-head')], click: (el) => el.querySelector('.dock-title') ?? el, reach: (el) => el },
};

/**
 * Space or Enter on an item of `zones`, clicked or reached from the keys:
 * no keyboard drag starts, and after a click the page hears the key, or
 * the zone's own claimant takes it where the zone has one for it.
 * Returns the zones and ways tried.
 */
async function ownKeys(zones) {
  const seen = new Set();
  await fc.assert(
    fc.asyncProperty(fc.constantFrom(...zones), fc.nat(), fc.constantFrom('Space', 'Enter'), fc.constantFrom('click', 'keys'), async (zone, i, key, how) => {
      const page = await mountPage([['sine.sine', 'sine.cosine'], ['aux.triangle']], 'columns', 1, ['vector.x', 'vector.y', 'vector.z']);
      const drags = [];
      // A drag's start is told to its zone, a consider event that does not bubble: seen on the way down.
      const consider = (e) => e.detail?.info?.source === 'keyboard' && e.detail.info.trigger === 'dragStarted' && drags.push(zone);
      window.addEventListener('consider', consider, true);
      try {
        const items = ZONES[zone].items();
        expect(items.length, zone).toBeGreaterThan(0);
        const el = items[i % items.length];
        // The page has the keys first: a press on an item takes no focus of its own.
        await focusPage();
        if (how === 'click') await commands.mouse([['move', ...centre(ZONES[zone].click(el))], ['down'], ['up']]);
        else {
          // A key pressed first, so the focus that follows is the keyboard's.
          await pressKey('Shift');
          ZONES[zone].reach(el).focus();
        }
        await frames();
        await pressKey(key);
        await frames();
        expect(drags, `${how}, ${key} on a ${zone}: a keyboard drag`).toEqual([]);
        // The key router says who was offered the key pressed last: after a click, the page's claimants.
        const own = ZONES[zone].own?.[key];
        if (how === 'click') expect(lastKey(), `${key} after a click on a ${zone}: heard by ${own ?? 'the page'}`).toMatchObject({ key: key === 'Space' ? ' ' : key, ...(own ? { by: own } : { page: true }) });
        seen.add(`${zone}:${how}`);
      } finally {
        window.removeEventListener('consider', consider, true);
        // A drag left running ends with the page.
        await pressKey('Escape');
        page.dispose();
      }
    }),
    withExamples(zones.flatMap((z) => [[z, 0, 'Space', 'click'], [z, 1, 'Enter', 'click'], [z, 0, 'Space', 'keys']])),
  );
  return seen;
}

describe('Space or Enter on a drag zone’s item', () => {
  it('starts no keyboard drag, and after a click the page hears it: a tab, a chip, a signal, a dock header', async () => {
    const seen = await ownKeys(Object.keys(ZONES));
    expect(seen.size).toBe(8);
  }, 600_000);
});

// View history on the page (round 2, View history #1, #2, #3, #5 and #6,
// round 1 #29): each plot's header has Back and Forward in place of the
// split buttons, which stay in its menu and keys. Random runs of drags that
// rest midway, wheel bursts, double-clicks that follow live, X cursors added
// from the menu and removed by following live, Ctrl+A and Escape, and Back
// and Forward pressed on any plot, with the real mouse: each gesture that
// moves a view is one step of the tab's, whatever plots it moves, and Back
// and Forward go through them, each plot's buttons pressable just when the
// tab has a step to go to, and neither moves a cursor: cursors are edits,
// which Ctrl+Z undoes. The window's slider, dragged through its detents
// with a rest midway, or stepped by its keys in a burst, is one step too.
// Then each kind of gesture once: arrow keys, the toolbar's Live, a plot's
// Pause on a selection, Y cursors, a spectrum's pan and y zoom,
// tabs and a plot moved between them, a maximized plot, and a plot that
// leaves the page mid-drag. Under 480 px Back and Forward go, as the split
// buttons did. A plot closed and brought back by Ctrl+Z (#7) shows its view
// as it was, holding no history while it is gone and reading it again once
// back, and Back goes on through its steps; when the hub has dropped where
// its view starts meanwhile, it starts at the oldest time held, with the
// notice, as Back's views do. A kind switch is an edit and a view step:
// Back crosses it and goes on, whatever undo and redo do between.
//
// The page's clock is virtual (harness page.js): a rest is `wait`, of the
// page's time, and a burst's turns are dispatched 40 ms of it apart
// (`wheel`), so which turns make one step depends on no machine's speed.
// Drags, presses and keys are the real mouse and keyboard.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, frames, mountPage, pickItem, plotMenu, pressKey, turnWheel, until, wait, wheelRest } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { GONE, IDLE_MS } from '../src/lib/viewhistory.svelte.js';

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine']];

/** A view state as a step compares it: a live view's edge moves with the newest samples, so it does not count. */
const seen = (s) => JSON.stringify(s.live ? { ...s, right: null, pausedAt: null } : s);

/**
 * The page, its plots laid out as `preset` (rows by default, each wide
 * enough to show Back and Forward) with `more` empty tabs after, their
 * signals announced, so each draws its time axis, and its clock running, so
 * a pause holds a time. A paused plot reads no history: the stub hub has no
 * server to read it from.
 */
async function page(preset = 'rows', more = 0) {
  const p = await mountPage(KEYS, preset, more, KEYS.flat());
  p.hub.session.offset = 1000;
  p.hub.session.fetchWindow = () => {};
  return p;
}

/** The cursors of every plot of the tab, as the tab keeps them, in one string. */
const cursorsOf = (p) => JSON.stringify(plotIds(p.ws.layout).map((id) => p.ws.plots[id].cursors ?? null));
/** What every plot of the tab shows, in one string. */
const shown = (p) => JSON.stringify(plotIds(p.ws.layout).map((id) => seen(p.views.get(id).state())));

const leaf = (id) => document.querySelector(`.leaf[data-plot="${id}"]`);
const bodyOf = (id) => leaf(id).querySelector('.plot-body');
/** Plot `id`'s header button whose label starts `label`. */
const button = (id, label) => leaf(id).querySelector(`.plot-head button[aria-label^="${label}"]`);

/** Presses plot `id`'s header button `label`, the pointer brought over the plot first so its header shows it. */
async function press(id, label) {
  await commands.mouse([['move', ...centre(leaf(id))]]);
  await commands.mouse([['move', ...centre(button(id, label))], ['down'], ['up']]);
  flushSync();
  await frames();
}

/** Right-clicks plot `id`'s body and picks the menu item that starts `label`. */
async function pickFromMenu(id, label) {
  const [x, y] = centre(bodyOf(id));
  bodyOf(id).dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, button: 2, clientX: x, clientY: y }));
  flushSync();
  await frames();
  [...document.querySelectorAll('[role=menuitem]')].find((m) => m.textContent.trim().startsWith(label)).click();
  flushSync();
  await frames();
}

/**
 * Turns the wheel over plot `id` of page `p` by each of `turns` px, 40 ms of
 * the page's time apart, at fraction `at` across its plot area, the pointer
 * brought there first, with Shift held when `shift`, and waits for the zoom
 * to rest.
 */
async function wheel(p, id, turns, { at = 0.5, shift = false } = {}) {
  const P = p.pv(id).geo.plot;
  await commands.mouse([['move', Math.round(P.x + at * P.w), Math.round(P.y + P.h / 2)]]);
  for (const [i, dy] of turns.entries()) {
    if (i) await wait(40);
    turnWheel(p, id, dy, { at, shift });
  }
  await wheelRest(p, id);
}

const gesture = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('drag'), at: fc.nat(), dx: fc.integer({ min: -160, max: 160 }), rest: fc.boolean() }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('wheel'), at: fc.nat(), turns: fc.array(fc.constantFrom(-120, 120), { minLength: 1, maxLength: 4 }) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('follow'), at: fc.nat() }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('addX'), at: fc.nat() }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('addXFollow'), at: fc.nat() }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('all'), on: fc.boolean() }) },
  { weight: 4, arbitrary: fc.record({ t: fc.constant('back'), at: fc.nat() }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('forward'), at: fc.nat() }) },
);

describe('a plot’s header', () => {
  it('has Back and Forward where the split buttons were, which its menu still has', async () => {
    const p = await page();
    try {
      for (const id of plotIds(p.ws.layout)) {
        expect(button(id, 'Back'), 'Back').not.toBeNull();
        expect(button(id, 'Forward'), 'Forward').not.toBeNull();
        expect(button(id, 'Split'), 'no split button').toBeNull();
      }
      const id = plotIds(p.ws.layout)[0];
      const [x, y] = centre(bodyOf(id));
      bodyOf(id).dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, button: 2, clientX: x, clientY: y }));
      flushSync();
      await frames();
      const items = [...document.querySelectorAll('[role=menuitem]')].map((m) => m.textContent.trim());
      expect(items.some((t) => t.startsWith('Split right')) && items.some((t) => t.startsWith('Split down'))).toBe(true);
      await pressKey('Escape');
    } finally {
      p.dispose();
    }
  });

  it('in a plot too narrow for them, has no Back and Forward, as it had no split buttons', async () => {
    const p = await page('grid');
    try {
      const ids = plotIds(p.ws.layout).filter((id) => leaf(id).getBoundingClientRect().width < 480);
      expect(ids.length).toBeGreaterThan(0);
      for (const id of ids) {
        expect(getComputedStyle(button(id, 'Back')).display, 'Back').toBe('none');
        expect(getComputedStyle(button(id, 'Forward')).display, 'Forward').toBe('none');
        for (const label of ['Show the legend', 'Pause', 'Maximize', 'Close plot']) expect(getComputedStyle(button(id, label)).display, label).not.toBe('none');
      }
    } finally {
      p.dispose();
    }
  });
});

describe('view history on the page', () => {
  it('makes each gesture one step of the tab, whatever plots it moves, and Back and Forward on any plot go through them', async () => {
    const count = { back: 0, several: 0, bursts: 0, rested: 0, cursorsKept: 0 };
    await fc.assert(
      // Each run, somewhere among the rest, places X cursors, so Back and
      // Forward go past steps with cursors placed, and drags every plot at
      // once, resting midway.
      fc.asyncProperty(fc.array(gesture, { minLength: 4, maxLength: 9 }), fc.nat(), fc.nat(), fc.nat(), async (rest, i, j, at) => {
        const gestures = rest.toSpliced(i % (rest.length + 1), 0, { t: 'addX', at }).toSpliced(j % (rest.length + 2), 0, { t: 'all', on: true }, { t: 'drag', at, dx: 120, rest: true });
        const p = await page();
        try {
          const ids = plotIds(p.ws.layout);
          // The model: what the tab showed after each step, and the one it shows now.
          const line = [shown(p)];
          let at = 0;
          const check = (what) => {
            expect(shown(p), `${what}: what the plots show`).toBe(line[at]);
            for (const id of ids) {
              expect(button(id, 'Back').disabled, `${what}: Back on ${id}`).toBe(at === 0);
              expect(button(id, 'Forward').disabled, `${what}: Forward on ${id}`).toBe(at === line.length - 1);
            }
          };
          const moved = (what) => {
            const now = shown(p);
            if (now === line[at]) return false;
            const before = JSON.parse(line[at]), after = JSON.parse(now);
            if (after.filter((s, i) => s !== before[i]).length > 1) count.several++;
            line.splice(at + 1, line.length, now);
            at++;
            check(what);
            return true;
          };
          const act = async (g) => {
            const id = ids[(g.at ?? 0) % ids.length];
            if (g.t === 'drag') {
              // A drag that rests longer than a burst's idle midway is still one step.
              const [x, y] = centre(bodyOf(id));
              const half = [['move', x + g.dx / 4, y], ['move', x + g.dx / 2, y]];
              await commands.mouse([['move', x, y], ['down'], ...half]);
              if (g.rest) await wait(IDLE_MS + 200);
              await commands.mouse([['move', x + (3 * g.dx) / 4, y], ['move', x + g.dx, y], ['up']]);
              await frames();
              if (moved('drag') && g.rest) count.rested++;
            } else if (g.t === 'wheel') {
              // The last burst has rested by now, so this one is a step of its own.
              await wait(IDLE_MS + 100);
              await wheel(p, id, g.turns);
              await frames();
              if (moved('wheel') && g.turns.length > 1) count.bursts++;
            } else if (g.t === 'follow') {
              const [x, y] = centre(bodyOf(id));
              await commands.mouse([['dblclick', x, y]]);
              await frames();
              moved('follow');
            } else if (g.t === 'addX' || g.t === 'addXFollow') {
              await pickFromMenu(id, 'Add X cursors');
              moved('add X cursors');
              // Following live then removes the cursors, which is an edit: no Back brings them back.
              if (g.t === 'addXFollow') await act({ t: 'follow', at: g.at });
            } else if (g.t === 'all') {
              await pressKey(g.on ? 'Control+a' : 'Escape');
              flushSync();
              check('select');
            } else {
              const back = g.t === 'back', before = line[at], cursors = cursorsOf(p);
              await press(id, back ? 'Back' : 'Forward');
              // Back and Forward retrace the views: no cursor moves.
              expect(cursorsOf(p), `${g.t}: the cursors`).toBe(cursors);
              if (back && at > 0) at--;
              else if (!back && at < line.length - 1) at++;
              check(g.t);
              if (back && line[at] !== before) {
                count.back++;
                if (/\d/.test(cursors)) count.cursorsKept++;
              }
            }
          };
          check('mounted');
          for (const g of gestures) await act(g);
          // Then all the way back and forward again, so every step made is checked.
          while (at > 0) await act({ t: 'back', at: at });
          while (at < line.length - 1) await act({ t: 'forward', at: at });
        } finally {
          p.dispose();
        }
      })
    );
    for (const [rule, n] of Object.entries(count)) expect(n, rule).toBeGreaterThan(0);
  }, 600_000);

  it('makes the window slider one step, dragged through its detents with a rest midway, or stepped by its keys in a burst', async () => {
    const p = await page();
    try {
      const ids = plotIds(p.ws.layout), id = ids[0];
      const start = shown(p);
      document.querySelector('button[aria-label="Time window"]').click();
      flushSync();
      await frames();
      const track = document.querySelector('[role=slider][aria-label="Time window"]');
      const r = track.getBoundingClientRect(), y = r.top + r.height / 2;
      const along = (f) => r.left + f * r.width;
      await commands.mouse([['move', along(0.2), y], ['down'], ['move', along(0.35), y], ['move', along(0.5), y]]);
      await wait(IDLE_MS + 200);
      await commands.mouse([['move', along(0.65), y], ['move', along(0.8), y], ['up']]);
      const dragged = shown(p);
      expect(dragged).not.toBe(start);
      // Every plot's window moved: none was selected, so the slider set them all.
      expect(new Set(ids.map((i) => p.views.get(i).window)).size).toBe(1);
      track.focus();
      for (const k of ['ArrowLeft', 'ArrowLeft', 'ArrowLeft']) await pressKey(k);
      flushSync();
      const keyed = shown(p);
      expect(keyed).not.toBe(dragged);
      await pressKey('Escape');
      await press(id, 'Back');
      expect(shown(p), 'Back past the keys').toBe(dragged);
      await press(id, 'Back');
      expect(shown(p), 'Back past the drag').toBe(start);
      expect(button(id, 'Back').disabled).toBe(true);
      await press(id, 'Forward');
      expect(shown(p)).toBe(dragged);
    } finally {
      p.dispose();
    }
  }, 60_000);
});

/** What plot `id` of page `p` shows, in one string. */
const one = (p, id) => seen(p.views.get(id).state());
/** Drags across plot `id`'s body by `dx` px, and lets go unless `hold`. */
async function dragOn(id, dx, hold = false) {
  const [x, y] = centre(bodyOf(id));
  await commands.mouse([['move', x, y], ['down'], ['move', x + dx / 2, y], ['move', x + dx, y], ...(hold ? [] : [['up']])]);
  await frames();
}
/** Clicks the top bar's button that reads `text`. */
function toolbar(text) {
  [...document.querySelectorAll('.top-bar button')].find((b) => b.textContent.trim() === text).click();
  flushSync();
}

describe('view history on the page, each kind of gesture', () => {
  it('makes arrow keys pressed quickly one step', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0], start = shown(p);
      p.ws.selection.only([id]);
      leaf(id).querySelector('.plot-card').focus();
      for (const k of ['ArrowLeft', 'ArrowLeft', 'ArrowLeft']) await pressKey(k);
      flushSync();
      expect(shown(p)).not.toBe(start);
      await press(id, 'Back');
      expect(shown(p)).toBe(start);
    } finally {
      p.dispose();
    }
  });

  it('makes the toolbar’s Live button one step of every plot', async () => {
    const p = await page();
    try {
      const ids = plotIds(p.ws.layout), start = shown(p);
      toolbar('Live');
      expect(ids.every((id) => !p.views.get(id).live)).toBe(true);
      await press(ids[1], 'Back');
      expect(shown(p)).toBe(start);
    } finally {
      p.dispose();
    }
  });

  it('has the toolbar’s Live say what the open tab’s plots do, after a tab switch with nothing selected too', async () => {
    const p = await page('rows', 1);
    try {
      const live = () => [...document.querySelectorAll('.top-bar button')].some((b) => b.textContent.trim() === 'Live');
      await dragOn(plotIds(p.ws.layout)[0], 120);
      expect(live(), 'a plot paused').toBe(false);
      await pressKey(']');
      flushSync();
      expect(live(), 'an empty tab follows live').toBe(true);
      await pressKey('[');
      flushSync();
      expect(live(), 'back, the plot is paused still').toBe(false);
      toolbar('Follow live');
      await press(plotIds(p.ws.layout)[0], 'Back');
      expect(p.views.get(plotIds(p.ws.layout)[0]).live, 'and Back pauses it again').toBe(false);
    } finally {
      p.dispose();
    }
  });

  it('makes the Y key one step of every plot selected', async () => {
    const p = await page();
    try {
      const ids = plotIds(p.ws.layout);
      ids.forEach((id) => p.pv(id).setYBounds(['shared'], 5, 6));
      const start = shown(p);
      p.ws.selection.only(ids);
      leaf(ids[0]).querySelector('.plot-card').focus();
      await pressKey('y');
      flushSync();
      expect(ids.every((id) => !JSON.stringify(p.views.get(id).state().yRanges).includes('[5,6]'))).toBe(true);
      await press(ids[2], 'Back');
      expect(shown(p)).toBe(start);
    } finally {
      p.dispose();
    }
  });

  it('makes a plot’s Pause one step of every plot selected', async () => {
    const p = await page();
    try {
      const ids = plotIds(p.ws.layout), start = shown(p);
      p.ws.selection.only(ids);
      flushSync();
      await press(ids[0], 'Pause');
      expect(ids.map((id) => p.views.get(id).live)).toEqual(ids.map(() => false));
      await press(ids[2], 'Back');
      expect(shown(p)).toBe(start);
    } finally {
      p.dispose();
    }
  });

  it('makes Y cursors placed from the menu an edit, which Ctrl+Z takes back, and no view step', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0];
      await pickFromMenu(id, 'Add Y cursors');
      expect(p.ws.plots[id].cursors?.y?.time, 'placed').toBeTruthy();
      expect(button(id, 'Back').disabled, 'no view moved, so Back has nowhere to go').toBe(true);
      // Once the menu has closed and given the page its keys back.
      await until(() => !document.querySelector('[role=menu]'));
      await pressKey('Control+z');
      flushSync();
      expect(p.ws.plots[id].cursors, 'undone').toBeUndefined();
    } finally {
      p.dispose();
    }
  });

  it('makes a spectrum’s pan and its y zoom steps, and Back puts each back, past the X cursors placed between, which stay', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0];
      p.history.group('Kind', () => p.ws.setPlot(id, { kind: 'spectrum' }));
      const f = Array.from({ length: 200 }, (_, i) => 1 + i), a = f.map((x) => 1 / x);
      p.hub.session.byKey.get(KEYS[0][0]).spectrum = { f, a, floor: null };
      // Zoomed in, so a drag has frequencies to pan to.
      p.ws.setView(id, { fRange: [20, 80] });
      flushSync();
      await frames(3);
      const start = { fRange: p.ws.plots[id].fRange ?? null, view: one(p, id) };
      await dragOn(id, -80);
      expect(p.ws.plots[id].fRange ?? null, 'the drag pans the frequencies').not.toBeNull();
      await pickFromMenu(id, 'Add X cursors');
      const placed = p.ws.plots[id].cursors;
      expect(placed?.f?.a, 'X cursors on the spectrum').toEqual(expect.any(Number));
      const P = p.pv(id).geo.plot, yAxis = bodyOf(id).getBoundingClientRect().left + 10;
      await wheel(p, id, [-120, -120], { at: (yAxis - P.x) / P.w });
      await frames();
      expect(Object.keys(p.views.get(id).state().yRanges), 'the y zoom').not.toEqual([]);
      await press(id, 'Back');
      expect(Object.keys(p.views.get(id).state().yRanges), 'Back past the y zoom').toEqual([]);
      await press(id, 'Back');
      expect([p.ws.plots[id].fRange ?? null, one(p, id)], 'Back past the pan').toEqual([start.fRange, start.view]);
      expect(p.ws.plots[id].cursors, 'the cursors stay').toEqual(placed);
    } finally {
      p.dispose();
    }
  });

  it('zooms a spectrum’s frequencies about the pointer by the wheel over its plot, a burst of turns one step that Back takes back', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0];
      p.history.group('Kind', () => p.ws.setPlot(id, { kind: 'spectrum' }));
      const f = Array.from({ length: 200 }, (_, i) => 1 + i);
      p.hub.session.byKey.get(KEYS[0][0]).spectrum = { f, a: f.map((x) => 1 / x), floor: null };
      flushSync();
      await frames(3);
      const pv = p.pv(id), P = pv.geo.plot, x = Math.round(P.x + P.w * 0.7), under = pv.dataAt(x);
      expect(p.ws.plots[id].fRange ?? null).toBeNull();
      await wheel(p, id, [-240, -240], { at: 0.7 });
      await frames(3);
      const [lo, hi] = p.ws.plots[id].fRange;
      expect([lo > 1, hi < 200], 'narrower at both ends').toEqual([true, true]);
      expect(Math.abs(pv.pxOf(under) - x), 'the frequency under the pointer stays there').toBeLessThan(1.5);
      // With Shift it zooms y, as over a time plot, and the frequencies stay.
      await wait(IDLE_MS + 100);
      await wheel(p, id, [-120], { at: 0.7, shift: true });
      await frames(3);
      expect(p.ws.plots[id].fRange).toEqual([lo, hi]);
      expect(Object.keys(p.views.get(id).state().yRanges)).not.toEqual([]);
      await press(id, 'Back');
      await press(id, 'Back');
      expect(p.ws.plots[id].fRange ?? null, 'Back past the y zoom, then the burst').toBeNull();
      expect(button(id, 'Back').disabled).toBe(true);
    } finally {
      p.dispose();
    }
  });

  it('keeps each tab’s steps, and goes back past those of a plot moved to another tab', async () => {
    const p = await page('rows', 1);
    try {
      const [a, b] = plotIds(p.ws.layout), start = one(p, a);
      await dragOn(a, 120);
      await dragOn(b, 120);
      const other = p.ws.tabs[1].id;
      p.history.group('Move', () => p.ws.movePlotToTab(b, other));
      flushSync();
      await frames();
      await press(a, 'Back');
      expect(one(p, a), 'Back went past the moved plot’s step to the drag').toBe(start);
      expect(button(a, 'Back').disabled).toBe(true);
      document.querySelector(`[role=tablist] [data-tab="${other}"]`).click();
      flushSync();
      await frames();
      expect(p.ws.activeId).toBe(other);
      expect(button(b, 'Back').disabled, 'the moved plot’s steps stay in the tab they were made in').toBe(true);
    } finally {
      p.dispose();
    }
  });

  it('while a plot is maximized, goes back past the steps of the plots hidden, and takes them up once they show', async () => {
    const p = await page();
    try {
      const [a, b] = plotIds(p.ws.layout), startA = one(p, a), startB = one(p, b);
      await dragOn(b, 120);
      await dragOn(a, 120);
      await press(a, 'Maximize');
      expect(p.ws.maximized).toEqual([a]);
      await press(a, 'Back');
      expect([one(p, a) === startA, one(p, b) === startB]).toEqual([true, false]);
      expect(button(a, 'Back').disabled).toBe(true);
      await press(a, 'Restore');
      await press(a, 'Back');
      expect(one(p, b)).toBe(startB);
    } finally {
      p.dispose();
    }
  });

  it('ends a drag’s step when its plot leaves the page mid-drag, so the next command is a step of its own', async () => {
    const p = await page('rows', 1);
    try {
      const id = plotIds(p.ws.layout)[0];
      await dragOn(id, 120, true);
      const dragged = one(p, id);
      expect(JSON.parse(dragged).live).toBe(false);
      await pressKey(']');
      await commands.mouse([['up']]);
      await pressKey('[');
      flushSync();
      await frames();
      await press(id, 'Follow live');
      expect(p.views.get(id).live).toBe(true);
      await press(id, 'Back');
      expect(one(p, id), 'Back past Follow live to the drag').toBe(dragged);
    } finally {
      p.dispose();
    }
  });
});

describe('a plot closed and brought back by Ctrl+Z', () => {
  it('shows its view as it was, and Back goes on through its steps', async () => {
    let checked = 0;
    await fc.assert(
      fc.asyncProperty(fc.nat(), fc.constantFrom('wheel', 'drag', 'addX'), async (k, how) => {
        const p = await page();
        try {
          const ids = plotIds(p.ws.layout), id = ids[k % ids.length], start = shown(p);
          if (how === 'wheel') await wheel(p, id, [-120, -120]);
          else if (how === 'drag') await dragOn(id, 120);
          else await pickFromMenu(id, 'Add X cursors');
          await frames();
          const was = one(p, id);
          expect(JSON.parse(was).live, 'the gesture paused it').toBe(false);
          await press(id, 'Close plot');
          expect(p.ws.plots[id], 'closed').toBeUndefined();
          expect(p.hub.holding, 'a closed plot holds no history').toBe(false);
          const read = [];
          p.hub.session.fetchWindow = (plot, what) => read.push([plot, what]);
          await pressKey('Control+z');
          flushSync();
          await frames();
          expect(p.ws.plots[id], 'back').toBeDefined();
          expect(one(p, id), 'its view').toBe(was);
          expect(p.hub.holding, 'paused again, it holds its history').toBe(true);
          expect(read, 'and reads it again').toContainEqual([id, 'detail']);
          await press(ids[(k + 1) % ids.length], 'Back');
          expect(shown(p), 'Back past the gesture').toBe(start);
          checked++;
        } finally {
          p.dispose();
        }
      })
    );
    expect(checked).toBeGreaterThan(0);
  }, 300_000);

  it('starts at the oldest time held, with the notice, when the hub dropped where it started while it was gone', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0], memory = (oldest) => p.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: 0, rate: 0, oldest: { 0: oldest } });
      memory(900);
      await dragOn(id, 120);
      const v = p.views.get(id), start = v.right - v.span;
      await press(id, 'Close plot');
      memory(start + 1);
      await pressKey('Control+z');
      flushSync();
      await frames();
      expect(v.right - v.span).toBeCloseTo(start + 1);
      expect(v.right).toBeLessThanOrEqual(p.hub.now(clock.now()));
      expect(document.body.textContent).toContain(GONE);
    } finally {
      p.dispose();
    }
  });
});

describe('a kind switch', () => {
  it('is an edit and a view step: Back crosses it and goes on through the time steps before it, and undo and redo never leave the two at odds', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0], kind = () => p.ws.plots[id].kind;
      const keys = async (k) => (await until(() => !document.querySelector('[role=menu]')), await pressKey(k), flushSync(), await frames());
      const start = one(p, id);
      await dragOn(id, 120);
      const panned = one(p, id);
      await plotMenu(id);
      await pickItem('Kind', 'Spectrum');
      expect(kind(), 'switched').toBe('spectrum');
      expect(p.history.undos.at(-1).label).toBe('Kind Spectrum');
      // Back crosses the switch, by an edit of its own, then goes on through the pan before it.
      await press(id, 'Back');
      expect([kind(), one(p, id)], 'Back across the switch').toEqual(['time', panned]);
      expect(p.history.undos.at(-1).label, 'Back’s kind change is an edit').toBe('Kind Time');
      await press(id, 'Back');
      expect([kind(), one(p, id)], 'Back on, past the pan').toEqual(['time', start]);
      await press(id, 'Forward');
      await press(id, 'Forward');
      expect([kind(), one(p, id)], 'Forward, twice').toEqual(['spectrum', panned]);
      // Ctrl+Z undoes Forward's kind change; that is a step too, so Back shows the spectrum again.
      await keys('Control+z');
      expect(kind(), 'undone').toBe('time');
      expect(button(id, 'Forward').disabled, 'nothing ahead of what an undo showed').toBe(true);
      await press(id, 'Back');
      expect(kind(), 'Back from what the undo showed').toBe('spectrum');
      await press(id, 'Forward');
      expect(kind()).toBe('time');
      // Redo: Back's own edit emptied it, so the kind stays, and the two still agree.
      await keys('Control+y');
      expect(kind(), 'nothing to redo').toBe('time');
      await press(id, 'Back');
      await keys('Control+z');
      await keys('Control+y');
      expect(kind(), 'undo, then redo, of Back’s switch').toBe('spectrum');
      await press(id, 'Back');
      expect(kind(), 'and Back crosses the redo').toBe('time');
    } finally {
      p.dispose();
    }
  });
});

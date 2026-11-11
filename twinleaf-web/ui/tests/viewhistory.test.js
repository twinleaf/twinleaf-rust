// View history on the page (round 2, View history #1, #2, #3, #5 and #6,
// round 1 #29): each plot's header has Back and Forward in place of the
// split buttons, which stay in its menu and keys. Random runs of drags that
// rest midway, wheel bursts, pauses, X cursors added from the menu, going
// live by each route (a double-click, End, Space, a plot's play button, the
// toolbar's Live), Ctrl+A and Escape, Back and Forward pressed on any plot,
// Ctrl+Z and Ctrl+Y, a tab switch and the page loaded again, with the real
// mouse and keys: each gesture that moves a view is one step of the tab's,
// whatever plots it moves, and Back and Forward go through them, each
// plot's buttons pressable just when the tab has a step to go to. Cursors
// are edits, which Ctrl+Z undoes, and no plot that follows live has time X
// cursors: going live takes them; Back or Forward that shows a live plot
// paused puts back those it had as it last went live from that view, by
// any route, Back and Forward among them; an undo or a redo changes them as the
// edit history has them, and one that brings them back to a live plot shows
// the paused view it left; a page loaded again on a hub that holds what
// it showed shows each plot paused as it was, with its cursors, and each
// live one live; nothing else changes them. The window field's arrows and wheel turns in a
// burst are one step too, and so is each window typed in it.
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
import { centre, clickOn, frames, mountPage, pickItem, plotMenu, pressKey, turnWheel, typeInto, until, wait, wheelRest, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { GONE, IDLE_MS } from '../src/lib/viewhistory.svelte.js';
import { hasTimeX, xOf } from '../src/lib/cursors.js';
import { SAVE_KEY } from '../src/lib/tabs.js';

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
async function page(preset = 'rows', more = 0, options = {}) {
  const p = await mountPage(KEYS, preset, more, KEYS.flat(), [], options);
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

/** Every way the page has of following live, each on plot `at` or the page: a double-click, End, Space, the plot's play button and the toolbar's Live. */
const LIVE = ['dblclick', 'End', 'Space', 'play', 'toolbar'];
const gesture = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('drag'), at: fc.nat(), dx: fc.integer({ min: -160, max: 160 }), rest: fc.boolean() }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('wheel'), at: fc.nat(), turns: fc.array(fc.constantFrom(-120, 120), { minLength: 1, maxLength: 4 }) }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('follow'), at: fc.nat(), how: fc.constantFrom(...LIVE) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('pause'), at: fc.nat() }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('addX'), at: fc.nat() }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('addXFollow'), at: fc.nat(), how: fc.constantFrom(...LIVE) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('all'), on: fc.boolean() }) },
  { weight: 4, arbitrary: fc.record({ t: fc.constant('back'), at: fc.nat() }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('forward'), at: fc.nat() }) },
  { weight: 2, arbitrary: fc.constant({ t: 'undo' }) },
  { weight: 1, arbitrary: fc.constant({ t: 'redo' }) },
  { weight: 1, arbitrary: fc.constant({ t: 'tab' }) },
  { weight: 1, arbitrary: fc.constant({ t: 'reload' }) },
);

/** The time X cursors of every plot of page `p`'s tab, in layout order. */
const timeXs = (p) => plotIds(p.ws.layout).map((id) => xOf(p.ws.plots[id].cursors, 'time'));
/** Whether a plot of any tab of page `p` whose view follows live has time X cursors. */
const liveWithX = (p) => p.ws.tabs.flatMap((t) => Object.entries(t.plots)).filter(([id, plot]) => p.views.get(id)?.live && hasTimeX(plot.cursors)).map(([id]) => id);
/** Waits for menus to close, as they give the page its keys back, then presses `k`. */
const keysOn = async (k) => (await until(() => !document.querySelector('[role=menu]')), await pressKey(k), flushSync(), await frames());
/** Goes live, or pauses where that button or key toggles, on plot `id` of page `p` or the page, by route `how` of LIVE. */
async function followBy(p, id, how) {
  if (how === 'dblclick') await commands.mouse([['dblclick', ...centre(bodyOf(id))]]);
  else if (how === 'End') (leaf(id).querySelector('.plot-card').focus(), await keysOn('End'));
  else if (how === 'Space') (document.activeElement?.blur(), await keysOn('Space'));
  else if (how === 'play') await press(id, p.views.get(id).live ? 'Pause' : 'Follow live');
  else toolbar([...document.querySelectorAll('.top-bar button')].some((b) => b.textContent.trim() === 'Live') ? 'Live' : 'Follow live');
  flushSync();
  await frames();
}

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
  it('makes each gesture one step of the tab, whatever plots it moves, and Back and Forward on any plot go through them; a plot that follows live never has time X cursors', async () => {
    const count = { back: 0, several: 0, bursts: 0, rested: 0, cursorsKept: 0, xTaken: 0, xBack: 0, xUndone: 0, reloaded: 0, ...Object.fromEntries(LIVE.map((how) => [how, 0])) };
    await fc.assert(
      // Each run, somewhere among the rest, places X cursors, so Back and
      // Forward go past steps with cursors placed, and drags every plot at
      // once, resting midway.
      fc.asyncProperty(fc.array(gesture, { minLength: 4, maxLength: 9 }), fc.nat(), fc.nat(), fc.nat(), async (rest, i, j, at) => {
        const gestures = rest.toSpliced(i % (rest.length + 1), 0, { t: 'addX', at }).toSpliced(j % (rest.length + 2), 0, { t: 'all', on: true }, { t: 'drag', at, dx: 120, rest: true });
        let p = await page('rows', 1);
        try {
          const ids = plotIds(p.ws.layout);
          // The model: what the tab showed after each step, with the time X
          // cursors of its plots as the step began and as it ended, and the
          // step it shows now; and the view each plot left as it last went live.
          let line = [{ shows: shown(p), xs: null }], at = 0;
          const left = new Map();
          const check = (what) => {
            expect(shown(p), `${what}: what the plots show`).toBe(line[at].shows);
            for (const id of ids) {
              expect(button(id, 'Back').disabled, `${what}: Back on ${id}`).toBe(at === 0);
              expect(button(id, 'Forward').disabled, `${what}: Forward on ${id}`).toBe(at === line.length - 1);
            }
            expect(liveWithX(p), `${what}: plots that follow live with time X cursors`).toEqual([]);
          };
          const moved = (what, xs) => {
            const now = shown(p);
            if (now === line[at].shows) return false;
            const before = JSON.parse(line[at].shows), after = JSON.parse(now);
            if (after.filter((s, i) => s !== before[i]).length > 1) count.several++;
            line.splice(at + 1, line.length, { shows: now, xs: { before: xs, after: timeXs(p) } });
            at++;
            check(what);
            return true;
          };
          const act = async (g) => {
            const id = ids[(g.at ?? 0) % ids.length];
            // What the gesture starts from: each plot's view, live or paused, and time X cursors.
            const views = ids.map((i) => one(p, i)), xs = timeXs(p), cursors = cursorsOf(p);
            const live = () => ids.map((i) => p.views.get(i).live);
            const wasLive = live();
            if (g.t === 'drag') {
              // A drag that rests longer than a burst's idle midway is still one step.
              const [x, y] = centre(bodyOf(id));
              const half = [['move', x + g.dx / 4, y], ['move', x + g.dx / 2, y]];
              await commands.mouse([['move', x, y], ['down'], ...half]);
              if (g.rest) await wait(IDLE_MS + 200);
              await commands.mouse([['move', x + (3 * g.dx) / 4, y], ['move', x + g.dx, y], ['up']]);
              await frames();
              if (moved('drag', xs) && g.rest) count.rested++;
            } else if (g.t === 'wheel') {
              // The last burst has rested by now, so this one is a step of its own.
              await wait(IDLE_MS + 100);
              await wheel(p, id, g.turns);
              await frames();
              if (moved('wheel', xs) && g.turns.length > 1) count.bursts++;
            } else if (g.t === 'follow' || g.t === 'pause') {
              await until(() => !document.querySelector('[role=menu]'));
              await followBy(p, id, g.t === 'pause' ? 'play' : g.how);
              if (g.t === 'follow') count[g.how]++;
              moved(g.how ?? 'pause', xs);
            } else if (g.t === 'addX' || g.t === 'addXFollow') {
              await pickFromMenu(id, 'Add X cursors');
              moved('add X cursors', xs);
              if (g.t === 'addXFollow') await act({ t: 'follow', at: g.at, how: g.how });
              return;
            } else if (g.t === 'all') {
              await keysOn(g.on ? 'Control+a' : 'Escape');
              check('select');
            } else if (g.t === 'undo' || g.t === 'redo') {
              const e = g.t === 'undo' ? p.history.undos.at(-1) : p.history.redos.at(-1);
              document.activeElement?.blur();
              await keysOn(g.t === 'undo' ? 'Control+z' : 'Control+y');
              moved(g.t, xs);
              // An undo or a redo changes the cursors just as the edit history has them.
              const side = e && (g.t === 'undo' ? e.before : e.after).tabs.find((t) => t.id === p.ws.activeId);
              if (side) expect(cursorsOf(p), `${g.t}: the cursors`).toBe(JSON.stringify(ids.map((i) => side.plots[i]?.cursors ?? null)));
              // One that brings time X cursors back to a live plot shows the paused view it left as it went live.
              ids.forEach((i, k) => {
                if (!wasLive[k] || !hasTimeX(p.ws.plots[i].cursors)) return;
                expect(p.views.get(i).live, `${g.t}: ${i} paused`).toBe(false);
                if (left.has(i)) expect(one(p, i), `${g.t}: ${i} as it left`).toBe(left.get(i));
                count.xUndone++;
              });
            } else if (g.t === 'tab') {
              // To the other tab and back: nothing moves.
              await keysOn(']');
              await keysOn('[');
              expect(cursorsOf(p), 'tab: the cursors').toBe(cursors);
              check('tab');
            } else if (g.t === 'reload') {
              // The page loaded again from what it saved, on a hub that holds all it showed: a plot saved
              // paused shows what it showed with its cursors, one saved live follows live; no step to go to.
              flushSync();
              const saved = JSON.parse(localStorage.getItem(SAVE_KEY)), now = p.hub.now(clock.now());
              p.dispose();
              p = await page('rows', 1, { saved: { [SAVE_KEY]: saved }, fromSaved: true });
              p.hub.session.offset = now + 1 - clock.now() / 1000;
              p.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: 0, rate: 0, oldest: Object.fromEntries(KEYS.flat().map((_, i) => [i, 0])) });
              flushSync();
              await frames();
              expect(plotIds(p.ws.layout), 'reload: the plots').toEqual(ids);
              expect(ids.map((i) => p.views.get(i).live), 'reload: live').toEqual(wasLive);
              expect(ids.map((i, k) => (wasLive[k] ? null : one(p, i))), 'reload: the paused views').toEqual(views.map((s, k) => (wasLive[k] ? null : s)));
              expect(timeXs(p), 'reload: time X cursors').toEqual(xs);
              line = [{ shows: shown(p), xs: null }];
              at = 0;
              left.clear();
              count.reloaded++;
              check('reload');
            } else {
              const back = g.t === 'back', before = line[at], from = JSON.parse(line[at].shows);
              const past = line[back ? at : at + 1];
              await press(id, back ? 'Back' : 'Forward');
              if (back && at > 0) at--;
              else if (!back && at < line.length - 1) at++;
              check(g.t);
              // Back and Forward retrace the views. A plot shown live loses its time X cursors; one live shown paused gets back those it had as it last went live from there.
              const to = JSON.parse(line[at].shows);
              const want = xs.map((x, k) => {
                if (line[at] === before) return x;
                if (JSON.parse(to[k]).live) return hasTimeX({ x }) ? (count.xTaken++, { a: null, b: null }) : x;
                const then = past.xs[back ? 'before' : 'after'][k];
                return JSON.parse(from[k]).live && hasTimeX({ x: then }) ? (count.xBack++, then) : x;
              });
              expect(timeXs(p), `${g.t}: the time X cursors`).toEqual(want);
              // A paused plot shown live leaves the step's paused side with the cursors it had, for going back there.
              if (line[at] !== before) xs.forEach((x, k) => JSON.parse(to[k]).live && !JSON.parse(from[k]).live && (past.xs[back ? 'after' : 'before'][k] = x));
              if (back && line[at] !== before) {
                count.back++;
                if (/\d/.test(cursors)) count.cursorsKept++;
              }
            }
            // Going live by any route takes the time X cursors of the plots it made live; nothing else here changes a cursor.
            if (!['addX', 'undo', 'redo', 'back', 'forward', 'reload'].includes(g.t)) {
              const now = live();
              expect(timeXs(p), `${g.t}: the time X cursors`).toEqual(xs.map((x, k) => (!wasLive[k] && now[k] ? { a: null, b: null } : x)));
            }
            ids.forEach((i, k) => !wasLive[k] && p.views.get(i).live && left.set(i, views[k]));
          };
          check('mounted');
          for (const g of gestures) await act(g);
          // Then all the way back and forward again, so every step made is checked.
          while (at > 0) await act({ t: 'back', at: at });
          while (at < line.length - 1) await act({ t: 'forward', at: at });
        } finally {
          p.dispose();
        }
      }),
      // X cursors placed, then live by each route, with Back, Forward, undo, redo, a tab switch and a reload between; then random runs.
      withExamples([[[
        { t: 'addXFollow', at: 0, how: 'dblclick' }, { t: 'back', at: 0 }, { t: 'forward', at: 1 },
        { t: 'addXFollow', at: 1, how: 'End' }, { t: 'undo' }, { t: 'redo' },
        { t: 'addX', at: 2 }, { t: 'all', on: true }, { t: 'drag', at: 0, dx: 80, rest: false }, { t: 'follow', at: 0, how: 'Space' }, { t: 'back', at: 1 },
        { t: 'addXFollow', at: 2, how: 'play' }, { t: 'tab' },
        { t: 'addXFollow', at: 0, how: 'toolbar' }, { t: 'undo' }, { t: 'reload' }, { t: 'addX', at: 1 }, { t: 'back', at: 1 },
      ], 0, 0, 0],
      // A press held still on an X cursor's line, after a drag moved it there: it moves no cursor.
      [[{ t: 'wheel', at: 0, turns: [-120] }, { t: 'wheel', at: 0, turns: [-120] }, { t: 'back', at: 0 }, { t: 'drag', at: 0, dx: 69, rest: false }, { t: 'forward', at: 0 }, { t: 'drag', at: 0, dx: 0, rest: false }], 2147483627, 0, 0]]),
    );
    // A replay of one failure runs that case alone.
    if (fc.readConfigureGlobal().path !== undefined) return;
    for (const [rule, n] of Object.entries(count)) expect(n, rule).toBeGreaterThan(0);
  }, 900_000);

  it('makes the window field one step: its arrows and wheel turns in a burst, and each window typed', async () => {
    const p = await page();
    try {
      const ids = plotIds(p.ws.layout), id = ids[0];
      const windows = () => [...new Set(ids.map((i) => p.views.get(i).window))];
      const input = document.querySelector('.top-bar input[aria-label="Time window"]');
      const start = shown(p);
      expect(windows()).toEqual([10]);
      await clickOn(input);
      await pressKey('ArrowDown');
      await pressKey('ArrowDown');
      input.dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY: -100 }));
      flushSync();
      // Every plot's window moved: none was selected, so the field set them all.
      expect(windows()).toEqual([1]);
      const stepped = shown(p);
      // A window typed is a step of its own, however soon after the steps.
      await typeInto(input, '45');
      await pressKey('Enter');
      flushSync();
      expect(windows()).toEqual([45]);
      await press(id, 'Back');
      expect(shown(p), 'Back past the window typed').toBe(stepped);
      await press(id, 'Back');
      expect(shown(p), 'Back past the steps').toBe(start);
      expect(button(id, 'Back').disabled).toBe(true);
      await press(id, 'Forward');
      expect(shown(p)).toBe(stepped);
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

  it('goes live at the span shown by Space and a plot’s play button, and back at the window by End, a double-click and the toolbar’s Live', async () => {
    // One plot, so Space, which pauses while any plot it acts on is live, acts on it alone.
    const p = await mountPage([KEYS[0]], 'rows', 0, KEYS[0]);
    p.hub.session.offset = 1000;
    p.hub.session.fetchWindow = () => {};
    try {
      const id = plotIds(p.ws.layout)[0], v = p.views.get(id);
      const KEEPS = { Space: true, play: true, End: false, dblclick: false, toolbar: false };
      expect(Object.keys(KEEPS).sort(), 'every route').toEqual([...LIVE].sort());
      for (const how of LIVE) {
        // Zoomed in, which pauses the plot at a span narrower than its window.
        await wait(IDLE_MS + 100);
        await wheel(p, id, [-120, -120]);
        const span = v.span;
        expect([v.live, span < v.window], `${how}: zoomed in, paused`).toEqual([false, true]);
        await until(() => !document.querySelector('[role=menu]'));
        await followBy(p, id, how);
        expect([v.live, v.span], how).toEqual([true, KEEPS[how] ? span : v.window]);
      }
    } finally {
      p.dispose();
    }
  });

  it('has Ctrl+Z take Follow live back to the paused view with its cursors, and Ctrl+Y follow live again as it did', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0], v = p.views.get(id);
      await pickFromMenu(id, 'Add X cursors');
      const paused = one(p, id), cursors = p.ws.plots[id].cursors;
      await followBy(p, id, 'End');
      const live = one(p, id);
      expect([v.live, hasTimeX(p.ws.plots[id].cursors)], 'live, no time X cursors').toEqual([true, false]);
      document.activeElement?.blur();
      await keysOn('Control+z');
      expect([one(p, id), p.ws.plots[id].cursors], 'undone').toEqual([paused, cursors]);
      await keysOn('Control+y');
      expect([one(p, id), hasTimeX(p.ws.plots[id].cursors)], 'redone').toEqual([live, false]);
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

// A page loaded again shows a plot saved paused paused at its saved view
// with its cursors until the hub first says what it holds; it stays so when
// the hub holds that stretch, and else follows live at its window without
// its time X cursors, by no step of either history. A plot saved live loads live.
describe('a page loaded again', () => {
  /** The hub's report of what it holds: each trace from `oldest`. */
  const memory = (p, oldest) => (p.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: 0, rate: 0, oldest: Object.fromEntries(KEYS.flat().map((_, i) => [i, oldest])) }), flushSync());

  it('keeps a plot saved paused paused with its cursors while the hub holds what it showed, and else has it follow live without its time X cursors', async () => {
    for (const held of [true, false]) {
      let p = await page();
      try {
        const [id, other] = plotIds(p.ws.layout);
        memory(p, 900);
        await pickFromMenu(id, 'Add X cursors');
        await pickFromMenu(id, 'Add Y cursors');
        const v = p.views.get(id), view = one(p, id), cursors = p.ws.plots[id].cursors, start = v.right - v.span, now = p.hub.now(clock.now());
        expect([v.live, hasTimeX(cursors), !!cursors.y], 'paused, with X and Y cursors').toEqual([false, true, true]);
        flushSync();
        const saved = JSON.parse(localStorage.getItem(SAVE_KEY));
        p.dispose();
        p = await page('rows', 0, { saved: { [SAVE_KEY]: saved }, fromSaved: true });
        // Five seconds later on the hub's clock.
        p.hub.session.offset = now + 5 - clock.now() / 1000;
        await frames();
        const w = p.views.get(id);
        // Before the hub says: as saved.
        expect([one(p, id), p.ws.plots[id].cursors, p.views.get(other).live], `${held}: before the hub says`).toEqual([view, cursors, true]);
        memory(p, held ? 900 : start + 1);
        await frames();
        if (held) expect([one(p, id), p.ws.plots[id].cursors], 'held: as saved').toEqual([view, cursors]);
        else {
          expect([w.live, w.span, p.ws.plots[id].cursors], 'gone: live at its window, the Y cursors kept').toEqual([true, w.window, { y: cursors.y }]);
          expect(liveWithX(p)).toEqual([]);
        }
        expect([p.history.undos.length, button(id, 'Back').disabled], `${held}: no step`).toEqual([0, true]);
        // The hub's later reports change nothing.
        memory(p, now + 3);
        await frames();
        expect(w.live, `${held}: after a later report`).toBe(!held);
      } finally {
        p.dispose();
      }
    }
  });
});

describe('a page loaded again, edited before the hub says', () => {
  it('drops the time X cursors of a plot that then follows live from every step undo and redo keep, so neither brings them back', async () => {
    let p = await page();
    try {
      const [id] = plotIds(p.ws.layout);
      await pickFromMenu(id, 'Add X cursors');
      const v = p.views.get(id), start = v.right - v.span, now = p.hub.now(clock.now());
      flushSync();
      const saved = JSON.parse(localStorage.getItem(SAVE_KEY));
      p.dispose();
      p = await page('rows', 0, { saved: { [SAVE_KEY]: saved }, fromSaved: true });
      p.hub.session.offset = now + 5 - clock.now() / 1000;
      await frames();
      // An edit while the plot is loaded paused with its cursors: the legend shown.
      await keysOn('Escape');
      p.ws.selection.only([id]);
      leaf(id).querySelector('.plot-card').focus();
      await keysOn('l');
      expect([p.ws.plots[id].legend, hasTimeX(p.ws.plots[id].cursors)], 'the legend shown, the cursors still placed').toEqual([true, true]);
      // The hub no longer holds what the plot showed.
      p.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: 0, rate: 0, oldest: Object.fromEntries(KEYS.flat().map((_, i) => [i, start + 1])) });
      flushSync();
      await frames();
      expect([p.views.get(id).live, hasTimeX(p.ws.plots[id].cursors)], 'live, no time X cursors').toEqual([true, false]);
      document.activeElement?.blur();
      for (const k of ['Control+z', 'Control+y', 'Control+z']) {
        await keysOn(k);
        expect([p.ws.plots[id].legend, p.views.get(id).live, hasTimeX(p.ws.plots[id].cursors)], k).toEqual([k === 'Control+y', true, false]);
      }
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

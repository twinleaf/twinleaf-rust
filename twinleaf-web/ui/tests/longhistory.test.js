// A plot on a hub that holds hours: the History setting goes to 24 hours,
// and a plot reads all of it. Each property runs over the History setting,
// from ten minutes to a day, how much of it the hub holds, how far the
// page's clock is behind the hub, and the device time, on a hub that says
// what it holds and answers reads of any range without the samples of an
// hour being fed (harness `holdHistory`):
// - zoomed all the way out, live or paused, the plot is as wide as the
//   History setting on a hub that has filled it, and as wide as what the
//   hub holds on one that has not, from the first turn of the wheel after a
//   pause; a view further behind the hub than the hub's headroom starts at
//   what the hub holds;
// - Back returns to the view before, to the bit, and Forward to the one
//   after;
// - the wheel, from the narrowest view to the widest and back about the
//   same point, ends where it began;
// - however long the span, the page asks the hub for a plot's width of
//   points, and holds no more;
// - on a hub a tenth full, half full and full, nothing is drawn before the
//   oldest time held, live or paused, by the window set, a double-click,
//   End, Back, Forward or the wheel, and a live plot regains a window wider
//   than the hub held once the hub holds it.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, frames, HISTORY_HEADROOM, holdHistory, leafOf, mountPage, pressKey, settle, turnWheel, turnWheelToRest, wheelRest, until, wait, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { WINDOW_LIMITS, windowText } from '../src/lib/window.js';
import { GONE } from '../src/lib/viewhistory.svelte.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** The History settings tried, in seconds: ten minutes, an hour, six hours, a day. */
const HISTORIES = [600, 3600, 6 * 3600, 86400];
/** Wheel turns of 400 px that take any view to the widest or the narrowest: each is a factor of e^0.6, and a day is e^17.6 of the narrowest. */
const ALL_THE_WAY = 40;
/** Random cases tried beyond the examples by the property that sweeps the wheel from the narrowest view to the widest and back. */
const SWEEPS = 4;

/**
 * What a hub holds as a plot looks at it: its History setting, how much of
 * it, the device time, and how far the page's clock is behind the hub's
 * newest sample, less than the hub's headroom with the view's delay. A hub
 * still filling holds a share of the setting; a full one holds the setting
 * and its headroom.
 */
const holding = fc.record({
  history: fc.constantFrom(...HISTORIES),
  fill: fc.oneof(fc.record({ full: fc.constant(false), share: fc.double({ min: 0.02, max: 0.99, noNaN: true }) }), fc.constant({ full: true })),
  behind: fc.double({ min: 0, max: HISTORY_HEADROOM - 0.5, noNaN: true }),
  now: fc.double({ min: 90_000, max: 1_000_000, noNaN: true }),
});
const held = ({ history, fill, behind, now }) => ({ history, now, behind, ...(fill.full ? {} : { seconds: history * fill.share }) });
/** A full hub at each History setting, and one a quarter full. */
const EACH = HISTORIES.flatMap((history) => [[{ history, fill: { full: true }, behind: 0.3, now: 100_000 }], [{ history, fill: { full: false, share: 0.25 }, behind: 0, now: 100_000 }]]);

describe('a plot on a hub that holds hours', () => {
  // A page each, so what one test leaves, or a property still running as
  // it times out, is not on the next one's page.
  let page, id, view;
  beforeEach(async () => {
    page = await mountPage([SIGNALS], 'columns', 0, SIGNALS);
    id = plotIds(page.ws.layout)[0];
    view = page.views.of(id);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    page.dispose();
  });

  /** The plot live, at its first window, on a hub holding `h`, drawn: what the hub holds. */
  async function live(h) {
    view.follow(true);
    view.setSpan(10);
    const hub = holdHistory(page, held(h));
    await settle(2);
    return hub;
  }
  /** The plot on a hub holding `h`, paused: what the hub holds, and where the plot paused. */
  async function paused(h) {
    const hub = await live(h);
    view.pause(clock.now());
    return { ...hub, at: view.pausedAt };
  }
  /** Clicks the plot's header button whose label starts `label`, as it stands once the page has taken in what came before. */
  const press = (label) => {
    flushSync();
    leafOf(id).querySelector(`.plot-head button[aria-label^="${label}"]`).click();
    flushSync();
  };
  const shows = () => ({ live: view.live, right: view.right, span: view.span, pausedAt: view.pausedAt });

  it('zooms out, paused, to the History setting exactly on a hub that has filled it, and to what the hub holds on one that has not', async () => {
    const seen = { full: 0, filling: 0 };
    await fc.assert(
      fc.asyncProperty(holding, async (h) => {
        const { oldest, at } = await paused(h);
        // No frame is drawn between the pause and the wheel; the ease after it draws.
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        const left = view.right - view.span;
        expect(view.right, 'ends at the pause').toBe(at);
        if (h.fill.full) {
          expect(view.span, `as wide as History ${h.history}`).toBeCloseTo(h.history, 6);
          expect(windowText(view.span), 'and reads so').toBe(windowText(h.history));
          expect(oldest, 'the hub holds all of it').toBeLessThanOrEqual(left);
        } else {
          expect(left, `starts at the oldest time held, ${oldest}`).toBeCloseTo(oldest, 6);
          expect(view.span).toBeLessThan(h.history);
        }
        // Drawn, and turned again, it stays.
        const widest = shows();
        await frames();
        await turnWheelToRest(page, id, 400, { n: 3 });
        expect(shows()).toEqual(widest);
        seen[h.fill.full ? 'full' : 'filling']++;
      }),
      withExamples(EACH),
    );
    expect(seen.full).toBeGreaterThanOrEqual(HISTORIES.length);
    expect(seen.filling).toBeGreaterThanOrEqual(HISTORIES.length);
  }, 60_000);

  it('zooms out, live, to the History setting exactly on a hub that has filled it, and goes on streaming', async () => {
    await fc.assert(
      fc.asyncProperty(holding, async (h) => {
        const { oldest, now } = await live(h);
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        expect(view.live).toBe(true);
        const [left, right] = view.range(now);
        expect(right).toBe(now - view.delay);
        if (h.fill.full) {
          expect(view.span, `as wide as History ${h.history}`).toBeCloseTo(h.history, 6);
          expect(windowText(view.span), 'and reads so').toBe(windowText(h.history));
          expect(oldest, 'the hub holds all of it').toBeLessThanOrEqual(left);
        } else expect(view.span, 'as wide as what the hub holds, or its window').toBeCloseTo(Math.max(10, right - oldest), 6);
        // Paused there, it shows the same.
        view.pause(clock.now());
        await turnWheelToRest(page, id, 400, { n: 3 });
        if (h.fill.full || right - oldest >= 10) expect(view.range(now)).toEqual([left, right]);
      }),
      withExamples(EACH),
    );
  }, 60_000);

  it('starts at what the hub holds, short of the History setting, when the view is further behind the hub than the headroom', async () => {
    for (const history of HISTORIES) {
      for (const behind of [HISTORY_HEADROOM, HISTORY_HEADROOM + 3]) {
        const { oldest, at } = await paused({ history, fill: { full: true }, behind, now: 100_000 });
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        // The view's time is its delay and the clock's lag behind the hub's newest sample: that much less the headroom is gone.
        const gone = view.delay + behind - HISTORY_HEADROOM;
        expect(view.right - view.span, `${history} s, ${behind} s behind`).toBeCloseTo(oldest, 6);
        expect(view.span).toBeCloseTo(history - gone, 6);
        expect(view.right).toBe(at);
      }
    }
  }, 60_000);

  it('goes Back to the view before to the bit, the widest view of a full hub among them, with no notice', async () => {
    await fc.assert(
      fc.asyncProperty(holding, fc.double({ min: 0.05, max: 0.95, noNaN: true }), fc.integer({ min: 1, max: 30 }), async (h, anchor, turns) => {
        await paused(h);
        const first = shows();
        // Three steps: out to the widest view, a turn on the y axis between, and in about `anchor`.
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        const widest = shows();
        turnWheel(page, id, 120, { shift: true });
        await turnWheelToRest(page, id, -400, { n: turns, at: anchor });
        const zoomed = shows();
        expect(zoomed.span).toBeLessThan(widest.span);
        press('Back');
        expect(shows(), 'Back to the widest view').toEqual(widest);
        press('Back');
        press('Back');
        expect(shows(), 'Back to the view it paused at').toEqual(first);
        for (let i = 0; i < 3; i++) press('Forward');
        expect(shows(), 'Forward to the last view').toEqual(zoomed);
        expect(document.body.textContent, 'nothing was dropped').not.toContain(GONE);
      }),
      withExamples(EACH.map(([h]) => [h, 0.5, 8])),
    );
  }, 60_000);

  it('ends where it began when the wheel goes from the narrowest view to the widest and back about the same point', async () => {
    await fc.assert(
      fc.asyncProperty(holding, fc.double({ min: 0.02, max: 0.98, noNaN: true }), async (h, anchor) => {
        await paused(h);
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        const widest = shows();
        const under = await turnWheelToRest(page, id, -400, { n: ALL_THE_WAY, at: anchor });
        const narrowest = shows();
        expect(narrowest.span).toBe(WINDOW_LIMITS[0]);
        // The moment under the pointer in the widest view is under it in the narrowest.
        const moment = widest.right - (1 - under) * widest.span;
        expect(Math.abs(narrowest.right - (1 - under) * narrowest.span - moment)).toBeLessThan(WINDOW_LIMITS[0] / 1000);
        // A turn on the y axis between, so the way out and back is a step of its own.
        turnWheel(page, id, 120, { shift: true });
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY, at: anchor });
        expect(shows(), 'the widest view again').toEqual(widest);
        await turnWheelToRest(page, id, -400, { n: ALL_THE_WAY, at: anchor });
        expect(view.span).toBe(WINDOW_LIMITS[0]);
        // The same two milliseconds, to a thousandth of them.
        expect(Math.abs(view.right - narrowest.right)).toBeLessThan(WINDOW_LIMITS[0] / 1000);
        // Back shows the narrowest view as it was before the way out and back, to the bit.
        press('Back');
        expect(shows()).toEqual(narrowest);
      }),
      // Each case turns the wheel all the way out and in three times, so few random ones beside the examples.
      { examples: EACH.map(([h]) => [h, 0.3]), numRuns: EACH.length + Math.min(SWEEPS, fc.readConfigureGlobal().numRuns) },
    );
  }, 60_000);

  it('asks the hub for a plot’s width of points however long the span, and holds no more', async () => {
    for (const history of HISTORIES) {
      const hub = await paused({ history, fill: { full: true }, behind: 0, now: 100_000 });
      const traces = [...page.hub.session.traces.values()], width = 2 * window.innerWidth;
      /** Waits out the reads a burst of turns asks for, and gives those asked since `from`. */
      const reads = async (from) => {
        await wait(250);
        await frames();
        return hub.asked.slice(from);
      };
      turnWheel(page, id, 400, { n: ALL_THE_WAY });
      let moving = await wheelRest(page, id);
      const wide = await reads(0);
      // The whole capture once for each signal, and the view with a span
      // either side. A signal's detail is read once a frame at most, so
      // turns within one frame and the frames their ease moves the view
      // read it once a frame of it and once after.
      const overview = wide.filter((a) => a.t0 === hub.oldest && a.t1 === hub.at);
      expect(overview.map((a) => a.id).sort(), `${history} s: one overview a signal`).toEqual(traces.map((t) => t.meta.id));
      expect(wide.length - overview.length, `${history} s: detail reads for turns within one frame and their ease`).toBeLessThanOrEqual((moving + 1) * traces.length);
      const from = hub.asked.length;
      turnWheel(page, id, -400, { n: 12, at: 0.4 });
      moving = await wheelRest(page, id);
      const narrow = await reads(from);
      expect(narrow.length).toBeGreaterThan(0);
      expect(narrow.length, `${history} s: detail reads for turns within one frame and their ease`).toBeLessThanOrEqual((moving + 1) * traces.length);
      expect(new Set(narrow.map((a) => `${a.id}|${a.at}`)).size, `${history} s: one read a signal a frame`).toBe(narrow.length);
      for (const a of narrow.slice(-traces.length)) expect(a.t1 - a.t0, 'the view and a span either side').toBeCloseTo(3 * view.span, 6);
      for (const a of hub.asked) {
        expect(a.width, `${history} s: the width asked for`).toBe(width);
        expect(a.points, `${history} s: the points of one read`).toBeLessThanOrEqual(2 * width);
      }
      for (const t of traces) {
        for (const layer of ['overview', 'detail']) {
          const held = page.hub.session.windowOf(id, layer, t);
          expect(held, `${history} s: the ${layer} of ${t.key}`).toBeDefined();
          expect(held.length).toBeLessThanOrEqual(2 * width);
        }
      }
      // The widest view of a day at 100 rows a second is far more rows than that.
      if (history === 86400) expect(Math.max(...wide.map((a) => a.points))).toBeGreaterThan(width);
    }
  }, 60_000);

  it('draws nothing before the oldest time held, live or paused, however the view is set, and regains a window a filling hub comes to hold', async () => {
    const pv = page.pv(id), history = 3600, WINDOW = 3000;
    /** The seconds drawn a frame on, which start no earlier than the oldest time `hub` holds, to a pixel column. */
    const drawn = async (hub, what) => {
      await frames();
      const { x0, x1 } = pv.drawn;
      expect(x0, `${what}: drawn from ${x0}, held from ${hub.oldest}`).toBeGreaterThanOrEqual(hub.oldest - (x1 - x0) / pv.geo.plot.w);
      return x1 - x0;
    };
    /** The toolbar's window, as it reads once it has read the views again. */
    const reads = async () => {
      await wait(600);
      flushSync();
      const field = document.querySelector('.top-bar .window-field');
      return `${field.querySelector('input').value} ${field.querySelector('.unit').textContent}`;
    };
    const zoomIn = async () => {
      await turnWheelToRest(page, id, -400, { n: 4, at: 0.3 });
      expect(view.live, 'a zoom in pauses').toBe(false);
    };
    for (const share of [0.1, 0.5, 1]) {
      const hub = await live({ history, fill: share === 1 ? { full: true } : { full: false, share }, behind: 0, now: 100_000 });
      const widest = Math.min(history, hub.now - view.delay - hub.oldest), most = Math.min(WINDOW, widest);
      // The window field sets it.
      view.setSpan(WINDOW);
      expect(await drawn(hub, `${share}: the window set`)).toBeCloseTo(most, 6);
      expect(view.window, 'the window asked for is kept').toBe(WINDOW);
      expect(await reads(), 'the toolbar reads what is shown').toBe(windowText(most));
      await zoomIn();
      await drawn(hub, `${share}: zoomed in`);
      // A double-click on the plot follows live at the window.
      await commands.mouse([['dblclick', ...centre(leafOf(id).querySelector('.plot-body'))]]);
      expect(await until(() => view.live), 'a double-click follows live').toBe(true);
      expect(await drawn(hub, `${share}: a double-click`)).toBeCloseTo(most, 6);
      expect(view.window).toBe(WINDOW);
      // So does End.
      await zoomIn();
      leafOf(id).querySelector('.plot-card').focus();
      await pressKey('End');
      expect(await until(() => view.live), 'End follows live').toBe(true);
      expect(await drawn(hub, `${share}: End`)).toBeCloseTo(most, 6);
      press('Back');
      await drawn(hub, `${share}: Back`);
      press('Forward');
      expect(await drawn(hub, `${share}: Forward`)).toBeCloseTo(most, 6);
      // All the way out, live and then paused, is what is held: the History on a full hub.
      await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY, at: 0.99 });
      expect(view.live).toBe(true);
      expect(await drawn(hub, `${share}: zoomed out, live`)).toBeCloseTo(widest, 6);
      view.pause(clock.now());
      await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
      expect(await drawn(hub, `${share}: zoomed out, paused`)).toBeCloseTo(widest, 6);
      if (share === 1) expect(widest).toBe(history);
    }
    // A hub a tenth full fills: the window comes back with no input.
    const hub = await live({ history, fill: { full: false, share: 0.1 }, behind: 0, now: 100_000 });
    view.setSpan(1000);
    const held = hub.now - view.delay - hub.oldest;
    expect(await drawn(hub, 'a tenth full')).toBeCloseTo(held, 6);
    expect(await reads()).toBe(windowText(held));
    const fuller = holdHistory(page, { history, seconds: 2000, now: 100_000 });
    expect(await drawn(fuller, 'holding the window')).toBeCloseTo(1000, 6);
    expect(await reads()).toBe(windowText(1000));
  }, 60_000);
});

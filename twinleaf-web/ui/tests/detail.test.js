// A paused plot zoomed by the wheel on a hub that answers a window read
// some time after it is asked (harness `holdHistory`, `delay`): out from
// where it paused, in at one spot, out again, and in at another, a turn a
// frame. At every frame of it:
// - each signal has one detail read in flight at most, and one sent a
//   frame at most;
// - no window of an aborted read is held, and a read whose window the view
//   has left, or that is coarser than the view wants, is aborted;
// - each signal is drawn from whichever of what the page holds spans the
//   most of the view's pixel columns, and of those the one with the most
//   points across it: the detail window, the overview or the live level;
// and from the last turn of each zoom, the view is drawn at full detail, as
// many points across it as the hub's reply to that view holds, within two
// frames of the time the reply takes.
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { frames, holdHistory, mountPage, settle, turnWheel } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** The turns of each zoom, one a frame, each a factor of e^0.36. */
const TURNS = 15;
/** The share of the view's span the page asks for either side of it: App.svelte DETAIL_PAD. */
const PAD = 1;

describe('a paused plot zoomed on a hub that answers after a delay', () => {
  let page, id, view, pv, session, traces;
  beforeAll(async () => {
    page = await mountPage([SIGNALS], 'columns', 0, SIGNALS);
    id = plotIds(page.ws.layout)[0];
    view = page.views.of(id);
    pv = page.pv(id);
    session = page.hub.session;
    traces = [...session.traces.values()];
  });
  afterAll(() => page.dispose());
  afterEach(() => vi.restoreAllMocks());

  /** The pixel columns of the view drawn that the points of `src` span, and its points across the view: every row there is, for a window of the raw rows. */
  const columns = (src) => {
    const { x0, x1 } = pv.drawn;
    if (!src.length) return 0;
    return Math.round(((Math.min(src.xAt(src.absEnd - 1), x1) - Math.max(src.xAt(src.absStart), x0)) / (x1 - x0)) * pv.geo.plot.w);
  };
  const across = (src) => src.lowerBound(pv.drawn.x1) - src.lowerBound(pv.drawn.x0);
  const points = (src) => (src.ratio === 1 && src.length ? Infinity : across(src));

  // The delays: within a frame, as a hub on the same machine answers, and nine frames.
  it.each([5, 150])('reads one window at a time, draws the densest it holds, and is at full detail two frames after a reply %i ms away', async (delay) => {
    view.follow(true);
    view.setSpan(10);
    const hub = holdHistory(page, { history: 3600, delay });
    await settle(2);
    view.pause(clock.now());
    const width = 2 * window.innerWidth;
    const overview = (a) => a.t0 === hub.oldest;
    const seen = { densest: { detail: 0, overview: 0, live: 0 }, aborted: 0, rival: 0 };

    /** One frame, and what holds at it. */
    async function frame() {
      const held = new Map(traces.map((t) => [t, ['detail', 'overview'].map((layer) => session.windowOf(id, layer, t)).filter(Boolean)]));
      await frames(1);
      for (const t of traces) {
        const mine = hub.asked.filter((a) => a.id === t.meta.id && !overview(a));
        expect(mine.filter((a) => a.answered === null && !a.aborted).length, 'detail reads in flight').toBeLessThanOrEqual(1);
        const at = mine.map((a) => a.at);
        expect(new Set(at).size, 'detail reads, one a frame').toBe(at.length);
        // What the page holds is the reply of a read that was not aborted.
        const d = session.windowOf(id, 'detail', t);
        if (d) expect(mine.some((a) => a.t0 === d.t0 && a.t1 === d.t1 && a.answered !== null), 'the window held was answered').toBe(true);
        // What it drew is the best of what it held as it drew.
        const plan = pv.drawn.plans.find((p) => p.t === t), rivals = [...held.get(t), t.levels[plan.level]];
        expect(rivals, 'drawn from what it holds').toContain(plan.src);
        const most = Math.max(...rivals.map(columns));
        expect(columns(plan.src), 'spans the most columns').toBe(most);
        const dense = rivals.filter((r) => columns(r) === most);
        expect(points(plan.src), 'the most points across the view').toBe(Math.max(...dense.map(points)));
        if (dense.length > 1) seen.rival++;
        seen.densest[plan.src.ratio === undefined ? 'live' : plan.src.t0 === hub.oldest ? 'overview' : 'detail']++;
      }
    }
    /** Whether every signal is drawn with as many points across the view as the hub's reply to the view holds. */
    const full = () =>
      traces.every((t) => {
        const { x0, x1, plans } = pv.drawn, src = plans.find((p) => p.t === t).src, pad = PAD * (x1 - x0);
        const fresh = hub.read(x0 - pad, x1 + pad, width).x.filter((x) => x >= x0 && x < x1).length;
        return columns(src) >= Math.round(pv.geo.plot.w) - 1 && across(src) >= fresh - 2;
      });
    /** Turns the wheel `TURNS` times by `deltaY` at `at` across the plot, a frame each, and gives the frames from the last turn until the view is at full detail. */
    async function zoom(deltaY, at) {
      const from = hub.asked.length;
      for (let i = 0; i < TURNS; i++) {
        turnWheel(page, id, deltaY, { at });
        await frame();
      }
      let after = 0;
      while (!full() && after < 60) {
        await frame();
        after++;
      }
      const reads = hub.asked.slice(from).filter((a) => !overview(a));
      // A read a frame a signal at most, and the one the last turn left waiting.
      expect(reads.length, 'detail reads of one zoom').toBeLessThanOrEqual((TURNS + 1) * traces.length);
      seen.aborted += reads.filter((a) => a.aborted).length;
      for (let i = 0; i < 10; i++) await frame();
      expect(full(), 'and stays at full detail').toBe(true);
      return after;
    }
    const within = Math.floor(delay / clock.FRAME_MS) + 2;
    expect(await zoom(240, 0.5), 'out').toBeLessThanOrEqual(within);
    expect(await zoom(-240, 0.25), 'in at one spot').toBeLessThanOrEqual(within);
    expect(await zoom(240, 0.25), 'out again').toBeLessThanOrEqual(within);
    expect(await zoom(-240, 0.75), 'in at another').toBeLessThanOrEqual(within);
    expect(view.span, 'zoomed in').toBeLessThan(20);
    // It drew from each kind of source, and chose between rivals that spanned as much.
    expect(seen.rival).toBeGreaterThan(0);
    expect(seen.densest.detail).toBeGreaterThan(0);
    expect(seen.densest.live).toBeGreaterThan(0);
    // A reply nine frames away is overtaken by the turns after it.
    if (delay > clock.FRAME_MS) expect(seen.aborted).toBeGreaterThan(0);
    else expect(seen.aborted).toBe(0);
  }, 60_000);
});

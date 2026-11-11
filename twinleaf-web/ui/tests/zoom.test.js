// A paused plot zooms out to what the hub holds of its signals, and no
// further. The page keeps each signal at several levels of detail, and a
// coarse level reaches further back than the hub does; the view's limit is
// the hub's oldest time, not the page's oldest point. Where the hub holds
// more of one signal than another, the limit is the longer history's. On a
// hub that holds its History setting before the pause, the widest view is
// the setting's length exactly, as the setting reads. It keeps the History
// of its pause: a setting lowered since moves its limit in, and one raised
// leaves its widest view the length it was, with none of what the hub holds
// beyond the setting; live again, it is by the setting as it is. Back shows
// a view within the same limit, so the widest view comes back as it was. The wheel's turns are dispatched with
// no frame between (harness `turnWheel`), and the view eases to rest in the
// frames after (`turnWheelToRest`): the limit holds from the first turn
// after a pause, whether or not the plot had drawn before it.
//
// The wheel over a time plot eases its span to rest on a span of a ladder
// (wheelzoom.js), on a plot of a hub that holds an hour: turns alone step
// fine spans, a few in quick succession middling ones, and a spin clock
// spans, each sequence ending on the span the rule says in the frames the
// ease takes; a trackpad's stream of small turns and its momentum tail
// step the same way and never past where they end; a turn the other way
// mid-ease turns the span round at the next frame; at the limit the ease
// ends there; a burst is one step for Back; Shift's y zoom does not ease.
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { easing, feed, frames, holdHistory, leafOf, mountPage, settle, turnWheel, turnWheelToRest, wait, wheelRest, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { GONE, IDLE_MS } from '../src/lib/viewhistory.svelte.js';
import { EASE_MS, LADDERS, RATE_MS, REST } from '../src/lib/wheelzoom.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** The seconds of samples the page is fed, from time 0. */
const FED = 40;
/** Wheel turns of 400 px that take any view of what is fed to its widest. */
const ALL_THE_WAY = 40;

describe('a paused plot, zoomed all the way out', () => {
  /** The page with one plot of both signals, fed, and what a test reads of it; `memory(oldest, maxAge)` is the hub saying what it holds. */
  async function mounted() {
    const page = await mountPage([SIGNALS], 'columns', 0, SIGNALS);
    feed(page.hub, FED);
    const id = plotIds(page.ws.layout)[0];
    const memory = (oldest, maxAge = 0) => page.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: maxAge, rate: 0, oldest });
    return { page, id, view: page.views.of(id), memory };
  }

  it('starts at the oldest time the hub holds of its signals, whatever older points the page still has', async () => {
    const { page, id, view, memory } = await mounted();
    let short = 0;
    try {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 2, max: FED - 5 }), fc.integer({ min: 2, max: FED - 5 }), async (a, b) => {
          view.follow(true);
          // The hub has dropped what it held before `a` of one signal and `b` of the other; the page's levels still start at 0.
          memory({ 0: a, 1: b });
          flushSync();
          await frames();
          view.pause(clock.now());
          await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
          expect(view.live).toBe(false);
          const left = view.right - view.span, held = Math.min(a, b);
          expect(left, `starts at ${held}`).toBeCloseTo(held, 6);
          expect(view.right, 'ends at the pause').toBe(view.pausedAt);
          if (held > 2) short++;
        }),
        withExamples([[20, 20], [10, 30], [30, 10]]),
      );
    } finally {
      page.dispose();
    }
    expect(short).toBeGreaterThan(0);
  }, 60_000);

  it('is as wide as the History setting exactly on a hub that holds that long before the pause, and starts at what the hub holds on one that does not', async () => {
    const { page, id, view, memory } = await mounted();
    try {
      // The hub holds from 10 s on: `beyond` s more than its History setting before the pause, or, negative, less.
      for (const beyond of [2, 0.3, 0, -0.6, -8]) {
        view.follow(true);
        await frames();
        const at = clock.now(), history = page.hub.now(at) - view.delay - 10 - beyond;
        memory({ 0: 10, 1: 10 }, history);
        view.pause(at);
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        expect(view.right).toBe(view.pausedAt);
        if (beyond >= 0) expect(view.span, `holding ${beyond} s beyond, as wide as the setting`).toBeCloseTo(history, 6);
        else expect(view.right - view.span, `holding ${-beyond} s short, starts at what is held`).toBeCloseTo(10, 6);
      }
    } finally {
      page.dispose();
    }
  }, 60_000);

  it('keeps the History of its pause while the setting is changed: in to a shorter one, and no wider than it was for a longer one', async () => {
    const { page, id, view, memory } = await mounted();
    try {
      // The hub holds from 10 s on: `beyond` s more than its History setting before the pause, within its headroom.
      for (const beyond of [2, 1, 0]) {
        view.follow(true);
        await frames();
        const at = clock.now(), history = page.hub.now(at) - view.delay - 10 - beyond;
        memory({ 0: 10, 1: 10 }, history);
        view.pause(at);
        const paused = view.pausedAt;
        // Raised with the view as it paused: the hub holds for the pause what it held, and the view shows none of the headroom.
        memory({ 0: 10, 1: 10 }, 3600);
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        expect(view.span, `holding ${beyond} s beyond, History set to an hour`).toBeCloseTo(history, 6);
        expect(view.right - view.span).toBeCloseTo(10 + beyond, 6);
        memory({ 0: 10, 1: 10 }, 5);
        await turnWheelToRest(page, id, 400, { n: 3 });
        expect(view.span, 'History set to 5 s').toBeCloseTo(5, 6);
        memory({ 0: 10, 1: 10 }, 86400);
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        expect(view.span, 'History set to a day').toBeCloseTo(history, 6);
        expect(view.right, 'ends at the pause throughout').toBe(paused);
        expect(view.pausedAt).toBe(paused);
        // Live again, it shows what the hub holds within the setting as it is.
        view.follow(true);
        expect(view.oldest(clock.now()), 'live, History a day').toBeCloseTo(10, 6);
      }
    } finally {
      page.dispose();
    }
  }, 60_000);

  it('brought Back paused after it went live, is as wide as the History of that pause, whatever the setting is since', async () => {
    const { page, id, view, memory } = await mounted();
    const back = () => {
      flushSync();
      leafOf(id).querySelector('.plot-head button[aria-label^="Back"]').click();
      flushSync();
    };
    try {
      view.follow(true);
      await frames();
      // The hub holds from 10 s on, a second more than its History setting before the pause.
      const at = clock.now(), history = page.hub.now(at) - view.delay - 11;
      memory({ 0: 10, 1: 10 }, history);
      view.pause(at);
      await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
      const widest = { right: view.right, span: view.span, pausedAt: view.pausedAt };
      expect(widest.span).toBeCloseTo(history, 6);
      // Live by the header's button, a step of its own, then the History is raised.
      flushSync();
      leafOf(id).querySelector('.plot-head button[aria-label="Follow live"]').click();
      flushSync();
      expect(view.live).toBe(true);
      memory({ 0: 10, 1: 10 }, 3600);
      back();
      expect({ right: view.right, span: view.span, pausedAt: view.pausedAt }, 'the widest view of that pause').toEqual(widest);
      await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
      expect(view.span, 'and no wider').toBe(widest.span);
      expect(document.body.textContent, 'nothing the view showed is gone').not.toContain(GONE);
    } finally {
      page.dispose();
    }
  }, 60_000);

  it('comes back as it was by Back, with no notice, on a hub that has dropped samples and holds more of one signal than the other', async () => {
    const { page, id, view, memory } = await mounted();
    const back = () => {
      flushSync();
      leafOf(id).querySelector('.plot-head button[aria-label^="Back"]').click();
      flushSync();
    };
    try {
      // The hub has dropped what it held before 12 s and 30 s, and holds `beyond` s more than its History setting before the pause, or less.
      for (const beyond of [1, -8]) {
        view.follow(true);
        await frames();
        const at = clock.now();
        memory({ 0: 10, 1: 10 });
        memory({ 0: 12, 1: 30 }, page.hub.now(at) - view.delay - 12 - beyond);
        view.pause(at);
        await turnWheelToRest(page, id, 400, { n: ALL_THE_WAY });
        const widest = view.state();
        expect(view.right - view.span, 'the widest view starts where the longer history does').toBeCloseTo(beyond > 0 ? 12 + beyond : 12, 6);
        // A turn on the y axis between, so the zoom in is a step of its own.
        turnWheel(page, id, 120, { shift: true });
        await turnWheelToRest(page, id, -400, { n: 3 });
        expect(view.span).toBeLessThan(widest.span);
        back();
        expect({ right: view.right, span: view.span, pausedAt: view.pausedAt }).toEqual({ right: widest.right, span: widest.span, pausedAt: widest.pausedAt });
        expect(document.body.textContent, 'nothing the view showed is gone').not.toContain(GONE);
      }
    } finally {
      page.dispose();
    }
  }, 60_000);
});

describe('the wheel over a time plot', () => {
  let page, id, view;
  beforeAll(async () => {
    page = await mountPage([SIGNALS], 'columns', 0, SIGNALS);
    id = plotIds(page.ws.layout)[0];
    view = page.views.of(id);
  });
  afterAll(() => page.dispose());
  afterEach(() => vi.restoreAllMocks());

  /** The frames the ease takes from span `from` to rest on `to`: all but e^(-t / EASE_MS) of the way along a log scale at t, until within REST. */
  const restFrames = (from, to) => Math.max(1, Math.ceil((EASE_MS * Math.log(Math.max(1, Math.abs(Math.log(from / to)) / REST))) / clock.FRAME_MS));
  /** Whether `spans` never turn back against `dir`. */
  const onward = (spans, dir) => spans.every((s, i) => !i || (s - spans[i - 1]) * dir >= 0);
  /** Plot `id` paused at `span` s on a hub holding an hour, drawn, its wheel still and its last view step over. */
  async function pausedAt(span) {
    view.follow(true);
    view.setSpan(span);
    holdHistory(page, { history: 3600 });
    await settle(2);
    view.pause(clock.now());
    await wait(IDLE_MS + 100);
  }
  /** The span at each frame until the ease rests. */
  async function watch() {
    const spans = [];
    while (easing(page, id)) {
      await frames(1);
      spans.push(view.span);
    }
    return spans;
  }
  const press = (label) => {
    flushSync();
    leafOf(id).querySelector(`.plot-head button[aria-label^="${label}"]`).click();
    flushSync();
  };

  it('steps a fine span for a turn alone, a middling one for a few in quick succession, and a clock span for a spin, each resting in the frames its ease takes', async () => {
    // Slow: each turn once the last has rested and the wheel has been still.
    await pausedAt(10);
    for (const want of [11, 12, 13]) {
      const from = view.span;
      turnWheel(page, id, 100);
      const spans = await watch();
      expect(spans.at(-1), `slow, ${from} s on`).toBe(want);
      expect(spans.length, `slow, ${from} s on: frames`).toBe(restFrames(from, want));
      expect(onward(spans, 1)).toBe(true);
      await wait(RATE_MS);
    }
    // Medium: three turns 40 ms apart, a fine step and then two middling ones.
    await pausedAt(10);
    turnWheel(page, id, 100);
    await wait(40);
    turnWheel(page, id, 100);
    await wait(40);
    const from = view.span;
    turnWheel(page, id, 100);
    let spans = await watch();
    expect(spans.at(-1), 'medium: 11, 12, then 15').toBe(15);
    expect(spans.length, 'medium: frames from the last turn').toBe(restFrames(from, 15));
    // Fast: eight at once, out and then in: a fine step, two middling, then clock spans.
    await pausedAt(10);
    turnWheel(page, id, 100, { n: 8 });
    spans = await watch();
    expect(spans.at(-1), 'fast out: 11, 12, 15, then 20, 30, 45, 60, 120').toBe(120);
    expect(spans.length, 'fast out: frames').toBe(restFrames(10, 120));
    expect(onward(spans, 1)).toBe(true);
    await wait(RATE_MS);
    turnWheel(page, id, -100, { n: 8 });
    spans = await watch();
    expect(spans.at(-1), 'fast in: 110, 100, 80, then 60, 45, 30, 20, 15').toBe(15);
    expect(spans.length, 'fast in: frames').toBe(restFrames(120, 15));
    expect(onward(spans, -1)).toBe(true);
    expect(view.live, 'paused throughout').toBe(false);
  }, 60_000);

  it('steps a trackpad’s stream of small turns and its momentum tail as the wheel, a step a notch’s worth, never past where it ends', async () => {
    await pausedAt(10);
    // A turn a frame: 8 px for half a second, then a tail that dies away.
    const deltas = [...Array(30).fill(8), ...Array.from({ length: 20 }, (_, k) => 8 * 0.85 ** (k + 1))];
    const spans = [];
    for (const dy of deltas) {
      turnWheel(page, id, dy);
      await frames(1);
      spans.push(view.span);
    }
    spans.push(...(await watch()));
    // Where the px turned add up to another notch's worth.
    const steps = [];
    deltas.reduce((sum, dy, i) => (Math.floor((sum + dy) / 100) > Math.floor(sum / 100) && steps.push(i), sum + dy), 0);
    expect(steps.length, 'two notches’ worth').toBe(2);
    expect(spans.at(-1), 'clock spans, as a stream this quick is: 15, then 20').toBe(20);
    expect(Math.max(...spans), 'never past where it ends').toBe(20);
    expect(onward(spans, 1)).toBe(true);
    const last = steps.at(-1), before = spans[last - 1];
    expect(spans.indexOf(20) - last + 1, 'frames from the last step to rest').toBe(restFrames(before, 20));
  }, 60_000);

  it('turns round at the next frame for a turn the other way mid-ease, from the span it shows', async () => {
    await pausedAt(10);
    turnWheel(page, id, 100, { n: 8 });
    await frames(3);
    const shown = view.span;
    expect(shown, 'mid-ease, on the way to 120').toBeGreaterThan(10);
    turnWheel(page, id, -100);
    const spans = await watch();
    expect(spans[0], 'the next frame goes the other way').toBeLessThan(shown);
    expect(spans.at(-1), 'to the clock span below what showed').toBe(LADDERS.clock.findLast((v) => v < shown * 0.999));
    expect(onward(spans, -1)).toBe(true);
  }, 60_000);

  it('eases out to the widest the view goes and ends there, with no bounce', async () => {
    await pausedAt(10);
    const [, widest] = view.spans(clock.now());
    turnWheel(page, id, 400, { n: 40 });
    const spans = await watch();
    expect(spans.at(-1)).toBe(widest);
    expect(Math.max(...spans)).toBe(widest);
    expect(spans.length, 'eased toward the widest itself').toBe(restFrames(10, widest));
    expect(onward(spans, 1)).toBe(true);
  }, 60_000);

  it('is one view step for Back however many frames a burst eases through', async () => {
    await pausedAt(10);
    const shows = () => ({ live: view.live, span: view.span, right: view.right });
    const before = shows();
    turnWheel(page, id, 100);
    await wait(40);
    turnWheel(page, id, 100);
    await wait(40);
    turnWheel(page, id, 100);
    await wheelRest(page, id);
    const after = shows();
    expect(after.span).toBe(15);
    press('Back');
    expect(shows(), 'Back to before the burst').toEqual(before);
    press('Forward');
    expect(shows(), 'Forward to where it rested').toEqual(after);
    // Back mid-ease: the view goes back, and the ease leaves it there.
    await wait(IDLE_MS + 100);
    turnWheel(page, id, 100, { n: 8 });
    await frames(3);
    expect(easing(page, id)).toBe(true);
    press('Back');
    expect(shows(), 'Back mid-ease').toEqual(after);
    await frames(40);
    expect(shows(), 'and stays').toEqual(after);
    expect(easing(page, id)).toBe(false);
  }, 60_000);

  it('zooms y with Shift at once, with no ease, and leaves the time span', async () => {
    await pausedAt(10);
    const ranges = () => JSON.stringify([...page.pv(id).y.values()].map((st) => st.range));
    const was = ranges();
    turnWheel(page, id, 120, { shift: true });
    expect(easing(page, id)).toBe(false);
    expect(ranges(), 'y zoomed').not.toBe(was);
    expect(view.span).toBe(10);
  }, 60_000);
});

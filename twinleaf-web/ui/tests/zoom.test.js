// A paused plot zooms out to what the hub holds of its signals, and no
// further. The page keeps each signal at several levels of detail, and a
// coarse level reaches further back than the hub does; the view's limit is
// the hub's oldest time, not the page's oldest point. Where the hub holds
// more of one signal than another, the limit is the longer history's. On a
// hub that holds its History setting before the pause, the widest view is
// the setting's length exactly, as the setting reads, and follows the
// setting as it is changed. Back shows a view within the same limit, so the
// widest view comes back as it was. The wheel's turns are dispatched with
// no frame between (harness `turnWheel`): the limit holds from the first
// turn after a pause, whether or not the plot has drawn since.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { feed, frames, leafOf, mountPage, turnWheel, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { GONE } from '../src/lib/viewhistory.svelte.js';

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
          view.pause(performance.now());
          turnWheel(page, id, 400, { n: ALL_THE_WAY });
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
        const at = performance.now(), history = page.hub.now(at) - view.delay - 10 - beyond;
        memory({ 0: 10, 1: 10 }, history);
        view.pause(at);
        turnWheel(page, id, 400, { n: ALL_THE_WAY });
        expect(view.right).toBe(view.pausedAt);
        if (beyond >= 0) expect(view.span, `holding ${beyond} s beyond, as wide as the setting`).toBeCloseTo(history, 6);
        else expect(view.right - view.span, `holding ${-beyond} s short, starts at what is held`).toBeCloseTo(10, 6);
      }
    } finally {
      page.dispose();
    }
  }, 60_000);

  it('follows the History setting changed while it is paused: in to a shorter one, and out to what the hub holds of a longer one', async () => {
    const { page, id, view, memory } = await mounted();
    try {
      view.follow(true);
      await frames();
      // The hub holds from 10 s on, a second more than its History setting before the pause.
      const at = performance.now(), history = page.hub.now(at) - view.delay - 11;
      memory({ 0: 10, 1: 10 }, history);
      view.pause(at);
      turnWheel(page, id, 400, { n: ALL_THE_WAY });
      expect(view.span).toBeCloseTo(history, 6);
      memory({ 0: 10, 1: 10 }, 5);
      turnWheel(page, id, 400, { n: 3 });
      expect(view.span, 'History set to 5 s').toBeCloseTo(5, 6);
      expect(view.right).toBe(view.pausedAt);
      // The hub holds for the pause what it held: a longer setting brings no more.
      memory({ 0: 10, 1: 10 }, 3600);
      turnWheel(page, id, 400, { n: ALL_THE_WAY });
      expect(view.right - view.span, 'History set to an hour').toBeCloseTo(10, 6);
      expect(view.right).toBe(view.pausedAt);
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
        const at = performance.now();
        memory({ 0: 10, 1: 10 });
        memory({ 0: 12, 1: 30 }, page.hub.now(at) - view.delay - 12 - beyond);
        view.pause(at);
        turnWheel(page, id, 400, { n: ALL_THE_WAY });
        const widest = view.state();
        expect(view.right - view.span, 'the widest view starts where the longer history does').toBeCloseTo(beyond > 0 ? 12 + beyond : 12, 6);
        // A turn on the y axis between, so the zoom in is a step of its own.
        turnWheel(page, id, 120, { shift: true });
        turnWheel(page, id, -400, { n: 3 });
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

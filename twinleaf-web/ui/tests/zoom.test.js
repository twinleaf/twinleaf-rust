// A paused plot zooms out to what the hub holds of its signals, and no
// further. The page keeps each signal at several levels of detail, and a
// coarse level reaches further back than the hub does; the view's limit is
// the hub's oldest time, not the page's oldest point. Where the hub holds
// more of one signal than another, the limit is the longer history's. Once
// the hub holds its full history, the widest view is that history's length
// exactly, as the History setting reads.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, feed, frames, leafOf, mountPage, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** The seconds of samples the page is fed, from time 0. */
const FED = 40;

describe('a paused plot, zoomed all the way out', () => {
  it('starts at the oldest time the hub holds of its signals, whatever older points the page still has', async () => {
    const page = await mountPage([SIGNALS], 'columns', 0, SIGNALS);
    let short = 0;
    try {
      feed(page.hub, FED);
      const id = plotIds(page.ws.layout)[0], view = page.views.of(id);
      const memory = (oldest, maxAge = 0) => page.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: maxAge, rate: 0, oldest });
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 2, max: FED - 5 }), fc.integer({ min: 2, max: FED - 5 }), async (a, b) => {
          view.follow(true);
          // The hub has dropped what it held before `a` of one signal and `b` of the other; the page's levels still start at 0.
          memory({ 0: a, 1: b });
          page.ws.selection.only([id]);
          flushSync();
          await frames();
          view.pause(performance.now());
          const [x, y] = centre(leafOf(id).querySelector('.plot-body'));
          await commands.mouse([['move', x, y], ...Array.from({ length: 40 }, () => ['wheel', 0, 400])]);
          await frames(4);
          expect(view.live).toBe(false);
          const left = view.right - view.span, held = Math.min(a, b);
          expect(left, `starts no earlier than ${held}`).toBeGreaterThanOrEqual(held - 1e-6);
          // And it does reach it: the whole of what is held shows.
          expect(left, `reaches ${held}`).toBeLessThan(held + 0.5);
          if (held > 2) short++;
        }),
        withExamples([[20, 20], [10, 30], [30, 10]]),
      );
    } finally {
      page.dispose();
    }
    expect(short).toBeGreaterThan(0);
  }, 300_000);

  it('is as wide as the hub\'s full history exactly once the hub holds it, and no wider than what it holds before', async () => {
    const page = await mountPage([SIGNALS], 'columns', 0, SIGNALS);
    try {
      feed(page.hub, FED);
      const id = plotIds(page.ws.layout)[0], view = page.views.of(id);
      const [x, y] = centre(leafOf(id).querySelector('.plot-body'));
      // The hub holds from 10 s on. `short` is how far that is short of its full history at the pause.
      for (const [short, exact] of [[0.6, true], [-0.9, true], [1.9, true], [8, false]]) {
        view.follow(true);
        page.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: 0, rate: 0, oldest: { 0: 10, 1: 10 } });
        await frames();
        view.pause(performance.now());
        const full = view.pausedAt - 10 + short;
        page.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: full, rate: 0, oldest: { 0: 10, 1: 10 } });
        await frames();
        await commands.mouse([['move', x, y], ...Array.from({ length: 40 }, () => ['wheel', 0, 400])]);
        await frames(4);
        if (exact) expect(view.span, `${short} s short reads as the full history`).toBeCloseTo(full, 6);
        else expect(view.right - view.span, `${short} s short starts at what is held`).toBeCloseTo(10, 6);
      }
    } finally {
      page.dispose();
    }
  }, 120_000);
});

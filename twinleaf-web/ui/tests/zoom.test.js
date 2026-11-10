// A paused plot zooms out to what the hub holds of its signals, and no
// further. The page keeps each signal at several levels of detail, and a
// coarse level reaches further back than the hub does; the view's limit is
// the hub's oldest time, not the page's oldest point. Where the hub holds
// more of one signal than another, the limit is the longer history's.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
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
      const memory = (oldest) => page.hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: 0, max_age: 0, rate: 0, oldest });
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
});

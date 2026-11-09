// The tab strip follows the tabs. On the whole page, tabs are dragged to new
// places with the real mouse and held there a while, with undo and redo
// between. After every step the page
// still has every tab it had, and the strip shows them in their order.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, drag, frames, mountPage } from './browser/page.js';

const stripOrder = () => [...document.querySelectorAll('[role=tablist] [data-tab]')].map((e) => e.dataset.tab);

const step = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('tab'), from: fc.nat(), to: fc.nat() }) },
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
);

/** How many drags moved a tab, over every run: none would leave the property vacuous. */
let moved = 0;

async function run(s, { ws, history }) {
  if (s.t === 'tab') {
    const tabs = [...document.querySelectorAll('[role=tablist] [data-tab]')];
    const a = tabs[s.from % tabs.length], b = tabs[s.to % tabs.length];
    if (a === b) return;
    const before = ws.tabs.map((t) => t.id).join();
    const [bx, by] = centre(b), r = b.getBoundingClientRect();
    // Past the far half of the tab it lands beside, held there a while.
    // Picked up by its name, as a person does; its buttons start no drag.
    const from = centre(a.querySelector('span'));
    await drag(from, [bx + (r.width / 3) * Math.sign(bx - from[0]), by], [[bx, by]], 500);
    await new Promise((done) => setTimeout(done, 300));
    if (ws.tabs.map((t) => t.id).join() !== before) moved++;
  } else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  flushSync();
  await frames();
}

describe('the tab strip', () => {
  it('keeps every tab, in the order the strip shows, after any run of tab drags, undos and redos', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 5 }), async (steps) => {
        const page = await mountPage([[], []], 'grid', 2);
        const ids = page.ws.tabs.map((t) => t.id).sort();
        try {
          for (const s of steps) {
            await run(s, page);
            expect(page.ws.tabs.map((t) => t.id).sort()).toEqual(ids);
            expect(stripOrder()).toEqual(page.ws.tabs.map((t) => t.id));
          }
        } finally {
          page.dispose();
        }
      }),
      {
        numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 10),
        // The first tab dragged behind the second, held, then undone.
        examples: [[[{ t: 'tab', from: 0, to: 1 }, { t: 'undo' }]]],
      },
    );
    expect(moved).toBeGreaterThan(0);
  }, 600_000);
});

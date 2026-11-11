// The tab strip follows the tabs. On the whole page, tabs are dragged to new
// places with the real mouse and held there a while, with undo and redo
// between. After every step the page
// still has every tab it had, and the strip shows them in their order.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, clickOn, drag, dropped, feed, focusPage, frames, mountPage, pressKey, settle, turnWheel, wait, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { SAVE_KEY, newPlot } from '../src/lib/tabs.js';

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
    await dropped();
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
        // The first tab dragged behind the second, held, then undone.
        withExamples([[[{ t: 'tab', from: 0, to: 1 }, { t: 'undo' }]]]),
    );
    expect(moved).toBeGreaterThan(0);
  }, 600_000);
});

// Selecting the tab that is open is no change: a click on it, or Enter on
// it, leaves the page as it was, whatever is maximized, selected or picked.
describe('the open tab', () => {
  const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine'], ['aux.sawtooth']];
  const RPCS = [{ name: 'sine.freq', channel: 'setting', value: 1 }, { name: 'sine.amp', channel: 'setting', value: 2 }];
  /** Everything a select of the open tab might change: the saved document, the selection, the maximized plots, and both histories' lengths, the view history's as its buttons offer it. */
  const stateOf = (page) =>
    JSON.stringify({
      saved: localStorage.getItem(SAVE_KEY),
      selection: page.ws.selection.snapshot(),
      rows: [...page.ws.selection.rows],
      maximized: page.ws.maximized,
      undos: page.history.undos.length,
      redos: page.history.redos.length,
      views: [...document.querySelectorAll('.plot-head button.view-step')].map((b) => b.disabled),
    });

  it('clicked or given Enter, changes nothing: not the saved document, the selection, the maximized plots or a history', async () => {
    const seen = new Set();
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.subarray([0, 1, 2, 3]), fc.boolean(), fc.constantFrom('click', 'Enter'), async (max, picked, rows, how) => {
        const page = await mountPage(KEYS, 'grid', 1, KEYS.flat(), RPCS);
        try {
          await focusPage();
          const ids = plotIds(page.ws.layout);
          // A step to undo and one to redo, so neither history is empty.
          page.history.group('Split', () => page.ws.addPlot(newPlot('time', 'overlay', []), ids[0]));
          page.history.group('Split', () => page.ws.addPlot(newPlot('time', 'overlay', []), ids[1]));
          page.history.undo();
          // A zoom, so the view history has a step back.
          feed(page.hub);
          await settle();
          turnWheel(page, ids[0], -120);
          await wait(1000);
          if (max) page.ws.maximize([ids[0], ids[2]]);
          if (rows) await clickOn(document.querySelector('.dock-panel tr[id^=prop-] td:last-child'));
          if (picked.length) page.ws.selection.only(page.ws.shownIds().filter((_, i) => picked.includes(i)));
          await settle();
          const tab = document.querySelector(`[role=tab][data-tab="${page.ws.activeId}"]`);
          const before = stateOf(page);
          expect(JSON.parse(before).views, 'the view steps offered').toContain(false);
          seen.add(`${!!page.ws.maximized} ${page.ws.selected.size > 0} ${page.ws.selection.rows.size > 0}`);
          if (how === 'click') await clickOn(tab.querySelector('span'));
          else {
            tab.focus();
            await pressKey('Enter');
          }
          await settle();
          expect(stateOf(page), `${how} on the open tab`).toBe(before);
        } finally {
          page.dispose();
        }
      }),
      withExamples([[true, [0, 1], false, 'click'], [false, [], true, 'Enter'], [true, [], true, 'click'], [false, [2], false, 'Enter']]),
    );
    expect([...seen].sort(), 'maximized, plots selected, rows picked').toEqual(expect.arrayContaining(['true true false', 'false false true', 'true false true']));
  }, 300_000);
});

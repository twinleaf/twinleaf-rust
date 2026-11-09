// A plot's chip row follows its signals. On the whole page, signals are
// dragged by their chips with the real mouse, resting on whatever chip rows
// and plots lie on the way, onto another plot's chips, its body, a side of
// its compass, or the layout's outer edge, with undo and redo between. After
// every step each plot's row shows its signals in order, and the tab keeps
// every signal it had: a move takes a signal somewhere, never away.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, drag, frames, mountPage } from './browser/page.js';
import { plotIds } from '../src/lib/layout.js';

/** The distance from a plot's middle to its compass's side cells, in px. */
const STEP = 42;
const KEYS = [['sine.sine', 'sine.cosine'], ['aux.triangle'], ['aux.sawtooth', 'sine.sine']];

const rowOf = (id) => [...document.querySelectorAll(`.leaf[data-plot="${id}"] .trace-zone [id^="chip-"]`)].map((e) => e.id.slice('chip-'.length));
const keysIn = (ws) => new Set(Object.values(ws.plots).flatMap((p) => p.items.map((i) => i.key)));

const step = fc.oneof(
  { weight: 4, arbitrary: fc.record({ t: fc.constant('chip'), a: fc.nat(), i: fc.nat(), b: fc.nat(), onto: fc.constantFrom('chips', 'body', 'left', 'right', 'top', 'bottom') }) },
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
);

/** How many chip drags moved a signal, over every run: none would leave the property vacuous. */
let moved = 0;

async function run(s, { ws, history }) {
  const ids = plotIds(ws.layout).filter((id) => ws.plots[id].items.length);
  if (s.t === 'chip' && ids.length) {
    const a = ids[s.a % ids.length], items = ws.plots[a].items;
    const el = document.getElementById(`chip-${items[s.i % items.length].id}`);
    const all = plotIds(ws.layout), b = all[s.b % all.length];
    const leaf = document.querySelector(`.leaf[data-plot="${b}"]`);
    const [cx, cy] = centre(leaf);
    const to =
      s.onto === 'chips'
        ? centre(leaf.querySelector('.trace-zone'))
        : s.onto === 'body'
          ? centre(leaf.querySelector('.plot-body') ?? leaf)
          : [cx + { left: -STEP, right: STEP, top: 0, bottom: 0 }[s.onto], cy + { left: 0, right: 0, top: -STEP, bottom: STEP }[s.onto]];
    const before = JSON.stringify(ws.tabs);
    // Into a plot by way of its chip row, as a hand coming from above does, or else through the middle of the way.
    const from = centre(el);
    const via = s.onto === 'chips' ? [(from[0] + to[0]) / 2, (from[1] + to[1]) / 2] : centre(leaf.querySelector('.trace-zone'));
    await drag(from, to, [via], 200);
    await new Promise((done) => setTimeout(done, 250));
    if (JSON.stringify(ws.tabs) !== before) moved++;
  } else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  flushSync();
  await frames();
}

describe('chip rows', () => {
  it('show their plots’ signals, and the tab keeps every signal, after any run of chip drags, undos and redos', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 6 }), async (steps) => {
        const page = await mountPage(KEYS, 'rows');
        try {
          const keys = keysIn(page.ws);
          for (const s of steps) {
            await run(s, page);
            for (const id of plotIds(page.ws.layout)) expect(rowOf(id), id).toEqual(page.ws.plots[id].items.map((i) => i.id));
            expect(keysIn(page.ws)).toEqual(keys);
          }
        } finally {
          page.dispose();
        }
      }),
      {
        numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 10),
        // A chip dragged from the top plot through the middle plot's row into its body.
        examples: [[[{ t: 'chip', a: 0, i: 0, b: 1, onto: 'body' }, { t: 'chip', a: 1, i: 0, b: 1, onto: 'chips' }]]],
      },
    );
    expect(moved).toBeGreaterThan(0);
  }, 600_000);
});

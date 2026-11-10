// The page's tiles follow its layout. The whole page is mounted with state
// of its own, and runs of what a person does to the tiles (plot moves by the
// grip onto a compass cell, gap drags, undo, redo, layout edits made in
// code, pauses) are played with the real mouse. After every step each plot
// of the layout is in one tile, at the box layout.js `rects` gives it, with
// a gap for every split.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, drag, focusPage, frames, mountPage, wait, withExamples } from './harness/page.js';
import { GUTTER, dock, gutters, plotIds, rects } from '../src/lib/layout.js';
import { OUTER_INSET, STEP } from '../src/lib/moves.js';

const EDGES = ['left', 'right', 'top', 'bottom'];
const setup = () => mountPage();

/** Every plot whose tile on screen is more than a pixel from the box layout.js `rects` gives it. */
function misplaced(layout) {
  const r = document.querySelector('.tile-box').getBoundingClientRect();
  const want = rects(layout, { x: r.left, y: r.top, w: r.width, h: r.height }, GUTTER);
  const out = [];
  for (const [id, w] of want) {
    const got = document.querySelector(`.leaf[data-plot="${id}"]`).getBoundingClientRect();
    if (Math.max(Math.abs(got.left - w.x), Math.abs(got.top - w.y), Math.abs(got.width - w.w), Math.abs(got.height - w.h)) > 1) {
      out.push({ id, want: w, got: { x: got.left, y: got.top, w: got.width, h: got.height } });
    }
  }
  return out;
}

const step = fc.oneof(
  fc.record({ t: fc.constant('move'), a: fc.nat(), b: fc.nat(), cell: fc.constantFrom('center', ...EDGES, ...EDGES.map((e) => `outer-${e}`)) }),
  fc.record({ t: fc.constant('gap'), at: fc.nat(), by: fc.integer({ min: -150, max: 150 }) }),
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
  fc.record({ t: fc.constant('arrange'), preset: fc.constantFrom('rows', 'columns', 'grid', 'focus') }),
  fc.record({ t: fc.constant('dock'), a: fc.nat(), b: fc.nat(), edge: fc.constantFrom(...EDGES) }),
  fc.record({ t: fc.constant('wait') }),
);

/** How often each kind of step changed the layout, and how many moves were checked against their preview, over every run. */
const changed = {};
let previewed = 0;

async function run(s, { ws, history }) {
  const ids = plotIds(ws.layout);
  const before = ws.layout;
  if (s.t === 'move' && ids.length > 1) {
    const a = ids[s.a % ids.length], b = ids.filter((id) => id !== a)[s.b % (ids.length - 1)];
    const grip = document.querySelector(`.leaf[data-plot="${a}"] .grip`);
    let to;
    if (s.cell.startsWith('outer-')) {
      const r = document.querySelector('.tiles').getBoundingClientRect(), edge = s.cell.slice(6);
      const mx = r.left + r.width / 2, my = r.top + r.height / 2;
      to = { left: [r.left + OUTER_INSET, my], right: [r.right - OUTER_INSET, my], top: [mx, r.top + OUTER_INSET], bottom: [mx, r.bottom - OUTER_INSET] }[edge];
    } else {
      const [cx, cy] = centre(document.querySelector(`.leaf[data-plot="${b}"]`));
      const [dx, dy] = { center: [0, 0], left: [-1, 0], right: [1, 0], top: [0, -1], bottom: [0, 1] }[s.cell];
      to = [cx + dx * STEP, cy + dy * STEP];
    }
    if (grip) {
      // Held over the cell, the preview shows where the plot will land; let go, it lands there.
      const [x, y] = centre(grip);
      await commands.mouse([['move', x, y], ['down'], ['move', x + 8, y + 8], ['move', (x + to[0]) / 2, (y + to[1]) / 2], ['move', ...to], ['wait', 150]]);
      const box = document.querySelector('.drop-preview')?.getBoundingClientRect();
      const steps = history.undos.length;
      await commands.mouse([['up']]);
      flushSync();
      await frames();
      // A preview shown at the drop is one step; none, none.
      expect(history.undos.length - steps, 'steps a drop makes').toBe(box ? 1 : 0);
      if (box && ws.layout !== before) {
        const landed = [...ws.selected.has(a) && ws.selected.size > 1 ? ws.selected : [a]].map((id) => document.querySelector(`.leaf[data-plot="${id}"]`).getBoundingClientRect());
        const x0 = Math.min(...landed.map((l) => l.left)), y0 = Math.min(...landed.map((l) => l.top));
        const w = Math.max(...landed.map((l) => l.right)) - x0, h = Math.max(...landed.map((l) => l.bottom)) - y0;
        expect(Math.max(Math.abs(box.left - x0), Math.abs(box.top - y0), Math.abs(box.width - w), Math.abs(box.height - h)), 'landed where the preview showed').toBeLessThan(1.5);
        previewed++;
      }
    }
  } else if (s.t === 'gap') {
    const gaps = [...document.querySelectorAll('.tiles .separator')];
    if (gaps.length) {
      const g = gaps[s.at % gaps.length], [x, y] = centre(g), r = g.getBoundingClientRect();
      await drag([x, y], r.height > r.width ? [x + s.by, y] : [x, y + s.by]);
    }
  } else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  else if (s.t === 'arrange') history.group('Arrange', () => ws.arrange(s.preset));
  else if (s.t === 'dock' && ids.length) history.group('Move', () => ws.setLayout(dock(ws.layout, ids[s.a % ids.length], ids[s.b % ids.length], s.edge)));
  else if (s.t === 'wait') await wait(300);
  flushSync();
  await frames();
  if (ws.layout !== before) changed[s.t] = (changed[s.t] ?? 0) + 1;
}

/** Holds when the tiles show the layout after every step of each run, `runs` tried first. */
async function tilesFollow(...runs) {
  await fc.assert(
    fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 10 }), async (steps) => {
      const page = await setup();
      try {
        for (const s of steps) {
          await run(s, page);
          const ids = plotIds(page.ws.layout);
          expect([...document.querySelectorAll('.tiles .leaf[data-plot]')].map((e) => e.dataset.plot).sort()).toEqual([...ids].sort());
          expect(misplaced(page.ws.layout)).toEqual([]);
          expect(document.querySelectorAll('.tiles .separator').length).toBe(gutters(page.ws.layout, { x: 0, y: 0, w: 1, h: 1 }).length);
        }
      } finally {
        page.dispose();
      }
    }),
    withExamples(runs.map((r) => [r])),
  );
}

describe('the tiles', () => {
  it('show the layout after any run of moves, gap drags, undos and edits', async () => {
    await tilesFollow(
      // A layout an undo put back, changed again: the tiles once kept showing the one before.
      [{ t: 'dock', a: 2, b: 1, edge: 'left' }, { t: 'wait' }, { t: 'undo' }, { t: 'dock', a: 0, b: 1, edge: 'bottom' }],
      [{ t: 'move', a: 2, b: 1, cell: 'left' }, { t: 'undo' }, { t: 'move', a: 0, b: 0, cell: 'bottom' }, { t: 'redo' }],
    );
    for (const kind of ['move', 'gap', 'undo', 'arrange', 'dock']) expect(changed[kind], kind).toBeGreaterThan(0);
    expect(previewed).toBeGreaterThan(0);
  }, 600_000);
});

describe('a gap drag', () => {
  it('that its page leaves in the middle writes nothing and leaves undo working', async () => {
    const page = await setup();
    page.history.group('Arrange', () => page.ws.arrange('columns'));
    flushSync();
    await frames();
    const ratio = page.ws.layout.ratio;
    const [x, y] = centre(document.querySelector('.tiles .separator'));
    await commands.mouse([['move', x, y], ['down'], ['move', x + 40, y]]);
    page.dispose();
    await commands.mouse([['up']]);
    expect(page.ws.layout.ratio).toBe(ratio);
    expect(page.history.undo()).toBe('Arrange');
  });
});

describe('a gap drag', () => {
  it('whose tiles a tab switch takes away ends there, and leaves the keys to the page', async () => {
    const page = await mountPage([[], []], 'columns', 1);
    try {
      const [a] = plotIds(page.ws.layout), tab = page.ws.activeId;
      await focusPage();
      const [x, y] = centre(document.querySelector('.tiles .separator'));
      await commands.mouse([['move', x, y], ['down'], ['move', x + 40, y]]);
      // The next tab, and back: the tiles the drag was in are gone.
      await commands.key(']');
      expect(page.ws.activeId).not.toBe(tab);
      await commands.key('[');
      expect(page.ws.activeId).toBe(tab);
      await commands.mouse([['up']]);
      flushSync();
      await frames();
      // Escape is the page's again: it deselects.
      page.ws.selection.only([a]);
      flushSync();
      await commands.key('Escape');
      flushSync();
      expect(page.ws.selected.size).toBe(0);
    } finally {
      page.dispose();
    }
  });
});

describe('a gap drag', () => {
  it('that an undo changes the layout under ends there and writes nothing', async () => {
    const page = await setup();
    page.history.group('Arrange', () => page.ws.arrange('columns'));
    flushSync();
    await frames();
    const [x, y] = centre(document.querySelector('.tiles .separator'));
    await commands.mouse([['move', x, y], ['down'], ['move', x + 40, y]]);
    page.history.undo();
    flushSync();
    await frames();
    const undone = page.ws.layout;
    await commands.mouse([['move', x + 80, y], ['up']]);
    flushSync();
    expect(page.ws.layout).toBe(undone);
    expect(page.history.redoLabel).toBe('Arrange');
    expect(misplaced(page.ws.layout)).toEqual([]);
    page.dispose();
  });
});

describe('a plot move', () => {
  it('that Escape stops puts nothing down, records nothing and clears the selection', async () => {
    const page = await mountPage([[], []], 'columns');
    const [a, b] = plotIds(page.ws.layout);
    page.ws.selected.add(b);
    flushSync();
    const layout = page.ws.layout;
    // Its grip shows once the pointer is over the plot.
    await commands.mouse([['move', ...centre(document.querySelector(`.leaf[data-plot="${a}"]`))]]);
    const [x, y] = centre(document.querySelector(`.leaf[data-plot="${a}"] .grip`));
    const [tx, ty] = centre(document.querySelector(`.leaf[data-plot="${plotIds(layout)[1]}"]`));
    await commands.mouse([['move', x, y], ['down'], ['move', x + 8, y + 8], ['move', tx, ty - STEP], ['wait', 100]]);
    expect(document.querySelector('.drop-preview')).not.toBe(null);
    await commands.key('Escape');
    await commands.mouse([['up']]);
    flushSync();
    await frames();
    expect(page.ws.layout).toBe(layout);
    expect(page.history.undos.length).toBe(0);
    expect(page.ws.selected.size).toBe(0);
    expect(page.ws.focusedId).toBe(null);
    expect(document.querySelector('.drop-preview')).toBe(null);
    page.dispose();
  });

  it('that a key opens another tab under puts nothing down in either tab', async () => {
    const page = await mountPage([[], []], 'columns', 1);
    const [a, b] = plotIds(page.ws.layout);
    const tabs = page.ws.tabs;
    await commands.mouse([['move', ...centre(document.querySelector(`.leaf[data-plot="${a}"]`))]]);
    const [x, y] = centre(document.querySelector(`.leaf[data-plot="${a}"] .grip`));
    const [tx, ty] = centre(document.querySelector(`.leaf[data-plot="${b}"]`));
    await commands.mouse([['move', x, y], ['down'], ['move', x + 8, y + 8], ['move', tx, ty - STEP], ['wait', 100]]);
    expect(document.querySelector('.drop-preview')).not.toBe(null);
    page.ws.selectTab(page.ws.tabs[1].id);
    flushSync();
    // The move ends there: the pointer going on shows no cell of it.
    await commands.mouse([['move', tx, ty - STEP + 2]]);
    await frames();
    expect(document.querySelector('.drop-preview, .compass, .move-ghost')).toBe(null);
    await commands.mouse([['up']]);
    flushSync();
    await frames();
    expect(page.ws.tabs).toBe(tabs);
    expect(page.history.undos.length).toBe(0);
    page.dispose();
  });
});

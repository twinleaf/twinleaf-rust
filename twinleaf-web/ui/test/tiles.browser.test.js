// The tiles on screen follow the layout: after any run of handle drags,
// undos, redos, layout edits and box resizes, every plot of the layout is
// in one tile, at the box layout.js `rects` gives it, with a handle for
// every split.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync, mount, unmount } from 'svelte';
import fc from 'fast-check';
import '../src/app.css';
import '../src/plot.css';
import TileArea from './browser/TileArea.svelte';
import { Workspace } from '../src/lib/workspace.svelte.js';
import { History } from '../src/lib/history.svelte.js';
import { Favorites } from '../src/lib/favorites.svelte.js';
import { GUTTER, dock, gutters, plotIds, presets, rects } from '../src/lib/layout.js';
import { makeTab, newPlot } from '../src/lib/tabs.js';

const EDGES = ['left', 'right', 'top', 'bottom'];
const frames = (n = 2) => new Promise((done) => {
  const next = (k) => (k ? requestAnimationFrame(() => next(k - 1)) : done());
  next(n);
});

function setup() {
  const plots = Object.fromEntries([0, 1, 2, 3].map(() => newPlot('time', 'overlay', [])).map((p) => [p.id, p]));
  const tab = makeTab('A', { plots, layout: presets.grid(Object.keys(plots)) });
  const ws = new Workspace({ tabs: [tab], activeId: tab.id });
  const history = new History(ws, new Favorites(null));
  const target = document.body.appendChild(document.createElement('div'));
  const app = mount(TileArea, { target, props: { ws, history } });
  return {
    ws,
    history,
    dispose: () => {
      unmount(app);
      target.remove();
    },
  };
}

/** Every plot whose tile on screen is more than a pixel from the box layout.js `rects` gives it. */
function misplaced(layout) {
  const r = document.querySelector('.tile-box').getBoundingClientRect();
  const want = rects(layout, { x: r.left, y: r.top, w: r.width, h: r.height }, GUTTER);
  const out = [];
  for (const [id, w] of want) {
    const got = document.querySelector(`[data-plot="${id}"]`).getBoundingClientRect();
    if (Math.max(Math.abs(got.left - w.x), Math.abs(got.top - w.y), Math.abs(got.width - w.w), Math.abs(got.height - w.h)) > 1) out.push({ id, want: w, got: { x: got.left, y: got.top, w: got.width, h: got.height } });
  }
  return out;
}

const step = fc.oneof(
  fc.record({ t: fc.constant('drag'), handle: fc.nat(), by: fc.integer({ min: -150, max: 150 }) }),
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
  fc.record({ t: fc.constant('arrange'), preset: fc.constantFrom('rows', 'columns', 'grid', 'focus') }),
  fc.record({ t: fc.constant('dock'), a: fc.nat(), b: fc.nat(), edge: fc.constantFrom(...EDGES) }),
  fc.record({ t: fc.constant('box'), w: fc.integer({ min: 500, max: 1100 }), h: fc.integer({ min: 350, max: 800 }) }),
);

/** How many drags moved a split, over every run: none would make the property vacuous. */
let resized = 0;

async function run(s, { ws, history }) {
  if (s.t === 'drag') {
    const handles = [...document.querySelectorAll('.tiles [role="separator"]')];
    if (!handles.length) return;
    const r = handles[s.handle % handles.length].getBoundingClientRect();
    const x = r.left + r.width / 2, y = r.top + r.height / 2;
    const [dx, dy] = r.height > r.width ? [s.by, 0] : [0, s.by];
    const path = [['move', x, y], ['down']];
    for (let k = 1; k <= 4; k++) path.push(['move', x + (dx * k) / 4, y + (dy * k) / 4]);
    path.push(['up']);
    const was = history.undos.length;
    await commands.mouse(path);
    if (history.undoLabel === 'Resize' && history.undos.length > was) resized++;
  } else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  else if (s.t === 'arrange') history.group('Arrange', () => ws.arrange(s.preset));
  else if (s.t === 'dock') {
    const ids = plotIds(ws.layout);
    history.group('Move', () => (ws.tab.layout = dock(ws.layout, ids[s.a % ids.length], ids[s.b % ids.length], s.edge)));
  }
  else if (s.t === 'box') Object.assign(document.querySelector('.tiles').style, { width: `${s.w}px`, height: `${s.h}px` });
  flushSync();
  await frames();
}

describe('the tiles', () => {
  it('show the layout after any run of drags, undos and edits', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 12 }), async (steps) => {
        const page = setup();
        try {
          await frames();
          for (const s of steps) {
            await run(s, page);
            const ids = plotIds(page.ws.layout);
            expect([...document.querySelectorAll('.tiles [data-plot]')].map((e) => e.dataset.plot).sort()).toEqual([...ids].sort());
            expect(misplaced(page.ws.layout)).toEqual([]);
            expect(document.querySelectorAll('.tiles [role="separator"]').length).toBe(gutters(page.ws.layout, { x: 0, y: 0, w: 1, h: 1 }).length);
          }
        } finally {
          page.dispose();
        }
      }),
      { numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 4) },
    );
    expect(resized).toBeGreaterThan(0);
  });
});

describe('a handle drag', () => {
  it('that its tiles leave in the middle of leaves undo working', async () => {
    const page = setup();
    await frames();
    page.history.group('Arrange', () => page.ws.arrange('columns'));
    flushSync();
    await frames();
    const r = document.querySelector('.tiles [role="separator"]').getBoundingClientRect();
    const x = r.left + r.width / 2, y = r.top + r.height / 2;
    await commands.mouse([['move', x, y], ['down'], ['move', x + 40, y]]);
    page.dispose();
    await commands.mouse([['up']]);
    expect(page.history.undo()).toBe('Arrange');
  });
});

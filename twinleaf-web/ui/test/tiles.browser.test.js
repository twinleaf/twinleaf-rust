// The tiles on screen follow the layout: after any run of handle drags,
// undos, redos and layout edits, every split shows its two sides at the
// ratio the layout holds, and every plot of the layout in one tile.
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
import { dock, plotIds, presets } from '../src/lib/layout.js';
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

/** The box around the tiles of `ids`, from the page. */
function boxOf(ids) {
  const rs = ids.map((id) => document.querySelector(`[data-plot="${id}"]`).getBoundingClientRect());
  const left = Math.min(...rs.map((r) => r.left)), top = Math.min(...rs.map((r) => r.top));
  return { left, top, width: Math.max(...rs.map((r) => r.right)) - left, height: Math.max(...rs.map((r) => r.bottom)) - top };
}

/**
 * Every split of `node` whose two sides on screen are not in its order,
 * side a first, or not at its ratio, to within a pixel.
 */
function misplaced(node, out = []) {
  if (!node || node.type === 'leaf') return out;
  const a = boxOf(plotIds(node.a)), b = boxOf(plotIds(node.b));
  const row = node.dir === 'row';
  const [sa, sb] = row ? [a.width, b.width] : [a.height, b.height];
  const inOrder = row ? a.left < b.left : a.top < b.top;
  if (!inOrder || Math.abs(sa - node.ratio * (sa + sb)) > 1) out.push({ a: plotIds(node.a), dir: node.dir, ratio: node.ratio, shown: sa / (sa + sb), inOrder });
  return misplaced(node.b, misplaced(node.a, out));
}

const step = fc.oneof(
  fc.record({ t: fc.constant('drag'), handle: fc.nat(), by: fc.integer({ min: -150, max: 150 }) }),
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
  fc.record({ t: fc.constant('arrange'), preset: fc.constantFrom('rows', 'columns', 'grid', 'focus') }),
  fc.record({ t: fc.constant('dock'), a: fc.nat(), b: fc.nat(), edge: fc.constantFrom(...EDGES) }),
  fc.record({ t: fc.constant('box'), w: fc.integer({ min: 500, max: 1100 }), h: fc.integer({ min: 350, max: 800 }) }),
);

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
    await commands.mouse(path);
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
          }
        } finally {
          page.dispose();
        }
      }),
      { numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 4) },
    );
  });
});

describe('a handle drag', () => {
  it.fails('that its tiles leave in the middle of leaves undo working', async () => {
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

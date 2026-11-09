// Random runs of what a person does to the tabs (split, close, maximize,
// click, select, arrange, dock, move a plot to another tab, add, duplicate,
// close and switch tabs, reset) with undo and redo between, checking after
// every step that what the page keeps about the tabs only names plots it has.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { flushSync } from 'svelte';
import { History } from '../src/lib/history.svelte.js';
import { Workspace } from '../src/lib/workspace.svelte.js';
import { Favorites } from '../src/lib/favorites.svelte.js';
import { makeTab, newPlot } from '../src/lib/tabs.js';
import { dock, plotIds } from '../src/lib/layout.js';

function setup() {
  const a = makeTab('A'), b = makeTab('B');
  const ws = new Workspace({ tabs: [a, b], activeId: a.id });
  const history = new History(ws, new Favorites(null));
  return { ws, history };
}

const pick = (ids, i) => (ids.length ? ids[i % ids.length] : null);

/**
 * One thing a person does, its plot and tab named by index into those there
 * are; a step with nothing to act on, as closing a plot of an empty tab,
 * does nothing.
 */
const step = fc.oneof(
  fc.record({ t: fc.constant('split'), at: fc.nat(), dir: fc.constantFrom('row', 'col') }),
  fc.record({ t: fc.constant('close'), at: fc.nat() }),
  fc.record({ t: fc.constant('maximize'), at: fc.nat() }),
  fc.record({ t: fc.constant('click'), at: fc.nat() }),
  fc.record({ t: fc.constant('select'), at: fc.nat() }),
  fc.record({ t: fc.constant('arrange'), name: fc.constantFrom('rows', 'columns', 'grid', 'focus', 'quadrants') }),
  fc.record({ t: fc.constant('dock'), at: fc.nat(), to: fc.nat(), edge: fc.constantFrom('left', 'right', 'top', 'bottom') }),
  fc.record({ t: fc.constant('toTab'), at: fc.nat(), tab: fc.nat() }),
  fc.record({ t: fc.constant('tab'), at: fc.nat() }),
  fc.record({ t: fc.constant('addTab') }),
  fc.record({ t: fc.constant('duplicateTab') }),
  fc.record({ t: fc.constant('closeTab'), at: fc.nat() }),
  fc.record({ t: fc.constant('reset') }),
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
);

/** What a step can change: the tabs, the open one, and what the page keeps about it. */
const stateOf = (ws) => JSON.stringify([ws.tabs, ws.activeId, ws.maximized, ws.focusedId, [...ws.selected]]);
/** How many steps of each kind changed anything, over every run: a kind that never did tests nothing. */
const changed = {};

function run(s, { ws, history }) {
  const ids = plotIds(ws.layout);
  const id = pick(ids, s.at ?? 0);
  const others = ws.tabs.filter((t) => t.id !== ws.activeId);
  const before = stateOf(ws);
  if (s.t === 'split' && id) history.group('Split', () => ws.addPlot(newPlot('time', 'overlay', []), id, s.dir));
  else if (s.t === 'close' && id) history.group('Close', () => ws.closePlot(id));
  else if (s.t === 'maximize' && id) ws.maximized = ws.maximized === id ? null : id;
  else if (s.t === 'click' && id) ws.focusedId = id;
  else if (s.t === 'select' && id) ws.selected.add(id);
  else if (s.t === 'arrange') history.group('Arrange', () => ws.arrange(s.name));
  else if (s.t === 'dock' && id) history.group('Move', () => (ws.tab.layout = dock(ws.layout, id, pick(ids, s.to), s.edge)));
  else if (s.t === 'toTab' && id && others.length) history.group('Move to tab', () => ws.movePlotToTab(id, pick(others, s.tab).id));
  else if (s.t === 'tab') ws.selectTab(ws.tabs[s.at % ws.tabs.length].id);
  else if (s.t === 'addTab') history.group('New tab', () => ws.addTab());
  else if (s.t === 'duplicateTab') history.group('Duplicate tab', () => ws.duplicateTab(ws.tab));
  else if (s.t === 'closeTab') history.group('Close tab', () => ws.closeTab(ws.tabs[s.at % ws.tabs.length]));
  else if (s.t === 'reset') history.group('Reset', () => ws.reset());
  else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  flushSync();
  if (stateOf(ws) !== before) changed[s.t] = (changed[s.t] ?? 0) + 1;
}

/** Holds when `ok(ws, ids)` does after every step of any run, the lists of steps `runs` tried first. */
const always = (ok, ...runs) =>
  fc.assert(
    fc.property(fc.array(step, { minLength: 1, maxLength: 20 }), (steps) => {
      const page = setup();
      for (const s of steps) {
        run(s, page);
        expect(ok(page.ws, new Set(plotIds(page.ws.layout)))).toBe(true);
      }
    }),
    { examples: runs.map((r) => [r]) },
  );

describe('after any run of edits, undos and redos', () => {
  // Each run breaks one by a different way of taking the plot away.
  it.fails('the selection holds only plots of the open tab', () =>
    always((ws, ids) => [...ws.selected].every((i) => ids.has(i)), [{ t: 'select', at: 0 }, { t: 'reset' }]),
  );
  it.fails('the maximized plot is one of the open tab, or none', () =>
    always(
      (ws, ids) => ws.maximized === null || ids.has(ws.maximized),
      [{ t: 'split', at: 0, dir: 'row' }, { t: 'maximize', at: 1 }, { t: 'undo' }],
    ),
  );
  it.fails('the plot clicked last is one of the open tab, or none', () =>
    always(
      (ws, ids) => ws.focusedId === null || ids.has(ws.focusedId),
      [{ t: 'split', at: 0, dir: 'row' }, { t: 'close', at: 1 }],
      [{ t: 'click', at: 0 }, { t: 'toTab', at: 0, tab: 0 }],
      [{ t: 'click', at: 0 }, { t: 'reset' }],
    ),
  );
  it('runs every kind of step to some effect', () => {
    // One run in which each kind does something, so no kind is left to chance.
    always(
      () => true,
      [
        { t: 'split', at: 0, dir: 'row' }, { t: 'close', at: 0 }, { t: 'maximize', at: 0 }, { t: 'click', at: 1 }, { t: 'select', at: 0 },
        { t: 'arrange', name: 'rows' }, { t: 'dock', at: 0, to: 1, edge: 'left' }, { t: 'toTab', at: 0, tab: 0 }, { t: 'tab', at: 1 },
        { t: 'addTab' }, { t: 'duplicateTab' }, { t: 'closeTab', at: 0 }, { t: 'reset' }, { t: 'undo' }, { t: 'redo' },
      ],
    );
    const kinds = ['split', 'close', 'maximize', 'click', 'select', 'arrange', 'dock', 'toTab', 'tab', 'addTab', 'duplicateTab', 'closeTab', 'reset', 'undo', 'redo'];
    for (const kind of kinds) expect(changed[kind], kind).toBeGreaterThan(0);
  });
});

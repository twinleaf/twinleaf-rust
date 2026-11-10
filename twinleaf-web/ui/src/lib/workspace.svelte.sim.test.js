// Random runs of what a person does to the tabs (split, close, maximize and
// restore by F, by a plot's button and by Escape, click, select, arrange,
// dock, move a plot to another tab, add, duplicate, close and switch tabs,
// reset) with undo and redo between, each step made as the page makes it,
// checking after every step that what the page keeps about the tabs only
// names plots the open tab shows, and that the maximized plots are those a
// model of the rules gives (workspace.svelte.js).
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { flushSync } from 'svelte';
import { History } from './history.svelte.js';
import { Workspace } from './workspace.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { makeTab, newPlot } from './tabs.js';
import { dock, plotIds } from './layout.js';

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
  fc.record({ t: fc.constant('maximize') }),
  fc.record({ t: fc.constant('maximizeOn'), at: fc.nat() }),
  fc.record({ t: fc.constant('escape') }),
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
const KINDS = ['split', 'close', 'maximize', 'maximizeOn', 'escape', 'click', 'select', 'arrange', 'dock', 'toTab', 'tab', 'addTab', 'duplicateTab', 'closeTab', 'reset', 'undo', 'redo'];

/** What a step can change: the tabs, the open one, and what the page keeps about it. */
const stateOf = (ws) => JSON.stringify([ws.tabs, ws.activeId, ws.maximized, ws.focusedId, [...ws.selected]]);
/** How many steps of each kind changed anything, over every run: a kind that never did tests nothing. */
const changed = {};

/** F, or a plot's Maximize, on plots `ids` (commands.js): it leaves a maximized view, else shows `ids` alone. */
const toggle = (ws, ids) => (ws.maximized ? ws.restoreLayout() : ids.length && ws.maximize(ids));

/**
 * Makes step `s` as the page does: a plot is clicked, selected, closed,
 * split, maximized by its button or moved to another tab only where it
 * shows. Returns the plots the rules leave maximized, from those before it
 * alone: the oracle.
 */
function run(s, { ws, history }) {
  const ids = plotIds(ws.layout), shown = ws.shownIds();
  const id = pick(shown, s.at ?? 0);
  const others = ws.tabs.filter((t) => t.id !== ws.activeId);
  const before = stateOf(ws), was = { max: ws.maximized, tab: ws.activeId };
  /** The plots maximized once the step is made, were none to go. */
  let max = was.max;
  if (s.t === 'split' && id) {
    const p = newPlot('time', 'overlay', []);
    history.group('Split', () => (ws.addPlot(p, id, s.dir), ws.selection.only([p.id])));
    max = null;
  } else if (s.t === 'close' && id) history.group('Close', () => ws.closePlot(id));
  else if (s.t === 'maximize') {
    const on = [...ws.selected];
    toggle(ws, on);
    max = was.max ? null : on;
  } else if (s.t === 'maximizeOn' && id) {
    // A press on a plot's button selects the plot first, unless it is selected.
    ws.selection.pressOn(id);
    const on = ws.selection.targets(id);
    toggle(ws, on);
    max = was.max ? null : on;
  } else if (s.t === 'escape') {
    // Escape leaves a maximized view first, the selection kept; then it deselects.
    if (ws.maximized) ws.restoreLayout();
    else ws.selection.clear();
    max = null;
  } else if (s.t === 'click' && id) ws.selection.only([id]);
  else if (s.t === 'select' && id) ws.selection.click(id, true, 1);
  else if (s.t === 'arrange') {
    history.group('Arrange', () => ws.arrange(s.name));
    max = null;
  } else if (s.t === 'dock' && id) history.group('Move', () => ws.setLayout(dock(ws.layout, id, pick(ids, s.to), s.edge)));
  else if (s.t === 'toTab' && id && others.length) history.group('Move to tab', () => ws.movePlotToTab(id, pick(others, s.tab).id));
  else if (s.t === 'tab') {
    // A tab opened shows every plot, the open tab too.
    ws.selectTab(ws.tabs[s.at % ws.tabs.length].id);
    max = null;
  } else if (s.t === 'addTab') history.group('New tab', () => ws.addTab());
  else if (s.t === 'duplicateTab') history.group('Duplicate tab', () => ws.duplicateTab(ws.tab));
  else if (s.t === 'closeTab') history.group('Close tab', () => ws.closeTab(ws.tabs[s.at % ws.tabs.length]));
  else if (s.t === 'reset') {
    history.group('Reset', () => ws.reset());
    max = null;
  } else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  flushSync();
  if (stateOf(ws) !== before) changed[s.t] = (changed[s.t] ?? 0) + 1;
  // A tab opened shows every plot; a plot that has left the tab is maximized no more, and with none left every plot shows.
  const kept = ws.activeId === was.tab ? (max ?? []).filter((i) => ws.plots[i]) : [];
  return kept.length ? kept : null;
}

/** Holds when `ok(ws, ids, want)` does after every step of any run, `ids` the open tab's plots and `want` the oracle's maximized plots, the lists of steps `runs` tried first. */
const always = (ok, ...runs) =>
  fc.assert(
    fc.property(fc.array(step, { minLength: 1, maxLength: 20 }), (steps) => {
      const page = setup();
      for (const s of steps) {
        const want = run(s, page);
        expect(ok(page.ws, new Set(plotIds(page.ws.layout)), want), JSON.stringify(s)).toBe(true);
      }
    }),
    { examples: runs.map((r) => [r]) },
  );

/** A plot closed, another maximized, and the close taken back: the plot it brings back is hidden. */
const HIDDEN_BY_UNDO = [{ t: 'click', at: 1 }, { t: 'close', at: 1 }, { t: 'click', at: 0 }, { t: 'maximize' }, { t: 'undo' }];
/** A split taken back, a plot maximized, and the split made again: the plot it adds is hidden. */
const HIDDEN_BY_REDO = [{ t: 'split', at: 0, dir: 'row' }, { t: 'undo' }, { t: 'click', at: 0 }, { t: 'maximize' }, { t: 'redo' }];

describe('after any run of edits, undos and redos', () => {
  // The runs tried first each take the plot away, or hide it, a different way.
  it('the selection holds only plots the open tab shows', () =>
    always(
      (ws) => {
        const shown = ws.shownIds();
        return [...ws.selected].every((i) => shown.includes(i));
      },
      [{ t: 'select', at: 0 }, { t: 'reset' }],
      HIDDEN_BY_UNDO,
      HIDDEN_BY_REDO,
      [{ t: 'click', at: 0 }, { t: 'select', at: 1 }, { t: 'maximizeOn', at: 2 }],
    ),
  );
  it('the maximized plots are of the open tab, each once, or there are none', () =>
    always(
      (ws, ids) => ws.maximized === null || (ws.maximized.length > 0 && new Set(ws.maximized).size === ws.maximized.length && ws.maximized.every((i) => ids.has(i))),
      [{ t: 'split', at: 0, dir: 'row' }, { t: 'maximize' }, { t: 'undo' }],
    ),
  );
  it('the maximized plots are those last maximized that the tab still has, and none once a tab is opened, a plot added, the tab arranged or reset, or the view left', () =>
    always(
      (ws, ids, want) => JSON.stringify(ws.maximized) === JSON.stringify(want),
      [{ t: 'click', at: 0 }, { t: 'select', at: 1 }, { t: 'maximize' }, { t: 'close', at: 0 }, { t: 'undo' }, { t: 'redo' }, { t: 'toTab', at: 0, tab: 0 }, { t: 'undo' }],
      [{ t: 'click', at: 0 }, { t: 'maximize' }, { t: 'split', at: 0, dir: 'col' }, { t: 'maximizeOn', at: 1 }, { t: 'arrange', name: 'rows' }],
      [{ t: 'click', at: 2 }, { t: 'maximize' }, { t: 'tab', at: 1 }, { t: 'tab', at: 0 }],
      [{ t: 'maximizeOn', at: 3 }, { t: 'escape' }, { t: 'maximize' }, { t: 'maximize' }],
      [{ t: 'maximizeOn', at: 0 }, { t: 'addTab' }, { t: 'undo' }],
    ),
  );
  it('the plot clicked last is one the open tab shows, or none', () =>
    always(
      (ws) => ws.focusedId === null || ws.shownIds().includes(ws.focusedId),
      [{ t: 'split', at: 0, dir: 'row' }, { t: 'close', at: 1 }],
      [{ t: 'click', at: 0 }, { t: 'toTab', at: 0, tab: 0 }],
      [{ t: 'click', at: 0 }, { t: 'reset' }],
      HIDDEN_BY_UNDO,
    ),
  );
  it('runs every kind of step to some effect', () => {
    // One run in which each kind does something, so no kind is left to chance.
    always(
      () => true,
      [
        { t: 'split', at: 0, dir: 'row' }, { t: 'close', at: 0 }, { t: 'click', at: 0 }, { t: 'select', at: 1 }, { t: 'maximize' }, { t: 'escape' },
        { t: 'maximizeOn', at: 2 }, { t: 'arrange', name: 'rows' }, { t: 'dock', at: 0, to: 1, edge: 'left' }, { t: 'toTab', at: 0, tab: 0 }, { t: 'tab', at: 1 },
        { t: 'addTab' }, { t: 'duplicateTab' }, { t: 'closeTab', at: 0 }, { t: 'reset' }, { t: 'undo' }, { t: 'redo' },
      ],
    );
    for (const kind of KINDS) expect(changed[kind], kind).toBeGreaterThan(0);
  });
});

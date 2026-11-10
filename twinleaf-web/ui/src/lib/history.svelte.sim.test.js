// History against a model of it: random runs of edits, view changes, groups
// that change nothing, groups that throw, undos and redos. The model is a
// list of the documents a person has made, and where undo has brought them
// back to; after every step the page's tabs and stars are the model's, as
// they read without view fields.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { History } from './history.svelte.js';
import { Workspace } from './workspace.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { makeTab, newPlot, VIEW_FIELDS } from './tabs.js';
import { plotIds } from './layout.js';
import { CursorEdits } from './cursors.js';

const edits = (ws, favorites) => JSON.stringify([JSON.parse(JSON.stringify(ws.tabs, (k, v) => (VIEW_FIELDS.has(k) ? undefined : v))), [...favorites.names].sort()]);

const step = fc.oneof(
  fc.record({ t: fc.constant('legend'), at: fc.nat() }),
  fc.record({ t: fc.constant('split'), at: fc.nat() }),
  fc.record({ t: fc.constant('close'), at: fc.nat() }),
  fc.record({ t: fc.constant('ratio'), r: fc.double({ min: 0.1, max: 0.9, noNaN: true }) }),
  fc.record({ t: fc.constant('star'), name: fc.constantFrom('a', 'b', 'c') }),
  fc.record({ t: fc.constant('view'), at: fc.nat(), lo: fc.integer({ min: 1, max: 100 }) }),
  fc.record({ t: fc.constant('same'), at: fc.nat() }),
  // A cursor drag over `n` plots: it shows each place it passes, and writes the last as it ends.
  fc.record({ t: fc.constant('cursor'), n: fc.integer({ min: 1, max: 3 }), path: fc.array(fc.option(fc.integer({ min: 0, max: 3 })), { minLength: 1, maxLength: 4 }) }),
  fc.record({ t: fc.constant('throw'), at: fc.nat() }),
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
);

describe('history, against a model of it', () => {
  it('holds every document made, undoes and redoes through them, and keeps no step of a no-op, a view change or a throw', () => {
    fc.assert(
      fc.property(fc.array(step, { minLength: 1, maxLength: 25 }), (steps) => {
        const tab = makeTab('A');
        const ws = new Workspace({ tabs: [tab], activeId: tab.id });
        const favorites = new Favorites(null);
        const history = new History(ws, favorites);
        const cursors = new CursorEdits(ws, history);
        const docs = [edits(ws, favorites)];
        let at = 0;
        const edit = (label, fn) => {
          history.group(label, fn);
          const now = edits(ws, favorites);
          if (now !== docs[at]) {
            docs.splice(at + 1, docs.length, now);
            at++;
          }
        };
        for (const s of steps) {
          const ids = plotIds(ws.layout), id = ids.length ? ids[(s.at ?? 0) % ids.length] : null;
          if (s.t === 'legend' && id) edit('Legend', () => ws.setPlot(id, { legend: !ws.plots[id].legend }));
          else if (s.t === 'split' && id) edit('Split', () => ws.addPlot(newPlot('time', 'overlay', []), id, 'row'));
          else if (s.t === 'close' && id) edit('Close', () => ws.closePlot(id));
          else if (s.t === 'ratio' && ws.layout?.type === 'split') edit('Resize', () => ws.setRatio('', s.r));
          else if (s.t === 'star') edit('Star', () => favorites.pin([s.name], !favorites.has(s.name)));
          else if (s.t === 'view' && id) ws.setView(id, { fRange: [s.lo, s.lo * 10] });
          else if (s.t === 'same' && id) edit('Same', () => ws.setPlot(id, { kind: ws.plots[id].kind }));
          else if (s.t === 'cursor' && id) {
            const steps = history.undos.length;
            for (const at of s.path) for (const i of ids.slice(0, s.n)) cursors.draft(i, at === null ? undefined : { x: { a: at, b: null } });
            expect(history.undos.length, 'a drag writes nothing until it ends').toBe(steps);
            edit('Cursor', () => cursors.commit('Move X1'));
            expect(history.undos.length - steps, 'one step at most, however many plots').toBeLessThanOrEqual(1);
          } else if (s.t === 'throw' && id) {
            expect(() => history.group('Broken', () => (ws.closePlot(id), favorites.pin(['z'], true), fail()))).toThrow();
          } else if (s.t === 'undo') {
            const label = history.undo();
            expect(label === null).toBe(at === 0);
            if (label) at--;
          } else if (s.t === 'redo') {
            const label = history.redo();
            expect(label === null).toBe(at === docs.length - 1);
            if (label) at++;
          }
          expect(edits(ws, favorites)).toBe(docs[at]);
          expect(history.undos.length).toBe(at);
          expect(history.redos.length).toBe(docs.length - 1 - at);
        }
      }),
    );
  });
});

function fail() {
  throw new Error('broken');
}

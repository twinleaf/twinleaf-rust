// The Selection against a model of it: random runs of presses, clicks with
// and without Shift, keyboard arrivals, row picks, of property rows and of
// signals, by a click with any keys or all at once, undo restores, plots and
// rows going away and time passing. After every act the plots selected, the
// plot clicked last, the property rows picked and the signals picked are
// what the rules say; none names a plot that is gone; property rows are
// never chosen with plots or with signals, and signals stay with plots;
// only a click on one of several selected plots leaves anything to happen
// later; and the plots and the signals a command acts on are what its
// origin and scope say.
import { afterEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { DOUBLE_CLICK_MS, SIGNAL, Selection } from './selection.svelte.js';
import * as clock from './clock.js';

/** The virtual clock a test runs on. */
let time = null;
afterEach(() => time?.end());

const IDS = ['a', 'b', 'c', 'd'];
/** The rows of the page in page order: the Signals list between two panels of properties, as a dock has them. */
const ROWS = ['s:gain', 's:offset', `${SIGNAL}x`, `${SIGNAL}y`, `${SIGNAL}z`, 'r:temp', 'r:volts'];
const PROPERTIES = ROWS.filter((k) => !k.startsWith(SIGNAL)), SIGNALS = ROWS.filter((k) => k.startsWith(SIGNAL));
const isSignal = (k) => k.startsWith(SIGNAL);
/** An element a press can land on: on a plot, in a keeper, in a table of rows, or elsewhere. */
function element(where) {
  const outer = document.createElement('div');
  if (where === 'plot') outer.className = 'leaf';
  if (where === 'keeper') outer.dataset.keepsSelection = '';
  if (where === 'rows') outer.setAttribute('role', 'grid');
  const inner = outer.appendChild(document.createElement('span'));
  document.body.appendChild(outer);
  return inner;
}

const act = fc.oneof(
  fc.record({ t: fc.constant('press'), where: fc.constantFrom('plot', 'keeper', 'rows', 'elsewhere'), shift: fc.boolean() }),
  fc.record({ t: fc.constant('pressOn'), at: fc.nat() }),
  fc.record({ t: fc.constant('click'), at: fc.nat(), shift: fc.boolean(), clicks: fc.constantFrom(1, 2) }),
  fc.record({ t: fc.constant('arrived'), at: fc.nat() }),
  fc.record({ t: fc.constant('only'), ids: fc.subarray(IDS) }),
  fc.record({ t: fc.constant('rows'), keys: fc.subarray(PROPERTIES) }),
  fc.record({ t: fc.constant('rows'), keys: fc.subarray(SIGNALS, { minLength: 1 }) }),
  { weight: 4, arbitrary: fc.record({ t: fc.constant('pick'), key: fc.constantFrom(...ROWS), how: fc.constantFrom('click', 'click', 'ctrl', 'shift', 'right'), clicks: fc.constantFrom(0, 1, 2) }) },
  fc.record({ t: fc.constant('move'), key: fc.constantFrom(...ROWS) }),
  fc.record({ t: fc.constant('unrow'), key: fc.constantFrom(...ROWS) }),
  fc.record({ t: fc.constant('snapshot') }),
  fc.record({ t: fc.constant('restore') }),
  fc.record({ t: fc.constant('remove'), at: fc.nat() }),
  fc.record({ t: fc.constant('wait'), ms: fc.constantFrom(100, DOUBLE_CLICK_MS + 10) }),
  fc.record({ t: fc.constant('targets'), at: fc.nat(), scope: fc.constantFrom('view', 'plot'), key: fc.constantFrom(...SIGNALS) }),
);

describe('the Selection, against a model of it', () => {
  it('chooses what the rules say after every act, names no plot that is gone, and leaves nothing for later but a narrowing click', () => {
    const seen = { withPlots: 0, runOver: 0, kindChange: 0, dropped: 0, signalClick: 0, rowNarrowed: 0 };
    fc.assert(
      fc.property(fc.array(act, { minLength: 1, maxLength: 30 }), (acts) => {
        time = clock.virtual();
        const plots = new Set(IDS);
        const s = new Selection((id) => plots.has(id), () => [...plots]);
        // The rows on the page, in page order; a row goes as its device's property does, or as the search leaves a signal out.
        const rows = new Set(ROWS);
        const order = () => ROWS.filter((k) => rows.has(k));
        const show = () => (s.rowOrder = () => order());
        show();
        let saved = s.snapshot();
        let narrowing = null; // the plot a click will narrow to, if nothing comes first
        // The model of the rows: every row picked, shown or not, in the order picked, and the row a Shift pick runs from.
        let picks = [], anchor = null;
        let rowNarrowing = null; // the signal a click on one of several picked will narrow the pick to, if nothing comes first
        const setRows = (keys) => {
          rowNarrowing = null;
          if (!keys.length) anchor = null;
          picks = [...new Set(keys)];
        };
        /** Lets go of the rows picked where they are property rows: signals stay with the plots. */
        const drop = () => picks.some((k) => !isSignal(k)) && setRows([]);
        const shownPicks = () => picks.filter((k) => rows.has(k));
        const pick = (k) => [...plots][k % Math.max(1, plots.size)];
        const state = () => JSON.stringify([[...s.plots].sort(), s.last, [...s.rows].sort(), [...s.signals].sort()]);
        for (const a of acts) {
          const was = new Set(s.plots);
          const id = plots.size ? pick(a.at ?? 0) : null;
          if (a.t !== 'wait' && a.t !== 'snapshot' && a.t !== 'targets') narrowing = null;
          if (a.t === 'press') {
            s.press({ button: 0, shiftKey: a.shift, target: element(a.where) });
            const keeps = a.shift || a.where === 'plot' || a.where === 'keeper';
            if (keeps) expect([...s.plots].sort()).toEqual([...was].sort());
            else expect(s.plots.size).toBe(0);
            // A press among property rows lets go of the plots only; one elsewhere, of every row.
            if (!keeps && a.where !== 'rows') setRows([]);
          } else if (a.t === 'pressOn' && id) {
            s.pressOn(id);
            expect([...s.plots].sort()).toEqual(was.has(id) ? [...was].sort() : [id]);
            expect(s.last).toBe(id);
            drop();
          } else if (a.t === 'click' && id) {
            s.click(id, a.shift, a.clicks);
            if (a.shift) {
              const want = new Set(was);
              if (!want.delete(id)) want.add(id);
              expect([...s.plots].sort()).toEqual([...want].sort());
            } else if (!was.has(id) || was.size === 1) expect([...s.plots]).toEqual([id]);
            else {
              expect([...s.plots].sort()).toEqual([...was].sort());
              if (a.clicks === 1) narrowing = id;
            }
            drop();
          } else if (a.t === 'arrived' && id) {
            s.arrived(id);
            expect([...s.plots]).toEqual([id]);
            drop();
          } else if (a.t === 'only') {
            const ids = a.ids.filter((i) => plots.has(i));
            s.only(ids);
            expect([...s.plots].sort()).toEqual([...ids].sort());
            if (ids.length) drop();
          } else if (a.t === 'rows') {
            s.pickRows(a.keys);
            setRows(a.keys);
            // Signals leave the plots selected; property rows, as no rows, let go of them.
            if (a.keys.some(isSignal)) expect([...s.plots].sort()).toEqual([...was].sort());
            else expect(s.plots.size).toBe(0);
          } else if (a.t === 'pick' && rows.has(a.key)) {
            // A click on a row the page shows, with Ctrl, with Shift, or the right button.
            const like = (k) => isSignal(k) === isSignal(a.key), ord = order().filter(like), before = shownPicks();
            if (before.length && !before.some(like)) seen.kindChange++;
            rowNarrowing = null;
            if (a.how === 'right') {
              s.rowMenu(a.key);
              if (!before.includes(a.key)) (setRows([a.key]), (anchor = a.key));
            } else if (a.how === 'click' && a.clicks && isSignal(a.key) && before.length > 1 && before.includes(a.key)) {
              // A click on one of several signals picked leaves them picked; a first click narrows to it later.
              s.pickRow(a.key, {}, a.clicks);
              anchor = a.key;
              if (a.clicks === 1) rowNarrowing = a.key;
              seen.signalClick++;
            } else {
              s.pickRow(a.key, { shiftKey: a.how === 'shift', ctrlKey: a.how === 'ctrl' }, a.clicks);
              if (a.how === 'shift' && ord.includes(anchor)) {
                const [i, j] = [ord.indexOf(anchor), ord.indexOf(a.key)].sort((x, y) => x - y);
                setRows(ord.slice(i, j + 1));
                // A run from one panel of properties to the other passes over the signals between them.
                if (!isSignal(a.key) && order().slice(order().indexOf(ord[i]), order().indexOf(ord[j])).some(isSignal)) seen.runOver++;
              } else {
                if (a.how === 'ctrl') {
                  const next = before.filter(like);
                  if (next.includes(a.key)) seen.dropped++;
                  setRows(next.includes(a.key) ? next.filter((k) => k !== a.key) : [...next, a.key]);
                } else setRows([a.key]);
                anchor = a.key;
              }
            }
            const changed = a.how !== 'right' || !before.includes(a.key);
            if (isSignal(a.key) || !changed) expect([...s.plots].sort(), 'a signal picked leaves the plots').toEqual([...was].sort());
            else expect(s.plots.size, 'a property row picked lets go of the plots').toBe(0);
            if (a.how !== 'right') expect(s.rowAt).toBe(a.key);
            if (isSignal(a.key) && was.size) seen.withPlots++;
          } else if (a.t === 'snapshot') saved = s.snapshot();
          else if (a.t === 'restore') {
            s.restore(saved);
            const back = saved.selected.filter((i) => plots.has(i));
            expect([...s.plots].sort()).toEqual(back.sort());
            // With a plot back, no property row; with none, the property rows picked then. The signals picked stay, but where property rows come back.
            const rowsBack = back.length ? [] : saved.rows.filter((k) => !isSignal(k));
            if (rowsBack.length) setRows(rowsBack);
            else drop();
          } else if (a.t === 'move') {
            // The arrows move the row they are on, and pick nothing.
            s.moveTo(a.key);
            expect(s.rowAt).toBe(rows.has(a.key) ? a.key : null);
          } else if (a.t === 'unrow') {
            // The page shows its rows anew.
            rows.delete(a.key);
            show();
          } else if (a.t === 'remove' && id) {
            plots.delete(id);
            s.prune();
            if (narrowing === id) narrowing = null;
          } else if (a.t === 'targets' && id) {
            // A command made on a plot acts on the selection it is in, or on it alone.
            expect(s.targets(id).sort()).toEqual((was.has(id) ? [...was] : [id]).sort());
            // One from the page or the keys acts on the selection; with none, a view command on what Ctrl+A would select, a plot command on nothing.
            const ctrlA = new Selection((i) => plots.has(i), () => [...plots]);
            ctrlA.only(ctrlA.all(), null);
            expect(s.pageTargets(a.scope).sort()).toEqual((was.size ? [...was] : a.scope === 'view' ? [...ctrlA.plots] : []).sort());
            // A command made on a signal's row acts on the signals picked when it is one of them, or on it alone; from the page, on the signals picked.
            const signals = shownPicks().filter(isSignal).map((k) => k.slice(SIGNAL.length)), key = a.key.slice(SIGNAL.length);
            expect(s.signalTargets(key)).toEqual(signals.includes(key) ? signals : [key]);
            expect(s.signalTargets()).toEqual(signals);
          } else if (a.t === 'wait') {
            const before = state();
            time.advance(a.ms);
            if (a.ms > DOUBLE_CLICK_MS) {
              // A click waiting to narrow the plots or the signals picked has; nothing else changes.
              expect([...s.plots].sort()).toEqual(narrowing ? [narrowing] : [...was].sort());
              if (rowNarrowing) (setRows([rowNarrowing]), seen.rowNarrowed++);
              narrowing = null;
            } else if (!narrowing && !rowNarrowing) expect(state()).toBe(before);
          }
          // The rows picked are those of the picks the page shows, in the order picked: property rows, or signals by key.
          expect([...s.rows], JSON.stringify(a)).toEqual(shownPicks().filter((k) => !isSignal(k)));
          expect([...s.signals], JSON.stringify(a)).toEqual(shownPicks().filter(isSignal).map((k) => k.slice(SIGNAL.length)));
          for (const p of s.plots) expect(plots.has(p), `${p} selected`).toBe(true);
          expect(s.last === null || plots.has(s.last)).toBe(true);
          expect(s.plots.size && s.rows.size, 'plots with property rows').toBeFalsy();
          expect(s.signals.size && s.rows.size, 'signals with property rows').toBeFalsy();
        }
        document.body.replaceChildren();
        time.end();
      }),
      {
        examples: [
          // A plot that goes between the snapshot and the restore is not selected again.
          [[{ t: 'only', ids: ['a', 'b'] }, { t: 'snapshot' }, { t: 'remove', at: 0 }, { t: 'restore' }]],
          // Nor is a row whose property went.
          [[{ t: 'rows', keys: ['s:gain', 'r:temp'] }, { t: 'snapshot' }, { t: 'unrow', key: 's:gain' }, { t: 'only', ids: ['a'] }, { t: 'restore' }]],
          // Signals are picked with plots selected, a run of them by Shift, one dropped by Ctrl; a property row picked lets go of both.
          [[{ t: 'only', ids: ['a', 'b'] }, { t: 'pick', key: `${SIGNAL}x`, how: 'click' }, { t: 'pick', key: `${SIGNAL}z`, how: 'shift' }, { t: 'pick', key: `${SIGNAL}y`, how: 'ctrl' }, { t: 'targets', at: 0, scope: 'plot', key: `${SIGNAL}y` }, { t: 'pick', key: 's:gain', how: 'click' }]],
          // The one signal picked, dropped by Ctrl, leaves the plots selected.
          [[{ t: 'only', ids: ['a'] }, { t: 'pick', key: `${SIGNAL}y`, how: 'click' }, { t: 'pick', key: `${SIGNAL}y`, how: 'ctrl' }]],
          // A click on one of two signals picked narrows to it once the double-click time has passed; a double-click leaves both.
          [[{ t: 'rows', keys: [`${SIGNAL}x`, `${SIGNAL}y`] }, { t: 'pick', key: `${SIGNAL}x`, how: 'click', clicks: 1 }, { t: 'wait', ms: 100 }, { t: 'wait', ms: DOUBLE_CLICK_MS + 10 }]],
          [[{ t: 'rows', keys: [`${SIGNAL}x`, `${SIGNAL}y`] }, { t: 'pick', key: `${SIGNAL}x`, how: 'click', clicks: 1 }, { t: 'pick', key: `${SIGNAL}x`, how: 'click', clicks: 2 }, { t: 'wait', ms: DOUBLE_CLICK_MS + 10 }]],
          // An undo puts back the plots, and leaves the signals picked since.
          [[{ t: 'only', ids: ['a'] }, { t: 'rows', keys: [`${SIGNAL}x`] }, { t: 'snapshot' }, { t: 'pick', key: `${SIGNAL}z`, how: 'click', clicks: 1 }, { t: 'only', ids: ['b'] }, { t: 'restore' }]],
          [[{ t: 'rows', keys: [`${SIGNAL}x`] }, { t: 'snapshot' }, { t: 'pick', key: `${SIGNAL}z`, how: 'click', clicks: 1 }, { t: 'restore' }]],
          // With no plot back, the property rows picked come back.
          [[{ t: 'rows', keys: ['s:gain'] }, { t: 'snapshot' }, { t: 'pick', key: `${SIGNAL}z`, how: 'click', clicks: 1 }, { t: 'restore' }]],
          // The arrows move off the signals picked and pick nothing.
          [[{ t: 'rows', keys: [`${SIGNAL}x`] }, { t: 'move', key: `${SIGNAL}y` }, { t: 'move', key: 'r:temp' }]],
          // A run of property rows from one panel to the other passes over the Signals list between them.
          [[{ t: 'pick', key: 's:offset', how: 'click' }, { t: 'pick', key: 'r:temp', how: 'shift' }, { t: 'pick', key: `${SIGNAL}y`, how: 'right' }, { t: 'press', where: 'rows', shift: false }]],
        ],
      },
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  });
});

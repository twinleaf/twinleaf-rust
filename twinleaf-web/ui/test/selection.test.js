// The Selection against a model of it: random runs of presses, clicks with
// and without Shift, keyboard arrivals, row picks, undo restores, plots
// going away and time passing. After every act the plots selected, the plot
// clicked last and the rows picked are what the rules say; none names a plot
// that is gone; plots and rows are never chosen together; only a click
// on one of several selected plots leaves anything to happen later; and the
// plots a command acts on are what its origin and scope say.
import { afterEach, describe, expect, it, vi } from 'vitest';
import fc from 'fast-check';
import fs from 'node:fs';
import path from 'node:path';
import { DOUBLE_CLICK_MS, Selection } from '../src/lib/selection.svelte.js';

afterEach(() => vi.useRealTimers());

const IDS = ['a', 'b', 'c', 'd'];
const ROWS = ['s:gain', 's:offset', 'r:temp'];
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
  fc.record({ t: fc.constant('rows'), keys: fc.subarray(ROWS) }),
  fc.record({ t: fc.constant('unrow'), key: fc.constantFrom(...ROWS) }),
  fc.record({ t: fc.constant('snapshot') }),
  fc.record({ t: fc.constant('restore') }),
  fc.record({ t: fc.constant('remove'), at: fc.nat() }),
  fc.record({ t: fc.constant('wait'), ms: fc.constantFrom(100, DOUBLE_CLICK_MS + 10) }),
  fc.record({ t: fc.constant('targets'), at: fc.nat(), scope: fc.constantFrom('view', 'plot') }),
);

describe('the Selection, against a model of it', () => {
  it('chooses what the rules say after every act, names no plot that is gone, and leaves nothing for later but a narrowing click', () => {
    fc.assert(
      fc.property(fc.array(act, { minLength: 1, maxLength: 30 }), (acts) => {
        vi.useFakeTimers();
        const plots = new Set(IDS);
        const s = new Selection((id) => plots.has(id), () => [...plots]);
        // The rows on the page; a row goes as its device's property does.
        const rows = new Set(ROWS);
        const show = () => (s.rowOrder = () => [...rows]);
        show();
        let saved = s.snapshot();
        let narrowing = null; // the plot a click will narrow to, if nothing comes first
        const pick = (k) => [...plots][k % Math.max(1, plots.size)];
        const state = () => JSON.stringify([[...s.plots].sort(), s.last, [...s.rows].sort()]);
        for (const a of acts) {
          const was = new Set(s.plots), wasRows = new Set(s.rows);
          const id = plots.size ? pick(a.at ?? 0) : null;
          if (a.t !== 'wait' && a.t !== 'snapshot' && a.t !== 'targets') narrowing = null;
          if (a.t === 'press') {
            s.press({ button: 0, shiftKey: a.shift, target: element(a.where) });
            const keeps = a.shift || a.where === 'plot' || a.where === 'keeper';
            if (keeps) expect([...s.plots].sort()).toEqual([...was].sort());
            else expect(s.plots.size).toBe(0);
            if (keeps || a.where === 'rows') expect([...s.rows]).toEqual([...wasRows]);
            else expect(s.rows.size).toBe(0);
          } else if (a.t === 'pressOn' && id) {
            s.pressOn(id);
            expect([...s.plots].sort()).toEqual(was.has(id) ? [...was].sort() : [id]);
            expect(s.last).toBe(id);
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
          } else if (a.t === 'arrived' && id) {
            s.arrived(id);
            expect([...s.plots]).toEqual([id]);
          } else if (a.t === 'only') {
            const ids = a.ids.filter((i) => plots.has(i));
            s.only(ids);
            expect([...s.plots].sort()).toEqual([...ids].sort());
          } else if (a.t === 'rows') {
            s.pickRows(a.keys);
            expect([...s.rows].sort()).toEqual(a.keys.filter((k) => rows.has(k)).sort());
            expect(s.plots.size).toBe(0);
          } else if (a.t === 'snapshot') saved = s.snapshot();
          else if (a.t === 'restore') {
            s.restore(saved);
            const back = saved.selected.filter((i) => plots.has(i));
            expect([...s.plots].sort()).toEqual(back.sort());
            // Rows never go with plots: with plots back, none is picked.
            if (back.length) expect(s.rows.size).toBe(0);
            // With no plot back, the rows picked come back, those still on the page.
            else expect([...s.rows].sort()).toEqual(saved.rows.filter((k) => rows.has(k)).sort());
          } else if (a.t === 'unrow') {
            // The page shows its rows anew.
            rows.delete(a.key);
            show();
          }
          else if (a.t === 'remove' && id) {
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
          } else if (a.t === 'wait') {
            const before = state();
            vi.advanceTimersByTime(a.ms);
            if (narrowing && a.ms > DOUBLE_CLICK_MS) {
              expect([...s.plots]).toEqual([narrowing]);
              narrowing = null;
            } else if (a.ms > DOUBLE_CLICK_MS || !narrowing) expect(state()).toBe(before);
          }
          for (const p of s.plots) expect(plots.has(p), `${p} selected`).toBe(true);
          expect(s.last === null || plots.has(s.last)).toBe(true);
          expect(s.plots.size && s.rows.size).toBeFalsy();
        }
        document.body.replaceChildren();
        vi.useRealTimers();
      }),
      {
        examples: [
          // A plot that goes between the snapshot and the restore is not selected again.
          [[{ t: 'only', ids: ['a', 'b'] }, { t: 'snapshot' }, { t: 'remove', at: 0 }, { t: 'restore' }]],
          // Nor is a row whose property went.
          [[{ t: 'rows', keys: ['s:gain', 'r:temp'] }, { t: 'snapshot' }, { t: 'unrow', key: 's:gain' }, { t: 'only', ids: ['a'] }, { t: 'restore' }]],
        ],
      },
    );
  });
});

describe('the plot selection', () => {
  it('is written only by the Selection', () => {
    const writes = /\.selected\.(add|delete|clear)\(|\bfocusedId\s*=[^=]|\.selection\.(plots\.(add|delete|clear)\(|last\s*=[^=]|rows\s*=[^=])/;
    const files = (dir) => fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? files(path.join(dir, e.name)) : /\.(js|svelte)$/.test(e.name) ? [path.join(dir, e.name)] : []));
    const found = files(path.resolve('src'))
      .filter((f) => !f.endsWith('selection.svelte.js'))
      .flatMap((f) => fs.readFileSync(f, 'utf8').split('\n').map((line, i) => [f, i + 1, line]))
      .filter(([, , line]) => writes.test(line) && !line.trim().startsWith('//') && !line.trim().startsWith('*'))
      .map(([f, n]) => `${path.relative('src', f)}:${n}`);
    expect(found).toEqual([]);
  });
});

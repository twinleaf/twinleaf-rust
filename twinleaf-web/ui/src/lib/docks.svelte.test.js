import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { DEFAULT, Docks, KEY, PANELS, SIDES, move, normalize, panelsOn } from './docks.svelte.js';

const IDS = Object.keys(PANELS);
const ids = (docks) => ({ left: docks.left.map((p) => p.id), right: docks.right.map((p) => p.id) });
/** The panels an arrangement {places} has on each side, by id. */
const sides = ({ places }) => ({ left: panelsOn(places, 'left').map((p) => p.id), right: panelsOn(places, 'right').map((p) => p.id) });

function memory(saved) {
  const store = new Map(saved === undefined ? [] : [[KEY, JSON.stringify(saved)]]);
  return { getItem: (k) => store.get(k) ?? null, setItem: (k, v) => store.set(k, v), store };
}

describe('the docks', () => {
  it('start as the default with nothing saved', () => {
    expect(sides(normalize(null))).toEqual(ids(DEFAULT));
    expect(ids(new Docks(null))).toEqual(ids(DEFAULT));
  });

  it('keep each known panel once, where it is named first, and add the missing ones to their side', () => {
    const docks = new Docks(memory({ left: [{ id: 'reports', open: false }, { id: 'nope' }, { id: 'reports' }], right: [{ id: 'devices' }, { id: 'reports' }] }));
    expect(docks.left[0]).toEqual({ id: 'reports', open: false });
    expect(docks.right[0].id).toBe('devices');
    expect([...docks.left, ...docks.right].map((p) => p.id).sort()).toEqual([...IDS].sort());
  });

  it('move a panel to the other side at an index', () => {
    const got = sides({ places: move(normalize(null).places, 'signals', 'right', 1) });
    expect(got.left).toEqual(['devices']);
    expect(got.right.slice(0, 3)).toEqual(['favorites', 'signals', 'settings']);
  });

  it('keep the arrangement across a reload', () => {
    const storage = memory();
    const docks = new Docks(storage);
    docks.moveTo('actions', 'left', 0);
    docks.toggle('actions');
    const again = new Docks(storage);
    expect(again.left[0]).toEqual({ id: 'actions', open: false });
    expect(again.sideOf('actions')).toBe('left');
  });

  it('save nothing until the arrangement changes', () => {
    const storage = memory();
    new Docks(storage);
    expect(storage.store.size).toBe(0);
  });

  it('step a panel within its side, stopping at the ends', () => {
    const docks = new Docks(memory());
    docks.step('devices', -1);
    expect(docks.left[0].id).toBe('devices');
    docks.step('devices', 1);
    expect(docks.left.map((p) => p.id)).toEqual(['signals', 'devices']);
  });

  it('start over from a saved value that is not an arrangement', () => {
    const storage = memory();
    storage.setItem(KEY, '{not json');
    expect(ids(new Docks(storage))).toEqual(ids(DEFAULT));
  });
});

describe('a hidden dock', () => {
  it('keeps its panels and is kept across a reload', () => {
    const storage = memory();
    const docks = new Docks(storage);
    docks.hide('right', true);
    expect(docks.shown('right')).toBe(false);
    const again = new Docks(storage);
    expect(again.hidden.right).toBe(true);
    expect(again.right.length).toBe(DEFAULT.right.length);
  });

  it('shows again when a panel moves into it', () => {
    const docks = new Docks(null);
    docks.hide('left', true);
    docks.moveTo('settings', 'left', 0);
    expect(docks.shown('left')).toBe(true);
  });
});

describe('a panel', () => {
  const panel = fc.constantFrom(...IDS), side = fc.constantFrom(...SIDES), index = fc.integer({ min: -2, max: IDS.length + 2 });
  // Every write the page makes, in any order, and a page loaded again from what was saved.
  const step = fc.oneof(
    fc.record({ t: fc.constant('moveTo'), panel, side, index }),
    fc.record({ t: fc.constant('step'), panel, by: fc.constantFrom(-1, 1) }),
    fc.record({ t: fc.constant('toggle'), panel }),
    fc.record({ t: fc.constant('hide'), side, hidden: fc.boolean() }),
    fc.record({ t: fc.constant('drop'), panel, side, index }),
    fc.record({ t: fc.constantFrom('startDrag', 'endDrag', 'reset', 'reload') }),
  );
  const whole = (docks) => ({ left: docks.left, right: docks.right, hidden: docks.hidden });

  it('is in one dock after any run of moves, drops, folds, hides, resets and reloads, moves only by the drop while one is dragged, and what is saved loads as the same arrangement', () => {
    fc.assert(
      fc.property(fc.array(step, { maxLength: 30 }), (steps) => {
        const storage = memory();
        let docks = new Docks(storage);
        for (const s of steps) {
          const before = whole(docks), was = docks.sideOf(s.panel), held = docks.dragging;
          if (s.t === 'moveTo') docks.moveTo(s.panel, s.side, s.index);
          else if (s.t === 'step') docks.step(s.panel, s.by);
          else if (s.t === 'toggle') docks.toggle(s.panel);
          else if (s.t === 'hide') docks.hide(s.side, s.hidden);
          else if (s.t === 'drop') docks.drop(s.panel, s.side, s.index);
          else if (s.t === 'reload') docks = new Docks(storage);
          else docks[s.t]();
          const said = JSON.stringify(s);
          expect([...docks.left, ...docks.right].map((p) => p.id).sort(), `after ${said}: the panels of both docks`).toEqual([...IDS].sort());
          for (const id of IDS) expect(docks[docks.sideOf(id)].filter((p) => p.id === id), `after ${said}: ${id} in the dock it is said to be in`).toHaveLength(1);
          // While a panel is dragged the arrangement stands, but for the drop.
          if (held && !['drop', 'reload'].includes(s.t)) {
            expect(whole(docks), `after ${said}, a panel dragged: the arrangement`).toEqual(before);
            if (storage.store.size) expect(whole(new Docks(storage)), `after ${said}, a panel dragged: what is saved, loaded`).toEqual(before);
            continue;
          }
          expect(docks.dragging, `after ${said}: a panel is dragged`).toBe(s.t === 'startDrag');
          // A panel that moved is where it was sent, the others in the order they had.
          if (s.t === 'moveTo' || s.t === 'drop') {
            expect(docks.sideOf(s.panel), `after ${said}: the side of ${s.panel}`).toBe(s.side);
            for (const k of SIDES) expect(docks[k].map((p) => p.id).filter((id) => id !== s.panel), `after ${said}: the other panels of the ${k} dock`).toEqual(before[k].map((p) => p.id).filter((id) => id !== s.panel));
            expect(docks.hidden[s.side], `after ${said}: the ${s.side} dock is hidden`).toBe(false);
          }
          if (s.t === 'step') expect(docks.sideOf(s.panel)).toBe(was);
          if (s.t === 'reload') expect(whole(docks), 'a page loaded again holds what the last one did').toEqual(before);
          if (storage.store.size) expect(whole(new Docks(storage)), `after ${said}: what is saved, loaded`).toEqual(whole(docks));
        }
      }),
    );
  });
});

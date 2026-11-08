import { describe, expect, it } from 'vitest';
import { DEFAULT, Docks, KEY, PANELS, move, normalize } from '../src/lib/docks.svelte.js';

const ids = (docks) => ({ left: docks.left.map((p) => p.id), right: docks.right.map((p) => p.id) });

function memory(saved) {
  const store = new Map(saved === undefined ? [] : [[KEY, JSON.stringify(saved)]]);
  return { getItem: (k) => store.get(k) ?? null, setItem: (k, v) => store.set(k, v), store };
}

describe('the docks', () => {
  it('start as the default with nothing saved', () => {
    expect(ids(normalize(null))).toEqual(ids(DEFAULT));
  });

  it('keep each known panel once and add the missing ones to their side', () => {
    const got = normalize({ left: [{ id: 'reports', open: false }, { id: 'nope' }, { id: 'reports' }], right: [{ id: 'devices' }] });
    expect(got.left[0]).toEqual({ id: 'reports', open: false });
    expect(got.right[0].id).toBe('devices');
    const all = [...got.left, ...got.right].map((p) => p.id).sort();
    expect(all).toEqual(Object.keys(PANELS).sort());
  });

  it('move a panel to the other side at an index', () => {
    const got = move(normalize(null), 'signals', 'right', 1);
    expect(got.left.map((p) => p.id)).toEqual(['devices']);
    expect(got.right.map((p) => p.id).slice(0, 3)).toEqual(['settings', 'signals', 'reports']);
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

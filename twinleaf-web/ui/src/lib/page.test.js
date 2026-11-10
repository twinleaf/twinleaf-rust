import { describe, expect, it } from 'vitest';
import { controlOf, mergeRows, writersOf } from './properties.js';
import { KEYMAP, actionFor, sheet } from './keymap.js';
import { lanesOf, siRange } from './plot.js';
import { Views } from './views.js';
import { bytesEstimate, bytesText, spanText } from './units.js';

const rpc = (name, type, access, channel = 'setting') => ({ name, type, access, channel, readable: access.includes('read'), persistent: false });

describe('the properties table', () => {
  it('merges devices by name, groups by channel, and marks a different declaration', () => {
    const rows = mergeRows([
      { route: '/0', table: { rpcs: [rpc('reset', null, [], 'action'), rpc('gain', 'f32', ['read', 'write'])] } },
      { route: '/1', table: { rpcs: [rpc('gain', 'u8', ['read', 'write']), rpc('temp', 'f32', ['read'], 'report')] } },
    ]);
    expect(rows.map((r) => [r.group, r.name])).toEqual([['Settings', 'gain'], ['Reports', 'temp'], ['Actions', 'reset']]);
    expect(rows[0].differs).toBe(true);
    expect(rows[0].entries.size).toBe(2);
  });

  it('infers a control and the devices a write reaches', () => {
    expect(controlOf(rpc('on', 'bool', ['read', 'write']))).toBe('toggle');
    expect(controlOf(rpc('t', 'f32', ['read']))).toBe('display');
    expect(controlOf(rpc('go', null, [], 'action'))).toBe('action');
    const devices = [{ route: '/0', table: { rpcs: [rpc('g', 'f32', ['read', 'write'])] } }, { route: '/1', table: { rpcs: [rpc('g', 'f32', ['read'])] } }];
    const [row] = mergeRows(devices);
    expect(writersOf(row, devices).map((d) => d.route)).toEqual(['/0']);
  });
});

describe('the keymap', () => {
  it('names each key’s action where focus is, and none with Ctrl', () => {
    expect(actionFor('plot', { key: 'a' })).toBe('addSignals');
    expect(actionFor('page', { key: '3' })).toBe('goToPlot');
    expect(actionFor('list', { key: 'Enter', shiftKey: true })).toBe('newPlot');
    expect(actionFor('list', { key: 'Enter', shiftKey: false })).toBe('add');
    expect(actionFor('plot', { key: 'a', ctrlKey: true })).toBeNull();
  });

  it('lists every action once per place', () => {
    for (const place of KEYMAP) {
      const actions = place.entries.map((e) => e.action).filter(Boolean);
      expect(new Set(actions).size, place.place).toBe(actions.length);
    }
  });

  it('sheets the places of one title as one section, a row per keys', () => {
    const lists = sheet().find((s) => s.title === 'Lists');
    expect(lists.rows.find((r) => r.id === 'Delete ').what).toBe('Remove signal, close tab');
    expect(lists.rows.filter((r) => r.what === 'Move')).toHaveLength(1);
    expect(sheet().filter((s) => s.title === 'Lists')).toHaveLength(1);
  });
});

describe('a plot', () => {
  it('has a lane per signal stacked, and one otherwise', () => {
    expect(lanesOf('stacked', 3)).toBe(3);
    expect(lanesOf('stacked', 0)).toBe(1);
    expect(lanesOf('independent', 3)).toBe(1);
    expect(lanesOf('overlay', 3)).toBe(1);
  });
});

describe('the views', () => {
  const hub = { now: () => 100 };

  it('go with the plots that leave every tab, so none is paused for them', () => {
    const views = new Views(hub, () => {});
    views.of('a').pause(0);
    views.of('b');
    expect(views.anyPaused(() => true)).toBe(true);
    views.prune(new Set(['b']));
    expect(views.get('a')).toBeUndefined();
    expect(views.anyPaused(() => true)).toBe(false);
  });

  it('a paused spectrum reads no history', () => {
    const views = new Views(hub, () => {});
    views.of('s').pause(0);
    expect(views.anyPaused((id) => id !== 's')).toBe(false);
  });
});

describe('a range of values', () => {
  it("shares the larger end's prefix", () => {
    expect(siRange(-2.438, 0.4413, 'V')).toBe('-2.438 … 0.4413 V');
    expect(siRange(-0.0012, 0.0031, 'V')).toBe('-1.200 … 3.100 mV');
    expect(siRange(-1, 2, 'arb')).toBe('-1.000 … 2.000');
  });
});

describe('sizes and spans', () => {
  it('reads as people write them', () => {
    expect(bytesText(740e3)).toBe('740 kB');
    expect(bytesText(12.4e6)).toBe('12.4 MB');
    expect(bytesText(1.5e9)).toBe('1.50 GB');
    expect([bytesEstimate(126.4e6), bytesEstimate(2.34e9), bytesEstimate(0)]).toEqual(['~130 MB', '~2.3 GB', '~0 kB']);
    expect([spanText(45), spanText(720), spanText(3.5 * 3600), spanText(2 * 86400), spanText(Infinity)]).toEqual(['45 s', '12 min', '3.5 h', '2 days', 'forever']);
  });
});

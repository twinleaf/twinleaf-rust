import { describe, expect, it } from 'vitest';
import { listed, matching } from './SignalList.svelte';

const signals = [
  { key: 'vector.x', meta: { units: 'nT' } },
  { key: 'vector.y', meta: { units: 'nT' } },
  { key: 'sensor.temp', meta: { units: 'C' } },
];

describe('the signal search', () => {
  it('keeps the signals with every word typed, by key or units', () => {
    expect(matching(signals, 'vector nt').map((t) => t.key)).toEqual(['vector.x', 'vector.y']);
    expect(matching(signals, '  c   temp ').map((t) => t.key)).toEqual(['sensor.temp']);
    expect(matching(signals, 'sensor vector')).toEqual([]);
    // A word with an s in it stays one word.
    expect(matching(signals, 'vectors')).toEqual([]);
  });
  it('keeps every signal when nothing is typed', () => {
    expect(matching(signals, '   ')).toEqual(signals);
  });
});

describe('the list', () => {
  /** A capture of `key`, titled `title`, in `units`. */
  const capture = (key, title, units) => ({ key, title, shots: [], meta: { units } });
  const session = {
    signals: [{ key: '0/aux.x', meta: { units: 'V' } }, { key: '0/aux.y', meta: { units: 'V' } }, ...signals.toSorted((a, b) => (a.key < b.key ? -1 : 1))],
    captures: [capture('0/capture:fid.capture', '0/FID', 'V'), capture('1/capture:fid.capture', '1/FID', 'V'), capture('capture:fid.capture', 'FID', 'V'), capture('capture:phase.capture', 'Unwrapped phase', 'rad')],
  };
  const keys = (query) => listed(session, query).map((t) => t.key);

  it('shows each device’s captures under its signals, and those of a device with no signal after them all', () => {
    expect(keys('')).toEqual(['0/aux.x', '0/aux.y', '0/capture:fid.capture', 'sensor.temp', 'vector.x', 'vector.y', 'capture:fid.capture', 'capture:phase.capture', '1/capture:fid.capture']);
  });

  it('searches the captures with the signals: by the name its metadata gives, its RPC’s, or its units', () => {
    expect(keys('fid')).toEqual(['0/capture:fid.capture', '1/capture:fid.capture', 'capture:fid.capture']);
    expect(keys('unwrapped')).toEqual(['capture:phase.capture']);
    expect(keys('phase.capture')).toEqual(['capture:phase.capture']);
    expect(keys('rad')).toEqual(['capture:phase.capture']);
    // A device's captures follow those of its signals that match.
    expect(keys('v 0/')).toEqual(['0/aux.x', '0/aux.y', '0/capture:fid.capture']);
    expect(keys('nothing')).toEqual([]);
  });
});

import { describe, expect, it } from 'vitest';
import { matching } from './SignalList.svelte';

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

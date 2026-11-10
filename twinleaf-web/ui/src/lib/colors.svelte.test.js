import { describe, expect, it } from 'vitest';
import { Colors, hexToHsv, hsvToHex } from './colors.svelte.js';

/** A stand-in for localStorage. */
const storage = () => {
  const kept = new Map();
  return { getItem: (k) => kept.get(k) ?? null, setItem: (k, v) => kept.set(k, v) };
};

describe('Colors', () => {
  it('gives the signals of a hub different colours while there are enough', () => {
    const c = new Colors(storage());
    const keys = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
    c.give(keys);
    expect(new Set(keys.map((k) => c.of(k))).size).toBe(8);
  });

  it('keeps the colour a signal was given as others come', () => {
    const c = new Colors(storage());
    c.give(['b']);
    const before = c.of('b');
    c.give(['a', 'b', 'c']);
    expect(c.of('b')).toBe(before);
    expect(new Set(['a', 'b', 'c'].map((k) => c.of(k))).size).toBe(3);
  });

  it('gives a new signal a colour no other has, picked ones included', () => {
    const c = new Colors(storage());
    c.give(['a']);
    c.pick('a', 1);
    c.give(['a', 'b']);
    expect(c.of('b')).not.toBe(1);
  });

  it('a picked colour stands in for the given one until Automatic', () => {
    const c = new Colors(storage());
    c.give(['a']);
    const given = c.of('a');
    c.pick('a', 5);
    expect(c.of('a')).toBe(5);
    c.pick('a', null);
    expect(c.of('a')).toBe(given);
  });

  it('keeps both from visit to visit', () => {
    const kept = storage();
    const c = new Colors(kept);
    c.give(['a', 'b']);
    c.pick('b', 6);
    const again = new Colors(kept);
    expect([again.of('a'), again.of('b')]).toEqual([c.of('a'), 6]);
  });
});

describe('hexToHsv and hsvToHex', () => {
  it('round-trip colours', () => {
    for (const hex of ['#ffb703', '#4cc9f0', '#000000', '#ffffff', '#808080', '#ff0000', '#2dd4bf']) expect(hsvToHex(hexToHsv(hex))).toBe(hex);
  });
  it('read pure hues', () => {
    expect(hexToHsv('#00ff00')).toEqual({ h: 120, s: 1, v: 1 });
    expect(hsvToHex({ h: 240, s: 1, v: 1 })).toBe('#0000ff');
  });
});

describe('Colors.last', () => {
  it('remembers the colour last picked through Automatic', () => {
    const c = new Colors(storage());
    c.give(['a']);
    c.pick('a', '#123456');
    c.pick('a', null);
    expect(c.last.a).toBe('#123456');
    expect(typeof c.of('a')).toBe('number');
  });
});

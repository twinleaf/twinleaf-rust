import { describe, expect, it } from 'vitest';
import { Favorites } from './favorites.svelte.js';

const memory = () => {
  const kept = new Map();
  return { getItem: (k) => kept.get(k) ?? null, setItem: (k, v) => kept.set(k, v) };
};

describe('Favorites', () => {
  it('star a property and take the star away', () => {
    const f = new Favorites(memory());
    f.toggle('test.noise');
    expect(f.has('test.noise')).toBe(true);
    f.toggle('test.noise');
    expect(f.has('test.noise')).toBe(false);
  });
  it('keep their stars across a reload', () => {
    const storage = memory();
    new Favorites(storage).toggle('dev.name');
    expect(new Favorites(storage).has('dev.name')).toBe(true);
  });
});

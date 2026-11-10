import { describe, expect, it } from 'vitest';
import { exportName, exportUrl } from './exportfile.js';

describe('An export', () => {
  it('is named for when it was made, with its format as the extension', () => {
    expect(exportName('csv', new Date(2026, 10, 8, 14, 5, 1))).toBe('twinleaf-20261108-140501.csv');
    expect(exportName('h5', new Date(2026, 0, 2, 3, 4, 5))).toBe('twinleaf-20260102-030405.h5');
  });
  it('asks the hub for its traces, time and format', () => {
    const url = new URL(exportUrl({ hub: 'h1', ids: [0, 2], t0: 1.5, t1: 9 }, 'csv', 'x.csv'), 'http://host');
    expect(url.pathname).toBe('/api/export');
    expect(Object.fromEntries(url.searchParams)).toEqual({ hub: 'h1', ids: '0,2', t0: '1.5', t1: '9', format: 'csv', name: 'x.csv' });
  });
});

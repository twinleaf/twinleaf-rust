import { describe, expect, it } from 'vitest';
import { blockTable, lostSeconds, toHtml, toTsv } from '../src/lib/copy.js';

describe('copying', () => {
  it('lays signals out on one time column, blank where a signal did not sample', () => {
    const { header, rows } = blockTable([
      { key: 'a', units: 'V', x: [1, 2, 3], y: [10, 20, 30] },
      { key: 'b', units: '', x: [2, 4], y: [7, 8] },
    ]);
    expect(header).toEqual(['time (s)', 'a (V)', 'b']);
    expect(rows).toEqual([
      [1, 10, ''],
      [2, 20, 7],
      [3, 30, ''],
      [4, '', 8],
    ]);
  });

  it('writes a block per plot, a blank line between, as text and as tables', () => {
    const block = { title: 'a · 1 s – 2 s', header: ['time (s)', 'a'], rows: [[1, 2]] };
    expect(toTsv([block, block])).toBe('a · 1 s – 2 s\ntime (s)\ta\n1\t2\n\na · 1 s – 2 s\ntime (s)\ta\n1\t2\n');
    const html = toHtml([{ ...block, title: 'x<y' }]);
    expect(html).toContain('<b>x&lt;y</b>');
    expect(html).toContain('<td>1</td><td>2</td>');
  });

  it('counts the start of a window the history no longer holds', () => {
    expect(lostSeconds(10, 50, 100)).toBe(40);
    expect(lostSeconds(10, 10.01, 100)).toBe(0);
    expect(lostSeconds(10, null, 100)).toBe(0);
  });
});

// The time window's field in the top bar, on the whole page, on a hub that
// holds a day: each kind of window typed, a number alone in the unit shown,
// sets every plot's window and reads back in the unit that reads shortest,
// in the field and in each plot's own readout; what is refused, too long
// for what the views can show among it, leaves the windows as they were
// and marks the field until it is corrected or Escape puts back what was
// there, focus kept, and Escape with nothing typed
// goes out a level as in any field; the arrows and the wheel step along the
// wheel zoom's clock ladder; and the field keeps its size and its unit
// inside its outline whatever it reads.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { flushSync } from 'svelte';
import { clickOn, frames, holdHistory, leafOf, mountPage, pressKey, typeInto, until, wait } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { IDLE_MS } from '../src/lib/viewhistory.svelte.js';
import { lastKey } from '../src/lib/keyrouter.js';
import { windowText } from '../src/lib/window.js';

const KEYS = [['sine.sine'], ['sine.cosine']];

afterEach(() => vi.restoreAllMocks());

/** The page, its plots live on a hub that has held a day of each signal. */
async function page() {
  const p = await mountPage(KEYS, 'rows', 0, KEYS.flat());
  holdHistory(p, { history: 86400 });
  await frames();
  return p;
}

const field = () => document.querySelector('.top-bar .window-field');
const input = () => field().querySelector('input');
/** What the field reads: its number and its unit. */
const reads = () => `${input().value} ${field().querySelector('.unit').textContent}`;
/** The windows of the tab's plots, each once. */
const windows = (p) => [...new Set(plotIds(p.ws.layout).map((id) => p.views.get(id).window))];
/** The text each plot draws in its readout of the window, on the next frames, as the readout shows a while after the window changes. */
async function readouts(p) {
  const fillText = vi.spyOn(CanvasRenderingContext2D.prototype, 'fillText');
  await frames(3);
  const drawn = fillText.mock.calls.map(([text]) => String(text));
  fillText.mockRestore();
  return drawn;
}

describe('the window field', () => {
  it('sets every plot’s window to each kind of window typed, and reads it back in the unit that reads shortest, in the field and in each plot', async () => {
    const p = await page();
    try {
      // A number alone is in the unit the field shows as it is typed: seconds, then minutes, then hours.
      const typed = [
        ['90', 90],
        ['5m', 300],
        ['2', 120],
        ['1.5 min', 90],
        ['2h', 7200],
        ['3', 10800],
        ['500ms', 0.5],
        ['20 SEC', 20],
        ['1.25 hr', 4500],
        ['45 s', 45],
        ['.25 H', 900],
      ];
      for (const [text, s] of typed) {
        const shown = field().querySelector('.unit').textContent;
        await typeInto(input(), text);
        flushSync();
        expect(field().querySelector('.unit').textContent, `${text}: the unit beside it`).toBe(/[a-z]/i.test(text) ? '' : shown);
        await pressKey('Enter');
        flushSync();
        expect(windows(p), text).toEqual([s]);
        expect(await until(() => reads() === windowText(s)), `${text} reads as ${windowText(s)}, not ${reads()}`).toBe(true);
        const drawn = await readouts(p);
        expect(drawn.filter((t) => t === windowText(s)).length, `${text}: each plot's readout, among ${drawn.join(' | ')}`).toBeGreaterThanOrEqual(plotIds(p.ws.layout).length);
        await wait(IDLE_MS + 100);
      }
    } finally {
      p.dispose();
    }
  }, 60_000);

  it('refuses no window, a negative or zero one, text, and more than the views can show: the windows stay, the field is marked until corrected or Escape, which keeps focus', async () => {
    const p = await page();
    try {
      const id = plotIds(p.ws.layout)[0], card = leafOf(id).querySelector('.plot-card');
      const before = windows(p), was = reads();
      const widest = p.views.get(id).spans(clock.now())[1];
      const tooWide = `${Math.ceil(widest / 3600) + 1}h`;
      for (const text of ['', '-5', '0', 'soon', '5 days', tooWide]) {
        card.focus();
        await typeInto(input(), text);
        await pressKey('Enter');
        flushSync();
        expect(windows(p), JSON.stringify(text)).toEqual(before);
        expect(input().getAttribute('aria-invalid'), JSON.stringify(text)).toBe('true');
        expect(field().classList.contains('invalid')).toBe(true);
        // Too long, the hint says the widest.
        expect(input().title, JSON.stringify(text)).toBe(text === tooWide ? `At most ${windowText(widest)}` : 'Not a window: type 90, 5m, 1.5 min or 2h');
        // A blur sets nothing and keeps the mark.
        card.focus();
        flushSync();
        expect(windows(p)).toEqual(before);
        expect(input().getAttribute('aria-invalid')).toBe('true');
        // Escape puts back what was there, focus kept, and is the field's; the next is the page's, as in any field: out a level.
        await clickOn(input());
        await pressKey('Escape');
        flushSync();
        expect(lastKey().by).toBe(null);
        expect(input().getAttribute('aria-invalid')).toBe('false');
        expect(reads()).toBe(was);
        expect(document.activeElement, 'focus kept').toBe(input());
        await pressKey('Escape');
        expect(lastKey().by).toBe('navigation');
      }
      // Corrected as it is typed, the mark goes, and Enter sets it, on every plot with none selected.
      p.ws.selection.clear();
      await typeInto(input(), 'soon');
      await pressKey('Enter');
      expect(input().getAttribute('aria-invalid')).toBe('true');
      await typeInto(input(), '30');
      flushSync();
      expect(input().getAttribute('aria-invalid')).toBe('false');
      expect(windows(p)).toEqual(before);
      await pressKey('Enter');
      flushSync();
      expect(windows(p)).toEqual([30]);
    } finally {
      p.dispose();
    }
  }, 60_000);

  it('steps along the wheel zoom’s clock ladder by its arrows and the wheel over it, no wider than the views can show, and sets what is typed on a blur', async () => {
    const p = await page();
    try {
      await typeInto(input(), '7');
      await pressKey('Tab');
      flushSync();
      expect(windows(p), 'a blur sets it').toEqual([7]);
      await clickOn(input());
      await pressKey('ArrowUp');
      flushSync();
      expect(windows(p)).toEqual([10]);
      for (const want of [15, 20, 30, 45, 60]) {
        await pressKey('ArrowUp');
        flushSync();
        expect(windows(p)).toEqual([want]);
      }
      await pressKey('ArrowDown');
      flushSync();
      expect(windows(p)).toEqual([45]);
      // The wheel steps whether or not the field has focus: away from you, longer.
      input().blur();
      for (const [deltaY, want] of [[100, 60], [100, 120], [-100, 60], [30, 60], [30, 120]]) {
        input().dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY }));
        flushSync();
        expect(windows(p), `after a turn of ${deltaY}`).toEqual([want]);
      }
      // On a hub that holds ten minutes, a step goes no wider than that.
      holdHistory(p, { history: 600 });
      await frames();
      await typeInto(input(), '5m');
      await pressKey('Enter');
      for (const want of [600, 600]) {
        await pressKey('ArrowUp');
        flushSync();
        expect(windows(p)).toEqual([want]);
      }
    } finally {
      p.dispose();
    }
  }, 60_000);

  it('keeps its size and its unit inside its outline whatever it reads, its figures of one width', async () => {
    const p = await page();
    try {
      const boxes = [];
      for (const text of ['500ms', '7.41', '90', '18.3 min', '2h', '1.25 hr', '24h']) {
        await typeInto(input(), text);
        flushSync();
        boxes.push([text, field().getBoundingClientRect(), input().getBoundingClientRect()]);
        await pressKey('Enter');
        flushSync();
        expect(await until(() => reads() === windowText(windows(p)[0])), `${text} read back`).toBe(true);
        const f = field().getBoundingClientRect(), u = field().querySelector('.unit').getBoundingClientRect();
        boxes.push([reads(), f, input().getBoundingClientRect()]);
        expect(u.left, `${reads()}: the unit inside the outline`).toBeGreaterThanOrEqual(f.left);
        expect(u.right).toBeLessThanOrEqual(f.right);
        expect(input().scrollWidth, `${reads()} fits`).toBeLessThanOrEqual(input().clientWidth);
      }
      const [, f0, i0] = boxes[0];
      for (const [what, f, i] of boxes) {
        expect([f.left, f.width, f.height, i.left, i.width], what).toEqual([f0.left, f0.width, f0.height, i0.left, i0.width]);
      }
      expect(getComputedStyle(input()).fontVariantNumeric).toContain('tabular-nums');
    } finally {
      p.dispose();
    }
  }, 60_000);
});

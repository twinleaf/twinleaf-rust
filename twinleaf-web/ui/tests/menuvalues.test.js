// Every menu row's value (round 1 "Menu values", round 2 Menus) says what
// its command acts on, from the same plots: a count over them, a setting
// they share or Mixed, or how many they are where the row names plots; and
// the Layout dropdown checks the one arrangement the tab is in. On a page
// of known plots, for selections of one plot, several and none, each value
// is the one worked out by hand below. A row that cannot be chosen says why,
// and a warning shows, whatever the hints; and Copy data's count follows the
// view of every plot it would copy.
import { describe, expect, it } from 'vitest';
import { MENUS, closeAll, openMenu, settle } from './harness/menus.js';
import { feed, mountPage, wait } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { prefs } from '../src/lib/prefs.svelte.js';

const UNITS = { 'sine.sine': 'V', 'aux.triangle': 'V', 'mag.x': 'nT' };

/**
 * The page, every plot's view 10 s wide, /api/rows counting `rowsPerSecond`
 * rows a second of each signal: A, a time overlay of sine.sine and
 * aux.triangle (both V) with X1 at 14 s, X2 at 16 s and a pair of Y
 * cursors; B, time, independent, sine.sine (V) and mag.x (nT); C, a
 * spectrum of sine.sine, its 50 bins; D, empty. One more tab, and a device
 * with gain 3 and offset 0.5.
 */
async function fixture(rowsPerSecond = 100) {
  const page = await mountPage([['sine.sine', 'aux.triangle'], ['sine.sine', 'mag.x'], ['sine.sine'], []], 'rows', 1, Object.keys(UNITS), [
    { name: 'gain', channel: 'setting', value: 3 },
    { name: 'offset', channel: 'setting', value: 0.5 },
  ], { units: UNITS, rowsPerSecond });
  feed(page.hub);
  const ids = plotIds(page.ws.layout), [A, B, C, D] = ids;
  page.history.group('Prepare', () => (page.ws.setPlot(B, { mode: 'independent' }), page.ws.setPlot(C, { kind: 'spectrum' })));
  await wait(300);
  await settle();
  const pv = page.pv(A);
  pv.view.cursors = { a: 14, b: 16 };
  pv.setYCursor('shared', 'a', 0.1);
  pv.setYCursor('shared', 'b', 0.5);
  await wait(200);
  await settle();
  return { ...page, ids, plots: { A, B, C, D } };
}

/** The row of `menu` named `label`, by how its text starts. */
function rowOf(menu, label) {
  const r = [...menu.querySelectorAll('[role^=menuitem], [data-slot$=-menu-label]')].find((m) => m.textContent.trim().startsWith(label));
  expect(r, `the row ${label}`).toBeTruthy();
  return r;
}
/** The value row `r` shows, ✓ for a check, or null with none. */
function valueOf(r) {
  const v = r.querySelector('.menu-value');
  if (!v) return null;
  expect(v.getClientRects().length, `${r.textContent.trim()}'s value shows`).toBeGreaterThan(0);
  return v.querySelector('svg') && !v.textContent.trim() ? '✓' : v.textContent.trim();
}

/** Each case: the plots selected, the menu and the plot it is opened on, and each row's value, worked out by hand; null for none. */
const CASES = [
  [['A', 'B', 'C'], 'the plot menu', 'A', {
    // 2 signals × 1,000 rows on A and on B, and C's 50 bins.
    'Copy data': '4,050 rows',
    'Export data': '3 plots',
    // A's pair only, 2 s apart.
    'Add X cursors': '2.000 s',
    // A's shared axis, in V, and B's axis of sine.sine, in V; C's is in V/√Hz.
    'Add Y cursors': 'Mixed',
    'Clear cursors': '4 placed',
    Kind: 'Mixed',
    Layout: 'Mixed',
    'Move to tab': 'A',
    'Close plot': '3 plots',
  }],
  [[], 'the plot menu', 'B', {
    'Copy data': '2,000 rows',
    'Export data': null,
    'Add X cursors': null,
    'Add Y cursors': 'sine.sine',
    'Clear cursors': null,
    Kind: 'Time',
    Layout: 'Independent Y',
    'Close plot': '2 signals',
  }],
  [['A', 'B'], 'a chip’s menu', 'A', { 'Show only this': 'hides 1', Colour: 'Auto', 'Move to': '3 plots' }],
  [['A', 'B'], 'a chip’s menu', 'B', { 'Show only this': 'hides 1', Axis: 'Left', 'Move to': '3 plots' }],
  [['A', 'B'], 'the readout’s menu', 'A', {
    // X, A's two signals, and the shared axis's Y.
    'Copy table': '4 rows',
    'Zoom to X1–X2': '2.000 s',
    'Clear X cursors': 'X1 14 s',
    'Clear Y cursors': '1 axis',
    'Clear all cursors': '4 placed',
  }],
  [['A', 'B'], 'the add-signal menu', 'A', { 'Remove every signal': '4 signals' }],
  // The Signals list's first signal is aux.triangle.
  [[], 'the Signals list’s menu', null, { 'Add to plot': 'No plot selected', 'New plot': 'Time · Overlay' }],
  [['A'], 'the Signals list’s menu', null, { 'Add to plot': 'Already shown' }],
  [['B'], 'the Signals list’s menu', null, { 'Add to plot': 'sine.sine, mag.x' }],
  [['B', 'D'], 'the Signals list’s menu', null, { 'Add to plot': '2 plots' }],
  [[], 'a tab’s menu', null, { Duplicate: '4 plots', Close: '4 plots' }],
  [[], 'the File dropdown', null, { 'Export every tab': '2 tabs', 'Export data': '4 plots' }],
  [['A', 'C'], 'the File dropdown', null, { 'Export data': '2 selected' }],
  [[], 'the Layout dropdown', null, { 'Arrange this tab': '4 plots', Rows: '✓', Columns: null, Grid: null, Quadrants: null, 'One large': null }],
  // Devices is the first of the left dock's two panels; the right dock holds six.
  [[], 'a dock panel’s menu', null, { 'Move up': '1 of 2', 'Move down': '1 of 2', 'Move to the right dock': '6 panels' }],
  // A right-click picks the first property, gain.
  [[], 'the properties menu', null, { 'Copy row': '3', Add: '0 starred' }],
  [['settings:gain', 'settings:offset'], 'the properties menu', null, { 'Copy 2 rows': '2 rows · 1 device' }],
];

describe('a menu row’s value', () => {
  it('is what its command acts on, worked out by hand, for one plot, several and none', async () => {
    const page = await fixture();
    try {
      prefs.setHints(false);
      for (const [picks, name, on, rows] of CASES) {
        await closeAll();
        if (picks.some((p) => p.includes(':'))) page.ws.selection.pickRows(picks);
        else page.ws.selection.only(picks.map((p) => page.plots[p]));
        await settle();
        const menu = await MENUS[name](page, on && page.plots[on]);
        expect(menu, name).toBeTruthy();
        // A plot's own menu acts on the selection its right-click left.
        for (const [label, want] of Object.entries(rows)) expect(valueOf(rowOf(openMenu() ?? menu, label)), `${name} on ${on ?? 'the page'} with ${picks}: ${label}`).toBe(want);
      }
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
  }, 120_000);

  it('says why its row cannot be chosen, and warns, whatever the hints', async () => {
    const page = await fixture(10_000);
    try {
      for (const hints of [true, false]) {
        prefs.setHints(hints);
        await closeAll();
        page.ws.selection.only([]);
        const list = await MENUS['the Signals list’s menu'](page);
        const add = rowOf(list, 'Add to plot');
        expect(add.hasAttribute('data-disabled')).toBe(true);
        expect(valueOf(add)).toBe('No plot selected');
        await closeAll();
        // 2 signals × 10 s × 10,000 rows a second, more than a paste holds.
        const menu = await MENUS['the plot menu'](page, page.plots.A);
        await wait(100);
        const copy = rowOf(menu, 'Copy data'), value = copy.querySelector('.menu-value');
        expect(valueOf(copy)).toBe('200,000 rows');
        expect(value.classList.contains('warn')).toBe(true);
        expect(value.title).toMatch(/paste/);
      }
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
  });

  it('of Copy data follows the view of every plot it would copy while the menu is open', async () => {
    const page = await fixture();
    try {
      prefs.setHints(false);
      page.ws.selection.only([page.plots.A, page.plots.B]);
      await settle();
      const menu = await MENUS['the plot menu'](page, page.plots.A);
      await wait(100);
      expect(valueOf(rowOf(menu, 'Copy data'))).toBe('4,000 rows');
      // B's window halves: its 2 signals × 500 rows, beside A's 2,000.
      page.pv(page.plots.B).view.setSpan(5);
      await wait(500);
      await settle();
      expect(valueOf(rowOf(menu, 'Copy data'))).toBe('3,000 rows');
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
  });
});

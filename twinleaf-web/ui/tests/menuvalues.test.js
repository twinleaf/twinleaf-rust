// Every menu row's value (round 1 "Menu values", round 2 Menus) says what
// its command acts on, from the same plots: a count over them, a setting
// they share or Mixed, or how many they are where the row names plots; and
// the Layout dropdown checks the one arrangement the tab is in. On a page
// of known plots, for selections of one plot, several and none, each value
// is the one worked out by hand below. A row that cannot be chosen says why,
// and a warning shows, whatever the hints; and Copy data's count follows the
// view of every plot it would copy. A signal's menu in the Signals list
// offers Remove from plot where every selected plot shows the signal and
// Add to plot otherwise, and the row's count is the plots its click
// changes, for every signal of the list and any plots selected. Left open
// on live plots while the hub streams, every row of every menu and popover
// keeps what it shows: it changes only as what it reports does, never back
// and forth within a moment, and Copy data over the limit never reads as
// a copy it could make.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { MENUS, OPEN, axisPopover, click, closeAll, menuPage, openMenu, rightClick, settle } from './harness/menus.js';
import { MEMORY_EVERY_MS, centre, editCursors, feed, mountPage, stream, wait, withExamples, withYPair } from './harness/page.js';
import { FRAME_MS } from '../src/lib/clock.js';
import { MAX_COPY_ROWS } from '../src/lib/copy.js';
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
  editCursors(page, A, (c) => withYPair({ ...c, x: { a: 14, b: 16 } }, 'time', 'shared', 0.1, 0.5));
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
    'Export…': '3 plots',
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
    'Export…': null,
    'Add X cursors': null,
    'Add Y cursors': 'sine.sine',
    'Clear cursors': null,
    Kind: 'Time',
    Layout: 'Independent Y',
    'Close plot': '2 signals',
  }],
  [['A', 'B'], 'a chip’s menu', 'A', { 'Show only this': 'Hides 1', Colour: 'Auto', 'Move to': '3 plots' }],
  [['A', 'B'], 'a chip’s menu', 'B', { 'Show only this': 'Hides 1', Axis: 'Left', 'Move to': '3 plots' }],
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
  [['A'], 'the Signals list’s menu', null, { 'Remove from plot': null }],
  [['B'], 'the Signals list’s menu', null, { 'Add to plot': null }],
  [['B', 'D'], 'the Signals list’s menu', null, { 'Add to plot': '2 plots' }],
  [[], 'a tab’s menu', null, { Duplicate: '4 plots', Close: '4 plots' }],
  [[], 'the File dropdown', null, { 'Export every tab': '2 tabs', 'Export…': '4 plots' }],
  [['A', 'C'], 'the File dropdown', null, { 'Export…': '2 selected' }],
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

  it('of a signal’s Add to plot or Remove from plot is the plots its click changes: Remove where every selected plot shows the signal, Add for the ones that lack it otherwise', async () => {
    const page = await fixture();
    const shows = (key) => Object.fromEntries(page.ids.map((id) => [id, page.ws.plots[id].items.some((i) => i.key === key)]));
    const seen = { removeSeveral: 0, addSome: 0, addNone: 0 };
    try {
      prefs.setHints(false);
      // The signals are the list's own, found on the page.
      const keys = [...document.querySelectorAll('.palette .chip')].map((c) => c.dataset.key);
      expect([...keys].sort()).toEqual(Object.keys(UNITS).sort());
      const { A, B, C, D } = page.plots;
      await fc.assert(
        fc.asyncProperty(fc.subarray(page.ids, { minLength: 1 }), fc.constantFrom(...keys), async (picks, key) => {
          await closeAll();
          page.ws.selection.only(picks);
          await settle();
          const before = shows(key), lacking = picks.filter((id) => !before[id]);
          const remove = !lacking.length, acts = remove ? picks : lacking;
          await rightClick(centre(document.querySelector(`.palette .chip[data-key="${key}"]`)));
          const menu = openMenu(), said = `${key} with ${picks.length} selected, ${lacking.length} lacking it`;
          const offered = [...menu.querySelectorAll('[role^=menuitem]')].map((r) => r.textContent.trim()).filter((t) => /^(Add to|Remove from) plot/.test(t));
          expect(offered.length, `${said}: ${offered}`).toBe(1);
          const row = rowOf(menu, remove ? 'Remove from plot' : 'Add to plot');
          expect(valueOf(row), said).toBe(acts.length > 1 ? `${acts.length} plots` : null);
          await click(centre(row));
          const after = shows(key);
          // The plots it counted are the ones that changed, each to the other state, and no other plot did.
          expect(page.ids.filter((id) => after[id] !== before[id]), said).toEqual(page.ids.filter((id) => acts.includes(id)));
          page.history.undo();
          await settle();
          expect(shows(key), 'undone').toEqual(before);
          if (remove && picks.length > 1) seen.removeSeveral++;
          else if (!remove && lacking.length < picks.length) seen.addSome++;
          else if (!remove) seen.addNone++;
        }),
        withExamples([[[A, B], 'aux.triangle'], [[A, B, C], 'sine.sine'], [[B, D], 'aux.triangle'], [[A], 'aux.triangle'], [[A, B, C, D], 'mag.x']]),
      );
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
    // Several plots that all show it, some that do and some that do not, and none that do.
    for (const [which, n] of Object.entries(seen)) expect(n, which).toBeGreaterThan(0);
  }, 120_000);
});

/** How a command's row reads: its text, whether it can be chosen, and whether its value warns. */
const looks = (r) => `${r.textContent.trim()} · ${r.hasAttribute('data-disabled') ? 'disabled' : 'enabled'}${r.querySelector('.menu-value.warn') ? ' · warns' : ''}`;
/** Whether a row reads as waiting on the hub's answer. */
const waiting = (look) => look.includes('Counting…');
/** The frames a row must show what it changed to before it may go back: a shorter stay is a flicker. */
const FLICKER_FRAMES = 30;
/** Each command's row in the menus and popovers open, by the menu it is in and its place there, as it reads. */
function openRows() {
  const rows = new Map();
  [...document.querySelectorAll(OPEN)].forEach((menu, m) => [...menu.querySelectorAll('[data-command]')].forEach((r, i) => rows.set(`${m}.${i} ${r.dataset.command}`, looks(r))));
  return rows;
}
/**
 * Leaves what is open as it is while `ms` of the page's time pass and the
 * hub streams, noting each row's look at every frame: by row, the looks in
 * order, with the frames each showed for.
 */
async function leftOpen(page, ms) {
  const runs = new Map();
  await stream(page.hub, ms, () => {
    for (const [row, look] of openRows()) {
      const seen = runs.get(row) ?? runs.set(row, []).get(row), last = seen.at(-1);
      if (last?.look === look) last.frames++;
      else seen.push({ look, frames: 1 });
    }
  });
  return runs;
}
/** The plot menus and popovers, opened on plot `id`: every menu MENUS opens, and each axis's popover. */
const ON_A_PLOT = { ...MENUS, 'the x axis': (page, id) => axisPopover(page, id, 'x'), 'the y axis': (page, id) => axisPopover(page, id, 'y') };
/** The hub's reply to a count, in ms: two frames, as a hub nearby answers. */
const ROWS_LATENCY = 30;

describe('a menu row left open on live plots', () => {
  it('of Copy data over the limit reads so at every frame while the hub notes what it holds each second', async () => {
    // 2 signals × 10 s × 10,000 rows a second on the first plot.
    const page = await menuPage({ rowsPerSecond: 10_000, rowsLatency: ROWS_LATENCY });
    try {
      prefs.setHints(false);
      page.ws.selection.only([]);
      await MENUS['the plot menu'](page, page.ids[0]);
      // Three of the hub's notes of what it holds, a second apart, while the menu stays open.
      const runs = await leftOpen(page, 3 * MEMORY_EVERY_MS + 100);
      const [copy] = [...runs].filter(([row]) => row.endsWith(' copyData')).map(([, seen]) => seen.map((s) => s.look));
      expect(copy, 'what Copy data showed, in order').toEqual([expect.stringMatching(/^Copy data200,000 rows.* · disabled · warns$/)]);
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
  }, 60_000);

  it('shows what it reports, changing only once the hub has answered, never back and forth, and over a limit never under it', async () => {
    const page = await menuPage({ rowsPerSecond: 10_000, rowsLatency: ROWS_LATENCY });
    const seen = { rows: 0, over: 0 };
    try {
      prefs.setHints(false);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...Object.keys(ON_A_PLOT)),
          fc.constantFrom(...page.ids),
          // When in the hub's second the menu opens, and how long it is left open.
          fc.integer({ min: 0, max: MEMORY_EVERY_MS - 1 }),
          fc.integer({ min: MEMORY_EVERY_MS, max: 1.2 * MEMORY_EVERY_MS }),
          async (name, id, phase, ms) => {
            await closeAll();
            page.ws.selection.only([]);
            await wait(phase);
            await ON_A_PLOT[name](page, id);
            const runs = await leftOpen(page, ms);
            for (const [row, looks] of runs) {
              const said = `${name} on ${id}, ${row}: ${looks.map((l) => `${l.look} ×${l.frames}`).join(' → ')}`;
              // Nothing it reports changes as the plots stream: a wait for the hub's answer, then the answer, and no more.
              const answers = looks.filter((l) => !waiting(l.look));
              expect(answers.length, said).toBeLessThanOrEqual(1);
              expect(looks.findIndex((l) => !waiting(l.look)), said).toBeGreaterThanOrEqual(looks.length - 1);
              // No look comes back after a stay of fewer than FLICKER_FRAMES frames.
              looks.forEach((l, i) => i > 0 && i < looks.length - 1 && looks[i + 1].look === looks[i - 1].look && expect(l.frames, said).toBeGreaterThanOrEqual(FLICKER_FRAMES));
              // Over the limit, it never reads under it.
              const over = looks.findIndex((l) => l.look.includes('warns'));
              if (over >= 0) expect(looks.slice(over).every((l) => l.look.includes('warns')), said).toBe(true);
              seen.rows++;
              if (row.endsWith(' copyData') && over >= 0) seen.over++;
            }
          },
        ),
        withExamples([['the plot menu', page.ids[0], 0, 2 * MEMORY_EVERY_MS], ['the plot menu', page.ids[1], MEMORY_EVERY_MS - 2 * FRAME_MS, MEMORY_EVERY_MS]]),
      );
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
    expect(seen.rows, 'rows of commands').toBeGreaterThan(0);
    expect(seen.over, `Copy data over ${MAX_COPY_ROWS.toLocaleString()} rows`).toBeGreaterThan(0);
  }, 180_000);
});

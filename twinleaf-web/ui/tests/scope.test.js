// A command made on a plot, from any of its menus, popovers or buttons, acts
// on every selected plot when the plot is one of them, or else on it alone
// (round 1 Selection #1 to #5): what it changes on this plot it changes on
// each of those, and on no other. A right-click, or a menu the keys open, on
// any part of a plot selects that plot first unless it is selected already
// (round 1 #2), so what its menu does acts on what is selected. A command
// made on a y axis acts, on each plot, on its axis in the same units, and
// leaves a plot with none alone; whatever plots it changes, it is one view
// step, which one Back takes back (round 2, View history #6).
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, closeAll, feed, leafOf, mountPage, rightClick, settle, wait, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** Every plot shows the same signals, so an axis of one plot is on every plot, laid out in rows wide enough for their headers. */
const KEYS = [SIGNALS, SIGNALS, SIGNALS, SIGNALS];

const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
/** Whether bounds `a` are `b`, as a log axis gives them back. */
const near = (a, b) => !!a && a.every((v, i) => Math.abs(v - b[i]) < 1e-9 * Math.abs(b[i]));
/** Gives every plot `patch`, as one step made before the case. */
function setEvery(page, patch) {
  page.history.group('Prepare', () => plotIds(page.ws.layout).forEach((id) => page.ws.setPlot(id, patch)));
}
/** Runs `fn(pv)` on the PlotView of every plot. */
const eachPv = (page, fn) => plotIds(page.ws.layout).forEach((id) => fn(page.pv(id)));

/** Where on plot `id` a press lands on each of its parts, in client px. */
function spots(page, id) {
  const pv = page.pv(id), P = pv.geo.plot, band = pv.axisBands[0], leaf = leafOf(id);
  const at = (sel) => {
    const el = leaf.querySelector(sel);
    return el ? centre(el) : null;
  };
  const head = leaf.querySelector('.plot-head').getBoundingClientRect(), zone = leaf.querySelector('.trace-zone').getBoundingClientRect();
  return {
    'a y axis': [(band.x0 + band.x1) / 2, P.y + P.h / 2],
    'the x axis': [P.x + P.w / 2, P.y + P.h + 8],
    'the plot area': [P.x + P.w / 2, P.y + P.h / 2],
    'the readout': at('.cursor-readout'),
    'the legend': at('.pinned-legend .row'),
    'a chip': at('.trace-chip .name'),
    'the chips row': [zone.right - 3, zone.top + zone.height / 2],
    'the header': [head.left + 2, head.top + head.height / 2],
  };
}

/** Opens the menu of what is at `at` from the keys, as the menu key does: a contextmenu with no press before it. */
async function menuKey(at) {
  const el = document.elementFromPoint(...at);
  el.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, button: 2, clientX: at[0], clientY: at[1] }));
  await settle();
}

/** Picks the items named `labels` in turn, each a submenu's trigger but the last. */
async function choose(...labels) {
  for (const label of labels) {
    const item = [...document.querySelectorAll('[role=menuitem], [role=menuitemradio]')].find((m) => m.textContent.trim().startsWith(label));
    expect(item, `the menu item ${label}`).toBeTruthy();
    item.click();
    await settle();
  }
}
/** Types `text` into the field named `label` of the open popover. */
function type(label, text) {
  const el = document.querySelector(`[data-slot=popover-content] input[aria-label="${label}"]`);
  el.value = text;
  el.dispatchEvent(new Event('input', { bubbles: true }));
  flushSync();
  return el;
}
/** Clicks the button named `label` in the open popover. */
async function button(label) {
  const el = document.querySelector(`[data-slot=popover-content] button[aria-label^="${label}"]`);
  expect(el, `the button ${label}`).toBeTruthy();
  await commands.mouse([['move', ...centre(el)], ['down'], ['up']]);
  await settle();
}
/** Double-clicks at `at`, on whatever is there. */
async function dblclick(at) {
  document.elementFromPoint(...at).dispatchEvent(new MouseEvent('dblclick', { bubbles: true, cancelable: true, clientX: at[0], clientY: at[1] }));
  await settle();
}
/** Types bounds `lo` and `hi` into the open popover's fields `fields` and presses Enter. */
async function bounds(fields, lo, hi) {
  type(fields[0], lo);
  type(fields[1], hi).dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true }));
  await settle();
}
const Y_FIELDS = ['Lowest value shown', 'Highest value shown'], F_FIELDS = ['Lowest frequency shown', 'Highest frequency shown'];

/** Every plot a spectrum, its X1 and X2 at 3 and 7 Hz. */
async function spectraWithCursors(page) {
  setEvery(page, { kind: 'spectrum' });
  await settle();
  eachPv(page, (pv) => (pv.view.specCursors = { a: 3, b: 7 }));
}
/** Every time plot's view with X1 and X2 placed in what it shows. */
function timeCursors(page) {
  eachPv(page, (pv) => (pv.view.cursors = { a: 14, b: 16 }));
}
/** Y1 and Y2 on every plot's shared axis. */
function yCursors(page) {
  eachPv(page, (pv) => (pv.setYCursor('shared', 'a', 0.1), pv.setYCursor('shared', 'b', 0.5)));
}

/**
 * Each command, made on plot `id` from where it is offered: `prepare(page)`
 * sets every plot up for it, `act(page, id)` makes it as a person would and
 * returns what `acted` needs, and `acted(page, id, got)` says whether it
 * changed plot `id`.
 */
const COMMANDS = {
  'typed y bounds': {
    act: async (page, id) => (await rightClick(spots(page, id)['a y axis']), await bounds(Y_FIELDS, '-3', '4')),
    acted: (page, id) => same(page.pv(id).yBounds('shared'), [-3, 4]),
  },
  'the y popover’s fit': {
    prepare: (page) => eachPv(page, (pv) => pv.setYBounds(['shared'], -3, 4)),
    act: async (page, id) => (await rightClick(spots(page, id)['a y axis']), await button('Fit to the data')),
    acted: (page, id) => !same(page.pv(id).yBounds('shared'), [-3, 4]),
  },
  'a double-click on a y axis': {
    prepare: (page) => eachPv(page, (pv) => pv.setYBounds(['shared'], -3, 4)),
    act: async (page, id) => (await dblclick(spots(page, id)['a y axis']), await wait(50)),
    acted: (page, id) => !same(page.pv(id).yBounds('shared'), [-3, 4]),
  },
  'the y popover’s axis side': {
    prepare: (page) => setEvery(page, { mode: 'independent' }),
    act: async (page, id) => {
      const key = page.pv(id).axisBands[0].keys[0];
      await rightClick(spots(page, id)['a y axis']);
      await button('Move this axis to the right');
      return key;
    },
    acted: (page, id, key) => page.ws.plots[id]?.axisSides?.[key] === 'right',
  },
  'the y popover’s cursor clear': {
    prepare: yCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['a y axis']), await button('Clear this axis')),
    acted: (page, id) => !page.pv(id).hasYCursors('shared'),
  },
  'a typed frequency range': {
    prepare: (page) => setEvery(page, { kind: 'spectrum' }),
    act: async (page, id) => (await rightClick(spots(page, id)['the x axis']), await bounds(F_FIELDS, '2', '9')),
    acted: (page, id) => same(page.ws.plots[id].fRange, [2, 9]),
  },
  'the x popover’s zoom to X1–X2': {
    prepare: spectraWithCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['the x axis']), await button('Zoom to the X cursors')),
    acted: (page, id) => same(page.ws.plots[id].fRange, [3, 7]),
  },
  'the x popover’s cursor clear': {
    prepare: spectraWithCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['the x axis']), await button('Clear the X cursors')),
    acted: (page, id) => page.pv(id).view.specCursors.a === null,
  },
  'the readout’s zoom to X1–X2, on spectra': {
    prepare: spectraWithCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['the readout']), await choose('Zoom to X1–X2')),
    acted: (page, id) => same(page.ws.plots[id].fRange, [3, 7]),
  },
  'the readout’s zoom to X1–X2, in time': {
    prepare: timeCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['the readout']), await choose('Zoom to X1–X2')),
    acted: (page, id) => !page.pv(id).view.live,
  },
  'the readout’s Clear X cursors': {
    prepare: spectraWithCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['the readout']), await choose('Clear X cursors')),
    acted: (page, id) => page.pv(id).view.specCursors.a === null,
  },
  'the readout’s Clear Y cursors': {
    prepare: yCursors,
    act: async (page, id) => (await rightClick(spots(page, id)['the readout']), await choose('Clear Y cursors')),
    acted: (page, id) => !page.pv(id).hasYCursors('shared'),
  },
  'Kind from the menu': {
    act: async (page, id) => (await rightClick(spots(page, id)['the plot area']), await choose('Kind', 'Spectrum')),
    acted: (page, id) => page.ws.plots[id].kind === 'spectrum',
  },
  'Layout from the menu': {
    act: async (page, id) => (await rightClick(spots(page, id)['the plot area']), await choose('Layout', 'Stacked')),
    acted: (page, id) => page.ws.plots[id].mode === 'stacked',
  },
  'Move to tab': {
    act: async (page, id) => (await rightClick(spots(page, id)['the plot area']), await choose('Move to tab', 'Layout 2')),
    acted: (page, id) => !page.ws.plots[id],
  },
  'Remove every signal': {
    act: async (page, id) => {
      // The add-signal menu, from the plot's + button.
      const add = leafOf(id).querySelector('button[aria-label="Add a signal"]');
      await commands.mouse([['move', ...centre(leafOf(id))], ['move', ...centre(add)], ['down'], ['up']]);
      await settle();
      await choose('Remove every signal');
    },
    acted: (page, id) => page.ws.plots[id].items.length === 0,
  },
};

describe('a command made on a plot', () => {
  it('acts on the selection it is in, or on it alone, whatever the command and where it is made', async () => {
    const seen = new Set();
    await fc.assert(
      fc.asyncProperty(fc.subarray([0, 1, 2, 3]), fc.nat(3), fc.constantFrom(...Object.keys(COMMANDS)), fc.option(fc.nat(3)), async (picks, k, name, focus) => {
        const page = await mountPage(KEYS, 'rows', 1, SIGNALS);
        try {
          feed(page.hub);
          await settle();
          const { prepare, act, acted } = COMMANDS[name];
          await prepare?.(page);
          // The readout and the legend show what a frame drew a few times a second.
          await wait(150);
          await settle();
          const ids = plotIds(page.ws.layout), picked = picks.map((i) => ids[i]), id = ids[k];
          page.ws.selection.only(picked);
          if (focus !== null) leafOf(ids[focus]).querySelector('.plot-card').focus();
          else document.activeElement?.blur();
          await settle();
          const got = await act(page, id);
          await settle();
          const want = picked.includes(id) ? picked : [id];
          for (const p of ids) expect(acted(page, p, got), `${name} on ${ids.indexOf(p)}, made on ${k} with ${picks} selected`).toBe(want.includes(p));
          seen.add(`${name}:${want.length > 1 ? 'many' : 'one'}`);
        } finally {
          page.dispose();
        }
      }),
        // Each command on a plot in a selection of three, and on a plot outside a selection of two.
        withExamples(Object.keys(COMMANDS).flatMap((name) => [[[0, 1, 2], 1, name, null], [[0, 1], 3, name, 0]])),
    );
    for (const name of Object.keys(COMMANDS)) expect(seen.has(`${name}:many`) && seen.has(`${name}:one`), name).toBe(true);
  }, 600_000);
});

describe('a right-click on a plot', () => {
  it('selects it first unless it is selected, on every part of it, from the mouse or the keys', async () => {
    const page = await mountPage(KEYS, 'rows', 1, SIGNALS);
    const seen = new Set();
    try {
      feed(page.hub);
      setEvery(page, { legend: true });
      timeCursors(page);
      await settle(6);
      const ids = plotIds(page.ws.layout);
      const parts = Object.keys(spots(page, ids[0]));
      await fc.assert(
        fc.asyncProperty(fc.subarray([0, 1, 2, 3]), fc.nat(3), fc.constantFrom(...parts), fc.boolean(), async (picks, k, part, mouse) => {
          // Whatever the last case opened closes first.
          await closeAll();
          const picked = picks.map((i) => ids[i]), id = ids[k];
          page.ws.selection.only(picked);
          await settle();
          const at = spots(page, id)[part];
          expect(at, part).toBeTruthy();
          await (mouse ? rightClick(at) : menuKey(at));
          const want = picked.includes(id) ? picked : [id];
          expect([...page.ws.selected].sort(), `${part} of ${k} by ${mouse ? 'mouse' : 'key'} with ${picks}`).toEqual([...want].sort());
          seen.add(`${part}:${mouse}:${picked.includes(id)}`);
        }),
        withExamples(parts.flatMap((part) => [[[0, 1], 3, part, false], [[0, 1], 3, part, true], [[1, 2], 1, part, false]])),
      );
      for (const part of parts) expect(seen.has(`${part}:false:false`) && seen.has(`${part}:true:false`) && seen.has(`${part}:false:true`), part).toBe(true);
    } finally {
      page.dispose();
    }
  }, 600_000);
});

describe('the y popover, closed without typing', () => {
  it('leaves the bounds of every plot as they were, its own and the selection’s', async () => {
    const page = await mountPage(KEYS, 'rows', 0, SIGNALS);
    try {
      feed(page.hub);
      await settle();
      const ids = plotIds(page.ws.layout);
      ids.forEach((id, i) => page.pv(id).setYBounds(['shared'], -i - 1, i + 1));
      page.ws.selection.only(ids);
      await settle();
      for (const id of ids) {
        await rightClick(spots(page, id)['a y axis']);
        document.querySelector('[data-slot=popover-content] input').focus();
        await closeAll();
        ids.forEach((p, i) => expect(page.pv(p).yBounds('shared'), `${ids.indexOf(p)} after the popover of ${ids.indexOf(id)}`).toEqual([-i - 1, i + 1]));
      }
    } finally {
      page.dispose();
    }
  });
});

describe('the command palette’s Add to the selected plots', () => {
  it('adds the signal to every selected plot that lacks it, and with none selected to none', async () => {
    const page = await mountPage([[], [], []], 'rows', 0, SIGNALS);
    const seen = new Set();
    try {
      await fc.assert(
        fc.asyncProperty(fc.subarray([0, 1, 2]), fc.constantFrom(...SIGNALS), async (picks, key) => {
          const ids = plotIds(page.ws.layout), picked = picks.map((i) => ids[i]);
          const before = Object.fromEntries(ids.map((id) => [id, page.ws.plots[id].items.map((i) => i.key)]));
          page.ws.selection.only(picked);
          await settle();
          await commands.key('Control+k');
          await settle();
          const item = document.querySelector(`[data-slot=command-item][data-value="add ${key}"]`);
          expect(item, `the palette's add ${key}`).toBeTruthy();
          item.click();
          await settle();
          for (const id of ids) {
            const want = picked.includes(id) && !before[id].includes(key) ? [...before[id], key] : before[id];
            expect(page.ws.plots[id].items.map((i) => i.key), `${ids.indexOf(id)} with ${picks}`).toEqual(want);
          }
          seen.add(picked.length ? 'some' : 'none');
        }),
        withExamples([[[], 'sine.sine'], [[0, 2], 'aux.triangle']]),
      );
      expect(seen.size).toBe(2);
    } finally {
      page.dispose();
    }
  }, 600_000);
});

/**
 * Four plots, all selected, their y axes in units of their own: A an
 * overlay of two signals in V; B independent, one axis in V and one in nT;
 * C a spectrum of a signal in V, its axis in V/√Hz; D an overlay of one
 * signal in nT.
 */
const UNITS = { 'sine.sine': 'V', 'aux.triangle': 'V', 'mag.x': 'nT' };
async function axesPage() {
  const page = await mountPage([['sine.sine', 'aux.triangle'], ['sine.sine', 'mag.x'], ['sine.sine'], ['mag.x']], 'rows', 0, Object.keys(UNITS), [], { units: UNITS });
  feed(page.hub);
  const [a, b, c, d] = plotIds(page.ws.layout);
  page.history.group('Prepare', () => (page.ws.setPlot(b, { mode: 'independent' }), page.ws.setPlot(c, { kind: 'spectrum' })));
  await wait(300);
  await settle();
  page.ws.selection.only([a, b, c, d]);
  await settle();
  return { page, plots: { A: a, B: b, C: c, D: d } };
}
/** What every plot of the page shows, as view history steps keep it, in one string. */
const shows = (page) =>
  JSON.stringify(plotIds(page.ws.layout).map((id) => {
    const { spectra, ...s } = page.views.get(id).state();
    return [s.live ? { ...s, right: null, pausedAt: null } : s, page.ws.plots[id].fRange ?? null];
  }));
/** Presses Back in plot `id`'s header, the pointer brought over the plot first so its header shows it. */
async function back(id) {
  await commands.mouse([['move', ...centre(leafOf(id))]]);
  await commands.mouse([['move', ...centre(leafOf(id).querySelector('.plot-head button[aria-label^="Back"]'))], ['down'], ['up']]);
  await settle();
}
/** Every y axis of the page, as [plot name, axis key]. */
const axesOf = (page, plots) => Object.entries(plots).flatMap(([name, id]) => page.pv(id).yAxes.map((x) => [name, x.key]));
/** Where y axis `key` of plot `id` is, in client px. */
function axisSpot(page, id, key) {
  const pv = page.pv(id), P = pv.geo.plot, band = pv.axisBands.find((b) => (b.keys ? b.keys[0] === key : key === 'shared'));
  return [(band.x0 + band.x1) / 2, P.y + P.h / 2];
}

/**
 * The axis each command acts on, made on axis [plot, key]: by the rule, the
 * axis in the same units on each plot, named here by hand.
 */
const MATCHES = [
  [['A', 'shared'], ['A:shared', 'B:sine.sine']],
  [['B', 'sine.sine'], ['A:shared', 'B:sine.sine']],
  [['B', 'mag.x'], ['B:mag.x', 'D:shared']],
  [['C', 'shared'], ['C:shared']],
  [['D', 'shared'], ['B:mag.x', 'D:shared']],
];
/** Each command made on a y axis: `prepare` sets every axis up, `act` makes it on axis `key` of plot `id`, and `acted` says whether it changed axis `key` of the PlotView `pv`. */
const ON_AXIS = {
  'typed bounds': {
    prepare: (pv, key) => pv.setYBounds([key], 2, 4),
    act: async (page, id, key) => (await rightClick(axisSpot(page, id, key)), await bounds(Y_FIELDS, '1', '5')),
    acted: (pv, key) => near(pv.yBounds(key), [1, 5]),
  },
  'the popover’s fit': {
    prepare: (pv, key) => pv.setYBounds([key], 2, 4),
    act: async (page, id, key) => (await rightClick(axisSpot(page, id, key)), await button('Fit to the data')),
    acted: (pv, key) => !near(pv.yBounds(key), [2, 4]),
  },
  'a double-click': {
    prepare: (pv, key) => pv.setYBounds([key], 2, 4),
    act: async (page, id, key) => (await dblclick(axisSpot(page, id, key)), await wait(50), await settle()),
    acted: (pv, key) => !near(pv.yBounds(key), [2, 4]),
  },
  'the popover’s Add Y cursors': {
    act: async (page, id, key) => (await rightClick(axisSpot(page, id, key)), await button('Add Y cursors on this axis')),
    acted: (pv, key) => pv.hasYCursors(key),
  },
  'the popover’s cursor clear': {
    prepare: (pv, key) => (pv.setYCursor(key, 'a', 0.1), pv.setYCursor(key, 'b', 0.2)),
    act: async (page, id, key) => (await rightClick(axisSpot(page, id, key)), await button('Clear this axis')),
    acted: (pv, key) => !pv.hasYCursors(key),
  },
  'Add Y cursors, then the cursor clear': {
    act: async (page, id, key, all) => {
      await rightClick(axisSpot(page, id, key));
      await button('Add Y cursors on this axis');
      await closeAll();
      // Escape, once the popover has closed, may deselect: the selection is made again.
      page.ws.selection.only(all);
      await rightClick(axisSpot(page, id, key));
      await button('Clear this axis');
    },
    acted: () => false,
  },
};

describe('a command made on a y axis', () => {
  for (const [name, { prepare, act, acted }] of Object.entries(ON_AXIS)) {
    it(`(${name}) acts on each selected plot's axis in the same units, and on no other axis`, async () => {
      for (const [[on, key], want] of MATCHES) {
        const { page, plots } = await axesPage();
        try {
          const axes = axesOf(page, plots);
          expect(axes.length, `every axis drawn: ${axes.join(' ')}`).toBe(5);
          if (prepare) for (const [n, k] of axes) prepare(page.pv(plots[n]), k);
          await settle();
          const before = shows(page);
          await act(page, plots[on], key, Object.values(plots));
          await settle();
          const got = axes.filter(([n, k]) => acted(page.pv(plots[n]), k)).map(([n, k]) => `${n}:${k}`);
          expect(got.sort(), `${name} on ${on}:${key}`).toEqual(name.includes('then') ? [] : want);
          // One step, on however many plots: one Back shows every plot as before. The pair is two steps.
          await closeAll();
          for (const _ of name.includes('then') ? [1, 2] : [1]) await back(plots.A);
          expect(shows(page), `Back after ${name} on ${on}:${key}`).toBe(before);
        } finally {
          page.dispose();
        }
      }
    }, 120_000);
  }

  it('(its scale) sets the scale of each selected plot with an axis in the same units, and of no other', async () => {
    const { page, plots } = await axesPage();
    try {
      await rightClick(axisSpot(page, plots.C, 'shared'));
      const linear = [...document.querySelectorAll('[data-slot=popover-content] button')].find((el) => el.textContent.trim() === 'Linear');
      await commands.mouse([['move', ...centre(linear)], ['down'], ['up']]);
      await settle();
      expect(Object.entries(plots).filter(([, id]) => page.ws.plots[id].yScale === 'linear').map(([n]) => n)).toEqual(['C']);
    } finally {
      page.dispose();
    }
  });
});

describe('a typed frequency range', () => {
  it('acts on the selected spectra, and on no time plot', async () => {
    const page = await mountPage([['sine.sine'], ['sine.sine'], ['aux.triangle']], 'rows', 0, SIGNALS);
    try {
      feed(page.hub);
      const [a, b, c] = plotIds(page.ws.layout);
      page.history.group('Prepare', () => (page.ws.setPlot(a, { kind: 'spectrum' }), page.ws.setPlot(c, { kind: 'spectrum' })));
      await wait(150);
      await settle();
      page.ws.selection.only([a, b, c]);
      await settle();
      await rightClick(spots(page, a)['the x axis']);
      await bounds(F_FIELDS, '2', '9');
      expect([a, b, c].map((id) => page.ws.plots[id].fRange ?? null)).toEqual([[2, 9], null, [2, 9]]);
    } finally {
      page.dispose();
    }
  });
});

describe('Add Y cursors on an axis, then its clear', () => {
  it('leaves no cursor on any selected plot, the axis of another signal included', async () => {
    const page = await mountPage([SIGNALS, SIGNALS], 'rows', 0, SIGNALS);
    try {
      feed(page.hub);
      const ids = plotIds(page.ws.layout);
      page.history.group('Prepare', () => ids.forEach((id) => page.ws.setPlot(id, { mode: 'independent' })));
      await wait(300);
      await settle();
      page.ws.selection.only(ids);
      await settle();
      await rightClick(axisSpot(page, ids[0], 'aux.triangle'));
      await button('Add Y cursors on this axis');
      expect(ids.map((id) => page.pv(id).hasYCursors('aux.triangle'))).toEqual([true, true]);
      await closeAll();
      // Escape, once the popover has closed, may deselect: the selection is made again.
      page.ws.selection.only(ids);
      await rightClick(axisSpot(page, ids[0], 'aux.triangle'));
      await button('Clear this axis');
      for (const id of ids) for (const x of page.pv(id).yAxes) expect(page.pv(id).hasYCursors(x.key), `${ids.indexOf(id)}:${x.key}`).toBe(false);
    } finally {
      page.dispose();
    }
  });
});

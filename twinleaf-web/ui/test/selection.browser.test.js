// What keeps the plots selected and what clears them, on the whole page, as
// the user's verdicts list it (round 2, Selection #1, #2, #16, #17). A
// press on each control of every kind the list names, with any plots
// selected, clears the selection or keeps it as the list says; a press on a
// plot's own control selects that plot first; keyboard focus selects;
// Escape leaves a maximized view first, the selection kept, then deselects;
// and the plot keys act on no plot from a property table.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { centre, focusPage, frames, mountPage, pickItem, plotMenu } from './browser/page.js';
import { plotIds } from '../src/lib/layout.js';

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine']];

/** The controls round 2 #16 names, each a selector for its elements on the page and whether a press there keeps the plots selected. */
const LIST = [
  ['the Live button', () => [...document.querySelectorAll('button')].filter((b) => /^(Live|Paused|Pause)/.test(b.textContent.trim())), false],
  ['the Layout menu', () => [...document.querySelectorAll('button')].filter((b) => b.textContent.trim() === 'Layout'), false],
  ['display settings', () => [...document.querySelectorAll('button[aria-label="Display settings"]')], false],
  ['the tab strip', () => [...document.querySelectorAll('[role=tablist] [data-tab]')], false],
  ['dock headers', () => [...document.querySelectorAll('.dock-head')], false],
  ['the status bar', () => [...document.querySelectorAll('.status-bar')], false],
  ['the gaps between plots', () => [...document.querySelectorAll('.tiles .separator')], false],
  ['the plots: their bodies and headers', () => [...document.querySelectorAll('.tiles .leaf .plot-body, .tiles .leaf .plot-head')], true],
  ['the window slider', () => [...document.querySelectorAll('button[aria-label="Time window"]')], true],
  ['the eraser', () => [...document.querySelectorAll('button[aria-label="Clear the cursors"]')], true],
  ['File', () => [...document.querySelectorAll('button')].filter((b) => b.textContent.trim() === 'File'), true],
  ['Commands', () => [...document.querySelectorAll('button.search')], true],
  ['the Signals panel: its search, its signals and its list', () => [...document.querySelectorAll('input[type=search], .palette .chip, .palette')], true],
];

/** A press and its release on `el`, as a click's are, so whatever it starts ends. */
function press(el, init = {}) {
  const at = { bubbles: true, cancelable: true, button: 0, pointerType: 'mouse', ...init };
  el.dispatchEvent(new PointerEvent('pointerdown', at));
  el.dispatchEvent(new PointerEvent('pointerup', at));
}

describe('a press', () => {
  it('on each control the list names keeps the plots selected or clears them, as it says', async () => {
    const page = await mountPage(KEYS, 'columns', 0, ['vector.x', 'vector.y']);
    try {
      for (const [name, find, keeps] of LIST) {
        const els = find();
        expect(els.length, `${name} on the page`).toBeGreaterThan(0);
        for (const el of els) {
          const ids = plotIds(page.ws.layout);
          page.ws.selected.clear();
          for (const id of ids.slice(0, 2)) page.ws.selected.add(id);
          // A plot pressed is selected alone unless it was in the selection; the rest of the list keeps it as it was.
          const inPlot = el.closest('.leaf')?.dataset.plot;
          press(el);
          flushSync();
          if (!keeps) expect(page.ws.selected.size, `${name}: clears`).toBe(0);
          else if (!inPlot || ids.slice(0, 2).includes(inPlot)) expect([...page.ws.selected], `${name}: keeps`).toEqual(ids.slice(0, 2));
        }
      }
    } finally {
      page.dispose();
    }
  });

  it('with Shift, or the wheel anywhere, never clears the selection', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b] = plotIds(page.ws.layout);
      for (const [, find] of LIST) {
        for (const el of find()) {
          page.ws.selected.clear();
          page.ws.selected.add(a);
          page.ws.selected.add(b);
          press(el, { shiftKey: true });
          el.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: 40 }));
          flushSync();
          expect(page.ws.selected.has(a) && page.ws.selected.has(b)).toBe(true);
        }
      }
    } finally {
      page.dispose();
    }
  });

  it('on a plot’s own control selects that plot first, or keeps the selection it is in', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      let checked = 0;
      fc.assert(
        fc.property(fc.subarray(ids), fc.nat(), fc.nat(), (picked, k, j) => {
          const id = ids[k % ids.length];
          const controls = [...document.querySelectorAll(`.leaf[data-plot="${id}"] button, .leaf[data-plot="${id}"] [role=combobox]`)];
          if (!controls.length) return;
          page.ws.selected.clear();
          for (const p of picked) page.ws.selected.add(p);
          press(controls[j % controls.length]);
          flushSync();
          expect([...page.ws.selected].sort()).toEqual(picked.includes(id) ? [...picked].sort() : [id]);
          expect(page.ws.focusedId).toBe(id);
          checked++;
        }),
      );
      expect(checked).toBeGreaterThan(20);
    } finally {
      page.dispose();
    }
  });
});

describe('Escape', () => {
  it('leaves a maximized view first, the selection kept, then deselects, then changes nothing, wherever focus is', async () => {
    await fc.assert(
      fc.asyncProperty(fc.option(fc.subarray([0, 1, 2], { minLength: 1 })), fc.subarray([0, 1, 2]), fc.option(fc.nat(2)), async (max, picks, focus) => {
        const page = await mountPage(KEYS, 'columns');
        try {
          // The page has the keys, however long the browser took to give them.
          await focusPage();
          const ids = plotIds(page.ws.layout);
          page.ws.maximized = max && max.map((i) => ids[i]);
          const shown = page.ws.shownIds();
          const picked = picks.map((i) => ids[i]).filter((id) => shown.includes(id));
          page.ws.selection.only(picked);
          const into = focus !== null && shown.includes(ids[focus]) ? ids[focus] : null;
          if (into) document.querySelector(`.leaf[data-plot="${into}"] .plot-card`).focus();
          else document.activeElement?.blur();
          flushSync();
          await frames();
          // What each Escape leaves: the layout restored, the selection kept; then nothing selected.
          const steps = [];
          if (max) steps.push({ maximized: null, selected: picked });
          if (picked.length) steps.push({ maximized: null, selected: [] });
          steps.push({ maximized: null, selected: [] });
          for (const want of steps) {
            await commands.key('Escape');
            flushSync();
            expect(page.ws.maximized).toEqual(want.maximized);
            expect([...page.ws.selected].sort()).toEqual([...want.selected].sort());
          }
          // Deselecting lets go of the plot clicked last too.
          if (picked.length) expect(page.ws.focusedId).toBe(null);
          expect(page.history.undos.length).toBe(0);
        } finally {
          page.dispose();
        }
      }),
      { numRuns: Math.max(20, Math.ceil(fc.readConfigureGlobal().numRuns / 5)), examples: [[[0], [0, 1], null], [[0, 1], [1], 1], [null, [2], 2], [[1], [], null]] },
    );
  }, 600_000);
});

describe('keyboard focus', () => {
  it('on a plot selects it alone', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      flushSync();
      await commands.key('2');
      flushSync();
      await frames();
      expect([...page.ws.selected]).toEqual([b]);
    } finally {
      page.dispose();
    }
  });

  it('is not needed for a plot key to act on the selection', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      document.body.focus();
      await commands.key('Control+a');
      flushSync();
      expect(page.ws.selected.size).toBe(ids.length);
      expect(page.ws.focusedId).toBe(null);
      await commands.key('l');
      flushSync();
      for (const id of ids) expect(page.ws.plots[id].legend, id).toBe(true);
    } finally {
      page.dispose();
    }
  });
});

describe('a signal clicked in the Signals list', () => {
  it('joins every selected plot that does not show it, and with none selected goes nowhere', async () => {
    const page = await mountPage(KEYS, 'columns', 0, ['vector.x', 'vector.y']);
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      const click = async () => {
        document.querySelector('.palette [data-key="vector.x"]').click();
        flushSync();
        await frames();
      };
      const tabs = page.ws.tabs;
      await click();
      expect(page.ws.tabs).toBe(tabs);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      await click();
      for (const id of [a, c]) expect(page.ws.plots[id].items.map((i) => i.key)).toContain('vector.x');
      expect(page.ws.plots[b].items.map((i) => i.key)).not.toContain('vector.x');
      expect(page.history.undos.length).toBe(1);
    } finally {
      page.dispose();
    }
  });
});

describe('Escape, pressed again', () => {
  it('with focus on a plot’s control goes out a level, and selects nothing', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      flushSync();
      await commands.key('Shift');
      document.querySelector(`.leaf[data-plot="${a}"] .plot-head button`).focus();
      for (let i = 0; i < 3; i++) {
        await commands.key('Escape');
        flushSync();
        expect(page.ws.selected.size, `after Escape ${i + 1}`).toBe(0);
      }
    } finally {
      page.dispose();
    }
  });
});

describe('F from the keys', () => {
  it('maximizes the selection, and again restores, whichever plot was clicked last', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      page.ws.selection.point(b);
      flushSync();
      document.body.focus();
      await commands.key('f');
      flushSync();
      expect([...page.ws.maximized].sort()).toEqual([a, c].sort());
      await commands.key('f');
      flushSync();
      expect(page.ws.maximized).toBe(null);
    } finally {
      page.dispose();
    }
  });
});

describe('a split', () => {
  it('undone and done again selects the new plots again', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      page.ws.selected.add(ids[0]);
      flushSync();
      await plotMenu(ids[0]);
      await pickItem('Split right');
      const made = plotIds(page.ws.layout).filter((p) => !ids.includes(p));
      expect([...page.ws.selected]).toEqual(made);
      page.history.undo();
      page.history.redo();
      flushSync();
      expect([...page.ws.selected]).toEqual(made);
    } finally {
      page.dispose();
    }
  });
});

describe('a click on one of several selected plots', () => {
  it('narrows the selection to it a moment later, unless a deselect comes first', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      document.body.focus();
      await commands.key('Control+a');
      const [x, y] = centre(document.querySelector(`.leaf[data-plot="${ids[0]}"] .plot-body`));
      await commands.mouse([['move', x, y], ['down'], ['up']]);
      await commands.key('Escape');
      await new Promise((done) => setTimeout(done, 400));
      flushSync();
      expect(page.ws.selected.size).toBe(0);
    } finally {
      page.dispose();
    }
  });
});

describe('a digit', () => {
  it('selects its plot alone, the plot that has focus already among them', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a] = plotIds(page.ws.layout);
      await commands.key('1');
      flushSync();
      await frames();
      await commands.key('Control+a');
      await commands.key('1');
      flushSync();
      expect([...page.ws.selected]).toEqual([a]);
    } finally {
      page.dispose();
    }
  });
});

describe('property rows', () => {
  const RPCS = [
    { name: 'gain', channel: 'setting' },
    { name: 'offset', channel: 'setting' },
    { name: 'temp', channel: 'report' },
  ];
  /** The row of property `name`, in whichever panel shows it: each name is one panel's here. */
  const row = (name) => [...document.querySelectorAll('tr[id^=prop-]')].find((tr) => tr.textContent.includes(name));
  const click = (el, init = {}) => {
    el.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, cancelable: true, button: 0, ...init }));
    el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, button: 0, ...init }));
  };
  const picked = () => [...document.querySelectorAll('tr[id^=prop-][aria-selected=true]')].map((tr) => tr.textContent.match(/gain|offset|temp/)?.[0]).sort();

  it('and plots are never chosen together, and a row pick spans panels', async () => {
    const page = await mountPage(KEYS, 'columns', 0, [], RPCS);
    try {
      const [a] = plotIds(page.ws.layout);
      expect(row('gain'), 'the Settings panel shows its rows').toBeTruthy();
      page.ws.selected.add(a);
      flushSync();
      click(row('gain'));
      flushSync();
      expect(page.ws.selected.size).toBe(0);
      expect(picked()).toEqual(['gain']);
      click(row('temp'), { ctrlKey: true });
      flushSync();
      expect(picked()).toEqual(['gain', 'temp']);
      click(row('offset'));
      flushSync();
      expect(picked()).toEqual(['offset']);
      const [x, y] = centre(document.querySelector(`.leaf[data-plot="${a}"] .plot-body`));
      await commands.mouse([['move', x, y], ['down'], ['up']]);
      flushSync();
      expect([...page.ws.selected]).toEqual([a]);
      expect(picked()).toEqual([]);
    } finally {
      page.dispose();
    }
  });

  it('keep the plot keys from acting on any plot, rows picked or plots still selected as the keys reach the table', async () => {
    // A row clicked, or every plot selected and the table reached from the keys, which picks no row.
    for (const reach of [() => click(row('gain')), (page) => page.ws.selection.only(plotIds(page.ws.layout))]) {
      const page = await mountPage(KEYS, 'columns', 0, [], RPCS);
      try {
        const ids = plotIds(page.ws.layout);
        reach(page);
        flushSync();
        await commands.key('Shift');
        row('gain').closest('[role=grid]').focus();
        for (const key of ['l', 'y', 'k', 'f', 'Delete']) await commands.key(key);
        flushSync();
        expect(plotIds(page.ws.layout)).toEqual(ids);
        for (const id of ids) expect(page.ws.plots[id].legend, id).toBe(false);
        expect(page.ws.maximized).toBe(null);
      } finally {
        page.dispose();
      }
    }
  });
});

describe('Space after a click on a dock header', () => {
  it('pauses the plots, as anywhere, and starts no keyboard drag of the panel', async () => {
    const page = await mountPage(KEYS, 'columns', 0, [], [{ name: 'gain', channel: 'setting' }, { name: 'temp', channel: 'report', access: ['read'] }]);
    try {
      const live = () => [...document.querySelectorAll('button')].find((b) => /^(Live|Follow live)$/.test(b.textContent.trim())).textContent.trim();
      const head = (title) => document.querySelector(`.dock-panel[aria-label="${title}"] .dock-head`);
      const was = live();
      // A click folds the Settings panel and focuses its header.
      await commands.mouse([['move', ...centre(head('Settings'))], ['down'], ['up']]);
      flushSync();
      await frames();
      expect(document.activeElement).toBe(head('Settings'));
      await commands.key(' ');
      flushSync();
      await frames();
      expect(live(), 'Space pauses').not.toBe(was);
      expect(head('Settings').tabIndex, 'the header stays in the Tab order').toBe(0);
      // The page is not in a drag: a click on a row picks it.
      const row = document.querySelector('.dock-panel[aria-label="Reports"] tr[id^=prop-] td:last-child');
      await commands.mouse([['move', ...centre(row)], ['down'], ['up']]);
      flushSync();
      expect([...page.ws.selection.rows]).toEqual(['reports:temp']);
    } finally {
      page.dispose();
    }
  });
});

describe('a split from the keys', () => {
  it('with several plots selected keeps every new plot selected, though it moves focus to one', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      await commands.key('1');
      flushSync();
      page.ws.selection.only([ids[0], ids[2]]);
      flushSync();
      await commands.key('|');
      flushSync();
      await frames();
      const made = plotIds(page.ws.layout).filter((p) => !ids.includes(p));
      expect(made.length).toBe(2);
      expect([...page.ws.selected].sort()).toEqual([...made].sort());
      expect(made).toContain(document.activeElement.closest('.leaf')?.dataset.plot);
    } finally {
      page.dispose();
    }
  });
});

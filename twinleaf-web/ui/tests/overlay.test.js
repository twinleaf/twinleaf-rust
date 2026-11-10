// At most one popup layer is open at a time, and no tooltip shows over one
// (bug class 9): whatever runs of presses, right-clicks, hovers, quick
// double presses, presses while a menu is still closing and keys a person
// makes on the page's menus, dropdowns, popovers, lists and dialogs, after
// every step at most one of them is open, a list or a submenu opened from
// inside it aside, and a tooltip shows only for a control inside it.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { centre, mountPage, pressKey, wait, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { toast } from '../src/lib/toast.svelte.js';
import { closeAll, menuPage, plotArea, settle } from './harness/menus.js';

/** A layer's content, open or closing: a menu, a dropdown, a popover, a list or a dialog; a submenu is its menu's. */
const LAYER = '[data-slot=context-menu-content], [data-slot=dropdown-menu-content], [data-slot=popover-content], [data-slot=select-content], [data-slot=dialog-content], [data-slot=alert-dialog-content]';
const isOpen = (el) => el.dataset.state === 'open' || el.getAttribute('data-open') !== null;

/**
 * The layers open, less those opened from inside another open one, as the
 * decimation list from the display settings: a layer whose trigger, the
 * element that controls it, is inside another open layer is that one's.
 */
function openLayers() {
  const open = [...document.querySelectorAll(LAYER)].filter((el) => el.dataset.state === 'open');
  return open.filter((el) => {
    const trigger = el.id && document.querySelector(`[aria-controls="${CSS.escape(el.id)}"]`);
    return !(trigger && open.some((o) => o !== el && o.contains(trigger)));
  });
}
/** The tooltips showing, each as [its text, its trigger]: the element it describes. */
const tooltips = () =>
  [...document.querySelectorAll('[data-slot=tooltip-content]')].filter((t) => /open/.test(t.dataset.state ?? '')).map((t) => [t.textContent.trim(), document.querySelector(`[aria-describedby="${CSS.escape(t.id)}"]`)]);

/** The invariant: one layer at most, and a tooltip only for a control inside the open one. */
function check(steps) {
  const layers = openLayers();
  expect(layers.length, `layers open after ${steps}: ${layers.map((l) => l.dataset.slot)}`).toBeLessThanOrEqual(1);
  if (!layers.length) return;
  const open = [...document.querySelectorAll(LAYER)].filter((el) => el.dataset.state === 'open');
  for (const [text, t] of tooltips()) expect(!!t && open.some((l) => l.contains(t)), `the tooltip ${text} over ${layers[0].dataset.slot} after ${steps}`).toBe(true);
}

const button = (text) => [...document.querySelectorAll('button')].find((b) => b.textContent.trim() === text || b.getAttribute('aria-label') === text);
/** What the steps press, each [where it is, the button that opens it]; null where the page does not show it now. */
const TARGETS = {
  Layout: () => button('Layout') && [centre(button('Layout')), 'left'],
  File: () => button('File') && [centre(button('File')), 'left'],
  'the time window': () => [centre(button('Time window')), 'left'],
  'display settings': () => [centre(button('Display settings')), 'left'],
  'the decimation list': () => {
    const el = document.querySelector('[data-slot=popover-content] [aria-label=Decimation]');
    return el && [centre(el), 'left'];
  },
  Commands: () => [centre(document.querySelector('button.search')), 'left'],
  'a plot': (page) => page.pv(page.ids[1])?.geo && [plotArea(page, page.ids[1]), 'right'],
  'a y axis': (page) => {
    const pv = page.pv(page.ids[1]), P = pv?.geo?.plot, band = pv?.axisBands?.[0];
    if (!P || !band) return null;
    return [[(band.x0 + band.x1) / 2, P.y + P.h / 2], 'right'];
  },
  'a chip': () => [centre(document.querySelector('.trace-chip .name')), 'right'],
  'a tab': () => [centre(document.querySelector('[role=tab]')), 'right'],
  'a dock header': () => [centre(document.querySelector('.dock-head')), 'right'],
  'the kind picker': () => [centre(document.querySelector('.kind-select')), 'left'],
  'the + button': (page) => [centre(document.querySelector(`.leaf[data-plot="${page.ids[1]}"] button[aria-label="Add a signal"]`)), 'left'],
};
const KEYS = ['Escape', 'Tab', 'Enter', 'Space', 'ArrowDown', 'ArrowLeft', 'ArrowRight'];

/** Closes every layer open, a dialog too, by Escape, and waits until they have gone. */
async function closeLayers() {
  for (let i = 0; i < 6 && document.querySelector(LAYER); i++) {
    await pressKey('Escape');
    await wait(150);
  }
  await closeAll();
  expect(document.querySelector(LAYER), 'every layer closed').toBe(null);
}

/** A press at `at` with `button`, no wait after. */
const pressAt = (at, button) => commands.mouse([['move', ...at], ['down', button], ['up', button]]);

const STEP = fc.oneof(
  fc.record({ press: fc.constantFrom(...Object.keys(TARGETS)) }),
  fc.record({ hover: fc.constantFrom(...Object.keys(TARGETS)) }),
  fc.record({ twice: fc.constantFrom(...Object.keys(TARGETS)) }),
  fc.record({ closing: fc.constantFrom(...Object.keys(TARGETS)), then: fc.constantFrom(...Object.keys(TARGETS)) }),
  fc.record({ key: fc.constantFrom(...KEYS) }),
);

/** Takes step `s` on the page, as a person would; a step on what the page does not show now does nothing. */
async function take(page, s) {
  const target = (name) => TARGETS[name](page);
  if (s.key) return pressKey(s.key);
  const t = target(s.press ?? s.hover ?? s.twice ?? s.closing);
  if (!t) return;
  const [at, button] = t;
  if (s.press) return pressAt(at, button);
  // Over the trigger long enough for its tooltip.
  if (s.hover) return commands.mouse([['move', ...at], ['wait', 450]]);
  if (s.twice) return (await pressAt(at, button), await pressAt(at, button));
  // Opens it, closes it with Escape, and presses the next while it fades.
  await pressAt(at, button);
  await wait(60);
  await pressKey('Escape');
  const next = target(s.then);
  if (next) await pressAt(...next);
}

describe('the page’s popup layers', () => {
  it('are one at a time, with no tooltip over them, after any run of gestures', async () => {
    const seen = new Set();
    await fc.assert(
      // A page of its own for each run, as a key may press a plot's buttons: close it, maximize it.
      fc.asyncProperty(fc.array(STEP, { minLength: 1, maxLength: 6 }), async (steps) => {
        const page = await menuPage();
        try {
          for (let i = 0; i < steps.length; i++) {
            await take(page, steps[i]);
            await wait(120);
            await settle();
            check(JSON.stringify(steps.slice(0, i + 1)));
            seen.add(openLayers().length);
          }
        } finally {
          page.dispose();
        }
      }),
        // The reported case: Layout's dropdown and File's both open, a tooltip on top.
        withExamples([
          [[{ press: 'Layout' }, { hover: 'File' }]],
          [[{ press: 'Layout' }, { press: 'File' }, { hover: 'Layout' }]],
          [[{ closing: 'Layout', then: 'File' }, { hover: 'Layout' }]],
          [[{ twice: 'Layout' }, { press: 'File' }]],
          [[{ press: 'a plot' }, { press: 'File' }, { hover: 'Layout' }]],
          [[{ press: 'display settings' }, { press: 'the decimation list' }, { press: 'Layout' }]],
        ]),
    );
    expect(seen.has(1), 'a layer was open').toBe(true);
  }, 900_000);
});

/** The keys of a run made from the keyboard alone, from a plot: they open its pickers and the palette, and move in and out of them. */
const FROM_KEYS = ['a', 'k', 'Control+k', 'Escape', 'Enter', 'ArrowDown', 'Tab'];

describe('a layer the keys go on from', () => {
  it('leaves focus on the page once every layer has closed, whatever replaced it', async () => {
    const page = await menuPage();
    let closed = 0;
    try {
      await fc.assert(
        fc.asyncProperty(fc.boolean(), fc.array(fc.constantFrom(...FROM_KEYS), { minLength: 1, maxLength: 6 }), async (menu, keys) => {
          await closeLayers();
          // Focus on the second plot, as the keys move it there.
          document.querySelector(`.leaf[data-plot="${page.ids[1]}"] .plot-card`).focus();
          await settle();
          // Or its menu, opened by a right-click, that the keys go on from.
          if (menu) await pressAt(plotArea(page, page.ids[1]), 'right');
          for (let i = 0; i < keys.length; i++) {
            await pressKey(keys[i]);
            await wait(150);
            await settle();
            if (openLayers().length) continue;
            const at = document.activeElement;
            expect(at && at !== document.body && at.isConnected, `focus after ${keys.slice(0, i + 1)}: ${at?.tagName}`).toBe(true);
            closed++;
          }
        }),
        withExamples([[true, ['Control+k', 'Escape']], [false, ['a', 'Control+k', 'Escape']], [false, ['k', 'Control+k', 'Escape']]]),
      );
      expect(closed, 'steps with every layer closed').toBeGreaterThan(10);
    } finally {
      page.dispose();
    }
  }, 900_000);
});

describe('the confirmation before running several actions', () => {
  it('is a layer: one at a time, no tooltip over it, while a notice is none', async () => {
    const page = await mountPage([['sine.sine']], 'rows', 0, ['sine.sine'], [
      { name: 'reboot', channel: 'action', type: null, access: [] },
      { name: 'save', channel: 'action', type: null, access: [] },
    ], { routes: ['/0', '/1'] });
    try {
      await settle();
      // Both actions picked with Ctrl, as runs of several are.
      for (const name of ['reboot', 'save']) document.querySelector(`.action-grid button[data-name="${name}"]`).dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, ctrlKey: true }));
      await settle();
      const run = [...document.querySelectorAll('.write-bar button')].find((b) => /^Run 2/.test(b.textContent.trim()));
      expect(run, 'the edit bar offers to run both').toBeTruthy();
      // A click, as the edit bar's own tests make: a real press on the bar lets go of the rows picked.
      run.click();
      await wait(150);
      await settle();
      expect(document.querySelector('[role=alertdialog]'), 'it asks first').toBeTruthy();
      check('the confirmation');
      expect(openLayers().map((l) => l.dataset.slot)).toEqual(['alert-dialog-content']);
      // The pointer over a toolbar button shows no tooltip over it.
      await commands.mouse([['move', ...centre(button('Display settings'))], ['wait', 450]]);
      check('a hover over Display settings');
      // The palette, opened over it, takes its place.
      await pressKey('Control+k');
      await wait(150);
      await settle();
      check('the palette');
      expect(openLayers().map((l) => l.dataset.slot)).toEqual(['dialog-content']);
      await closeLayers();
      // A notice is no layer: one shown while a menu is open leaves the menu open.
      await pressAt(plotArea({ pv: page.pv, ids: plotIds(page.ws.layout) }, plotIds(page.ws.layout)[0]), 'right');
      await settle();
      toast('A notice');
      await wait(150);
      await settle();
      expect(openLayers().map((l) => l.dataset.slot)).toEqual(['context-menu-content']);
    } finally {
      page.dispose();
    }
  });
});

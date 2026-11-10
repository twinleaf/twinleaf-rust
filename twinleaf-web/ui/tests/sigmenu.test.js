// A right-click on a signal opens that signal's menu, every time: in the
// Signals list and on a plot's chips, whatever came before it. Runs of
// right-clicks on signals, one after another with the menu before still
// open or fading, between drags of a signal that land or that Escape
// cancels, presses outside, the search taking focus, typing in it,
// selections changing and the list scrolling: after each right-click a menu
// shows, at the pointer, for that signal and with a signal's rows.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { centre, dropped, frames, leafOf, mountPage, pressKey, settle, until, withExamples } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = Array.from({ length: 30 }, (_, i) => `vector.c${i}`);
const KEYS = [SIGNALS.slice(0, 2), SIGNALS.slice(2, 4)];

/** The signals of the page, found on it: the Signals list's, and each plot's chips. */
const SIGNAL_ELS = { list: () => [...document.querySelectorAll('.palette .chip[data-key]')], chip: () => [...document.querySelectorAll('.tiles .leaf .trace-chip[data-key]')] };
const search = () => document.querySelector('.dock-panel input[type=search]');
/** The menu that shows: open, and neither hidden nor a copy fading. */
const shownMenu = () => [...document.querySelectorAll('[data-slot=context-menu-content][data-state=open]')].find((m) => getComputedStyle(m).visibility === 'visible' && !m.inert) ?? null;
const rowsOf = (menu) => [...menu.querySelectorAll('[role^=menuitem]')].map((r) => r.textContent.trim());
/** Every menu on the page, as a failure says them. */
const menusSaid = () => JSON.stringify([...document.querySelectorAll('[data-slot=context-menu-content]')].map((m) => [m.dataset.state, m.dataset.frozen ?? 'shown', getComputedStyle(m).visibility, m.inert ? 'copy' : 'menu', rowsOf(m).join('|').slice(0, 60)]));
/** The middle of signal `el`'s name, scrolled into view. */
function spot(el) {
  const name = el.querySelector('.sig-key, .name') ?? el;
  name.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  return centre(name);
}
/** Whether a press at signal `el`'s name lands on it: nothing covers it. */
const hits = (el) => el.contains(document.elementFromPoint(...spot(el)));
/** Whether signal `el` shows whole in the box that scrolls it. */
function inView(el) {
  const r = el.getBoundingClientRect(), v = el.closest('[data-slot=scroll-area-viewport]').getBoundingClientRect();
  return r.top >= v.top && r.bottom <= v.bottom;
}

/** The real time after a right-click before the next step, in ms: none, within a menu's fade, and past it. */
const rest = fc.constantFrom(0, 0, 30, 120, 450);
/** A step of a run. `at` picks a signal among those on the page. */
const step = fc.oneof(
  { weight: 6, arbitrary: fc.record({ t: fc.constant('right'), on: fc.constant('list'), at: fc.nat(), rest }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('right'), on: fc.constant('chip'), at: fc.nat(), rest }) },
  // Two right-clicks with nothing between them, on signals a menu's width apart.
  { weight: 2, arbitrary: fc.record({ t: fc.constant('rapid'), first: fc.nat(), at: fc.nat(), rest }) },
  fc.record({ t: fc.constant('drag'), at: fc.nat(), to: fc.nat(1), cancel: fc.boolean() }),
  fc.record({ t: fc.constantFrom('outside', 'escape', 'search', 'type', 'clear', 'scroll') }),
  fc.record({ t: fc.constant('select'), ids: fc.subarray([0, 1]) }),
);

describe('a right-click on a signal', () => {
  it('opens that signal’s menu at the pointer, with its rows, whatever came before: a signal’s menu open or fading, a drag, a cancelled drag, the search, a selection, a scroll', async () => {
    const seen = { list: 0, chip: 0, rapid: 0, overOpen: 0, overFading: 0, drag: 0, cancelled: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 2, maxLength: 12 }), async (steps) => {
        const page = await mountPage(KEYS, 'columns', 0, SIGNALS);
        try {
          const ids = plotIds(page.ws.layout);
          await settle();
          for (const [n, s] of steps.entries()) {
            const said = `step ${n} of ${JSON.stringify(steps)}`;
            if (s.t === 'right' || s.t === 'rapid') {
              const on = s.on ?? 'list';
              const all = SIGNAL_ELS[on]();
              // The search may match no signal.
              if (!all.length) continue;
              const el = all[s.at % all.length], key = el.dataset.key;
              // A signal under an open menu cannot be pressed: the menu goes first, as a person has it go.
              if (!hits(el)) {
                await pressKey('Escape');
                await until(() => !document.querySelector('[data-slot=context-menu-content]'), 1500);
              }
              expect(hits(el), `${key} can be pressed, ${said}`).toBe(true);
              const path = [];
              if (s.t === 'rapid') {
                // Another signal of the list, in view with this one and far enough that its menu does not cover this one.
                const [x, y] = spot(el);
                const apart = all.filter((c) => c !== el && inView(c) && inView(el) && Math.abs(centre(c)[1] - y) > 330);
                if (apart.length) {
                  path.push(['move', x, centre(apart[s.first % apart.length])[1]], ['down', 'right'], ['up', 'right']);
                  seen.rapid++;
                }
              }
              if (shownMenu()) seen.overOpen++;
              else if (document.querySelector('[data-slot=context-menu-content]')) seen.overFading++;
              const [x, y] = spot(el);
              path.push(['move', x, y], ['down', 'right'], ['up', 'right']);
              await commands.mouse(path);
              /** The menu that shows, once it is this signal's: its list marks the signal it is open for, and it is at the pointer. */
              const menuHere = () => {
                const menu = shownMenu(), box = menu?.getBoundingClientRect();
                const marked = [...document.querySelectorAll('[data-menu-for]')];
                const at = box && (Math.abs(box.left - x) < 24 || Math.abs(box.right - x) < 24) && box.top < y + 24 && box.bottom > y - 24;
                return at && marked.length === 1 && marked[0] === el ? menu : null;
              };
              expect(await until(menuHere, 1500), `a menu shows at the pointer for ${key}, ${said}: the menus are ${menusSaid()}, open for ${document.querySelector('[data-menu-for]')?.dataset.key}`).toBe(true);
              const rows = rowsOf(menuHere());
              // A signal's rows: its colour, and what puts it on plots or takes it off.
              expect(rows.some((r) => r.startsWith('Colour')), `Colour among ${rows}, ${said}`).toBe(true);
              expect(rows.some((r) => r.startsWith(on === 'list' ? 'New plot' : 'Remove')), `${on === 'list' ? 'New plot' : 'Remove'} among ${rows}, ${said}`).toBe(true);
              seen[on]++;
              if (s.rest) await commands.mouse([['wait', s.rest]]);
            } else if (s.t === 'drag') {
              await pressKey('Escape');
              await until(() => !document.querySelector('[data-slot=context-menu-content]'), 1500);
              const all = SIGNAL_ELS.list();
              if (!all.length) continue;
              const from = spot(all[s.at % all.length]), to = centre(leafOf(ids[s.to]).querySelector('.plot-body'));
              const path = [['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8]];
              for (let k = 1; k <= 4; k++) path.push(['move', from[0] + ((to[0] - from[0]) * k) / 4, from[1] + ((to[1] - from[1]) * k) / 4]);
              await commands.mouse(path);
              if (s.cancel) await pressKey('Escape');
              await commands.mouse([['up']]);
              await dropped();
              await settle();
              seen[s.cancel ? 'cancelled' : 'drag']++;
            } else if (s.t === 'outside') await commands.mouse([['move', ...centre(document.querySelector('.status-bar'))], ['down'], ['up']]);
            else if (s.t === 'escape') await pressKey('Escape');
            else if (s.t === 'search') await commands.mouse([['move', ...centre(search())], ['down'], ['up']]);
            else if (s.t === 'type') (search().focus(), await pressKey('c'), await pressKey('1'));
            else if (s.t === 'clear') (search().focus(), await pressKey('Control+a'), await pressKey('Backspace'));
            else if (s.t === 'scroll') {
              const view = document.querySelector('.palette').closest('[data-slot=scroll-area-viewport]');
              view.scrollTop = view.scrollTop ? 0 : view.scrollHeight;
            } else if (s.t === 'select') page.ws.selection.only(s.ids.map((i) => ids[i]));
            await settle(1);
          }
        } finally {
          await commands.mouse([['up']]);
          page.dispose();
        }
      }),
      withExamples([
        // One signal's menu, then another's while it is open: at once, within its fade, and past it.
        [[{ t: 'right', on: 'list', at: 0, rest: 0 }, { t: 'right', on: 'list', at: 0, rest: 0 }, { t: 'right', on: 'list', at: 2, rest: 30 }, { t: 'right', on: 'list', at: 4, rest: 120 }, { t: 'right', on: 'list', at: 1, rest: 450 }, { t: 'right', on: 'list', at: 3, rest: 0 }]],
        [[{ t: 'right', on: 'chip', at: 0, rest: 0 }, { t: 'right', on: 'chip', at: 0, rest: 0 }, { t: 'right', on: 'chip', at: 3, rest: 30 }, { t: 'right', on: 'list', at: 1, rest: 0 }, { t: 'right', on: 'chip', at: 1, rest: 0 }, { t: 'right', on: 'list', at: 5, rest: 0 }]],
        [[{ t: 'rapid', first: 0, at: 0, rest: 0 }, { t: 'rapid', first: 1, at: 14, rest: 30 }, { t: 'outside' }, { t: 'right', on: 'list', at: 3, rest: 0 }]],
        [[{ t: 'drag', at: 6, to: 1, cancel: false }, { t: 'right', on: 'list', at: 1, rest: 0 }, { t: 'drag', at: 2, to: 1, cancel: true }, { t: 'right', on: 'list', at: 2, rest: 0 }, { t: 'right', on: 'list', at: 3, rest: 0 }]],
        [[{ t: 'search' }, { t: 'right', on: 'list', at: 0, rest: 0 }, { t: 'type' }, { t: 'right', on: 'list', at: 1, rest: 0 }, { t: 'clear' }, { t: 'scroll' }, { t: 'right', on: 'list', at: 29, rest: 0 }, { t: 'select', ids: [0, 1] }, { t: 'right', on: 'list', at: 20, rest: 0 }, { t: 'outside' }, { t: 'right', on: 'list', at: 21, rest: 0 }]],
      ]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 900_000);

  it('on another signal of the list, its menu open, shows the menu there within a few frames, not once the one before has faded', async () => {
    const page = await mountPage(KEYS, 'columns', 0, SIGNALS);
    try {
      await settle();
      // Two signals in view together, the second clear of the first one's menu.
      const [a, b] = [SIGNAL_ELS.list()[0], SIGNAL_ELS.list()[9]];
      await commands.mouse([['move', ...spot(a)], ['down', 'right'], ['up', 'right']]);
      expect(await until(shownMenu, 1500), 'the first menu').toBe(true);
      const [, y] = spot(b);
      expect(hits(b), 'the second signal can be pressed').toBe(true);
      await commands.mouse([['move', ...spot(b)], ['down', 'right'], ['up', 'right']]);
      let shown = null;
      for (let i = 0; i < 4 && !shown; i++) (await frames(1), (shown = shownMenu()));
      expect(shown, `the menu shows again: ${menusSaid()}`).toBeTruthy();
      expect(Math.abs(shown.getBoundingClientRect().top - y), 'at the second signal').toBeLessThan(24);
    } finally {
      page.dispose();
    }
  });

  it('that no menu answers leaves the open menu showing once its copy has faded', async () => {
    const page = await mountPage(KEYS, 'columns', 0, SIGNALS);
    try {
      await settle();
      const [a, b] = SIGNAL_ELS.list();
      await commands.mouse([['move', ...spot(a)], ['down', 'right'], ['up', 'right']]);
      expect(await until(shownMenu, 1500), 'the menu').toBe(true);
      const menu = shownMenu();
      await settle();
      // The press alone, as when the button is let go off the page: the menu's own list closes nothing for it.
      b.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, cancelable: true, button: 2, pointerType: 'mouse' }));
      expect(getComputedStyle(menu).visibility, 'hidden behind its copy').toBe('hidden');
      expect(await until(() => shownMenu() === menu, 1500), `the menu shows again: ${menusSaid()}`).toBe(true);
    } finally {
      page.dispose();
    }
  });
});

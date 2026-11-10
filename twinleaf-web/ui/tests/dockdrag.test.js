// A panel dragged between the docks, on the whole page with the real
// mouse. Every panel is in one dock, whatever is done: dragged to the other
// dock or to its rail, carried over a dock and back, let go over the plots,
// with a command to hide or show a dock given in the middle of the drag,
// folded, moved from its menu, the docks hidden, shown and reset, and the
// page loaded again from what it saved.
import fc from 'fast-check';
import { afterAll, describe, expect, it } from 'vitest';
import { page as browser, commands } from 'vitest/browser';
import { DEFAULT, KEY, PANELS } from '../src/lib/docks.svelte.js';
import { centre, clickOn, dropped, glide, mountPage, pickItem, pressKey, rested, rightClick, settle, until, wait, watchFrames } from './harness/page.js';

const SIDES = ['left', 'right'];
const IDS = Object.keys(PANELS);
const across = (side) => (side === 'left' ? 'right' : 'left');
const frameOf = (side) => document.querySelector(`.dock-frame.${side}`);
const railed = (side) => frameOf(side).classList.contains('rail');
const idOf = (panel) => IDS.find((id) => PANELS[id].title === panel.getAttribute('aria-label'));
const panelOf = (id) => document.querySelector(`.dock-panel[aria-label="${PANELS[id].title}"]`);
const headOf = (id) => panelOf(id)?.querySelector('.dock-head');
const sideOn = (id) => SIDES.find((side) => frameOf(side).contains(panelOf(id)));
/** The arrangement the page has saved, or the one it starts with while it has saved none. */
const kept = () => JSON.parse(localStorage.getItem(KEY) ?? 'null') ?? { ...DEFAULT, hidden: { left: false, right: false } };
const said = (p) => `${p.id}${p.open ? '' : ', folded'}`;
/** The panels dock `side` draws, each as `said` gives one of the arrangement, in order; a placeholder of a drag is 'a placeholder'. */
const drawn = (side) => [...frameOf(side).querySelectorAll('.dock-panel')].map((panel) => (idOf(panel) ? said({ id: idOf(panel), open: panel.classList.contains('open') }) : 'a placeholder'));
const shows = (side, held = kept()) => held[side].length > 0 && !held.hidden[side];
/** Whether no drag is under way and each dock has slid to where the arrangement has it. */
const slid = () => !document.getElementById('dnd-action-dragged-el') && !document.querySelector('.layout.sliding, .dock-frame.opening, .dock-frame.closing') && SIDES.every((side) => railed(side) === !shows(side));
/** How wide each dock's frame is and what it draws, as text: the same from frame to frame once nothing slides. */
const stand = () => SIDES.map((side) => `${frameOf(side).getBoundingClientRect().width} px, ${frameOf(side).className}: ${drawn(side)}`).join('; ');
/** Waits for a drop to land and the docks to slide to where the arrangement has them, and stand still. */
async function rest() {
  await dropped();
  expect(await until(slid), 'the docks slide to where the arrangement has them').toBe(true);
  expect(await rested(stand), 'the docks stand still').toBe(true);
  await settle(4);
}
/** Lets the rails widen, or a dock slide, while a panel is held: the page's time passes, then the frames stand still. */
async function held() {
  await wait(500);
  expect(await rested(stand), 'the docks stand still with a panel held').toBe(true);
}

/** Holds the page to the rule, `when` naming the step: every panel saved in one dock, and each dock drawing its own panels, a hidden one none. */
function expectOnce(when) {
  const held = kept();
  expect(SIDES.flatMap((side) => held[side].map((p) => p.id)).sort(), `${when}: the panels saved, over both docks`).toEqual([...IDS].sort());
  for (const side of SIDES) expect(drawn(side), `${when}: the panels the ${side} dock draws, ${shows(side, held) ? 'shown' : 'hidden'}`).toEqual(shows(side, held) ? held[side].map(said) : []);
}

/** A point over the plots, between the docks. */
const overPlots = () => centre(document.querySelector('.tiles') ?? document.body);
/** Where a panel carried to dock `side` is let go: on its rail, or `at` of the way down its panels. */
function spot(side, at) {
  if (railed(side)) return centre(frameOf(side));
  const r = frameOf(side).querySelector('.dock').getBoundingClientRect();
  return [r.left + r.width / 2, r.top + 8 + at * (Math.min(r.height, 420) - 16)];
}
/** A point of the header of panel `id` that the pointer hits: its middle, or else the first found of what shows of it in its dock's frame. */
function grip(id) {
  const head = headOf(id), h = head.getBoundingClientRect(), f = frameOf(sideOn(id)).getBoundingClientRect();
  const left = Math.max(h.left, f.left), right = Math.min(h.right, f.right);
  const points = [[(left + right) / 2, h.top + h.height / 2]];
  for (const y of [h.top + 4, h.bottom - 4]) for (let x = left + 3; x < right; x += 4) points.push([x, y]);
  return points.find((p) => head.contains(document.elementFromPoint(...p))) ?? points[0];
}
/** Presses on the header of panel `id` and starts to drag it; the rails have widened when it returns. */
async function pickUp(id) {
  headOf(id).scrollIntoView({ block: 'nearest' });
  await settle();
  const from = grip(id);
  await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 6, from[1] + 6], ['wait', 60], ['move', from[0] + 12, from[1] + 12]]);
  await held();
  return [from[0] + 12, from[1] + 12];
}
/** Carries what is held from `from` through each of `points`, each read as it is reached, resting at each; returns where it ended. */
async function carry(from, ...points) {
  let at = from;
  for (const point of points) {
    const to = point();
    await commands.mouse([...glide(at, to, 5), ['wait', 120], ['move', to[0] + 2, to[1] + 3], ['wait', 120]]);
    await settle(4);
    at = [to[0] + 2, to[1] + 3];
  }
  return at;
}
async function letGo() {
  await commands.mouse([['up']]);
  await rest();
}
/** Gives the command that hides dock `side`, or shows it, from the command palette, as the keyboard can with the mouse busy. */
async function byPalette(side) {
  const palette = () => document.querySelector('[data-slot=dialog-overlay]');
  await pressKey('Control+k');
  expect(await until(palette, 2000), 'the palette opens').toBe(true);
  for (const ch of `dock ${side}`) await pressKey(ch);
  await wait(100);
  await pressKey('Enter');
  expect(await until(() => !palette(), 3000), 'the palette closes').toBe(true);
  await held();
}

const table = Array.from({ length: 2 }, (_, i) => [{ name: `setting.p${i}`, channel: 'setting', value: i }, { name: `report.p${i}`, channel: 'report', value: i, access: ['read'] }]).flat();
const mount = (saved) => mountPage([['s0.x']], 'columns', 0, ['s0.x', 's1.x'], table, { saved: saved ? { [KEY]: saved } : {} });

const panel = fc.constantFrom(...IDS), side = fc.constantFrom(...SIDES), at = fc.constantFrom(0, 0.3, 0.6, 1);
/**
 * A step. Each that drags a panel is skipped while its dock is hidden.
 * - drag: to the other dock, `at` of the way down it, or onto its rail.
 * - back: over the other dock, or its rail, and back to its own, let go there.
 * - cancel: out over the plots, Escape, and let go there.
 * - amid: picked up, a command given to hide dock `side`, or show it, then let go over `to`: its own dock or the other.
 * - fold: its header clicked. menu: moved to the other dock from its header's menu.
 * - toggle: dock `side`'s button clicked. reset: every panel put back, from a header's menu.
 * - reload: the page mounted again from what it saved.
 */
const step = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('drag'), panel, at }) },
  fc.record({ t: fc.constant('back'), panel }),
  fc.record({ t: fc.constant('cancel'), panel }),
  fc.record({ t: fc.constant('amid'), panel, side, to: fc.constantFrom('own', 'other') }),
  fc.record({ t: fc.constantFrom('fold', 'menu'), panel }),
  { weight: 2, arbitrary: fc.record({ t: fc.constant('toggle'), side }) },
  fc.record({ t: fc.constantFrom('reset', 'reload') }),
);
const EXAMPLES = [
  // Settings to the left dock; a panel carried over the left dock and back; the left dock hidden; Settings dragged out of what the left dock still draws.
  [[{ t: 'drag', panel: 'settings', at: 0 }, { t: 'back', panel: 'favorites' }, { t: 'toggle', side: 'left' }, { t: 'drag', panel: 'settings', at: 0.6 }, { t: 'reload' }]],
  // Settings picked up, a command given to hide its dock, and let go in the other dock.
  [[{ t: 'amid', panel: 'settings', side: 'right', to: 'other' }, { t: 'reload' }]],
  // Onto a hidden dock's rail, and back out of the dock that showed.
  [[{ t: 'toggle', side: 'left' }, { t: 'drag', panel: 'reports', at: 0.3 }, { t: 'drag', panel: 'reports', at: 0 }, { t: 'reset' }]],
  // Carried over a dock and back, then moved by the menu and put back, with nothing let go on the dock passed over.
  [[{ t: 'back', panel: 'devices' }, { t: 'menu', panel: 'settings' }, { t: 'reset' }, { t: 'cancel', panel: 'signals' }, { t: 'fold', panel: 'actions' }]],
];
/** Random cases tried beyond the examples: each drags with the real mouse, so few. */
const RANDOM = 4;

afterAll(() => browser.viewport(1200, 900));

describe('a panel', () => {
  it('is in one dock, in what is saved and on the page, whatever is dragged, hidden, folded, reset or loaded again', async () => {
    await browser.viewport(1200, 900);
    const seen = { drags: 0, rails: 0, amid: 0, reloads: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 5 }), async (steps) => {
        let page = await mount(null);
        try {
          await settle(6);
          expectOnce('as mounted');
          for (const [i, s] of steps.entries()) {
            const when = `after step ${i + 1}, ${JSON.stringify(s)}`;
            const own = s.panel && sideOn(s.panel), held = s.panel && !!headOf(s.panel);
            if (s.t === 'drag' && held) {
              if (railed(across(own))) seen.rails++;
              await carry(await pickUp(s.panel), overPlots, () => spot(across(own), s.at));
              await letGo();
              expect(kept()[across(own)].map((p) => p.id), `${when}: the panels of the ${across(own)} dock, where ${s.panel} was let go`).toContain(s.panel);
              seen.drags++;
            } else if (s.t === 'back' && held) {
              await carry(await pickUp(s.panel), overPlots, () => spot(across(own), 0.3), overPlots, () => spot(own, 0.3));
              await letGo();
            } else if (s.t === 'cancel' && held) {
              await carry(await pickUp(s.panel), overPlots);
              await pressKey('Escape');
              await letGo();
            } else if (s.t === 'amid' && held) {
              const from = await carry(await pickUp(s.panel), overPlots);
              await byPalette(s.side);
              await carry(from, () => spot(s.to === 'own' ? own : across(own), 0.3));
              await letGo();
              seen.amid++;
            } else if (s.t === 'fold' && held) {
              await clickOn(headOf(s.panel).querySelector('.dock-title'));
            } else if (s.t === 'menu' && held) {
              await rightClick(centre(headOf(s.panel).querySelector('.dock-title')));
              await pickItem('Move to the');
            } else if (s.t === 'toggle' && frameOf(s.side).querySelector('.dock-button')) {
              await clickOn(frameOf(s.side).querySelector('.dock-button'));
            } else if (s.t === 'reset' && document.querySelector('.dock-title')) {
              await rightClick(centre(document.querySelector('.dock-title')));
              await pickItem('Reset every panel');
            } else if (s.t === 'reload') {
              const was = JSON.parse(localStorage.getItem(KEY) ?? 'null');
              page.dispose();
              page = await mount(was);
              await settle(6);
              if (was) expect(kept(), `${when}: the arrangement the page holds, loaded from what it saved`).toEqual(was);
              seen.reloads++;
            }
            await rest();
            expectOnce(when);
          }
        } finally {
          page.dispose();
        }
      }),
      { examples: EXAMPLES, numRuns: EXAMPLES.length + Math.min(RANDOM, fc.readConfigureGlobal().numRuns) },
    );
    expect(Object.entries(seen).filter(([, n]) => n < 1).map(([what]) => what), 'not seen: panels dragged to the other dock, onto a rail, a command given amid a drag, reloads').toEqual([]);
  }, 600_000);
});

// While a panel is dragged, a dock that was shown keeps its width and its
// panels, whatever is asked of the docks meanwhile; a dock hidden before
// the drag is a rail, widened to take the panel; and a rail draws no panel.
// Each is read at every frame, from before the panel is picked up until
// the docks rest after it is let go.
describe('a dock, while a panel is dragged', () => {
  /** The least width a dock is drawn with its panels, in px: narrower, a frame is a rail or sliding to one. */
  const LEAST = 100;
  /** How wide dock `side`'s frame is, to the px. */
  const widthOf = (side) => Math.round(frameOf(side).getBoundingClientRect().width);
  /** What is wrong with the docks as they are drawn, as text; '' when nothing is. */
  function faults() {
    const out = [];
    for (const side of SIDES) {
      const frame = frameOf(side), f = frame.getBoundingClientRect();
      // A dock sliding shut has faded its panels out by the time it is narrow.
      const faded = Number(getComputedStyle(frame.querySelector('.dock')).opacity) < 0.01;
      const heads = faded ? [] : [...frame.querySelectorAll('.dock-head')].map((head) => head.getBoundingClientRect()).filter((r) => r.width > 0 && r.height > 0 && r.left < f.right && r.right > f.left);
      const narrow = frame.classList.contains('rail') || f.width < LEAST;
      if (narrow && heads.length) out.push(`the ${side} dock, ${frame.classList.contains('rail') ? 'a rail ' : ''}${Math.round(f.width)} px wide, draws ${heads.length} panel${heads.length > 1 ? 's' : ''}`);
      const b = frame.querySelector('.dock-button')?.getBoundingClientRect();
      if (b && heads.some((r) => r.left < b.right && r.right > b.left && r.top < b.bottom && r.bottom > b.top)) out.push(`the ${side} dock's button is over a header`);
    }
    return out.join('; ');
  }
  const press = async (side) => {
    await clickOn(frameOf(side).querySelector('.dock-button'));
    await rest();
  };
  /**
   * The drags, by what each is. `before` readies the page; `panel` is
   * dragged to the `other` dock or back into its `own`; and a command to
   * hide dock `amid` is given while it is held.
   */
  const DRAGS = [
    { what: 'from the right dock to the left', panel: 'settings', to: 'other' },
    { what: 'from the left dock to the right', panel: 'devices', to: 'other' },
    { what: 'from the right dock to the rail of the left, hidden before', before: () => press('left'), panel: 'reports', to: 'other' },
    { what: 'from the left dock to the rail of the right, hidden before', before: () => press('right'), panel: 'signals', to: 'other' },
    {
      what: 'within the right dock, once a panel was carried over the left dock and back and the left dock hidden',
      before: async () => {
        await carry(await pickUp('favorites'), overPlots, () => spot('left', 0.3), overPlots, () => spot('right', 0.3));
        await letGo();
        await press('left');
      },
      panel: 'reports',
      to: 'own',
    },
    { what: 'out of the right dock, with a command to hide it meanwhile', panel: 'settings', to: 'other', amid: 'right' },
    { what: 'into the left dock, with a command to hide it meanwhile', panel: 'settings', to: 'other', amid: 'left' },
  ];

  it('keeps its width and its panels if it was shown, is a widened rail if it was hidden, and as a rail draws no panel and has its button over none, at every frame', async () => {
    await browser.viewport(1200, 900);
    const seen = { widened: 0, placed: 0 };
    for (const d of DRAGS) {
      const page = await mount(null);
      let stopFaults = () => [];
      try {
        await settle(6);
        await d.before?.();
        expectOnce(`${d.what}, before the drag`);
        stopFaults = watchFrames(faults);
        const own = sideOn(d.panel), shown = SIDES.filter((side) => !railed(side)), rails = SIDES.filter(railed);
        const was = Object.fromEntries(SIDES.map((side) => [side, widthOf(side)]));
        const stops = Object.fromEntries(SIDES.map((side) => [side, watchFrames(() => widthOf(side))]));
        let at = await carry(await pickUp(d.panel), overPlots);
        if (d.amid) await byPalette(d.amid);
        at = await carry(at, () => spot(d.to === 'own' ? own : across(own), 0.3));
        if (frameOf(across(own)).querySelector('.dock-panel.shadow')) seen.placed++;
        const widths = Object.fromEntries(SIDES.map((side) => [side, stops[side]()]));
        for (const side of shown) expect(widths[side], `${d.what}: how wide the ${side} dock, shown, was at each frame the panel was held`).toEqual([was[side]]);
        for (const side of rails) {
          expect(Math.max(...widths[side]), `${d.what}: how wide the ${side} dock's rail, ${was[side]} px before, became for the panel`).toBeGreaterThan(was[side]);
          seen.widened++;
        }
        await letGo();
        if (d.amid) expect(SIDES.filter((side) => !railed(side)), `${d.what}: the docks shown once the panel is let go`).toEqual(SIDES);
        expectOnce(d.what);
      } finally {
        const were = stopFaults();
        page.dispose();
        expect(were.filter(Boolean), `${d.what}: what was wrong with the docks at some frame`).toEqual([]);
      }
    }
    expect(Object.entries(seen).filter(([, n]) => n < 1).map(([what]) => what), 'not seen: a rail widened, a placeholder in the other dock').toEqual([]);
  }, 300_000);
});

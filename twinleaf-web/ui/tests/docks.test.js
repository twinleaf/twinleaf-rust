// The docks' panels are lists inside a list: a panel is as tall as what it
// shows while its dock has the room. Where the panels do not fit, a panel
// taller than a share of its dock's height gives way, down to that share,
// its list scrolling within it, and the dock scrolls the panels that still
// do not fit. So a panel past its share has only the room the others leave,
// and no list hides rows above empty dock. The subjects are found on the
// page: every dock, every panel in it and every list in a panel.
import fc from 'fast-check';
import { afterAll, describe, expect, it } from 'vitest';
import { page as browser, commands } from 'vitest/browser';
import { PANELS } from '../src/lib/docks.svelte.js';
import { centre, clickOn, mountPage, pickItem, pressKey, rightClick, settle, typeInto, until, wait, withExamples } from './harness/page.js';

const LIST = '[data-slot=scroll-area-viewport]';
const BAR = '[data-slot=scroll-area-scrollbar]';
const TITLES = Object.values(PANELS).map((p) => p.title);
const docks = () => [...document.querySelectorAll('.dock-side:not(.rail) > .dock')];
const titleOf = (panel) => panel.getAttribute('aria-label');
const panelsOf = (dock) => [...dock.querySelectorAll(':scope > .dock-panel')];
const openOf = (dock) => panelsOf(dock).filter((p) => p.classList.contains('open'));
const listsOf = (panel) => [...panel.querySelectorAll(LIST)];
const heightOf = (el) => el.getBoundingClientRect().height;
/** Whether the list of viewport `list` draws a scroll bar. */
const barOf = (list) => [...list.parentElement.querySelectorAll(`:scope > ${BAR}`)].some((bar) => heightOf(bar) > 0);
const rpcs = (n, channel, extra = {}) => Array.from({ length: n }, (_, i) => ({ name: `${channel ?? 'other'}.p${i}`, channel, value: i, ...extra }));
const signals = (n, from = 0) => Array.from({ length: n }, (_, i) => `s${from + i}.x`);
/** The RPC table of `counts` entries by channel, as `mountPage` takes it. */
const tableOf = ({ setting, report, action, other }) => [...rpcs(setting, 'setting'), ...rpcs(report, 'report', { access: ['read'] }), ...rpcs(action, 'action', { type: null, access: [] }), ...rpcs(other, null)];

/**
 * What of a panel's own decides its height, each kind counted apart, as an
 * empty list's notice is not as tall as a row.
 */
const OWN = { rows: 'tr, .chip, .device, [data-name]', notices: '.props-empty, .sig-empty, .empty', 'edit bars': '.write-bar' };

/** The share of its dock's height that `panel` gives way down to, as the stylesheet states it: '66.667%'. */
const shareOf = (panel) => panel.computedStyleMap().get('height').toString();
/** That share of the dock of `panel` in px. */
const sharePx = (panel) => (parseFloat(shareOf(panel)) / 100) * panel.parentElement.clientHeight;

/**
 * What each panel of every dock is, by title: its height in px, whether it
 * is open, whether it is past its share of the dock, in the room the
 * others leave, and what of its own decides its height, by OWN.
 */
const measured = () =>
  Object.fromEntries(
    docks().flatMap(panelsOf).map((panel) => [
      titleOf(panel),
      {
        height: heightOf(panel),
        open: panel.classList.contains('open'),
        grown: heightOf(panel) > sharePx(panel) + 1,
        own: Object.entries(OWN).map(([what, found]) => `${panel.querySelectorAll(found).length} ${what}`).join(', '),
      },
    ]),
  );

/**
 * Holds every dock, panel and list to the rule as the page stands; `said`
 * names the case. It counts in `seen` the docks and the lists it found
 * scrolling, and the panels past their share of a dock.
 */
function expectLists(said, seen) {
  const shares = new Set();
  for (const dock of docks()) {
    const room = dock.clientHeight;
    expect(getComputedStyle(dock).overflowY, `${said}: the dock scrolls what does not fit`).toMatch(/auto|scroll/);
    const all = panelsOf(dock);
    // Every panel is reached by scrolling the dock, and none lies over another.
    if (dock.scrollHeight > room) seen.docks++;
    dock.scrollTop = dock.scrollHeight;
    expect(all.at(-1).getBoundingClientRect().bottom, `${said}: the last panel's bottom, scrolled to`).toBeLessThanOrEqual(dock.getBoundingClientRect().bottom + 1);
    dock.scrollTop = 0;
    expect(all[0].getBoundingClientRect().top, `${said}: the first panel's top, scrolled to`).toBeGreaterThanOrEqual(dock.getBoundingClientRect().top - 1);
    all.forEach((panel, i) => i && expect(panel.getBoundingClientRect().top, `${said}: ${titleOf(panel)} starts below ${titleOf(all[i - 1])}`).toBeGreaterThanOrEqual(all[i - 1].getBoundingClientRect().bottom - 1));
    for (const panel of openOf(dock)) {
      const at = `${said}: ${titleOf(panel)}`;
      const share = shareOf(panel), height = heightOf(panel), grown = height > sharePx(panel) + 1;
      shares.add(share);
      expect(share, `${at} gives way down to a share of its dock's height`).toMatch(/%$/);
      // Past its share, a panel has only room the dock has: the dock does not scroll.
      if (grown) seen.grown++;
      expect(grown ? dock.scrollHeight - room : 0, `${at}, ${height} px of a dock of ${room} px: px the dock scrolls`).toBeLessThanOrEqual(1);
      // Nothing of the panel is cut off but the rows its lists scroll to.
      for (const box of [panel, panel.querySelector('.dock-body'), ...listsOf(panel).map((l) => l.parentElement)]) {
        expect(box.scrollHeight - box.clientHeight, `${at}: px cut off ${box.className.split(' ')[0] || box.dataset.slot}`).toBeLessThanOrEqual(1);
      }
      for (const list of listsOf(panel)) {
        const hidden = list.scrollHeight - list.clientHeight;
        expect(barOf(list), `${at}: a scroll bar, with ${hidden} px of rows out of view`).toBe(hidden > 0);
        if (hidden <= 0) continue;
        seen.lists++;
        expect(getComputedStyle(list).overflowY, `${at}: its list scrolls`).toMatch(/auto|scroll/);
        expect(height, `${at} hides rows only once it has given way to its share, ${sharePx(panel)} px`).toBeGreaterThanOrEqual(sharePx(panel) - 1);
      }
    }
    // The dock's room is used before a list hides rows.
    const empty = dock.getBoundingClientRect().top + room - all.at(-1).getBoundingClientRect().bottom;
    const cut = openOf(dock).filter((panel) => listsOf(panel).some((list) => list.scrollHeight > list.clientHeight));
    expect(empty > 1 ? cut.map(titleOf) : [], `${said}: the panels with rows out of view above ${empty} px of empty ${dock.getAttribute('aria-label')}`).toEqual([]);
  }
  expect([...shares].length, `${said}: one share for every panel, of ${[...shares]}`).toBeLessThanOrEqual(1);
}

afterAll(() => browser.viewport(1200, 900));

describe('a list in a dock', () => {
  it('whose rows fit does not scroll and draws no scroll bar', async () => {
    await browser.viewport(1200, 900);
    for (const routes of [['/'], ['/', '/0'], ['/', '/0', '/1']]) {
      const page = await mountPage([['sine.sine']], 'columns', 0, ['sine.sine', 'sine.cos'], [...rpcs(2, 'setting'), ...rpcs(2, 'report', { access: ['read'] })], { routes });
      try {
        await settle(6);
        const lists = docks().flatMap(openOf).flatMap((panel) => listsOf(panel).map((list) => [titleOf(panel), list]));
        expect(lists.map(([title]) => title)).toEqual(expect.arrayContaining(['Devices', 'Signals', 'Settings']));
        for (const [title, list] of lists) {
          // The pointer on a list, where a person sees its scroll bar.
          await commands.mouse([['move', ...centre(list)]]);
          await wait(100);
          await settle();
          const said = `${title} with ${routes.length} device${routes.length === 1 ? '' : 's'}`;
          expect(list.scrollHeight - list.clientHeight, `${said}: px of rows out of view`).toBe(0);
          expect(barOf(list), `${said}: a scroll bar`).toBe(false);
        }
      } finally {
        page.dispose();
      }
    }
  }, 120_000);
});

describe('a panel in a dock', () => {
  // A step folds or unfolds the panel titled `fold`, or gives `grow` more rows.
  const step = fc.oneof(
    fc.record({ fold: fc.constantFrom(...TITLES) }),
    fc.record({ grow: fc.constantFrom('signals', 'setting', 'report', 'action', 'other'), by: fc.integer({ min: 1, max: 12 }) }),
  );
  const cases = fc.record({
    height: fc.constantFrom(900, 760, 520, 340),
    rows: fc.record({ signals: fc.integer({ min: 1, max: 30 }), setting: fc.nat(14), report: fc.nat(14), action: fc.nat(10), other: fc.nat(8) }),
    steps: fc.array(step, { minLength: 1, maxLength: 6 }),
  });
  const EXAMPLES = [
    // Other unfolds below three panels that fill the dock.
    [{ height: 760, rows: { signals: 1, setting: 9, report: 11, action: 8, other: 6 }, steps: [{ fold: 'Other' }] }],
    // The signals outgrow the left dock, beside the devices.
    [{ height: 520, rows: { signals: 4, setting: 2, report: 2, action: 2, other: 0 }, steps: [{ grow: 'signals', by: 12 }, { grow: 'signals', by: 12 }, { fold: 'Devices' }, { fold: 'Devices' }] }],
    // Every panel open in a short window, then one above the rest gains rows.
    [{ height: 340, rows: { signals: 2, setting: 6, report: 6, action: 6, other: 6 }, steps: [{ fold: 'Diagnostics' }, { fold: 'Other' }, { grow: 'setting', by: 8 }, { fold: 'Settings' }] }],
    // A panel with nothing to show gets its first row, in place of its notice.
    [{ height: 900, rows: { signals: 1, setting: 0, report: 0, action: 0, other: 1 }, steps: [{ grow: 'action', by: 1 }, { grow: 'signals', by: 1 }] }],
    // The settings outgrow their share above shorter panels, one of which folds; then the reports outgrow theirs too.
    [{ height: 900, rows: { signals: 2, setting: 14, report: 2, action: 2, other: 0 }, steps: [{ grow: 'setting', by: 12 }, { grow: 'setting', by: 12 }, { fold: 'Reports' }, { fold: 'Reports' }, { grow: 'report', by: 12 }, { grow: 'report', by: 12 }, { grow: 'report', by: 12 }] }],
  ];

  it('keeps its height whatever the other panels do unless it is past its share, in the room they leave, scrolls its own list, and is reached by scrolling its dock', async () => {
    const seen = { docks: 0, lists: 0, grown: 0 };
    await fc.assert(
      fc.asyncProperty(cases, async ({ height, rows, steps }) => {
        await browser.viewport(1200, height);
        const counts = { ...rows };
        const page = await mountPage([['s0.x']], 'columns', 0, signals(counts.signals), tableOf(counts));
        const session = page.hub.session;
        try {
          await settle(6);
          expectLists('as mounted', seen);
          for (const [i, s] of steps.entries()) {
            const said = `after step ${i + 1}, ${JSON.stringify(s)}`;
            const before = measured();
            if (s.fold) await clickOn(document.querySelector(`.dock-panel[aria-label="${s.fold}"] .dock-title`));
            else if (s.grow === 'signals') {
              for (const key of signals(s.by, counts.signals)) session.text({ type: 'trace', trace: { id: session.traces.size, route: '/', stream: key.split('.')[0], column: 'x', units: 'V', rate: 100 } });
              counts.signals += s.by;
            } else {
              counts[s.grow] += s.by;
              session.text({ type: 'table', route: '/', rpcs: tableOf(counts).map(({ name, channel, type = 'f32', access = ['read', 'write'] }) => ({ name, channel, type, access, readable: access.includes('read') })) });
            }
            await settle(6);
            await wait(50);
            await settle();
            const after = measured();
            for (const [title, was] of Object.entries(before)) {
              const is = after[title];
              // A panel that the step left as it was: not the one folded, not one whose own rows changed,
              // and not one past its share, whose height is the room the others leave.
              if (title === s.fold || !was.open || !is.open || was.own !== is.own || was.grown || is.grown) continue;
              expect(is.height, `${said}: the height of ${title}, with ${is.own}`).toBe(was.height);
            }
            expectLists(said, seen);
          }
        } finally {
          page.dispose();
        }
      }),
      withExamples(EXAMPLES),
    );
    expect(seen.docks, 'docks seen scrolling').toBeGreaterThan(0);
    expect(seen.lists, 'lists seen scrolling').toBeGreaterThan(0);
    expect(seen.grown, 'panels seen past their share of a dock').toBeGreaterThan(0);
  }, 600_000);
});

describe('the button that hides a dock', () => {
  const buttonOf = (dock) => dock.parentElement.querySelector('button[aria-label^="Hide the"]');
  const within = (a, b) => a.left >= b.left - 1 && a.right <= b.right + 1 && a.top >= b.top - 1 && a.bottom <= b.bottom + 1;
  /** The rows of `dock` that show within it and that its hide button lies over, each by its text. */
  function covered(dock) {
    const b = buttonOf(dock).getBoundingClientRect(), d = dock.getBoundingClientRect();
    const top = Math.max(b.top, d.top), bottom = Math.min(b.bottom, d.bottom);
    return [...dock.querySelectorAll(OWN.rows)]
      .filter((row) => {
        const r = row.getBoundingClientRect();
        return r.height > 0 && r.left < b.right && r.right > b.left && r.top < bottom && r.bottom > top;
      })
      .map((row) => row.textContent.trim());
  }

  it("lies over no row however its dock is scrolled, sits in the first header's row, and hides the dock", async () => {
    await browser.viewport(1200, 520);
    const page = await mountPage([['s0.x']], 'columns', 0, signals(30), tableOf({ setting: 14, report: 14, action: 10, other: 0 }), { routes: ['/', '/0', '/1', '/2', '/3', '/4'] });
    try {
      await settle(6);
      let scrolled = 0;
      for (const dock of docks()) {
        const side = dock.getAttribute('aria-label');
        const most = dock.scrollHeight - dock.clientHeight;
        if (most > 0) scrolled++;
        for (let top = 0; top <= most; top += 7) {
          dock.scrollTop = top;
          expect(covered(dock), `${side}, scrolled ${top} px of ${most}: the rows under its hide button`).toEqual([]);
        }
        dock.scrollTop = 0;
        // The row is the header's height across its panel's width: the header ends short of the button.
        const head = dock.querySelector('.dock-panel .dock-head').getBoundingClientRect(), across = dock.querySelector('.dock-panel').getBoundingClientRect();
        expect(within(buttonOf(dock).getBoundingClientRect(), { left: across.left, right: across.right, top: head.top, bottom: head.bottom }), `${side}: its hide button is within its first header's row`).toBe(true);
        expect(buttonOf(dock).contains(document.elementFromPoint(...centre(buttonOf(dock)))), `${side}: its hide button is what the pointer hits`).toBe(true);
      }
      expect(scrolled, 'docks seen scrolling').toBeGreaterThan(0);
      await clickOn(buttonOf(docks().at(-1)));
      await until(() => docks().length === 1);
      expect(docks().map((dock) => dock.getAttribute('aria-label')), "the docks open after a click on the right one's hide button").toEqual(['Left dock']);
    } finally {
      page.dispose();
    }
  }, 60_000);
});

describe("the filter of a dock's properties", () => {
  const FIELD = 'input[aria-label^="Filter the properties"]';
  const PROPERTIES = Object.values(PANELS).filter((p) => p.channels).map((p) => p.title);
  const fieldsOf = (dock) => [...dock.parentElement.querySelectorAll(FIELD)];
  const placeOf = (el) => (({ left, top, width, height }) => ({ left, top, width, height }))(el.getBoundingClientRect());
  const panel = (title) => document.querySelector(`.dock-panel[aria-label="${title}"]`);
  const namesIn = (title) => [...panel(title).querySelectorAll('tr[id^=prop-] .pname, [data-name]')].map((el) => el.textContent.trim());

  it('is one field in each dock with a panel of properties, above its panels, where it stays as they fold, and filters every one', async () => {
    await browser.viewport(1200, 900);
    const page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 4, report: 4, action: 3, other: 2 }));
    try {
      await settle(6);
      const [left, right] = docks();
      expect(docks().map((dock) => fieldsOf(dock).length), 'fields in the left dock, of no properties, and the right').toEqual([0, 1]);
      const field = fieldsOf(right)[0], at = placeOf(field);
      expect(right.contains(field), 'the field is among the panels, which scroll').toBe(false);
      expect(field.getBoundingClientRect().bottom, "the field's bottom, above the panels").toBeLessThanOrEqual(right.getBoundingClientRect().top);
      // Every panel of properties folds in turn, the open ones first, until none is open.
      const open = () => PROPERTIES.filter((title) => panel(title).classList.contains('open'));
      for (let title; (title = open()[0]); ) {
        await clickOn(panel(title).querySelector('.dock-title'));
        await settle(6);
        expect(fieldsOf(right), `with ${title} folded: the field`).toEqual([field]);
        expect(placeOf(field), `with ${title} folded: where the field is`).toEqual(at);
      }
      for (const title of ['Settings', 'Reports', 'Actions']) await clickOn(panel(title).querySelector('.dock-title'));
      await settle(6);
      expect(placeOf(field), 'with three panels unfolded: where the field is').toEqual(at);
      await typeInto(field, 'p1');
      await settle(6);
      expect(['Settings', 'Reports', 'Actions'].map(namesIn), 'the rows of each panel, filtered by p1').toEqual([['setting.p1'], ['report.p1'], ['action.p1']]);
      // A panel of properties moved to the other dock brings that dock a field of the same filter.
      await rightClick(centre(panel('Reports').querySelector('.dock-title')));
      await pickItem('Move to the left dock');
      await settle(6);
      expect(fieldsOf(left).map((f) => f.value), 'what the left dock filters by, holding Reports').toEqual(['p1']);
      expect(placeOf(field), 'with Reports in the other dock: where the field is').toEqual(at);
    } finally {
      page.dispose();
    }
  }, 60_000);
});

// What a person presses or reads in a dock, found on the page: every
// control, every run of text, every scroll bar's thumb, and the focus ring
// around a header. No two of them overlap, however tall the window and
// wherever the dock is scrolled, and every scroll bar has one look.
describe('what a dock shows', () => {
  const PRESSED = 'button, input, a[href], [role=button], [role=checkbox], [role=switch]';
  const THUMB = '[data-slot=scroll-area-thumb]';
  /** Box {l, t, r, b} of DOMRect `r`. */
  const box = (r) => ({ l: r.left, t: r.top, r: r.right, b: r.bottom });
  const meet = (a, b) => ({ l: Math.max(a.l, b.l), t: Math.max(a.t, b.t), r: Math.min(a.r, b.r), b: Math.min(a.b, b.b) });
  /** Whether box `a` has more than half a px each way. */
  const some = (a) => a.r - a.l > 0.5 && a.b - a.t > 0.5;
  /** The box inside `el` that its contents show in: its padding box, without its own scroll bar. */
  const inner = (el) => {
    const r = el.getBoundingClientRect();
    if (!el.clientWidth && !el.clientHeight) return box(r);
    return { l: r.left + el.clientLeft, t: r.top + el.clientTop, r: r.left + el.clientLeft + el.clientWidth, b: r.top + el.clientTop + el.clientHeight };
  };
  /**
   * What shows of `at`, the box of something in or of `el`: cut to every
   * box from `el` up to `dock` that clips or scrolls, and to below the
   * header of its own table where that header stays put as the rows scroll.
   */
  function shown(el, at, dock) {
    let out = at;
    for (let a = el; a && a !== dock.parentElement; a = a.parentElement) {
      const cs = getComputedStyle(a);
      if (cs.overflowX !== 'visible' || cs.overflowY !== 'visible') out = meet(out, inner(a));
    }
    const head = el.closest('table')?.querySelector('thead');
    if (head && !head.contains(el) && getComputedStyle(head.querySelector('th')).position === 'sticky') out = meet(out, { ...out, t: head.getBoundingClientRect().bottom });
    return out;
  }
  /** `dock`'s own scroll bar, which the browser draws: its `look`, as `lookOf` gives a list's, and the box of its thumb, `at`; null while the dock does not scroll. */
  function ownBar(dock) {
    const lane = dock.offsetWidth - dock.clientWidth - dock.clientLeft * 2, most = dock.scrollHeight - dock.clientHeight;
    if (most <= 0 || lane <= 0) return null;
    const cs = getComputedStyle(dock, '::-webkit-scrollbar-thumb'), px = (v) => parseFloat(v) || 0;
    const r = dock.getBoundingClientRect(), left = r.left + dock.clientLeft + dock.clientWidth, top = r.top + dock.clientTop;
    const width = lane - px(cs.borderLeftWidth) - px(cs.borderRightWidth);
    // The browser draws the thumb as styled only while the dock leaves the bar's width and colour to that styling.
    const own = getComputedStyle(dock), styled = own.scrollbarWidth === 'auto' && own.scrollbarColor === 'auto';
    if (!styled) return { look: { drawn: `as the browser's own, ${own.scrollbarWidth} and ${own.scrollbarColor}` }, at: { l: left, t: top, r: left + lane, b: top + dock.clientHeight } };
    const length = Math.max((dock.clientHeight / dock.scrollHeight) * dock.clientHeight, lane), y = top + (dock.scrollTop / most) * (dock.clientHeight - length);
    return {
      look: { lane, width, inset: px(cs.borderLeftWidth), colour: cs.backgroundColor, round: px(cs.borderTopLeftRadius) >= width / 2 },
      at: { l: left + px(cs.borderLeftWidth), t: y + px(cs.borderTopWidth), r: left + lane - px(cs.borderRightWidth), b: y + length - px(cs.borderBottomWidth) },
    };
  }
  /** The look of the scroll bar whose thumb is element `thumb`: its lane's and its thumb's widths in px, how far the thumb is in from the lane's left, its colour, and whether its ends are round. */
  function lookOf(thumb) {
    const bar = thumb.parentElement.getBoundingClientRect(), t = thumb.getBoundingClientRect(), cs = getComputedStyle(thumb);
    return { lane: bar.width, width: t.width, inset: t.left - bar.left, colour: cs.backgroundColor, round: parseFloat(cs.borderTopLeftRadius) >= t.width / 2 };
  }
  /** Everything pressed or read in `dock` that shows, each {what, el, at}. */
  function things(dock) {
    const out = [];
    const add = (what, el, at) => some(at) && out.push({ what, el, at });
    for (const el of dock.querySelectorAll(PRESSED)) add(`${el.tagName.toLowerCase()} "${(el.getAttribute('aria-label') ?? el.textContent).trim().slice(0, 30)}"`, el, shown(el, box(el.getBoundingClientRect()), dock));
    const walk = document.createTreeWalker(dock, NodeFilter.SHOW_TEXT);
    for (let node; (node = walk.nextNode()); ) {
      if (!node.data.trim() || getComputedStyle(node.parentElement).visibility === 'hidden') continue;
      const range = document.createRange();
      range.selectNodeContents(node);
      add(`text "${node.data.trim().slice(0, 30)}"`, node.parentElement, shown(node.parentElement, box(range.getBoundingClientRect()), dock));
    }
    for (const thumb of dock.querySelectorAll(THUMB)) add(`the scroll bar of ${titleOf(thumb.closest('.dock-panel'))}`, thumb, shown(thumb, box(thumb.getBoundingClientRect()), dock));
    const own = ownBar(dock);
    if (own) add(`the scroll bar of the ${dock.getAttribute('aria-label')}`, null, own.at);
    return out;
  }
  const related = (a, b) => !!a.el && !!b.el && (a.el.contains(b.el) || b.el.contains(a.el));
  /** Each pair of `list` that overlaps, of things neither of which is inside the other, as text. */
  function overlaps(list) {
    const out = [];
    list.forEach((a, i) => list.slice(i + 1).forEach((b) => !related(a, b) && some(meet(a.at, b.at)) && out.push(`${a.what} and ${b.what}`)));
    return out;
  }

  it('overlaps no two things a person presses or reads, a focused header’s ring among them, at any window height and wherever it is scrolled; and gives every scroll bar one look', async () => {
    const seen = { scrolled: 0, both: 0, rings: 0 }, looks = new Map();
    for (const height of [900, 520, 340]) {
      await browser.viewport(1200, height);
      const page = await mountPage([['s0.x']], 'columns', 0, signals(30), tableOf({ setting: 14, report: 14, action: 10, other: 6 }), { routes: ['/', '/0', '/1'] });
      try {
        await settle(6);
        // A row picked, so a panel shows its edit bar, and Other unfolded.
        await clickOn(document.querySelector('.dock-panel[aria-label=Settings] tr[id^=prop-] td'));
        await clickOn(document.querySelector('.dock-panel[aria-label=Other] .dock-title'));
        await settle(6);
        for (const dock of docks()) {
          const side = `${dock.getAttribute('aria-label')} in a window ${height} px tall`, most = Math.max(0, dock.scrollHeight - dock.clientHeight);
          if (most > 0) seen.scrolled++;
          for (const [where, top] of Object.entries({ top: 0, middle: most / 2, bottom: most })) {
            dock.scrollTop = top;
            await settle();
            const all = things(dock);
            expect(all.length, `${side}: things found`).toBeGreaterThan(8);
            expect(overlaps(all), `${side}, scrolled to its ${where}: what overlaps`).toEqual([]);
            const own = ownBar(dock);
            // A dock that scrolls has a bar in a lane of its own, beside its panels and not over them.
            expect(most > 0 && !own, `${side}: its scroll bar is drawn over its panels`).toBe(false);
            if (own) looks.set(JSON.stringify(own.look), `the ${side}`);
            for (const thumb of dock.querySelectorAll(THUMB)) {
              if (!some(box(thumb.getBoundingClientRect()))) continue;
              looks.set(JSON.stringify(lookOf(thumb)), `${titleOf(thumb.closest('.dock-panel'))} in the ${side}`);
              if (own) seen.both++;
            }
          }
          // Each header with focus from the keys, its ring around it.
          for (const head of dock.querySelectorAll('.dock-head')) {
            await pressKey('Shift');
            head.focus();
            await settle();
            const ring = document.querySelector('.focus-ring');
            expect(ring, `${side}: the ring around ${head.textContent.trim()}`).not.toBe(null);
            const around = { what: `the ring around ${head.textContent.trim()}`, el: head, at: box(ring.getBoundingClientRect()) };
            expect(overlaps([around, ...things(dock)]).filter((pair) => pair.startsWith(around.what)), `${side}: what the ring overlaps`).toEqual([]);
            seen.rings++;
          }
          dock.scrollTop = 0;
        }
      } finally {
        page.dispose();
      }
    }
    expect(Object.entries(seen).filter(([, n]) => n < 1).map(([what]) => what), 'not seen: docks scrolled, a list’s bar beside its dock’s, rings').toEqual([]);
    expect([...looks.keys()].length, `one look for every scroll bar, of ${[...looks].map(([look, where]) => `${look} (${where})`).join('; ')}`).toBe(1);
  }, 180_000);
});

// A press on a header that lets go where it pressed, or within a couple of
// px, is a click, which folds the panel and does nothing else: no drag is
// under way, so neither dock's frame changes, whether a dock is shown or
// hidden, by its classes or by where it is at any frame.
describe('a click on a panel’s header', () => {
  const SIDES = ['left', 'right'];
  const frameOf = (side) => document.querySelector(`.dock-frame.${side}`);
  /** How the frame of dock `side` stands: its classes, its dock's, and where it is, in px. */
  const stateOf = (side) => {
    const frame = frameOf(side), r = frame.getBoundingClientRect();
    return `${frame.className} / ${frame.querySelector('.dock-side').className} / from ${Math.round(r.left)} px, ${Math.round(r.width)} px wide`;
  };
  /** Notes `read()` at every frame until the function it returns is called, which gives each different value noted, in order. */
  function watch(read) {
    const noted = [];
    let on = true;
    const note = () => {
      if (!on) return;
      const now = read();
      if (noted.at(-1) !== now) noted.push(now);
      requestAnimationFrame(note);
    };
    note();
    return () => ((on = false), noted);
  }
  /** How far the pointer moves between press and release, in px: not at all, and short of a drag along one axis and along both. */
  const MOVES = [[0, 0], [2, 0], [2, 2]];
  /** Where across a header's width a click lands, as a share of it: near its left end, its middle, near its right end. */
  const ACROSS = [0.04, 0.5, 0.96];

  it('folds the panel and changes neither dock’s frame at any frame, the other dock shown or hidden, from every header of each dock, anywhere across it', async () => {
    await browser.viewport(1200, 900);
    let clicks = 0;
    for (const hidden of [null, ...SIDES]) {
      const page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 3, report: 3, action: 2, other: 2 }));
      try {
        await settle(6);
        if (hidden) {
          await clickOn(frameOf(hidden).querySelector('button[aria-label^="Hide the"]'));
          expect(await until(() => frameOf(hidden).classList.contains('rail')), `the ${hidden} dock is its rail`).toBe(true);
          await settle(6);
        }
        for (const side of SIDES.filter((s) => s !== hidden)) {
          const other = SIDES.find((s) => s !== side);
          const titles = [...frameOf(side).querySelectorAll('.dock-panel')].map(titleOf);
          expect(titles.length, `panels in the ${side} dock`).toBeGreaterThan(1);
          for (const [i, title] of titles.entries()) {
            for (const [k, across] of ACROSS.entries()) {
              const [dx, dy] = MOVES[(i + k) % MOVES.length];
              const panel = () => document.querySelector(`.dock-panel[aria-label="${title}"]`);
              const head = panel().querySelector('.dock-head');
              head.scrollIntoView({ block: 'nearest' });
              await settle();
              const r = head.getBoundingClientRect(), at = [r.left + across * r.width, r.top + r.height / 2], was = panel().classList.contains('open');
              const said = `a click on ${title}, ${Math.round(across * 100)}% across its header, moving ${dx}, ${dy} px, with the ${other} dock ${other === hidden ? 'hidden' : 'shown'}`;
              await commands.mouse([['move', ...at]]);
              await settle();
              const stop = watch(() => `the ${side} dock: ${stateOf(side)}; the ${other} dock: ${stateOf(other)}`);
              await commands.mouse([['down'], ['wait', 50], ['move', at[0] + dx, at[1] + dy], ['wait', 80], ['up'], ['wait', 80]]);
              await settle(6);
              const stood = stop();
              expect(stood, `${said}: how the docks' frames stood, frame by frame`).toEqual(stood.slice(0, 1));
              expect(panel().classList.contains('open'), `${said}: the panel is open`).toBe(!was);
              clicks++;
            }
          }
        }
      } finally {
        page.dispose();
      }
    }
    expect(clicks).toBeGreaterThan(40);
  }, 180_000);
});

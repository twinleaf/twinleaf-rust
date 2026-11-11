// A plot's chip row follows its signals. On the whole page, signals are
// dragged by their chips with the real mouse, resting on whatever chip rows
// and plots lie on the way, onto another plot's chips, its body, a side of
// its compass, or the layout's outer edge, with undo and redo between. After
// every step each plot's row shows its signals in order, and the tab keeps
// every signal it had: a move takes a signal somewhere, never away. And
// under a row that runs past an edge, a dash per signal, in its colour and
// order, dim exactly while its chip is out of view as a person would say it,
// one lit while the row shows any chip, as it scrolls, resizes and its chips
// and their values change; a click on a dim one brings its chip into view.
// Under a row that fits, none. The header's + follows the row's last chip at
// the chips' spacing, or holds the row's end while it runs past it, still
// as the pointer comes and goes, and opens the add-signal menu under it.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { commands } from 'vitest/browser';
import { centre, clickOn, closeAll, drag, dropped, feed, frames, leafOf, mountPage, pressKey, settle, until, wait, withExamples } from './harness/page.js';
import { openMenu } from './harness/menus.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { STEP } from '../src/lib/moves.js';
import { chip } from '../src/lib/tabs.js';

const KEYS = [['sine.sine', 'sine.cosine'], ['aux.triangle'], ['aux.sawtooth', 'sine.sine']];

const rowOf = (id) => [...document.querySelectorAll(`.leaf[data-plot="${id}"] .trace-zone [id^="chip-"]`)].map((e) => e.id.slice('chip-'.length));
const keysIn = (ws) => new Set(Object.values(ws.plots).flatMap((p) => p.items.map((i) => i.key)));

const step = fc.oneof(
  { weight: 4, arbitrary: fc.record({ t: fc.constant('chip'), a: fc.nat(), i: fc.nat(), b: fc.nat(), onto: fc.constantFrom('chips', 'body', 'left', 'right', 'top', 'bottom') }) },
  fc.record({ t: fc.constant('undo') }),
  fc.record({ t: fc.constant('redo') }),
);

/** How many chip drags moved a signal, over every run: none would leave the property vacuous. */
let moved = 0;

async function run(s, { ws, history }) {
  const ids = plotIds(ws.layout).filter((id) => ws.plots[id].items.length);
  if (s.t === 'chip' && ids.length) {
    const a = ids[s.a % ids.length], items = ws.plots[a].items;
    const el = document.getElementById(`chip-${items[s.i % items.length].id}`);
    const all = plotIds(ws.layout), b = all[s.b % all.length];
    const leaf = document.querySelector(`.leaf[data-plot="${b}"]`);
    const [cx, cy] = centre(leaf);
    const to =
      s.onto === 'chips'
        ? centre(leaf.querySelector('.trace-zone'))
        : s.onto === 'body'
          ? centre(leaf.querySelector('.plot-body') ?? leaf)
          : [cx + { left: -STEP, right: STEP, top: 0, bottom: 0 }[s.onto], cy + { left: 0, right: 0, top: -STEP, bottom: STEP }[s.onto]];
    const before = JSON.stringify(ws.tabs);
    // Into a plot by way of its chip row, as a hand coming from above does, or else through the middle of the way.
    const from = centre(el);
    const via = s.onto === 'chips' ? [(from[0] + to[0]) / 2, (from[1] + to[1]) / 2] : centre(leaf.querySelector('.trace-zone'));
    await drag(from, to, [via], 200);
    await dropped();
    if (JSON.stringify(ws.tabs) !== before) moved++;
  } else if (s.t === 'undo') history.undo();
  else if (s.t === 'redo') history.redo();
  flushSync();
  await frames();
}

describe('chip rows', () => {
  it('show their plots’ signals, and the tab keeps every signal, after any run of chip drags, undos and redos', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 6 }), async (steps) => {
        const page = await mountPage(KEYS, 'rows');
        try {
          const keys = keysIn(page.ws);
          for (const s of steps) {
            await run(s, page);
            for (const id of plotIds(page.ws.layout)) expect(rowOf(id), id).toEqual(page.ws.plots[id].items.map((i) => i.id));
            expect(keysIn(page.ws)).toEqual(keys);
          }
        } finally {
          page.dispose();
        }
      }),
        // A chip dragged from the top plot through the middle plot's row into its body.
        withExamples([[[{ t: 'chip', a: 0, i: 0, b: 1, onto: 'body' }, { t: 'chip', a: 1, i: 0, b: 1, onto: 'chips' }]]]),
    );
    expect(moved).toBeGreaterThan(0);
  }, 600_000);
});

/** Each signal's name as long as `signal`, or one letter long, and two values, for the examples. */
const L = Array(14).fill(6), S = Array(14).fill(1), Y = [0.5, -0.25];
/** Whether `chip` is wholly within the box its row shows, or, wider than the row, filling it. */
const wholly = (chip, row) => {
  const c = chip.getBoundingClientRect(), r = row.getBoundingClientRect();
  return (c.left >= r.left - 0.5 && c.right <= r.right + 0.5) || (c.left <= r.left + 0.5 && c.right >= r.right - 0.5);
};
/**
 * Whether each of `chips` is in view in `row`, as a person would say it,
 * from their boxes on the page: most of its width clear of the fades at the
 * row's edges, or covering most of the row, or, failing any such, holding
 * as much of the row as any chip does.
 */
function inView(chips, row) {
  // The row shows chips but for the room it keeps at its right end for the +.
  const box = row.getBoundingClientRect(), add = parseFloat(row.style.getPropertyValue('--add')) || 0;
  const b = { left: box.left, right: box.right - add, width: box.width - add }, fade = parseFloat(getComputedStyle(row).getPropertyValue('--fade'));
  const l = b.left + (row.classList.contains('more-left') ? fade : 0), r = b.right - (row.classList.contains('more-right') ? fade : 0);
  const span = (c, a, z) => Math.max(0, Math.min(c.right, z) - Math.max(c.left, a));
  const boxes = chips.map((c) => c.getBoundingClientRect()), held = boxes.map((c) => span(c, b.left, b.right)), most = Math.max(0, ...held);
  return boxes.map((c, i) => span(c, l, r) >= c.width / 2 || held[i] >= b.width / 2 || (most > 0 && held[i] >= most - 0.5));
}
/** Gives each of `hub`'s traces one sample more, at time `t` in s, of value `y`, as a live device sends it. */
function sample(hub, t, y) {
  const session = hub.session, perf = clock.now();
  for (const tr of session.traces.values()) session.apply({ kind: 'points', chunks: [{ id: tr.meta.id, level: 0, brk: false, xs: Float64Array.of(t), ys: Float32Array.of(y) }] }, perf);
  session.tick(perf);
}

/** Whether client boxes `a` and `b` overlap by more than half a px. */
const overlaps = (a, b) => Math.min(a.right, b.right) - Math.max(a.left, b.left) > 0.5 && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 0.5;
/**
 * Checks where the + of header `head` lies by its row of chips `row`, which
 * runs past an edge while `over`: within the chips' gap after the last chip,
 * or the text an empty row shows, while the row fits, and at the row's right
 * end while it does not; on no chip, dash or action of the header.
 */
function checkPlus(row, head, over, where) {
  const plus = head.querySelector('.add-signal').getBoundingClientRect(), r = row.getBoundingClientRect(), gap = parseFloat(getComputedStyle(row).columnGap);
  const items = [...row.children].filter((el) => el.matches('.trace-chip, .trace-empty')).map((el) => el.getBoundingClientRect());
  // The room the row keeps at its right end for the +, where chips scrolled under it are masked out.
  const add = parseFloat(row.style.getPropertyValue('--add')) || 0, shows = r.right - add;
  const said = `${where}: the + at ${plus.left}..${plus.right}, the row at ${r.left}..${r.right}, showing chips to ${shows}`;
  if (over) expect(plus.left >= shows - 0.5 && plus.right <= r.right + 0.5, `${said}, at the end of a row that runs past it, clear of its fade`).toBe(true);
  else {
    expect(items.length, `${where}: a chip or the empty row's text before the +`).toBeGreaterThan(0);
    const d = plus.left - items.at(-1).right;
    expect(d >= -0.5 && d <= gap + 0.5, `${said}, ${d} px after the last of the row's ${items.length}, within its gap of ${gap} px`).toBe(true);
  }
  // On nothing else: the chips as the row shows them, the dashes, the actions.
  const clip = (c, by) => ({ left: Math.max(c.left, by.left), right: Math.min(c.right, by.right), top: c.top, bottom: c.bottom });
  const shown = items.map((c) => clip(c, { left: r.left, right: shows })).filter((c) => c.right > c.left);
  const strip = head.querySelector('.chip-dashes')?.getBoundingClientRect();
  const dashes = [...head.querySelectorAll('.chip-dash')].map((d) => clip(d.getBoundingClientRect(), strip)).filter((d) => d.right > d.left);
  const actions = [...head.querySelectorAll('.plot-actions button')].map((b) => b.getBoundingClientRect()).filter((b) => b.width > 0);
  for (const [what, boxes] of [['a chip', shown], ['a dash', dashes], ['an action', actions]]) for (const b of boxes) expect(overlaps(plus, b), `${said}, on ${what} at ${b.left}..${b.right}`).toBe(false);
  // In the header, before its actions, where the header holds what it has; narrower, it clips them all.
  const h = head.getBoundingClientRect(), first = Math.min(h.right, ...actions.map((b) => b.left));
  const inner = h.right - parseFloat(getComputedStyle(head).paddingRight);
  if (Math.max(...actions.map((b) => b.right), plus.right) <= inner + 0.5) {
    expect(plus.left >= h.left && plus.right <= first + 0.5, `${said}, in the header at ${h.left}..${h.right} before its actions from ${first}`).toBe(true);
    // The actions keep to the header's right end, the room between them and the + free.
    if (actions.length) expect(Math.abs(Math.max(...actions.map((b) => b.right)) - inner), `${where}: the actions at the header's right end, ${inner}`).toBeLessThan(1);
  }
}

/**
 * Checks plot `id`'s row of chips against the dashes under it, and gives
 * how its row stands: whether it runs past an edge, and its dashes.
 */
function checkDashes(id, where) {
  const leaf = document.querySelector(`.leaf[data-plot="${id}"]`), row = leaf.querySelector('.trace-zone'), head = leaf.querySelector('.plot-head');
  const chips = [...row.querySelectorAll(':scope > .trace-chip')], strip = head.querySelector('.chip-dashes');
  const past = row.scrollWidth > row.clientWidth + 1, over = past && chips.length > 0;
  // The header is as high with the strip as without it.
  expect(head.getBoundingClientRect().height, `${where}: the header's height`).toBe(28);
  checkPlus(row, head, past, where);
  if (!over) {
    expect(strip, `${where}: no dashes under a row that fits, or that has no chips`).toBe(null);
    return { over, dashes: [] };
  }
  expect(strip, `${where}: dashes under a row that runs past an edge`).not.toBe(null);
  const dashes = [...strip.querySelectorAll('.chip-dash')];
  // One per signal, in the chips' order and colours.
  expect(dashes.map((d) => d.dataset.key), `${where}: a dash per chip, in order`).toEqual(chips.map((c) => c.dataset.key));
  dashes.forEach((d, i) => expect(getComputedStyle(d, '::after').backgroundColor, `${where}: dash ${i}'s colour`).toBe(getComputedStyle(chips[i].querySelector('.swatch i')).backgroundColor));
  // Dim exactly while its chip is out of view, and, while the row shows any chip, one lit.
  const seen = inView(chips, row), boxes = () => JSON.stringify({ row: row.getBoundingClientRect(), chips: chips.map((c) => [Math.round(c.getBoundingClientRect().left), Math.round(c.getBoundingClientRect().right)]) });
  dashes.forEach((d, i) => expect(getComputedStyle(d, '::after').opacity, `${where}: dash ${i}, its chip ${seen[i] ? '' : 'not '}in view, ${boxes()}`).toBe(seen[i] ? '1' : '0.25'));
  expect(dashes.some((d) => !d.classList.contains('out')), `${where}: a dash lit, ${boxes()}`).toBe(true);
  // Within the row's width, between their least width and their full one; past that the strip clips them under a fade.
  const s = strip.getBoundingClientRect(), r = row.getBoundingClientRect();
  expect(s.left >= r.left && s.right <= r.right && s.top >= r.bottom - 2 && s.bottom <= head.getBoundingClientRect().bottom + 1, `${where}: the strip ${JSON.stringify(s)} is under the row ${JSON.stringify(r)}`).toBe(true);
  for (const d of dashes) expect(d.getBoundingClientRect().width, `${where}: a dash's width`).toBeGreaterThanOrEqual(4);
  const fits = dashes.length * 7 - 3 <= s.width;
  expect(strip.classList.contains('crowded'), `${where}: ${dashes.length} dashes in ${s.width} px fade at the end`).toBe(!fits);
  if (fits) for (const d of dashes) expect(d.getBoundingClientRect().right, `${where}: every dash shows`).toBeLessThanOrEqual(s.right + 0.5);
  return { over, dashes, chips, row, strip };
}

describe('the dashes under a row of chips', () => {
  it('are there only while the row runs past an edge, one per signal in its colour, dim while its chip is out of view, one lit, and a click on a dim one brings its chip into view; the + follows the last chip, or holds the row’s end', async () => {
    const count = { fits: 0, over: 0, out: 0, crowded: 0, revealed: 0, unlit: 0, edge: 0, empty: 0, free: 0 };
    const change = fc.constantFrom('none', 'add', 'remove', 'reverse', 'narrow', 'hide', 'values', 'legend', 'widen', 'wheel');
    // How far along the row is scrolled: a fraction of the way, or chip `chip`'s left edge put `at` px from the row's.
    const scroll = fc.oneof(fc.double({ min: 0, max: 1, noNaN: true }), fc.record({ chip: fc.nat(), at: fc.integer({ min: -80, max: 80 }) }));
    const values = fc.array(fc.oneof(fc.double({ min: -1e6, max: 1e6, noNaN: true }), fc.double({ min: -1e-3, max: 1e-3, noNaN: true })), { minLength: 2, maxLength: 2 });
    const names = fc.array(fc.integer({ min: 1, max: 22 }), { minLength: 14, maxLength: 14 });
    // Where in the header's free room, past the +, a signal is dropped and a right-click made: a fraction of
    // the way across it; and where the signal comes from, the Signals list or the next plot.
    const free = fc.record({ at: fc.double({ min: 0, max: 1, noNaN: true }), from: fc.constantFrom('list', 'plot') });
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 14 }), fc.integer({ min: 1, max: 4 }), fc.option(fc.integer({ min: 150, max: 900 })), names, scroll, fc.nat(), change, values, free, async (n, columns, width, lengths, scrolled, pick, then, [y0, y1], room) => {
        const signals = lengths.map((len, i) => `dev.${'signalnamewithmanyparts'.slice(0, len)}${i}`);
        // The next plot, where there is one, holds the last signal.
        const keys = [signals.slice(0, n), ...Array.from({ length: columns - 1 }, (_, k) => (k === 0 && n < 14 ? [signals[13]] : []))];
        const page = await mountPage(keys, 'columns', 0, signals);
        try {
          feed(page.hub);
          sample(page.hub, 20, y0);
          const id = plotIds(page.ws.layout)[0];
          // A plot `width` px wide, or as wide as its pane where that is narrower.
          if (width) Object.assign(leafOf(id).querySelector('.plot-card').style, { flex: `0 0 ${width}px`, maxWidth: '100%' });
          await wait(200);
          await settle();
          const row = leafOf(id).querySelector('.trace-zone');
          if (typeof scrolled === 'number') row.scrollLeft = scrolled * (row.scrollWidth - row.clientWidth);
          else if (n) {
            const c = row.querySelectorAll(':scope > .trace-chip')[scrolled.chip % n];
            row.scrollLeft += c.getBoundingClientRect().left - row.getBoundingClientRect().left - scrolled.at;
          }
          await frames();
          const where = `${n} signals of names ${JSON.stringify(lengths.slice(0, n))} long, a plot ${width ? `${width} px` : columns === 1 ? 'the page' : `a ${columns}th of the page`} wide, scrolled ${JSON.stringify(scrolled)}`;
          let seen = checkDashes(id, where);
          count[seen.over ? 'over' : 'fits']++;
          if (!n) count.empty++;
          if (seen.over) {
            count.out += seen.dashes.some((d) => d.classList.contains('out')) ? 1 : 0;
            count.crowded += seen.strip.classList.contains('crowded') ? 1 : 0;
            // Rows where no chip is wholly in view, nor fills it, though a person sees one in it.
            count.unlit += seen.chips.some((c) => wholly(c, seen.row)) ? 0 : 1;
            // Rows where a chip lit is not wholly in view.
            count.edge += seen.chips.some((c, i) => !wholly(c, seen.row) && !seen.dashes[i].classList.contains('out')) ? 1 : 0;
          }
          // The row changes: a signal added or taken away, the signals reordered, one hidden, the header
          // narrowed, the legend taking the values from the chips, a value's text widened in its chip, or
          // the wheel turned over the row; and the values change.
          const items = page.ws.plots[id].items;
          if (then === 'add' || then === 'remove' || then === 'reverse' || then === 'hide' || then === 'legend')
            page.history.group('Change', () =>
              page.ws.setPlot(
                id,
                then === 'legend'
                  ? { legend: true }
                  : {
                      items: then === 'add' ? [...items, ...signals.filter((k) => !items.some((it) => it.key === k)).slice(0, 1).map(chip)] : then === 'remove' ? items.slice(1) : then === 'reverse' ? items.toReversed() : items.map((it, k) => (k === pick % items.length ? { ...it, hidden: true } : it)),
                    },
              ),
            );
          else if (then === 'narrow') leafOf(id).querySelector('.plot-card').style.flex = '0 0 55%';
          else if (then === 'widen' && n) row.querySelectorAll(':scope > .trace-chip .val')[pick % n].style.width = '30ch';
          else if (then === 'wheel') row.dispatchEvent(new WheelEvent('wheel', { deltaY: 40 + (pick % 120), bubbles: true, cancelable: true }));
          sample(page.hub, 21, y1);
          flushSync();
          await wait(200);
          await frames(4);
          seen = checkDashes(id, `${where}, after '${then}'`);
          if (then === 'hide' && seen.over && items.length) {
            const k = pick % items.length;
            expect(getComputedStyle(seen.dashes[k]).opacity, `${where}: the hidden signal's dash looks as its chip does`).toBe(getComputedStyle(seen.chips[k]).opacity);
          }
          if (seen.over && seen.chips.length) {
            // A dash that shows, dim if any is: a click on it brings its chip wholly into view, and lights it.
            const shown = seen.dashes.filter((d) => d.getBoundingClientRect().right <= seen.strip.getBoundingClientRect().right - 1);
            const dim = shown.filter((d) => d.classList.contains('out')), from = dim.length ? dim : shown;
            const dash = from[pick % from.length], i = seen.dashes.indexOf(dash);
            const was = wholly(seen.chips[i], seen.row);
            // A plot too small for its header shows it floating while the pointer is over it.
            const head = leafOf(id).querySelector('.plot-head');
            await commands.mouse([['move', ...centre(head)]]);
            await until(() => getComputedStyle(head).opacity === '1');
            await clickOn(dash);
            await frames();
            expect(wholly(seen.chips[i], seen.row), `${where}: chip ${i} after a click on its dash`).toBe(true);
            // A chip wider than the row shows its start, its name.
            const c = seen.chips[i].getBoundingClientRect(), r = seen.row.getBoundingClientRect();
            if (c.width >= r.width) expect(Math.abs(c.left - r.left), `${where}: chip ${i}, wider than the row, at its start`).toBeLessThan(1);
            if (!was) count.revealed++;
            expect(dash.classList.contains('out'), `${where}: dash ${i} after a click on it`).toBe(false);
            checkDashes(id, `${where}, after '${then}' and a click on dash ${i}`);
          }
          // The + and the chips stay where they are as the pointer comes onto the header, Copy image
          // showing there, and leaves it.
          const head = leafOf(id).querySelector('.plot-head'), plus = head.querySelector('.add-signal');
          const boxes = () => JSON.stringify([plus, ...row.children].map((el) => ((b) => [b.left, b.top, b.width])(el.getBoundingClientRect())));
          await commands.mouse([['move', 1, 1]]);
          await until(() => getComputedStyle(head.querySelector('.copy-image') ?? head).opacity !== '1' || !head.querySelector('.copy-image'));
          const away = boxes();
          await commands.mouse([['move', ...centre(head)]]);
          await until(() => getComputedStyle(head).opacity === '1' && getComputedStyle(head.querySelector('.copy-image') ?? head).opacity === '1');
          expect(boxes(), `${where}: the + and the chips with the pointer on the header`).toBe(away);
          await commands.mouse([['move', 1, 1]]);
          await frames();
          expect(boxes(), `${where}: the + and the chips with the pointer gone again`).toBe(away);
          checkDashes(id, `${where}, after '${then}', the pointer gone`);
          // A press on the + opens the add-signal menu under it, and A does from the plot.
          await commands.mouse([['move', ...centre(head)]]);
          await until(() => getComputedStyle(head).opacity === '1');
          await clickOn(plus);
          expect(await until(() => openMenu()?.querySelector('.add-filter')), `${where}: the add-signal menu from the +`).toBe(true);
          const m = openMenu().getBoundingClientRect(), p = plus.getBoundingClientRect();
          expect(m.top >= p.bottom - 1 && m.top <= p.bottom + 12 && m.left < p.right && m.right > p.left, `${where}: the menu at ${JSON.stringify(m)} under the + at ${JSON.stringify(p)}`).toBe(true);
          await closeAll();
          leafOf(id).querySelector('.plot-card').focus();
          await pressKey('a');
          expect(await until(() => openMenu()?.querySelector('.add-filter')), `${where}: the add-signal menu from A`).toBe(true);
          const q = openMenu().getBoundingClientRect();
          expect(Math.abs(q.left - m.left) < 1 && Math.abs(q.top - m.top) < 1, `${where}: the menu from A at ${JSON.stringify(q)}, where the +'s is`).toBe(true);
          await closeAll();
          // The header's free room past the +, short of the actions and the header's gap before them, is
          // the row's: a right-click there opens the add-signal menu, a press-drag picks the plot up, and
          // a signal dropped there lands last, the shadow of it last in the row while it is held there.
          const p2 = plus.getBoundingClientRect(), actions = head.querySelector('.plot-actions').getBoundingClientRect();
          const end = actions.left - parseFloat(getComputedStyle(head).columnGap), h = head.getBoundingClientRect();
          const source = room.from === 'plot' && columns > 1 && n < 14 ? leafOf(plotIds(page.ws.layout)[1]).querySelector('.trace-chip') : document.querySelector(`.palette .chip[data-key="${signals[13]}"]`);
          if (end - p2.right > 8 && n < 14 && source) {
            count.free++;
            const at = [p2.right + 2 + room.at * (end - p2.right - 4), h.top + h.height / 2];
            await commands.mouse([['move', ...at], ['down', 'right'], ['up', 'right']]);
            await settle();
            expect(await until(() => openMenu()?.querySelector('.add-filter')), `${where}: the add-signal menu from a right-click at ${at[0]} in the free room ${p2.right}..${end}`).toBe(true);
            await closeAll();
            await commands.mouse([['move', ...at], ['down'], ['move', at[0] + 20, at[1] + 40], ['move', at[0] + 40, at[1] + 80]]);
            await frames(4);
            expect(document.querySelector('.move-ghost'), `${where}: the plot picked up by a press-drag at ${at[0]}`).not.toBe(null);
            await pressKey('Escape');
            await commands.mouse([['up']]);
            await settle();
            source.scrollIntoView({ block: 'nearest', inline: 'nearest' });
            const from = centre(source), key = source.dataset.key;
            const path = [['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8]];
            for (let k = 1; k <= 8; k++) path.push(['move', from[0] + ((at[0] - from[0]) * k) / 8, from[1] + ((at[1] - from[1]) * k) / 8]);
            await commands.mouse([...path, ['wait', 150]]);
            await frames(4);
            const kids = [...row.querySelectorAll(':scope > .trace-chip')];
            expect(kids.at(-1)?.classList.contains('ghost'), `${where}: the shadow of ${key}, held at ${at[0]}, last in the row of ${kids.map((c) => c.dataset.key)}`).toBe(true);
            await commands.mouse([['up']]);
            await dropped();
            await settle();
            expect(page.ws.plots[id].items.at(-1)?.key, `${where}: ${key} from the ${room.from === 'plot' && columns > 1 ? 'next plot' : 'Signals list'}, dropped at ${at[0]}`).toBe(key);
          }
        } finally {
          page.dispose();
        }
      }),
      // Three that fit; twelve scrolled to the left, the middle and the right; fourteen in a quarter of the
      // page; four in a plot with room for about one, the third chip cut at both edges of the row, its left
      // in the fade; and the second chip mostly out of view at the left, its value's text widened after
      // the last scroll so that it, not the third, is mostly in view.
      withExamples([
        [1, 1, null, L, 0, 0, 'none', Y],
        [2, 1, null, L, 0, 0, 'none', Y],
        [3, 1, null, L, 0, 0, 'none', Y],
        [12, 1, null, L, 0, 11, 'none', Y],
        [12, 1, null, L, 0.5, 0, 'reverse', Y],
        [12, 1, null, L, 1, 0, 'narrow', Y],
        [14, 4, null, L, 0.3, 2, 'hide', Y],
        [4, 1, 500, L, { chip: 2, at: 8 }, 0, 'none', Y],
        [4, 1, 600, L, { chip: 1, at: -100 }, 1, 'widen', Y],
        // The + after the last of two chips that fit, before and after the legend takes their values; at
        // the end of a row of twelve that runs past it; after an empty row's text, before and after a
        // signal comes.
        [2, 1, null, S, 0, 0, 'legend', Y],
        [12, 2, null, L, 0.5, 0, 'none', Y],
        [0, 1, null, L, 0, 0, 'add', Y],
        // One signal taken away from a narrow plot, half the page or 150 px wide, whose empty row's text
        // then runs past its end: no dashes.
        [1, 2, null, S, 0, 0, 'remove', Y],
        [1, 1, 150, S, 0, 0, 'remove', Y],
        // Two chips that fit once the legend takes their values, with room past the +, a signal dropped
        // in the middle of it from the Signals list, and from the next plot.
        [2, 1, null, S, 0, 0, 'legend', Y, { at: 0.5, from: 'list' }],
        [2, 2, null, S, 0, 0, 'legend', Y, { at: 0.5, from: 'plot' }],
      ].map((e) => [...e.slice(0, 8), e[8] ?? { at: 0.5, from: 'list' }])),
    );
    for (const [what, k] of Object.entries(count)) expect(k, what).toBeGreaterThan(0);
  }, 600_000);
});

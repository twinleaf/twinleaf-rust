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
// Under a row that fits, none.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { commands } from 'vitest/browser';
import { centre, clickOn, drag, dropped, feed, frames, leafOf, mountPage, settle, until, wait, withExamples } from './harness/page.js';
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

/** Each signal's name as long as `signal`, and two values, for the examples. */
const L = Array(14).fill(6), Y = [0.5, -0.25];
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
  const b = row.getBoundingClientRect(), fade = parseFloat(getComputedStyle(row).getPropertyValue('--fade'));
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

/**
 * Checks plot `id`'s row of chips against the dashes under it, and gives
 * how its row stands: whether it runs past an edge, and its dashes.
 */
function checkDashes(id, where) {
  const leaf = document.querySelector(`.leaf[data-plot="${id}"]`), row = leaf.querySelector('.trace-zone'), head = leaf.querySelector('.plot-head');
  const chips = [...row.querySelectorAll(':scope > .trace-chip')], strip = head.querySelector('.chip-dashes');
  const over = row.scrollWidth > row.clientWidth + 1;
  // The header is as high with the strip as without it.
  expect(head.getBoundingClientRect().height, `${where}: the header's height`).toBe(28);
  if (!over) {
    expect(strip, `${where}: no dashes under a row that fits`).toBe(null);
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
  it('are there only while the row runs past an edge, one per signal in its colour, dim while its chip is out of view, one lit, and a click on a dim one brings its chip into view', async () => {
    const count = { fits: 0, over: 0, out: 0, crowded: 0, revealed: 0, unlit: 0, edge: 0 };
    const change = fc.constantFrom('none', 'add', 'remove', 'reverse', 'narrow', 'hide', 'values', 'legend', 'widen', 'wheel');
    // How far along the row is scrolled: a fraction of the way, or chip `chip`'s left edge put `at` px from the row's.
    const scroll = fc.oneof(fc.double({ min: 0, max: 1, noNaN: true }), fc.record({ chip: fc.nat(), at: fc.integer({ min: -80, max: 80 }) }));
    const values = fc.array(fc.oneof(fc.double({ min: -1e6, max: 1e6, noNaN: true }), fc.double({ min: -1e-3, max: 1e-3, noNaN: true })), { minLength: 2, maxLength: 2 });
    const names = fc.array(fc.integer({ min: 1, max: 22 }), { minLength: 14, maxLength: 14 });
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 14 }), fc.integer({ min: 1, max: 4 }), fc.option(fc.integer({ min: 150, max: 900 })), names, scroll, fc.nat(), change, values, async (n, columns, width, lengths, scrolled, pick, then, [y0, y1]) => {
        const signals = lengths.map((len, i) => `dev.${'signalnamewithmanyparts'.slice(0, len)}${i}`);
        const keys = [signals.slice(0, n), ...Array.from({ length: columns - 1 }, () => [])];
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
          else {
            const c = row.querySelectorAll(':scope > .trace-chip')[scrolled.chip % n];
            row.scrollLeft += c.getBoundingClientRect().left - row.getBoundingClientRect().left - scrolled.at;
          }
          await frames();
          const where = `${n} signals of names ${JSON.stringify(lengths.slice(0, n))} long, a plot ${width ? `${width} px` : columns === 1 ? 'the page' : `a ${columns}th of the page`} wide, scrolled ${JSON.stringify(scrolled)}`;
          let seen = checkDashes(id, where);
          count[seen.over ? 'over' : 'fits']++;
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
          else if (then === 'widen') row.querySelectorAll(':scope > .trace-chip .val')[pick % n].style.width = '30ch';
          else if (then === 'wheel') row.dispatchEvent(new WheelEvent('wheel', { deltaY: 40 + (pick % 120), bubbles: true, cancelable: true }));
          sample(page.hub, 21, y1);
          flushSync();
          await wait(200);
          await frames(4);
          seen = checkDashes(id, `${where}, after '${then}'`);
          if (then === 'hide' && seen.over) {
            const k = pick % items.length;
            expect(getComputedStyle(seen.dashes[k]).opacity, `${where}: the hidden signal's dash looks as its chip does`).toBe(getComputedStyle(seen.chips[k]).opacity);
          }
          if (seen.over) {
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
      ]),
    );
    for (const [what, k] of Object.entries(count)) expect(k, what).toBeGreaterThan(0);
  }, 600_000);
});

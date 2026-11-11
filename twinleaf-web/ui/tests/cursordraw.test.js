// Where a plot draws its cursors, read from what its PlotView last drew:
// the box of each cursor's tag (its name and its value, or the tag at the
// plot's edge of one out of view), each tick label's box, and the stub
// each cursor line runs on by past the plot's edge. For any cursors, on
// plots of every mode and kind, with axes on either side: no tag lies over
// another tag or a tick label, every tag is inside the plot area, and each stub is in the
// axis's gutter beside the plot, short of the tick labels, so the line can
// be set against a tick. And where the tabs of the Y cursors not yet placed
// rest: at the foot of each y axis, in plots of every height. And the y
// numbers of a stacked plot's lanes, of every height and count: clear of
// each other and of the next lane's, several, one or none by the lane's
// height, never back as it changes, the gutter as wide as they need and the
// plot area taking the rest. And two X
// cursors close together: their tags on their outer sides, each against
// its own line, at every separation, in either order and at the plot's edges.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { flushSync } from 'svelte';
import { commands } from 'vitest/browser';
import { editCursors, feed, frames, leafOf, mountPage, settle, until, wait, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** Whether boxes `a` and `b`, each {x, y, w, h}, overlap. */
const overlap = (a, b) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
/** Where along a cursor's axis it is put, as a share of what the axis shows: in view, out of it on either side, or not placed. */
const place = fc.option(fc.oneof(fc.double({ min: 0, max: 1, noNaN: true }), fc.constantFrom(-0.3, 1.3, 0, 1, 0.5)));
const pair = fc.record({ a: place, b: place });

describe('a plot’s cursors, as drawn', () => {
  it('have their tags inside the plot and clear of every tick label, and lines that run a little into the axis, short of its labels', async () => {
    // An overlay, an independent plot with an axis on the right, a stacked plot and a spectrum.
    const page = await mountPage([SIGNALS, SIGNALS, SIGNALS, SIGNALS], 'rows', 0, SIGNALS);
    const count = { tags: 0, stubs: 0, edge: 0, labels: 0 };
    try {
      feed(page.hub);
      const ids = plotIds(page.ws.layout);
      page.history.group('Prepare', () => {
        page.ws.setPlot(ids[1], { mode: 'independent', axisSides: { [SIGNALS[1]]: 'right' } });
        page.ws.setPlot(ids[2], { mode: 'stacked' });
        page.ws.setPlot(ids[3], { kind: 'spectrum' });
      });
      for (const id of ids) page.views.get(id).pause(clock.now());
      await wait(300);
      await settle();
      await fc.assert(
        fc.asyncProperty(fc.array(fc.record({ x: pair, y: pair }), { minLength: 4, maxLength: 4 }), async (cursors) => {
          ids.forEach((id, i) => {
            const pv = page.pv(id), kind = pv.config().kind, log = (v) => (pv.logX ? Math.log10(v) : v), unlog = (v) => (pv.logX ? 10 ** v : v);
            const along = ([lo, hi], f, to = (v) => v, from = (v) => v) => (f === null ? null : from(to(lo) + f * (to(hi) - to(lo))));
            const ylog = pv.logY ? [Math.log10, (v) => 10 ** v] : [];
            const x = { a: along(pv.xd, cursors[i].x.a, log, unlog), b: along(pv.xd, cursors[i].x.b, log, unlog) };
            const y = Object.fromEntries(pv.yAxes.map((axis) => [axis.key, { a: along(pv.yBounds(axis.key), cursors[i].y.a, ...ylog), b: along(pv.yBounds(axis.key), cursors[i].y.b, ...ylog) }]));
            editCursors(page, id, () => ({ [kind === 'spectrum' ? 'f' : 'x']: x, y: { [kind]: y } }));
          });
          await settle();
          for (const id of ids) {
            const pv = page.pv(id), P = pv.geo.plot, body = pv.body.getBoundingClientRect(), n = ids.indexOf(id);
            expect(pv.tickLabels.length, `plot ${n} labels its ticks`).toBeGreaterThan(3);
            count.labels += pv.tickLabels.length;
            const tags = pv.pressable.filter((p) => p.part === 'tag');
            expect(tags.length > 0, `plot ${n} draws a tag for each cursor placed`).toBe(pv.cursorsPlaced() > 0);
            for (const { x, y, w, h } of tags) {
              const tag = { x, y, w, h };
              for (const label of pv.tickLabels) expect(overlap(tag, label), `plot ${n}: tag ${JSON.stringify(tag)} over tick label ${JSON.stringify(label)}`).toBe(false);
              expect(tag.x >= P.x - 0.5 && tag.x + tag.w <= P.x + P.w + 0.5 && tag.y >= P.y - 0.5 && tag.y + tag.h <= P.y + P.h + 0.5, `plot ${n}: tag ${JSON.stringify(tag)} inside the plot ${JSON.stringify(P)}`).toBe(true);
              count.tags++;
            }
            tags.forEach((a, i) => tags.slice(i + 1).forEach((b) => expect(overlap(a, b), `plot ${n}: tag ${a.text} at ${[a.x, a.y]} over tag ${b.text} at ${[b.x, b.y]}`).toBe(false)));
            count.edge += tags.filter((t) => t.edge).length;
            for (const s of pv.cursorStubs) {
              const box = { x: Math.min(s.x0, s.x1) - 0.5, y: Math.min(s.y0, s.y1) - 0.5, w: Math.abs(s.x1 - s.x0) + 1, h: Math.abs(s.y1 - s.y0) + 1 };
              // Wholly beside the plot, in the body, and some px long.
              const beside = box.x + box.w <= P.x + 1 || box.x >= P.x + P.w - 1 || box.y >= P.y + P.h - 1;
              const within = box.x >= body.left && box.x + box.w <= body.right && box.y + box.h <= body.bottom;
              expect([beside, within, Math.hypot(s.x1 - s.x0, s.y1 - s.y0) >= 3], `plot ${n}: stub ${JSON.stringify(s)}`).toEqual([true, true, true]);
              for (const label of pv.tickLabels) expect(overlap(box, label), `plot ${n}: stub ${JSON.stringify(s)} through tick label ${JSON.stringify(label)}`).toBe(false);
              count.stubs++;
            }
          }
        }),
        // Every cursor in view; every one out of it, on each side; a pair at the ends of the axes.
        withExamples([0.5, -0.3, 1.3, 0, 1].map((f) => [Array.from({ length: 4 }, () => ({ x: { a: f, b: f === 0.5 ? 0.25 : f }, y: { a: f, b: f === 0.5 ? 0.8 : f } }))])),
      );
      for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(10);
    } finally {
      page.dispose();
    }
  }, 600_000);
});

describe('the tabs Y cursors are dragged out of', () => {
  it('rest at the foot of each y axis, Y2 above Y1, above the time labels and clear of the header, in every lane with room for them', async () => {
    const count = { both: 0, one: 0, none: 0, monitors: 0, reached: 0, right: 0 };
    // Plots from the tile area's whole height down to ones too short for their header, which floats over them, each layout among them.
    for (const rows of [1, 4, 7, 10]) {
      const page = await mountPage(Array.from({ length: rows }, () => SIGNALS), 'rows', 0, SIGNALS);
      try {
        feed(page.hub);
        const ids = plotIds(page.ws.layout);
        page.history.group('Prepare', () => {
          ids.forEach((id, i) => page.ws.setPlot(id, [{}, { mode: 'independent', axisSides: { [SIGNALS[1]]: 'right' } }, { mode: 'stacked' }, { kind: 'spectrum', mode: rows === 1 ? 'stacked' : 'overlay' }][(i + rows) % 4]));
        });
        for (const id of ids) page.views.get(id).pause(clock.now());
        await wait(300);
        await settle(6);
        // With none placed both tabs rest; with Y1 placed on every axis, Y2's alone.
        for (const placed of [false, true]) {
          for (const id of ids) {
            const pv = page.pv(id), kind = pv.config().kind;
            editCursors(page, id, () => (placed ? { y: { [kind]: Object.fromEntries(pv.yAxes.map((axis) => [axis.key, { a: (pv.yBounds(axis.key)[0] + pv.yBounds(axis.key)[1]) / 2, b: null }])) } } : undefined));
          }
          await commands.mouse([['move', 4, 4]]);
          await wait(100);
          await settle();
          for (const id of ids) {
            const pv = page.pv(id), P = pv.geo.plot, n = `${rows} rows, plot ${ids.indexOf(id)}${placed ? ', Y1 placed' : ''}`;
            const all = pv.pressable.filter((p) => p.part === 'tab'), xTabs = all.filter((t) => t.kind === 'x');
            expect(xTabs.length, `${n}: X1 and X2 rest`).toBe(2);
            if (leafOf(id).querySelector('.plot-card.monitor')) count.monitors++;
            for (const axis of pv.yAxes) {
              const lane = axis.lane, foot = lane.y + lane.h, tabs = all.filter((t) => t.kind === 'y' && t.key === axis.key), resting = placed ? ['b'] : ['a', 'b'];
              // The lane's height below the header, where that floats over the plot's top.
              const head = leafOf(id).querySelector('.plot-card.monitor .plot-head')?.getBoundingClientRect().bottom ?? -Infinity, room = foot - Math.max(lane.y, head);
              const where = `${n}, the axis of ${axis.key}, its lane ${Math.round(lane.h)} px high with ${Math.round(room)} px clear`;
              // A tab is 14 px high with 2 px about it: both where two fit, the first alone where one does, none in a lane shorter than that.
              const fit = room >= 34 ? resting : room >= 18 ? resting.slice(0, 1) : [];
              expect(tabs.map((t) => t.name), `${where}: the tabs that rest`).toEqual(fit);
              count[room >= 34 ? 'both' : room >= 18 ? 'one' : 'none']++;
              if (axis.side === 'right') count.right++;
              for (const tab of tabs) {
                const at = `${where}: ${tab.text} at ${[tab.x, tab.y].map(Math.round)}`;
                expect(tab.y >= Math.max(lane.y, head) && tab.y + tab.h <= foot, `${at} is within its lane, below the header`).toBe(true);
                expect(tab.x >= axis.x0 && tab.x + tab.w <= axis.x1, `${at} is on its axis, ${axis.x0} to ${axis.x1}`).toBe(true);
                // At the axis's outer edge, away from the plot.
                expect(axis.side === 'left' ? tab.x - axis.x0 : axis.x1 - tab.x - tab.w, `${at} is at its axis's outer edge`).toBe(2);
                expect(tab.y + tab.h <= P.y + P.h, `${at} is above the time axis`).toBe(true);
                for (const other of [...xTabs, ...pv.tickLabels.filter((l) => l.y >= P.y + P.h)]) expect(overlap(tab, other), `${at} is clear of the time axis's tabs and labels`).toBe(false);
              }
              // Each has its own place where both fit, Y1 at the foot and Y2 a row up, whether or not the other rests there.
              for (const tab of tabs) expect(foot - tab.y - tab.h, `${where}: ${tab.text} from the lane's foot`).toBe(tab.name === 'b' && room >= 34 ? 18 : 2);
              if (tabs.length === 2) expect(tabs[1].x, `${where}: Y2 is above Y1`).toBe(tabs[0].x);
              // Under the pointer each shows, with nothing of the page over it: not the header, floating or not.
              if (!placed)
                for (const tab of tabs) {
                  await commands.mouse([['move', tab.x + tab.w / 2, tab.y + tab.h / 2]]);
                  await until(() => getComputedStyle(leafOf(id).querySelector('.plot-head')).opacity === '1');
                  await frames(2);
                  const now = pv.pressable.find((t) => t.part === 'tab' && t.kind === 'y' && t.key === tab.key && t.name === tab.name);
                  expect(document.elementFromPoint(now.x + now.w / 2, now.y + now.h / 2), `${where}: ${tab.text} is under the pointer, clear of the header`).toBe(pv.overlay);
                  expect(now.alpha, `${where}: ${tab.text} shows under the pointer`).toBe(1);
                  count.reached++;
                }
            }
          }
        }
      } finally {
        page.dispose();
      }
    }
    for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(3);
  }, 600_000);
});

describe('the y numbers of a stacked plot’s lanes', () => {
  const MANY = Array.from({ length: 12 }, (_, i) => `dev.signal${i}`);
  /** The least room, in px, between two numbers, and between a lane's numbers and the next lane's. */
  const CLEAR = 2;
  /** How far past its lane a lane's numbers may reach: half the gap between lanes, less half the clearance. */
  const REACH = 2;
  /** How many numbers a lane shows, as the rule counts them: several, one or none. */
  const kindOf = (n) => (n >= 2 ? 2 : n);

  /** A page with one stacked plot of `lanes` signals, paused, its y ranges [lo, lo + span] each, and `at(h)`, which makes the plot's tile `h` px high, draws it, and gives what it drew. */
  async function stacked(lanes, ranges = []) {
    const page = await mountPage([MANY.slice(0, lanes)], 'grid', 0, MANY);
    feed(page.hub);
    const id = plotIds(page.ws.layout)[0];
    page.history.group('Prepare', () => page.ws.setPlot(id, { mode: 'stacked' }));
    page.views.get(id).pause(clock.now());
    await wait(300);
    await settle();
    const pv = page.pv(id), leaf = leafOf(id);
    ranges.forEach(([lo, span], i) => i < lanes && pv.setYBounds([MANY[i]], lo, lo + span));
    const at = (h) => {
      leaf.style.height = `${h}px`;
      // The gutter fits what the frame before drew; a second frame draws in it.
      pv.draw(clock.now());
      pv.draw(clock.now());
      const body = pv.body.getBoundingClientRect(), P = pv.geo.plot;
      const labels = pv.tickLabels.filter((l) => l.axis !== undefined);
      const lanesDrawn = pv.yAxes.map((a) => ({ key: a.key, lane: a.lane, labels: labels.filter((l) => l.axis === a.key) }));
      // The foot of the header, where it floats over the plot's top.
      const head = leaf.querySelector('.plot-card.monitor .plot-head')?.getBoundingClientRect().bottom ?? -Infinity;
      return { body, P, labels, lanes: lanesDrawn, head, left: P.x - body.left, right: body.right - P.x - P.w, xLabels: pv.tickLabels.filter((l) => l.axis === undefined) };
    };
    return { page, pv, at };
  }

  /** Checks what a stacked plot drew: its numbers clear of each other and within their lanes, and its gutter as wide as they need. */
  function check(drawn, where) {
    const { labels, lanes, left, right, body, P, xLabels, head } = drawn;
    labels.forEach((a, i) =>
      labels.slice(i + 1).forEach((b) => {
        const apart = a.x + a.w <= b.x || b.x + b.w <= a.x || a.y + a.h + CLEAR <= b.y || b.y + b.h + CLEAR <= a.y;
        expect(apart, `${where}: ${a.axis}'s number at ${a.y} and ${b.axis}'s at ${b.y}, each ${a.h} px high, are ${CLEAR} px apart`).toBe(true);
      }),
    );
    for (const a of labels) for (const b of xLabels) expect(a.y + a.h + CLEAR <= b.y, `${where}: ${a.axis}'s number at ${a.y} is clear of the time labels at ${b.y}`).toBe(true);
    for (const { key, lane, labels: mine } of lanes)
      for (const l of mine) expect(l.y >= lane.y - REACH && l.y + l.h <= lane.y + lane.h + REACH, `${where}: ${key}'s number at ${l.y}, ${l.h} high, within its lane ${lane.y} + ${lane.h}`).toBe(true);
    // The gutter: as wide as the widest number and the room about it, at least a tab's room, while any lane draws numbers;
    // else a tab's room while a lane has room below the header for a resting tab, 14 px with 2 about it; else bare. It
    // narrows only once it is 10 px wider than that.
    const widest = Math.max(0, ...labels.map((l) => l.w)), tab = lanes.some(({ lane }) => lane.y + lane.h - Math.max(lane.y, head) >= 18);
    const want = labels.length ? Math.max(24, Math.ceil(widest + 12)) : tab ? 24 : 12;
    expect(left >= want && left <= want + 10, `${where}: the gutter, ${left} px, for ${labels.length ? `numbers ${widest} px wide` : tab ? 'a tab and no numbers' : 'nothing'}`).toBe(true);
    if (!labels.length && !tab) expect(left, `${where}: the bare gutter`).toBe(12);
    // The plot area takes the rest of the body's width.
    expect(Math.abs(P.w - (body.width - left - right)), `${where}: the plot area's width`).toBeLessThan(0.01);
  }

  it('keep their numbers clear of each other and of the next lane’s, show several, one or none by the lane’s height, and narrow the gutter to what is drawn', async () => {
    const count = { several: 0, one: 0, none: 0, bare: 0 };
    const range = fc.tuple(fc.double({ min: -1e4, max: 1e4, noNaN: true }), fc.double({ min: 1e-3, max: 1e4, noNaN: true }));
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 12 }), fc.integer({ min: 100, max: 900 }), fc.array(range, { maxLength: 12 }), async (lanes, h, ranges) => {
        const { page, at } = await stacked(lanes, ranges);
        try {
          const drawn = at(h);
          check(drawn, `${lanes} lanes in a tile ${h} px high`);
          for (const l of drawn.lanes) count[['none', 'one', 'several'][kindOf(l.labels.length)]]++;
          if (!drawn.labels.length && drawn.left === 12) count.bare++;
        } finally {
          page.dispose();
        }
      }),
      // Two, six and twelve lanes at a usual height, and twelve in a short plot.
      withExamples([[2, 600, []], [6, 600, []], [12, 600, []], [12, 160, []], [12, 100, []]]),
    );
    for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(0);
  }, 600_000);

  it('go from several to one to none as the plot shortens a pixel at a time, never back, and the gutter gives its room to the plot', async () => {
    for (const lanes of [1, 3, 12]) {
      const { page, at } = await stacked(lanes);
      try {
        const seen = new Map(); // lane index -> the kinds it showed, tallest first
        let was = null;
        for (let h = 900; h >= 100; h--) {
          const drawn = at(h), where = `${lanes} lanes in a tile ${h} px high`;
          check(drawn, where);
          drawn.lanes.forEach((l, i) => {
            const list = seen.get(i) ?? [];
            if (list.at(-1)?.[0] !== kindOf(l.labels.length)) list.push([kindOf(l.labels.length), h, Math.round(l.lane.h * 10) / 10]);
            seen.set(i, list);
          });
          // As the gutter narrows the plot area widens by as much.
          if (was) expect(Math.abs(drawn.P.w + drawn.left - (was.P.w + was.left)), `${where}: the plot area and gutter together`).toBeLessThan(0.01);
          was = drawn;
        }
        for (const [i, list] of seen) {
          const said = `${lanes} lanes, lane ${i}: [kind, tile height, lane height] as it shortens, ${JSON.stringify(list)}`;
          // Each change is a step down, never back up, so a lane never flickers between two.
          list.slice(1).forEach((r, k) => expect(r[0], said).toBeLessThan(list[k][0]));
        }
        if (lanes === 12) expect(seen.get(0).map((r) => r[0]), '12 lanes reach every kind').toEqual([2, 1, 0]);
      } finally {
        page.dispose();
      }
    }
  }, 600_000);
});

describe('two X cursors close together', () => {
  const NAMES = ['X1', 'X2'];
  /** How near, in px, the tags of two cursors come before they part, and how much further before they come back. */
  const NEAR = 4, HELD = 6;
  /** The page with one plot of `kind`, paused, and `put(a, b)`, which places X1 and X2 at client x `a` and `b` and gives what the plot then draws of each. */
  async function mounted(kind) {
    const page = await mountPage([SIGNALS], 'grid', 0, SIGNALS);
    feed(page.hub);
    const id = plotIds(page.ws.layout)[0];
    page.history.group('Prepare', () => page.ws.setPlot(id, { kind, readoutAt: { x: 0.5, y: 0.5 } }));
    page.views.get(id).pause(clock.now());
    await wait(300);
    await settle(6);
    const pv = page.pv(id);
    const put = (a, b) => {
      editCursors(page, id, () => ({ [kind === 'spectrum' ? 'f' : 'x']: { a: pv.dataAt(a), b: pv.dataAt(b) } }));
      flushSync();
      pv.draw(clock.now());
      const tags = pv.pressable.filter((p) => p.part === 'tag' && p.kind === 'x');
      const of = (name) => {
        const mine = tags.filter((t) => t.name === name);
        return { cursor: name, at: mine[0]?.at, name: mine.find((t) => NAMES.includes(t.text)), value: mine.find((t) => !NAMES.includes(t.text)) };
      };
      return { a: of('a'), b: of('b'), all: pv.pressable.filter((p) => p.part === 'tag'), P: pv.geo.plot };
    };
    return { page, pv, put };
  }
  /** Which side of its line a tag is on: 'left' or 'right' of it, or 'on' it. */
  const side = (tag, at) => (tag.x + tag.w <= at + 1 ? 'left' : tag.x >= at - 1 ? 'right' : 'on');
  /** The cursor on the left and the one on the right, X1 the left one where they meet. */
  const ordered = ({ a, b }) => (a.at <= b.at ? [a, b] : [b, a]);
  const KINDS = ['time', 'spectrum'];

  it('have their tags on their outer sides, each against its own line and clear of the other, at every separation and in either order', async () => {
    const count = { apart: 0, together: 0, namesApart: 0 };
    for (const kind of KINDS) {
      const { page, pv, put } = await mounted(kind);
      try {
        const P = pv.geo.plot;
        for (const base of [0.5, 0.4])
          for (const order of [1, -1])
            for (let sep = 0; sep <= 200; sep++) {
              const x = Math.round(P.x + base * P.w), drawn = put(x, x + order * sep), [l, r] = ordered(drawn);
              const d = r.at - l.at, where = `${kind}: X1 at ${drawn.a.at} and X2 at ${drawn.b.at}`;
              drawn.all.forEach((s, i) => drawn.all.slice(i + 1).forEach((t) => expect(overlap(s, t), `${where}: ${s.text} at ${[s.x, s.y]} over ${t.text} at ${[t.x, t.y]}`).toBe(false)));
              for (const c of [l, r])
                for (const tag of [c.name, c.value]) expect(c.at >= tag.x - 3 && c.at <= tag.x + tag.w + 3, `${where}: ${tag.text} at ${tag.x}, ${tag.w} wide, is against its line at ${c.at}`).toBe(true);
              // Each kind of tag keeps its row: the values at the top of the plot and the names at its foot.
              expect([l.value.y, l.name.y], `${where}: the rows of the tags`).toEqual([r.value.y, r.name.y]);
              // The values part before the left one's tag, right of its line, would come within NEAR px of the right one's.
              const need = l.value.w + NEAR;
              if (d < need) {
                expect([side(l.value, l.at), side(r.value, r.at)], `${where}: the values, ${d} px apart`).toEqual(['left', 'right']);
                count.apart++;
              } else if (d > need + HELD + 1) {
                expect([side(l.value, l.at), side(r.value, r.at)], `${where}: the values, ${d} px apart`).toEqual(['right', 'right']);
                count.together++;
              }
              // The names, centred on their lines, part likewise.
              if (d < l.name.w + NEAR) {
                expect([side(l.name, l.at), side(r.name, r.at)], `${where}: the names, ${d} px apart`).toEqual(['left', 'right']);
                count.namesApart++;
              } else if (d > l.name.w + NEAR + HELD + 1) expect([side(l.name, l.at), side(r.name, r.at)], `${where}: the names, ${d} px apart`).toEqual(['on', 'on']);
            }
      } finally {
        page.dispose();
      }
    }
    for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(100);
  }, 600_000);

  it('keep their tags inside the plot at its edges, each against its own line and clear of the other', async () => {
    for (const kind of KINDS) {
      const { page, pv, put } = await mounted(kind);
      try {
        const P = pv.geo.plot;
        for (const [x, order] of [[P.x + 1, 1], [P.x + 20, 1], [P.x + 20, -1], [P.x + P.w - 1, -1], [P.x + P.w - 20, -1], [P.x + P.w - 20, 1]])
          for (let sep = 0; sep <= 200; sep++) {
            const b = Math.round(x) + order * sep;
            if (b < P.x + 1 || b > P.x + P.w - 1) continue;
            const drawn = put(Math.round(x), b), where = `${kind}: X1 at ${drawn.a.at} and X2 at ${drawn.b.at}, the plot from ${P.x} to ${P.x + P.w}`;
            drawn.all.forEach((s, i) => drawn.all.slice(i + 1).forEach((t) => expect(overlap(s, t), `${where}: ${s.text} at ${[s.x, s.y]} over ${t.text} at ${[t.x, t.y]}`).toBe(false)));
            for (const c of [drawn.a, drawn.b])
              for (const tag of [c.name, c.value]) {
                expect(tag.x >= P.x - 0.5 && tag.x + tag.w <= P.x + P.w + 0.5, `${where}: ${tag.text} at ${tag.x}, ${tag.w} wide, is inside the plot`).toBe(true);
                // A value is beside its line; a name held in at the plot's edge is as near its line as the edge lets it be.
                if (tag === c.value) expect(c.at >= tag.x - 3 && c.at <= tag.x + tag.w + 3, `${where}: ${tag.text} at ${tag.x}, ${tag.w} wide, is against its line at ${c.at}`).toBe(true);
              }
          }
      } finally {
        page.dispose();
      }
    }
  }, 600_000);

  it('change the side of each tag once at most as the other comes near, once as they cross and once as it leaves, a pixel at a time, and never for a single pixel', async () => {
    for (const kind of KINDS) {
      const { page, pv, put } = await mounted(kind);
      try {
        const P = pv.geo.plot, mid = Math.round(P.x + P.w / 2);
        // X2 across X1 and X1 across X2, each from the left and from the right.
        for (const moving of ['b', 'a'])
          for (const dir of [1, -1]) {
            const runs = { 'a value': [], 'a name': [], 'b value': [], 'b name': [] };
            for (let k = -200; k <= 200; k++) {
              const x = mid + dir * k, drawn = moving === 'b' ? put(mid, x) : put(x, mid);
              for (const c of [drawn.a, drawn.b])
                for (const what of ['value', 'name']) {
                  const seen = side(c[what], c.at), list = runs[`${c.cursor} ${what}`];
                  if (list.at(-1)?.[0] === seen) list.at(-1)[1]++;
                  else list.push([seen, 1, k]);
                }
            }
            for (const [what, list] of Object.entries(runs)) {
              const said = `${kind}, ${moving} moving ${dir > 0 ? 'right' : 'left'} across the other: the sides of ${what}, ${JSON.stringify(list)}`;
              // Each run is [side, px it lasts, where it starts]: one change at most before they meet, one after, and the swap where they cross.
              expect(list.length, said).toBeLessThanOrEqual(4);
              expect(Math.max(list.filter((r) => r[2] > -200 && r[2] < 0).length, list.filter((r) => r[2] > 1).length), said).toBeLessThanOrEqual(1);
              expect(list.slice(1, -1).every((r) => r[1] > 1), said).toBe(true);
            }
            // The one that moves is on the left, then on the right, so its value's tag changes sides as it crosses.
            expect(runs[`${moving} value`].length, `${kind}, ${moving} moving: its value's tag changes sides`).toBeGreaterThan(1);
          }
      } finally {
        page.dispose();
      }
    }
  }, 600_000);

  it('keep the sides of their tags while one is moved back and forth about the distance where they part', async () => {
    for (const kind of KINDS) {
      const { page, pv, put } = await mounted(kind);
      try {
        const P = pv.geo.plot, mid = Math.round(P.x + P.w / 2), w = Math.floor(put(mid, mid + 300).a.value.w);
        const sides = (d) => {
          const drawn = put(mid, mid + d);
          return [side(drawn.a.value, drawn.a.at), side(drawn.b.value, drawn.b.at)].join(' ');
        };
        // From far apart to a few px short of parting and back, again and again: side by side throughout.
        const beside = Array.from({ length: 12 }, (_, i) => sides(w + (i % 2 ? 12 : 7)));
        expect(new Set(beside), `${kind}: never nearer than parts them`).toEqual(new Set(['right right']));
        // Once parted, out to the same few px short and back: on their outer sides throughout.
        const parted = Array.from({ length: 12 }, (_, i) => sides(w + (i % 2 ? 7 : 1)));
        expect(new Set(parted), `${kind}: parted, and never far enough to come back`).toEqual(new Set(['left right']));
        expect(sides(w + 14), `${kind}: far enough to come back`).toBe('right right');
      } finally {
        page.dispose();
      }
    }
  }, 600_000);
});

// What a view can show, live or paused, over the History settings the page
// offers, a minute to a day, at device times up to the Unix epoch's, where a
// double resolves a quarter of a microsecond:
// - `oldestShown`, the one rule: the History setting before the view's
//   time, or the oldest time the hub holds, whichever is later;
// - a View, live or paused, reads its limits from that rule whenever it is
//   asked, with the hub's History as it is then; zoomed all the way out it
//   is as wide as the setting on a hub that holds it; and it comes back by
//   `restore` where a zoom or a pan left it, unmoved by `startHeld`, which
//   moves only a view whose start the hub has dropped;
// - a zoom from the narrowest view to the widest and back about one point
//   ends where it began;
// - the time axis's ticks and labels, and the texts that name a span, from
//   2 ms to a day.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { oldestShown, timeAxis, View } from './plot.js';
import { HISTORY_SPANS } from './constants.js';
import { WINDOW_LIMITS, windowText } from './window.js';
import { spanText } from './units.js';

/** The seconds a hub keeps of each stream beyond its History setting: twinleaf-web/src/hub.rs HISTORY_HEADROOM. */
const HEADROOM = 2;
const history = fc.constantFrom(...HISTORY_SPANS);
/** A device time: seconds since a device started, or since 1970. */
const deviceTime = fc.oneof(fc.double({ min: 90_000, max: 1e6, noNaN: true }), fc.double({ min: 1.7e9, max: 1.8e9, noNaN: true }));
/** How far behind the hub's newest sample a view's time is, in seconds, within the hub's headroom. */
const behind = fc.double({ min: 0, max: HEADROOM, noNaN: true });
/** The share of its History a hub still filling holds. */
const filling = fc.double({ min: 0.01, max: 0.99, noNaN: true });
/** How near two times computed at device time `t` can be told apart: a few steps of its double. */
const step = (t) => 4 * Number.EPSILON * Math.abs(t);
/** The oldest time a hub whose newest sample is at `newest` holds, once it has filled History `h`. */
const filled = (newest, h) => newest - h - HEADROOM;

describe('the oldest time a view can show', () => {
  it('is the History setting before the view’s time on a hub that has filled it, however far within the headroom the view is behind', () => {
    fc.assert(
      fc.property(history, deviceTime, behind, (h, newest, lag) => {
        const time = newest - lag, oldest = oldestShown(filled(newest, h), time, h);
        expect(oldest).toBe(time - h);
        expect(Math.abs(time - oldest - h), 'as wide as the setting').toBeLessThanOrEqual(step(time));
      }),
    );
  });

  it('is the oldest time the hub holds while that is later: a hub still filling, or a view further behind than the headroom', () => {
    fc.assert(
      fc.property(history, deviceTime, filling, fc.double({ min: HEADROOM + 0.001, max: 30, noNaN: true }), (h, newest, share, lag) => {
        const young = newest - h * share;
        expect(oldestShown(young, newest, h)).toBe(young);
        // Past the headroom the hub has dropped the oldest of the setting's span: the view starts at what is held.
        expect(oldestShown(filled(newest, h), newest - lag, h)).toBe(filled(newest, h));
      }),
    );
  });

  it('is never further before the view’s time than the History setting, and never before what the hub holds', () => {
    fc.assert(
      fc.property(history, deviceTime, fc.double({ min: -2, max: 2, noNaN: true }), (h, time, by) => {
        // The hub holds from anywhere between twice its History before the view's time and after it.
        const held = time - h + by * h, oldest = oldestShown(held, time, h);
        expect(time - oldest).toBeLessThanOrEqual(h + step(time));
        expect(oldest).toBeGreaterThanOrEqual(held);
        expect([held, time - h]).toContain(oldest);
      }),
    );
  });

  it('is the oldest time the hub holds while the hub has said no History setting, and nothing while nothing says what is held', () => {
    expect(oldestShown(40, 700, null)).toBe(40);
    expect(oldestShown(40, 700, 0)).toBe(40);
    expect(oldestShown(null, 700, 600)).toBeNull();
  });

  it('reads as the History setting does, in the window’s seconds and the capture’s span, at every setting', () => {
    for (const h of HISTORY_SPANS) {
      for (const lag of [0, 0.15, 0.65, 1.2, HEADROOM]) {
        const newest = 100_000.37, time = newest - lag, widest = time - oldestShown(filled(newest, h), time, h);
        expect(windowText(widest), `${h} s, the view ${lag} s behind`).toBe(String(h));
        expect(spanText(widest)).toBe(spanText(h));
      }
    }
    expect(HISTORY_SPANS.map(spanText)).toEqual(['60 s', '5 min', '10 min', '30 min', '60 min', '4 h', '24 h']);
  });
});

/**
 * A view on a hub whose clock reads device time `now`, which holds `held`,
 * as Session `held` gives it, and whose decimation keeps `live` seconds.
 * `hubHolds` and `hub.at` can be changed.
 */
function viewOn(now, held, live = Infinity) {
  const hub = { at: now, now: () => hub.at, liveSeconds: () => live };
  const v = new View(hub);
  v.held = () => v.hubHolds;
  v.hubHolds = held;
  return v;
}
const shows = (v) => ({ live: v.live, right: v.right, span: v.span, pausedAt: v.pausedAt });
/** A gesture on a paused view: a zoom about a point, or a pan. */
const gesture = fc.oneof(
  fc.record({ zoom: fc.double({ min: 0.01, max: 100, noNaN: true }), anchor: fc.double({ min: 0, max: 1, noNaN: true }) }),
  fc.record({ pan: fc.double({ min: -3, max: 3, noNaN: true }) }),
);
const make = (v, g) => (g.pan === undefined ? v.zoom(g.zoom, g.anchor, 0) : v.pan(g.pan, 0));

describe('a view on a hub that holds hours', () => {
  it('can show the same, live and paused: the History setting up to its time, the delay behind the clock', () => {
    fc.assert(
      fc.property(history, deviceTime, fc.double({ min: 0, max: HEADROOM - 0.5, noNaN: true }), (h, now, lag) => {
        // The page's clock is `lag` behind the hub's newest sample, and the view the delay, at most 0.5 s, behind that.
        const v = viewOn(now, { oldest: filled(now + lag, h), dropped: true, history: h }, h);
        v.delay = 0.5;
        const time = now - v.delay;
        expect(v.limits(0)).toEqual([time - h, time]);
        v.pause(0);
        expect(v.pausedAt).toBe(time);
        expect(v.limits(0)).toEqual([time - h, time]);
        // The clock goes on: a paused view's time stays, a live one's moves with it.
        v.hub.at = now + 30;
        expect(v.limits(0)).toEqual([time - h, time]);
        v.follow();
        expect(v.limits(0)[1]).toBe(now + 30 - v.delay);
      }),
    );
  });

  it('is as wide as the History setting zoomed all the way out, live and paused, and reads so', () => {
    fc.assert(
      fc.property(history, deviceTime, fc.boolean(), (h, now, pause) => {
        const v = viewOn(now, { oldest: filled(now, h), dropped: true, history: h }, h);
        if (pause) v.pause(0);
        for (let i = 0; i < 60; i++) v.zoom(2, 1, 0);
        expect(v.live).toBe(!pause);
        expect(Math.abs(v.span - h)).toBeLessThanOrEqual(step(now));
        expect(windowText(v.span)).toBe(String(h));
        if (pause) expect(v.right).toBe(v.pausedAt);
        else expect(v.window).toBe(v.span);
      }),
    );
  });

  it('goes as wide as what a hub still filling holds, and live keeps a window wider than that', () => {
    fc.assert(
      fc.property(history, deviceTime, filling, (h, now, share) => {
        const oldest = now - h * share, v = viewOn(now, { oldest, dropped: false, history: h }, h);
        const held = now - v.delay - oldest;
        for (let i = 0; i < 60; i++) v.zoom(2, 1, 0);
        expect(v.span).toBe(Math.max(10, held));
        v.pause(0);
        for (let i = 0; i < 60; i++) v.zoom(2, 0.5, 0);
        expect(v.span).toBe(held);
        expect(v.right - v.span).toBeCloseTo(oldest, 5);
      }),
    );
  });

  it('follows the hub’s History setting while paused: lowered, it moves in; raised, it reaches what the hub holds and no further', () => {
    fc.assert(
      fc.property(history, history, deviceTime, behind, (h, later, now, lag) => {
        fc.pre(later !== h);
        const oldest = filled(now + lag, h), v = viewOn(now, { oldest, dropped: true, history: h });
        v.pause(0);
        v.hubHolds = { ...v.hubHolds, history: later };
        const [lo, hi] = v.limits();
        expect(hi).toBe(v.pausedAt);
        if (later < h) expect(lo).toBe(v.pausedAt - later);
        else expect(lo).toBe(oldest);
        // Raised, the widest view is the History it paused with and what of the headroom the hub held then.
        expect(hi - lo).toBeLessThanOrEqual(Math.min(h, later) + HEADROOM + step(now));
      }),
    );
  });

  it('comes back by restore where gestures left it, unmoved, on a hub that has dropped samples', () => {
    let widest = 0;
    fc.assert(
      fc.property(history, deviceTime, fc.oneof(behind, filling.map((share) => ({ share }))), fc.array(gesture, { maxLength: 12 }), (h, now, hold, gestures) => {
        const oldest = hold.share === undefined ? filled(now + hold, h) : now - h * hold.share;
        const v = viewOn(now, { oldest, dropped: true, history: h });
        v.pause(0);
        v.zoom(1, 0.5, 0);
        for (const g of gestures) make(v, g);
        const [lo, hi] = v.limits();
        expect(v.right).toBeLessThanOrEqual(hi);
        expect(v.right - v.span).toBeGreaterThanOrEqual(lo - step(now));
        const was = shows(v), state = v.state();
        make(v, { zoom: 0.5, anchor: 0.5 });
        v.restore(state, 0);
        expect(v.startHeld(0), 'nothing it showed is gone').toBe(false);
        expect(shows(v)).toEqual(was);
        if (was.span === hi - lo) widest++;
      }),
      { examples: HISTORY_SPANS.map((h) => [h, 100_000, 0.6, [{ zoom: 100, anchor: 0.5 }, { zoom: 100, anchor: 0.5 }, { zoom: 100, anchor: 0.5 }, { zoom: 100, anchor: 0.5 }]]) },
    );
    expect(widest).toBeGreaterThanOrEqual(HISTORY_SPANS.length);
  });

  it('moves later, to start at the oldest time it can show, once the hub has dropped where it started', () => {
    fc.assert(
      fc.property(history, deviceTime, fc.double({ min: 0.1, max: 0.9, noNaN: true }), fc.double({ min: 0.05, max: 0.5, noNaN: true }), (h, now, at, dropped) => {
        // A hub a tenth full; the view shows a twentieth of what it holds, starting `at` of the way to the room there is.
        const held = h / 10, oldest = now - held;
        const v = viewOn(now, { oldest, dropped: false, history: h });
        v.pause(0);
        v.show(oldest + at * dropped * held, oldest + at * dropped * held + held / 20, 0);
        const state = v.state(), start = v.right - v.span;
        // The hub drops what it held before a time past the view's start.
        const later = oldest + dropped * held;
        v.hubHolds = { oldest: later, dropped: true, history: h };
        v.restore(state, 0);
        expect(v.startHeld(0)).toBe(true);
        expect(v.right - v.span).toBeCloseTo(later, 5);
        expect(v.right - v.span).toBeGreaterThan(start);
        expect(v.span).toBe(state.span);
        // And stays there when asked again.
        expect(v.startHeld(0)).toBe(false);
      }),
    );
  });

  it('stays, though it starts before the hub’s first sample, while the hub has dropped none', () => {
    const v = viewOn(1000, { oldest: 997, dropped: false, history: 600 });
    v.pause(0);
    const was = shows(v);
    expect(v.right - v.span).toBeLessThan(997);
    expect(v.startHeld(0)).toBe(false);
    expect(shows(v)).toEqual(was);
  });

  it('stays where a pan to the start of its limits left it, where the sum of that start and its span rounds down', () => {
    // At these values the view's right edge less its span is a step of the double before the limit it was held at.
    const at = 262739.36333671573, v = viewOn(at, { oldest: filled(at, 600), dropped: true, history: 600 });
    v.delay = 0;
    v.pause(0);
    v.span = 35.43773154160031;
    v.pan(-100, 0);
    const [oldest] = v.limits(), was = shows(v);
    expect(v.right).toBe(oldest + v.span);
    expect(v.right - v.span).toBeLessThan(oldest);
    v.restore(v.state(), 0);
    expect(v.startHeld(0)).toBe(false);
    expect(shows(v)).toEqual(was);
  });

  it('ends where it began when a zoom goes from the narrowest view to the widest and back about the same point', () => {
    fc.assert(
      fc.property(history, deviceTime, behind, fc.double({ min: 0.01, max: 0.99, noNaN: true }), (h, now, lag, anchor) => {
        const v = viewOn(now, { oldest: filled(now + lag, h), dropped: true, history: h });
        v.pause(0);
        for (let i = 0; i < 40; i++) v.zoom(2, 0.5, 0);
        const widest = shows(v);
        for (let i = 0; i < 40; i++) v.zoom(0.5, anchor, 0);
        const narrowest = shows(v);
        expect(narrowest.span).toBe(WINDOW_LIMITS[0]);
        // The moment at `anchor` across the widest view is at `anchor` across the narrowest: it stayed under the pointer.
        const moment = widest.right - (1 - anchor) * widest.span;
        expect(Math.abs(narrowest.right - (1 - anchor) * narrowest.span - moment)).toBeLessThanOrEqual(100 * step(now));
        for (let i = 0; i < 40; i++) v.zoom(2, anchor, 0);
        expect(shows(v)).toEqual(widest);
        for (let i = 0; i < 40; i++) v.zoom(0.5, anchor, 0);
        expect(v.span).toBe(WINDOW_LIMITS[0]);
        // The same moment, to what a double at that time resolves over the turns made.
        expect(Math.abs(v.right - narrowest.right)).toBeLessThanOrEqual(100 * step(now));
      }),
    );
  });
});

/** A span from the narrowest view to a day, spread evenly over its decades. */
const spanShown = fc.double({ min: Math.log10(WINDOW_LIMITS[0]), max: Math.log10(86400), noNaN: true }).map((e) => 10 ** e);
/** How many ticks a time axis asks for: a plot's width in px over 120, from a narrow plot to one across a 4K screen. */
const tickCount = fc.integer({ min: 240, max: 3840 }).map((w) => Math.max(2, w / 120));

describe('the time axis, from 2 ms to a day', () => {
  it('in a device’s own seconds, has ticks in order within the view, each with a label of its own that reads as its time', () => {
    fc.assert(
      fc.property(fc.double({ min: 0, max: 1e6, noNaN: true }), spanShown, tickCount, (x0, span, n) => {
        const axis = timeAxis('zero'), ticks = axis.ticks(x0, x0 + span, n), labels = ticks.map(axis.label);
        for (let i = 0; i < ticks.length; i++) {
          expect(ticks[i]).toBeGreaterThanOrEqual(x0);
          expect(ticks[i]).toBeLessThanOrEqual(x0 + span);
          if (i) expect(ticks[i]).toBeGreaterThan(ticks[i - 1]);
          expect(labels[i]).toMatch(/^\d+(\.\d+)? s$/);
        }
        expect(new Set(labels).size, labels.join(' | ')).toBe(labels.length);
        // A label is its tick's time, to well within the step between ticks.
        const between = ticks.length > 1 ? ticks[1] - ticks[0] : span;
        labels.forEach((l, i) => expect(Math.abs(parseFloat(l) - ticks[i])).toBeLessThan(between / 100));
        expect(ticks.length).toBeLessThanOrEqual(2 * n + 1);
        if (n >= 4) expect(ticks.length).toBeGreaterThanOrEqual(2);
      }),
      { examples: [[90000.1234, 0.002, 32], [0, 86400, 10], [86399.5, 1, 4]] },
    );
  });

  it('by the clock, has ticks in order within the view, each named by what changed at it, and two that read the same only with a coarser one between', () => {
    /** How coarse a label is: 0 for milliseconds, 1 for seconds, 2 for the hour and minute. */
    const rank = (label) => (label.startsWith('.') ? 0 : label.startsWith(':') ? 1 : 2);
    fc.assert(
      fc.property(fc.double({ min: 1.7e9, max: 1.8e9, noNaN: true }), spanShown, tickCount, (x0, span, n) => {
        const axis = timeAxis('unix'), ticks = axis.ticks(x0, x0 + span, n), labels = ticks.map(axis.label);
        const last = new Map();
        for (let i = 0; i < ticks.length; i++) {
          // To the millisecond the clock reads to.
          expect(ticks[i]).toBeGreaterThanOrEqual(x0 - 0.0005);
          expect(ticks[i]).toBeLessThanOrEqual(x0 + span + 0.0005);
          if (i) expect(ticks[i]).toBeGreaterThan(ticks[i - 1]);
          expect(labels[i]).toMatch(/^(\.\d{3}|:\d\d|\d\d:\d\d)$/);
          const d = new Date(Math.round(ticks[i] * 1000));
          expect(rank(labels[i]), `${labels[i]} at ${d.toISOString()}`).toBe(d.getMilliseconds() ? 0 : d.getSeconds() ? 1 : 2);
          const before = last.get(labels[i]);
          // The hour and minute of ticks a day apart read the same: the axis names no date.
          if (before !== undefined && rank(labels[i]) < 2) {
            expect(labels.slice(before + 1, i).some((l) => rank(l) > rank(labels[i])), labels.join(' | ')).toBe(true);
          }
          if (before !== undefined && rank(labels[i]) === 2) expect(ticks[i] - ticks[before]).toBeGreaterThanOrEqual(86400 - 3600);
          last.set(labels[i], i);
        }
        expect(ticks.length).toBeLessThanOrEqual(2 * n + 2);
        if (n >= 4 && span >= 0.01) expect(ticks.length).toBeGreaterThanOrEqual(2);
      }),
      { examples: [[1.7e9, 0.002, 32], [1.7e9 + 0.5, 1, 10], [1.7e9, 86400, 10], [1.7e9, 90, 10]] },
    );
  });

  it('names a span in the unit that reads shortest: seconds under two minutes, minutes under two hours, then hours', () => {
    fc.assert(
      fc.property(fc.double({ min: 1, max: 86400, noNaN: true }), fc.double({ min: 1, max: 86400, noNaN: true }), (a, b) => {
        const read = (text) => {
          const [, n, unit] = text.match(/^(\d{1,3}(?:\.\d)?) (s|min|h)$/) ?? [];
          expect(unit, text).toBeDefined();
          return [Number(n) * { s: 1, min: 60, h: 3600 }[unit], { s: 1, min: 60, h: 360 }[unit]];
        };
        const [back, to] = read(spanText(a));
        // It reads back as the span, to the last digit shown.
        expect(Math.abs(back - a)).toBeLessThanOrEqual(to / 2 + 1e-9);
        // A longer span never reads as a shorter one.
        if (a <= b) expect(read(spanText(b))[0]).toBeGreaterThanOrEqual(back);
      }),
    );
  });
});

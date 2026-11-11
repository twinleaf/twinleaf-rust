import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { aimed, EASE_MS, eased, LADDERS, ladderOf, NOTCH_PX, RATE_MS, REST, STILL, stepped, turn } from './wheelzoom.js';
import { FRAME_MS } from './clock.js';
import { WINDOW_LIMITS } from './window.js';

const span = fc.double({ min: 0.01, max: 3600, noNaN: true });
const ms = fc.integer({ min: 0, max: 20_000 }).map((k) => k / 20);
/** The frames an ease from `start` to `goal` takes to rest on it. */
const restFrames = (start, goal) => Math.max(0, Math.ceil((EASE_MS * Math.log(Math.abs(Math.log(start / goal)) / REST)) / FRAME_MS));

/**
 * Runs `turns` on a view showing `start` with spans [lo, hi] open to it,
 * each {dy, gap} its px and the frames before it, a frame at a time as the
 * plot does, then `tail` frames more: the span at each frame and the goal
 * it eased toward then, and each turn's goal.
 */
function run(start, turns, [lo, hi] = WINDOW_LIMITS.with(1, 86400), tail = 60) {
  let wheel = STILL, ease = null, shown = start, t = 0;
  const spans = [], toward = [], goals = [];
  const frame = () => {
    t += FRAME_MS;
    toward.push(ease?.goal ?? shown);
    if (ease) shown = eased(ease.start, ease.goal, t - ease.t0);
    if (shown === ease?.goal) ease = null;
    spans.push(shown);
  };
  for (const { dy, gap } of turns) {
    for (let i = 0; i < gap; i++) frame();
    const turned = turn(wheel, dy, t);
    wheel = turned.wheel;
    ease = aimed(ease, shown, t, turned, [lo, hi]);
    goals.push(ease.goal);
  }
  for (let i = 0; i < tail; i++) frame();
  return { spans, toward, goals, shown };
}

describe('the wheel’s ease', () => {
  it('moves only toward its goal, never past it, along the way the goal lies', () => {
    fc.assert(
      fc.property(span, span, ms, ms, (start, goal, a, b) => {
        const [t1, t2] = a <= b ? [a, b] : [b, a];
        const s1 = eased(start, goal, t1), s2 = eased(start, goal, t2), dir = Math.sign(goal - start);
        expect(Math.sign(s2 - s1) === dir || s2 === s1).toBe(true);
        expect((goal - s2) * dir).toBeGreaterThanOrEqual(0);
        expect((s1 - start) * dir).toBeGreaterThanOrEqual(-1e-9 * start);
      }),
    );
  });

  it(`rests exactly on its goal once within ${REST} of it, ${EASE_MS} ms closing all but 1/e of the way`, () => {
    fc.assert(
      fc.property(span, span, (start, goal) => {
        const frames = restFrames(start, goal);
        expect(eased(start, goal, frames * FRAME_MS)).toBe(goal);
        if (frames > 1) expect(eased(start, goal, (frames - 1) * FRAME_MS)).not.toBe(goal);
        if (Math.abs(Math.log(start / goal)) > 0.1) expect(Math.log(eased(start, goal, EASE_MS) / goal)).toBeCloseTo(Math.log(start / goal) / Math.E, 9);
      }),
    );
  });

  it('starts where it is: turned to a new goal mid-ease, the span moves on without a jump', () => {
    fc.assert(
      fc.property(fc.array(fc.record({ dy: fc.constantFrom(-400, -120, -100, -30, 30, 100, 120, 400), gap: fc.nat(12) }), { minLength: 1, maxLength: 12 }), span, (turns, start) => {
        const { spans, toward } = run(start, turns);
        // Each frame moves from the span the last showed, by no more than a frame's ease toward the goal of the moment.
        const share = 1 - Math.exp(-FRAME_MS / EASE_MS);
        spans.forEach((s, i) => {
          const was = i ? spans[i - 1] : start;
          expect(Math.abs(Math.log(s / was))).toBeLessThanOrEqual(Math.abs(Math.log(toward[i] / was)) * share + REST + 1e-12);
        });
      }),
    );
  });

  it('answers a turn the other way on the next frame, from the span it shows', () => {
    fc.assert(
      fc.property(fc.constantFrom(1, -1), fc.integer({ min: 1, max: 6 }), fc.integer({ min: 1, max: 4 }), span, (dir, notches, frames, start) => {
        // Turning one way in a burst, then the other way mid-ease.
        const turns = [...Array.from({ length: notches }, () => ({ dy: dir * 100, gap: 0 })), { dy: -dir * 100, gap: frames }];
        const { spans, goals } = run(start, turns, [0.002, 86400], 1);
        const before = spans[frames - 1], after = spans[frames];
        if (goals[0] === start) return;
        expect(Math.sign(goals.at(-1) - before), 'the goal lies the other way from what shows').toBe(-dir);
        expect(Math.sign(after - before), 'and the next frame moves that way').toBe(-dir);
      }),
    );
  });

  it('never takes the span outside the spans the view can zoom to, and at their end rests there', () => {
    fc.assert(
      fc.property(fc.array(fc.record({ dy: fc.constantFrom(-400, -100, 100, 400), gap: fc.nat(6) }), { minLength: 1, maxLength: 20 }), fc.double({ min: 0.5, max: 50, noNaN: true }), fc.double({ min: 0, max: 1, noNaN: true }), (turns, hi, k) => {
        const lo = 0.01, start = lo * (hi / lo) ** k;
        const { spans, shown, goals } = run(start, turns, [lo, hi]);
        for (const s of spans) expect(s >= lo && s <= hi, `${s} within [${lo}, ${hi}]`).toBe(true);
        expect(shown, 'rests on its last goal').toBe(goals.at(-1));
      }),
    );
  });
});

describe('the wheel’s steps', () => {
  it('go by how fast it turns: alone a fine span, two or three in quick succession a middling one, more a clock span', () => {
    let w = STILL;
    const at = (t) => (w = turn(w, 100, t).wheel);
    expect(turn(STILL, 100, 0).ladder).toBe(LADDERS.fine);
    at(0);
    expect(turn(w, 100, RATE_MS).ladder, 'a turn after the wheel was still').toBe(LADDERS.fine);
    expect(turn(w, 100, RATE_MS - 1).ladder).toBe(LADDERS.middle);
    at(50);
    expect(turn(w, 100, 100).ladder, 'the third').toBe(LADDERS.middle);
    at(100);
    expect(turn(w, 100, 150).ladder, 'the fourth').toBe(LADDERS.clock);
    expect(ladderOf(1)).toBe(LADDERS.fine);
    expect(stepped(10, 1, LADDERS.fine)).toBe(11);
    expect(stepped(10, 1, LADDERS.middle)).toBe(12);
    expect(stepped(10, 1, LADDERS.clock)).toBe(15);
    expect(stepped(10, -2, LADDERS.clock)).toBe(2);
    expect(stepped(231, 1, LADDERS.clock), 'from between spans, the next one up').toBe(300);
    expect(stepped(231, -1, LADDERS.clock), 'and the next one down').toBe(120);
  });

  it('land on a span of the ladder the way the wheel turned, never back past where it started', () => {
    fc.assert(
      fc.property(span, fc.integer({ min: -8, max: 8 }), fc.constantFrom(...Object.keys(LADDERS)), (s, steps, name) => {
        const ladder = LADDERS[name], to = stepped(s, steps, ladder);
        if (steps === 0) expect(to).toBe(s);
        else {
          expect(to === s || Math.sign(to - s) === Math.sign(steps)).toBe(true);
          if (to !== s) expect(ladder).toContain(to);
        }
      }),
    );
  });

  it('add up a trackpad’s small turns, a step for each notch’s worth, and start afresh when it turns the other way', () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: 1, max: 30 }), { minLength: 1, maxLength: 60 }), (dys) => {
        let w = STILL, steps = 0;
        dys.forEach((dy, i) => {
          const r = turn(w, dy, i * 16);
          w = r.wheel;
          steps += r.steps;
        });
        const sum = dys.reduce((a, b) => a + b, 0);
        expect(steps).toBe(Math.trunc(sum / NOTCH_PX));
        expect(turn(w, -1, dys.length * 16)).toMatchObject({ reversed: true, steps: 0 });
      }),
    );
  });

  it('span from the narrowest a view shows to a day, each ladder ascending', () => {
    for (const ladder of Object.values(LADDERS)) {
      expect(ladder[0]).toBe(WINDOW_LIMITS[0]);
      expect(ladder.at(-1)).toBeLessThanOrEqual(86400);
      for (let i = 1; i < ladder.length; i++) expect(ladder[i]).toBeGreaterThan(ladder[i - 1]);
    }
    expect(LADDERS.clock.at(-1)).toBe(86400);
  });
});

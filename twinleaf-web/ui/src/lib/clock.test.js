// The clock in virtual mode: time moves only when the test moves it, and
// timers and frames run in the order the clock states, against a model that
// sorts them by due time and then by the order they were made.
import { afterEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import * as clock from './clock.js';

let v = null;
afterEach(() => v?.end());

const act = fc.oneof(
  { weight: 3, arbitrary: fc.record({ t: fc.constant('after'), ms: fc.integer({ min: 0, max: 50 }) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('every'), ms: fc.integer({ min: 1, max: 30 }) }) },
  { weight: 1, arbitrary: fc.record({ t: fc.constant('cancel'), k: fc.nat() }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('advance'), ms: fc.integer({ min: 0, max: 60 }) }) },
);

describe('the virtual clock', () => {
  it('stands still until it is advanced, and runs each timer at its time: the earliest due first, then the one made first', () => {
    const seen = { sameTime: 0, repeated: 0, cancelled: 0 };
    fc.assert(
      fc.property(fc.array(act, { minLength: 1, maxLength: 30 }), (acts) => {
        v = clock.virtual({ start: 100 });
        // The model: each timer {id, due, every, made, off}, and the time.
        let at = 100, timers = [], ran = [];
        const handles = [];
        for (const a of acts) {
          if (a.t === 'after' || a.t === 'every') {
            const id = timers.length;
            timers.push({ id, due: at + a.ms, every: a.t === 'every' ? a.ms : 0, off: false });
            handles.push(clock[a.t](a.ms, () => ran.push([id, clock.now()])));
          } else if (a.t === 'cancel' && timers.length) {
            const k = a.k % timers.length;
            timers[k].off = true;
            clock.cancel(handles[k]);
            seen.cancelled++;
          } else if (a.t === 'advance') {
            const until = at + a.ms, want = [];
            for (;;) {
              const due = timers.filter((m) => !m.off && m.due <= until).sort((x, y) => x.due - y.due || x.id - y.id)[0];
              if (!due) break;
              if (want.some(([, when]) => when === due.due)) seen.sameTime++;
              if (want.some(([id]) => id === due.id)) seen.repeated++;
              want.push([due.id, due.due]);
              if (due.every) due.due += due.every;
              else due.off = true;
            }
            ran = [];
            v.advance(a.ms);
            expect(ran).toEqual(want);
            at = until;
          }
          expect(clock.now()).toBe(at);
        }
        v.end();
      }),
      { examples: [[[{ t: 'after', ms: 5 }, { t: 'after', ms: 5 }, { t: 'every', ms: 2 }, { t: 'cancel', k: 0 }, { t: 'advance', ms: 6 }]]] },
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  });

  it('runs a timer made by a timer in the same advance when it comes due within it', () => {
    v = clock.virtual();
    const ran = [];
    clock.after(10, () => (ran.push('a'), clock.after(5, () => ran.push('b')), clock.after(50, () => ran.push('late'))));
    clock.after(15, () => ran.push('c'));
    v.advance(20);
    expect(ran).toEqual(['a', 'c', 'b']);
  });

  it('runs a frame’s callbacks after the timers due by it, in the order asked, the ones they ask for a frame later', () => {
    v = clock.virtual({ start: 0 });
    const ran = [];
    clock.frame((t) => (ran.push(['first', t]), clock.frame(() => ran.push(['next frame', clock.now()]))));
    const off = clock.frame(() => ran.push(['called off']));
    clock.frame(() => ran.push(['third']));
    clock.after(1, () => ran.push(['timer', clock.now()]));
    clock.frame(() => clock.cancel(off));
    clock.cancel(off);
    v.advance(1000);
    expect(ran, 'no frame without nextFrame').toEqual([['timer', 1]]);
    v.nextFrame();
    expect(ran.slice(1)).toEqual([['first', 1000 + clock.FRAME_MS], ['third']]);
    v.frames(2);
    expect(ran.slice(3)).toEqual([['next frame', 1000 + 2 * clock.FRAME_MS]]);
  });

  it('does not run a frame callback another of the same frame cancelled', () => {
    v = clock.virtual();
    const ran = [];
    let second;
    clock.frame(() => clock.cancel(second));
    second = clock.frame(() => ran.push('second'));
    v.nextFrame();
    expect(ran).toEqual([]);
  });

  it('runs every one-off timer by runTimers, those made on the way too, and stops with repeating ones left', () => {
    v = clock.virtual({ start: 0 });
    const ran = [];
    clock.every(40, () => ran.push(`tick ${clock.now()}`));
    clock.after(100, () => (ran.push('a'), clock.after(30, () => ran.push('b'))));
    v.runTimers();
    expect(ran).toEqual(['tick 40', 'tick 80', 'a', 'tick 120', 'b']);
    expect(clock.now()).toBe(130);
  });

  it('tells the time of day from where it was put, moving with the time', () => {
    v = clock.virtual({ start: 5, wallStart: 1_000_000 });
    v.advance(250);
    expect([clock.now(), clock.wall()]).toEqual([255, 1_000_250]);
  });

  it('gives the browser’s clock back when it ends, and what it held never runs', async () => {
    v = clock.virtual();
    let ran = false;
    clock.after(0, () => (ran = true));
    v.end();
    expect(Math.abs(clock.now() - performance.now())).toBeLessThan(1000);
    await new Promise((done) => clock.after(5, done));
    v.advance(100);
    expect(ran).toBe(false);
  });
});

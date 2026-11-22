// The page's one clock: the only module that reads the time, asks for a
// frame or sets a timer (src/architecture.test.js). Everything else in the
// page reads the time here, where it is first needed, and passes it down.
//
// On the real clock, time is the browser's and frames and timers are its
// own. A test puts the clock in virtual mode (`virtual`): time stands still
// until the test moves it, and no frame or timer of the page runs but
// during a call of the test's.
//
// What the clock does not reach: CSS transitions and animations, the
// browser's own observers and events, and the timers inside libraries.

/** The ms a virtual frame takes: a sixtieth of a second. */
export const FRAME_MS = 1000 / 60;

const REAL = {
  now: () => performance.now(),
  wall: () => Date.now(),
  frame(fn) {
    const id = requestAnimationFrame(fn);
    return () => cancelAnimationFrame(id);
  },
  after(ms, fn) {
    const id = setTimeout(fn, ms);
    return () => clearTimeout(id);
  },
  every(ms, fn) {
    const id = setInterval(fn, ms);
    return () => clearInterval(id);
  },
};
let clock = REAL;

/** The page's time, in ms: monotonic, from an origin of its own. */
export const now = () => clock.now();
/** The time of day, in ms since 1970. */
export const wall = () => clock.wall();
/** Runs `fn(now)` once, before the next frame is drawn. Returns what `cancel` takes. */
export const frame = (fn) => clock.frame(fn);
/** Runs `fn` once, `ms` from now. Returns what `cancel` takes. */
export const after = (ms, fn) => clock.after(ms, fn);
/** Runs `fn` every `ms`, first `ms` from now. Returns what `cancel` takes. */
export const every = (ms, fn) => clock.every(ms, fn);
/** Calls off what `frame`, `after` or `every` returned; nothing, for null or one that has run. */
export const cancel = (handle) => void handle?.();

/**
 * Puts the clock in virtual mode, at time `start` ms and time of day
 * `wallStart`, and returns what moves it. Until `end()` puts the real clock
 * back, time moves only in these calls, and frames and timers run only
 * inside them, each seeing the time it was due:
 * - `advance(ms)` moves the time on by `ms`, running each timer that comes
 *   due on the way: the earliest due first, and of those due at the same
 *   time, the one made first. A timer made by one that ran runs in the same
 *   call when it comes due within it. An `every` timer runs at each of its
 *   times, in its place among the others by the order it was made. No frame
 *   runs.
 * - `nextFrame()` is `advance(FRAME_MS)`, then every frame callback asked
 *   for before it, in the order asked, each given the time. A callback
 *   asked for by one of them waits for the frame after; one cancelled by
 *   one of them does not run.
 * - `frames(n)` is `nextFrame()` `n` times.
 * - `runTimers()` advances to each `after` timer in turn until none is
 *   left, those made on the way among them; `every` timers run on the way
 *   and do not keep it going.
 * A timer or frame asked for on the real clock is the browser's still, and
 * what a virtual clock still held when it ended, or when another began,
 * never runs.
 */
export function virtual({ start = 10_000, wallStart = Date.UTC(2026, 0, 1) } = {}) {
  let t = start, made = 0, timers = [], asked = [];
  /** Adds `entry` to `list` and returns what calls it off. */
  const hold = (list, entry) => {
    list.push(entry);
    return () => {
      entry.fn = null;
    };
  };
  /** The timer to run next of those due by `until`, if any. */
  const next = (until, among = timers) => {
    let first = null;
    for (const e of among) if (e.fn && e.due <= until && (!first || e.due < first.due || (e.due === first.due && e.made < first.made))) first = e;
    return first;
  };
  const advance = (ms) => {
    const until = t + ms;
    for (let e; (e = next(until)); ) {
      t = e.due;
      const fn = e.fn;
      if (e.every) e.due += e.every;
      else e.fn = null;
      fn();
    }
    timers = timers.filter((e) => e.fn);
    t = until;
  };
  const nextFrame = () => {
    advance(FRAME_MS);
    const run = asked;
    asked = [];
    for (const e of run) e.fn?.(t);
  };
  const mine = {
    now: () => t,
    wall: () => wallStart + (t - start),
    frame: (fn) => hold(asked, { fn }),
    after: (ms, fn) => hold(timers, { fn, due: t + Math.max(0, ms || 0), made: made++ }),
    every: (ms, fn) => hold(timers, { fn, due: t + Math.max(1, ms || 0), made: made++, every: Math.max(1, ms || 0) }),
  };
  clock = mine;
  return {
    advance,
    nextFrame,
    frames(n) {
      for (let i = 0; i < n; i++) nextFrame();
    },
    runTimers() {
      for (let e; (e = next(Infinity, timers.filter((x) => !x.every))); ) advance(e.due - t);
    },
    end() {
      timers = [];
      asked = [];
      if (clock === mine) clock = REAL;
    },
  };
}

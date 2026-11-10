// The page's one clock: the only module that reads the time, asks for a
// frame or sets a timer (src/architecture.test.js). Everything else in the
// page reads the time here, where it is first needed, and passes it down.
//
// What the clock does not reach: CSS transitions and animations, the
// browser's own observers and events, and the timers inside libraries.

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
const clock = REAL;

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

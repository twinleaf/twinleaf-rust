// The browser command the tests call through `commands` from
// 'vitest/browser'. It runs in node, on the Playwright page, so the page
// gets trusted input as a person's would make it.

/**
 * Moves the real mouse through `steps`: ['move', x, y], x and y in the test
 * frame's CSS px; ['down'] and ['up'], or ['down', 'right'] and
 * ['up', 'right'] for the right button; ['dblclick', x, y]; ['hold', key]
 * and ['let go', key] for a keyboard key held meanwhile, as Playwright
 * names it; or ['wait', ms], of real time, for what a library times: the
 * page's own clock does not move in it (page.js). The wheel is not here:
 * a turn is dispatched (page.js turnWheel). ['release'] lets go of every
 * button and key still held, as a test that failed midway leaves them. A
 * step the browser has not taken within STEP_MS fails, naming it.
 */
export async function mouse(ctx, steps) {
  const frame = await ctx.frame();
  const el = await frame.frameElement();
  const box = await el.boundingBox();
  const inner = await frame.evaluate(() => window.innerWidth);
  const scale = box.width / inner;
  const at = (x, y) => [box.x + x * scale, box.y + y * scale];
  const { buttons, keys } = heldOn(ctx.page);
  for (const step of steps) {
    const [type, x, y] = step;
    let take;
    if (type === 'move') take = () => ctx.page.mouse.move(...at(x, y));
    else if (type === 'down') take = () => (buttons.add(x ?? 'left'), ctx.page.mouse.down({ button: x ?? 'left' }));
    else if (type === 'up') take = () => (buttons.delete(x ?? 'left'), ctx.page.mouse.up({ button: x ?? 'left' }));
    else if (type === 'dblclick') take = () => ctx.page.mouse.dblclick(...at(x, y));
    else if (type === 'hold') take = () => (keys.add(x), ctx.page.keyboard.down(x));
    else if (type === 'let go') take = () => (keys.delete(x), ctx.page.keyboard.up(x));
    else if (type === 'wait') take = () => new Promise((done) => setTimeout(done, x));
    else if (type === 'release') take = async () => {
      for (const b of [...buttons]) {
        buttons.delete(b);
        await ctx.page.mouse.up({ button: b });
      }
      for (const k of [...keys]) {
        keys.delete(k);
        await ctx.page.keyboard.up(k);
      }
    };
    await within(take(), STEP_MS + (type === 'wait' ? x : 0), `the mouse step ${JSON.stringify(step)}`);
  }
}

/** The mouse buttons and keyboard keys `mouse` holds down on each Playwright page. */
const held = new WeakMap();
const heldOn = (page) => held.get(page) ?? held.set(page, { buttons: new Set(), keys: new Set() }).get(page);

/** How long, in ms, a step of the mouse may take the browser. */
const STEP_MS = 10_000;

/** What `promise` gives, or a failure naming `what` once `ms` have passed without it. */
function within(promise, ms, what) {
  let late;
  const timeout = new Promise((_, fail) => (late = setTimeout(() => fail(new Error(`${what} not taken in ${ms} ms`)), ms)));
  return Promise.race([promise, timeout]).finally(() => clearTimeout(late));
}

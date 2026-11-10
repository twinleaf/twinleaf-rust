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
 * a turn is dispatched (page.js turnWheel).
 */
export async function mouse(ctx, steps) {
  const frame = await ctx.frame();
  const el = await frame.frameElement();
  const box = await el.boundingBox();
  const inner = await frame.evaluate(() => window.innerWidth);
  const scale = box.width / inner;
  const at = (x, y) => [box.x + x * scale, box.y + y * scale];
  for (const [type, x, y] of steps) {
    if (type === 'move') await ctx.page.mouse.move(...at(x, y));
    else if (type === 'down') await ctx.page.mouse.down({ button: x ?? 'left' });
    else if (type === 'up') await ctx.page.mouse.up({ button: x ?? 'left' });
    else if (type === 'dblclick') await ctx.page.mouse.dblclick(...at(x, y));
    else if (type === 'hold') await ctx.page.keyboard.down(x);
    else if (type === 'let go') await ctx.page.keyboard.up(x);
    else if (type === 'wait') await new Promise((done) => setTimeout(done, x));
  }
}

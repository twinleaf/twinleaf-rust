// Browser commands the tests call through `commands` from
// '@vitest/browser/context'. They run in node, on the Playwright page, so the
// page gets trusted input as a person's would make it.

/**
 * Moves the real mouse through `steps`, [type, x, y] with type 'move',
 * 'down' or 'up' and x, y in the test frame's CSS px.
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
    else if (type === 'down') await ctx.page.mouse.down();
    else if (type === 'up') await ctx.page.mouse.up();
  }
}

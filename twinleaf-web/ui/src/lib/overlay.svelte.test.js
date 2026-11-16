// The page's one popup layer (bug class 9, overlay.svelte.js): for any tree
// of layers, each opened from the page or from inside another, and any run
// of them opening and closing, those open are a chain, each opened from the
// one before; opening one closes exactly the open layers it was not opened
// from; and while one is open, a tooltip shows only inside an open layer
// from inside which none is open.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { Layers } from './overlay.svelte.js';

const within = (l, outer) => {
  for (; l; l = l.parent) if (l === outer) return true;
  return false;
};

describe('the popup layers', () => {
  it('are a chain, and opening one closes exactly the others it was not opened from', () => {
    let opened = 0;
    fc.assert(
      fc.property(fc.array(fc.nat(), { minLength: 1, maxLength: 8 }), fc.array(fc.tuple(fc.boolean(), fc.nat()), { maxLength: 30 }), (parents, steps) => {
        const layers = new Layers();
        // Layer i was opened from an earlier one, or from the page.
        const all = [];
        parents.forEach((p, i) => {
          const layer = { parent: i && p % (i + 1) < i ? all[p % (i + 1)] : null, keepFocus: false, close: () => layers.drop(layer), closed: 0 };
          const close = layer.close;
          layer.close = () => (layer.closed++, close());
          all.push(layer);
        });
        for (const [open, k] of steps) {
          const layer = all[k % all.length], before = [...layers.shown];
          // A layer opens from inside one that is open, as its trigger is in that one's content.
          if (open && layer.parent && !before.includes(layer.parent)) continue;
          all.forEach((l) => (l.closed = 0));
          if (open) layers.raise(layer);
          else layers.drop(layer);
          const shown = layers.shown;
          shown.forEach((l, i) => i && expect(l.parent).toBe(shown[i - 1]));
          if (shown.length) expect(shown[0].parent).toBe(null);
          if (open && before.includes(layer)) expect(shown, 'a layer open already opens nothing').toEqual(before);
          else if (open) {
            expect(shown.at(-1)).toBe(layer);
            for (const l of before) expect(l.closed, 'closed once, if it was not opened from').toBe(l === layer || within(layer, l) ? 0 : 1);
            opened++;
          } else expect(shown.some((l) => within(l, layer))).toBe(false);
          // Every tooltip shows while no layer is open; while one is, only those inside an open layer from inside which none is open.
          const shows = (l) => !shown.length || (l !== null && shown.includes(l) && !all.some((m) => m.parent === l && shown.includes(m)));
          for (const l of [null, ...all]) expect(layers.hides(l), `a tooltip in ${l ? all.indexOf(l) : 'the page'}`).toBe(!shows(l));
        }
      }),
    );
    expect(opened).toBeGreaterThan(50);
  });
});

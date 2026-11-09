// The page's one popup layer: a menu, a dropdown, a popover, a list or a
// dialog, with what opens from inside it, as a submenu or a list in a
// popover does. Every one of them opens through here, as the components/ui
// roots do: one opening closes every other open layer it was not opened
// from, so at most one shows at a time, however it opened, by a press, the
// keys or the page. While one is open, the only tooltips that show are those
// of controls inside it.
//
// A layer closed by a press outside it, or by another opening, leaves focus
// where that press or the other layer put it, rather than handing it back
// to its own trigger, so a press on another menu's button opens that menu at
// once. A layer that opened in another's place hands focus back, as it
// closes, to what had it before that one opened, as the one it replaced
// would have: the plot or button the keys or the pointer were on, not a
// menu item gone with its menu.
//
// Which layer a layer was opened from is Svelte's context: a root reads the
// layer whose content it is inside, which the content says for what it holds.

import { getContext, onDestroy, setContext, untrack } from 'svelte';

const ROOT = Symbol('overlay root');
const INSIDE = Symbol('overlay inside');
/** What a layer's content is, on the page. */
const CONTENT = '[data-slot$=-content]';

/** The element focused last outside every layer. */
let outside = null;
if (typeof document !== 'undefined') document.addEventListener('focusin', (e) => e.target instanceof Element && !e.target.closest(CONTENT) && (outside = e.target), true);

/** Whether layer `layer` is `outer` or was opened from inside it. */
function within(layer, outer) {
  for (let l = layer; l; l = l.parent) if (l === outer) return true;
  return false;
}

/**
 * The layers open, each a {parent, close, keepFocus, returnTo}: `parent`
 * the layer it was opened from, or null for the page; `close()` closing
 * it; whether it leaves focus where it is as it closes; and, for a layer
 * that opened in others' place, where it hands focus back. They are a
 * chain, each opened from the one before it.
 */
export class Layers {
  shown = $state.raw([]);

  /**
   * Notes that `layer` is open: every other open layer it was not opened
   * from closes first, and it hands focus back, as it closes, to `before`,
   * what had focus outside every layer before they opened.
   */
  raise(layer, before = null) {
    if (this.shown.includes(layer)) return;
    for (const other of this.shown) {
      if (within(layer, other)) continue;
      other.keepFocus = true;
      other.close();
      layer.returnTo = other.returnTo ?? before;
    }
    this.shown = [...this.shown.filter((l) => within(layer, l)), layer];
  }

  /** Notes that `layer` has closed, and with it what was opened from inside it. */
  drop(layer) {
    if (this.shown.some((l) => within(l, layer))) this.shown = this.shown.filter((l) => !within(l, layer));
  }

  /** Whether a tooltip inside layer `inside`, or on the page with null, is hidden now: some layer is open, and it is not the innermost. */
  hides(inside) {
    return this.shown.length > 0 && this.shown.at(-1) !== inside;
  }
}

/** The page's layers. */
const layers = new Layers();

/**
 * Makes the calling root component a layer, `close()` closing it, and
 * returns `shown(open)`, which it calls with whether it is open.
 */
export function overlayRoot(close) {
  const layer = { parent: getContext(INSIDE) ?? null, close, keepFocus: false, returnTo: null };
  setContext(ROOT, layer);
  onDestroy(() => untrack(() => layers.drop(layer)));
  return (open) => untrack(() => (open ? layers.raise(layer, outside) : layers.drop(layer)));
}

/**
 * Marks the calling content component as its root's layer, for what opens
 * inside it. A menu's, a popover's or a list's content calls `outside()`
 * as a press outside closes it; and every content calls `closing(e)` from
 * its close autofocus, which keeps focus where that press or another layer
 * put it, or hands it to where a layer that opened in another's place
 * gives it back.
 */
export function overlayContent() {
  const layer = getContext(ROOT) ?? null;
  setContext(INSIDE, layer);
  return {
    outside() {
      if (layer) layer.keepFocus = true;
    },
    closing(e) {
      if (!layer) return;
      const to = layer.returnTo;
      if (layer.keepFocus || to?.isConnected) e.preventDefault();
      if (!layer.keepFocus && to?.isConnected) to.focus({ preventScroll: true });
      layer.keepFocus = false;
      layer.returnTo = null;
    },
  };
}

/** For a tooltip: a function that says whether an open layer it is not inside hides it now. */
export function tooltipHidden() {
  const inside = getContext(INSIDE) ?? null;
  return () => layers.hides(inside);
}

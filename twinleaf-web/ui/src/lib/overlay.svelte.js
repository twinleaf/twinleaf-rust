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

/** The menus' elements on the page, each given by its content as it mounts. */
const menus = new Set();
/** How long a menu's copy stays if its fade never ends, in ms. */
const FADE_MS = 400;
/**
 * A press on `target`, seen before anything it does. A menu shows what its
 * commands act on, live, while it is open; the press that closes it can
 * change that, and the menu would show the new words as it fades. So each
 * menu the press is outside of is hidden, and a copy of it as it reads now
 * fades in its place. The menu itself is untouched: opened again before it
 * has gone, or left open by the press, it shows again.
 */
export function freezeMenus(target) {
  // A press in a menu or a submenu is no press outside.
  if (target.closest?.('[data-slot$=-menu-content], [data-slot$=-menu-sub-content]')) return;
  for (const el of menus) {
    if (!el.isConnected) menus.delete(el);
    else if (el.dataset.state === 'open' && el.dataset.frozen === undefined) fadeCopy(el);
  }
}
function fadeCopy(el) {
  const copy = el.cloneNode(true);
  copy.removeAttribute('id');
  copy.inert = true;
  Object.assign(copy.style, { position: 'absolute', top: '0', left: '0', width: `${el.offsetWidth}px`, pointerEvents: 'none' });
  copy.dataset.state = 'closed';
  // Hidden by an attribute of its own (plot.css): the menu's style is its library's to write.
  el.dataset.frozen = '';
  el.after(copy);
  // The copy goes when its fade ends. The menu stays hidden until it opens
  // again, if it does before it has left the page.
  const copyGone = () => (clearTimeout(timer), copy.remove());
  let closed = false;
  const watch = new MutationObserver(() => {
    const again = closed && el.dataset.state === 'open';
    if (el.dataset.state === 'closed') closed = true;
    if (el.isConnected && !again) return;
    watch.disconnect();
    copyGone();
    delete el.dataset.frozen;
  });
  watch.observe(el, { attributes: true, attributeFilter: ['data-state'] });
  watch.observe(el.parentNode, { childList: true });
  copy.addEventListener('animationend', copyGone, { once: true });
  // A press that closed nothing leaves the menu open: it shows again.
  const timer = setTimeout(() => {
    copyGone();
    if (closed || el.dataset.state !== 'open') return;
    watch.disconnect();
    delete el.dataset.frozen;
  }, FADE_MS);
}

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
    /** A menu's content gives its element, for `freezeMenus`. */
    element(el) {
      menus.add(el);
      return () => menus.delete(el);
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

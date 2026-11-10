<script>
  // The page's one focus ring: one look, drawn here and nowhere else, around
  // what the keys act on (`ringTarget`).
  // - What has focus, after the keyboard moved focus to it, and a field
  //   however focus came, as keys.js tells them apart. A click that only
  //   gives focus, as one that deselects the plots, shows none, nor does a
  //   key pressed after it, Space pausing. The ring goes around what looks
  //   like the field: an input with an icon or a unit beside it is one box.
  // - The current item of a list on the page that keeps focus on itself, as
  //   the Signals list, a plot's signals and a table of properties do: the
  //   item the list names as its active descendant, once the keys are in use
  //   there.
  // - While a menu is open, the item it was opened for, which its list marks
  //   `data-menu-for`.
  // A menu's or a popup list's own items show as highlighted instead, as
  // does the list a filter field (the command palette's, the add-signal
  // menu's) chooses from; the field shows no ring.
  //
  // The ring is one element, in the layer that holds its target: above the
  // page, under every popover, menu and tooltip, or, for focus in a popover
  // or dialog, inside it, so whatever opens over that covers the ring as it
  // covers the field. It stands outside its target, so no box clips it but
  // one that scrolls the target out of view.
  import { onMount } from 'svelte';
  import { focusedByKeyboard } from './keys.js';
  import * as clock from './clock.js';

  /** A menu, whose items are highlighted. */
  const MENU = '[role=menu]';
  /** What opens over the page, whose lists highlight their items. */
  const POPUP = '[data-slot$=-content]';
  // A selected plot's border already marks it, so a ring around it would
  // only thicken that one plot's border beside the other selected plots'.
  // A plot's drawing takes focus only to open its menu, as a click there
  // does, Shift-click too, which the browser counts as from the keys: the
  // plot is the stop the keys reach, so the drawing shows no ring of its own.
  const NO_RING = '[role=dialog], [role=alertdialog], [data-slot$=-content], [role=option], [role=combobox][aria-expanded=true], input[role=combobox], [data-slot=command-input], .plot-card.selected, .plot-body';
  /** What shows a ring however focus came to it: what is typed into. */
  const FIELD = 'input, textarea, select, [contenteditable]';
  /** What a ring goes around in place of the element with focus inside it. */
  const BOX = '[data-slot=input-group], .window-field';
  /** The layers over the page that hold their own ring: a popover's or a dialog's content. */
  const LAYER = '[data-slot=popover-content], [data-slot=dialog-content], [data-slot=alert-dialog-content]';
  /** How far the ring stands outside what it surrounds, and its width, in px. */
  const GAP = 1, WIDTH = 2;

  /** The element the ring goes around now, or null for none. */
  function ringTarget() {
    const el = document.activeElement;
    if (!el || el === document.body) return null;
    if (el.closest(MENU)) return document.querySelector('[data-menu-for]');
    if (el.matches(NO_RING)) return null;
    const current = el.getAttribute('aria-activedescendant');
    if (current !== null) return el.matches(':focus-visible') && !el.closest(POPUP) ? document.getElementById(current) : null;
    if (!el.matches(FIELD) && !focusedByKeyboard(el)) return null;
    return el.closest(BOX) ?? el;
  }

  /**
   * Box `box` {l, t, r, b} around `el`, cut to what shows of it: to the
   * window, and to each box around `el` that scrolls or clips, taken `out`
   * px larger, as the ring stands outside `el`. Null when `el` is scrolled
   * out of one.
   */
  function shown(el, box, out) {
    const r = el.getBoundingClientRect();
    let { l, t, r: right, b } = box;
    [l, t, right, b] = [Math.max(0, l), Math.max(0, t), Math.min(innerWidth, right), Math.min(innerHeight, b)];
    for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) {
      const cs = getComputedStyle(a);
      if (cs.overflowX === 'visible' && cs.overflowY === 'visible') continue;
      const c = a.getBoundingClientRect();
      if (r.right <= c.left || r.left >= c.right || r.bottom <= c.top || r.top >= c.bottom) return null;
      [l, t, right, b] = [Math.max(l, c.left - out), Math.max(t, c.top - out), Math.min(right, c.right + out), Math.min(b, c.bottom + out)];
    }
    return right > l && b > t ? { l, t, r: right, b } : null;
  }

  /** The ring's box around its target, and the layer holding it, or null for none. */
  function ringBox() {
    const target = ringTarget();
    if (!target) return null;
    const r = target.getBoundingClientRect();
    if (!r.width && !r.height) return null;
    const out = GAP + WIDTH;
    const box = shown(target, { l: r.left - out, t: r.top - out, r: r.right + out, b: r.bottom + out }, out);
    if (!box) return null;
    const radius = (parseFloat(getComputedStyle(target).borderTopLeftRadius) || 0) + out;
    return { x: box.l, y: box.t, w: box.r - box.l, h: box.b - box.t, radius, layer: target.closest(LAYER) };
  }

  // The target moves with the layout, so the ring follows it each frame,
  // and at once when focus moves. The ring is an element of its own, moved
  // into the layer it belongs to.
  onMount(() => {
    const ring = document.createElement('div');
    ring.className = 'focus-ring';
    ring.setAttribute('aria-hidden', 'true');
    ring.style.borderWidth = `${WIDTH}px`;
    const update = () => {
      const box = ringBox();
      if (!box) return ring.remove();
      const parent = box.layer ?? document.body;
      if (ring.parentNode !== parent) parent.appendChild(ring);
      ring.classList.toggle('in-layer', !!box.layer);
      // In a layer the ring is placed in its box, which a transform may move.
      let ox = 0, oy = 0;
      const p = box.layer && ring.offsetParent;
      if (p) {
        const r = p.getBoundingClientRect();
        [ox, oy] = [r.left + p.clientLeft - p.scrollLeft, r.top + p.clientTop - p.scrollTop];
      }
      const style = { left: `${box.x - ox}px`, top: `${box.y - oy}px`, width: `${box.w}px`, height: `${box.h}px`, borderRadius: `${box.radius}px` };
      for (const [k, v] of Object.entries(style)) if (ring.style[k] !== v) ring.style[k] = v;
    };
    let frame;
    const follow = () => {
      update();
      frame = clock.frame(follow);
    };
    follow();
    const events = ['focusin', 'focusout', 'pointerdown'];
    for (const type of events) window.addEventListener(type, update);
    return () => {
      clock.cancel(frame);
      for (const type of events) window.removeEventListener(type, update);
      ring.remove();
    };
  });
</script>

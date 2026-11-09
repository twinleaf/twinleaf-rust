<script>
  // The page's one focus ring, around what has focus, in the layer that
  // holds it: above the page, under every popover, menu and tooltip, or,
  // for focus in a popover or dialog, inside it, so whatever opens over
  // that covers the ring as it covers the field. Every part of the page
  // shows the same ring, and no scrolling box clips it. It shows after
  // the keyboard moved focus, and in a field however focus came, as keys.js
  // tells them apart. A click that only gives focus, as one that deselects
  // the plots, shows none, nor does a key pressed after it, Space pausing. It goes around what looks like the
  // field: an input with an icon beside it is one box. A menu's or a list's
  // own items show as highlighted instead, as does an item the arrows move
  // to inside a node that keeps its own, or in the list a filter field
  // (a combobox, as the command palette's) chooses from.
  import { onMount } from 'svelte';
  import { focusedByKeyboard } from './keys.js';

  // A selected plot's border already marks it, so a ring around it would
  // only thicken that one plot's border beside the other selected plots'.
  // A plot's drawing takes focus only to open its menu, as a click there
  // does, Shift-click too, which the browser counts as from the keys: the
  // plot is the stop the keys reach, so the drawing shows no ring of its own.
  const NO_RING = '[role=menu], [role=dialog], [role=alertdialog], [data-slot$=-content], [role=menuitem], [role=option], [aria-activedescendant], [role=combobox], [data-slot=command-input], .plot-card.selected, .plot-body';
  /** What shows a ring however focus came to it: what is typed into. */
  const FIELD = 'input, textarea, select, [contenteditable]';
  /** What a ring goes around in place of the element with focus inside it. */
  const BOX = '[data-slot=input-group]';
  /** The layers over the page that hold their own ring: a popover's or a dialog's content. */
  const LAYER = '[data-slot=popover-content], [data-slot=dialog-content], [data-slot=alert-dialog-content]';
  /** How far the ring stands outside what it surrounds, and its width, in px. */
  const GAP = 1, WIDTH = 2;

  /** The ring's box around the element with focus, kept on the screen, and the layer holding it, or null for none. */
  function ringBox() {
    const el = document.activeElement;
    if (!el || el === document.body || el.matches(NO_RING)) return null;
    if (!el.matches(FIELD) && !focusedByKeyboard(el)) return null;
    const shown = el.closest(BOX) ?? el;
    const r = shown.getBoundingClientRect();
    if (!r.width && !r.height) return null;
    const out = GAP + WIDTH;
    const x0 = Math.max(0, r.left - out), y0 = Math.max(0, r.top - out);
    const x1 = Math.min(innerWidth, r.right + out), y1 = Math.min(innerHeight, r.bottom + out);
    const radius = (parseFloat(getComputedStyle(shown).borderTopLeftRadius) || 0) + out;
    return { x: x0, y: y0, w: x1 - x0, h: y1 - y0, radius, layer: el.closest(LAYER) };
  }

  // What has focus moves with the layout, so the ring follows it each
  // frame, and at once when focus moves. The ring is an element of its own,
  // moved into the layer it belongs to.
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
      frame = requestAnimationFrame(follow);
    };
    follow();
    const events = ['focusin', 'focusout', 'keyup', 'pointerdown'];
    for (const type of events) window.addEventListener(type, update);
    return () => {
      cancelAnimationFrame(frame);
      for (const type of events) window.removeEventListener(type, update);
      ring.remove();
    };
  });
</script>


// The page's one key router: it hears every key pressed, and gives it to the
// first claimant in ORDER that takes it. No component listens for keys
// itself, but a text field for its own editing: a part of the page says
// what it claims with `claim`, and what the keys are is keymap.js's table,
// which the keys sheet lists.
//
// A key is offered down ORDER, top to bottom:
//
//   Before the element with focus hears it
//     drag        a drag or a resize under way: Escape cancels it
//     menu        an open menu: a key one of its rows shows runs that row
//     panel       a dock panel's header: Space folds, Enter goes in
//     tabs        a layout tab
//     signals     a plot's signals
//     list        the Signals list, and its search field
//     properties  a table of property rows
//     slider      the window slider
//     kind        a plot's kind and layout dropdowns, while one is open
//   The element with focus
//     element     what is typed into a field, an open menu's, list's or
//                 dialog's own keys, and the keys that press a button the
//                 keyboard moved focus to
//   After it
//     palette     Ctrl+K, wherever it is pressed
//     plot        a plot command, pressed in a plot
//     restore     Escape leaves a maximized view, the selection kept
//     deselect    Escape deselects
//     navigation  Tab, the arrows, Enter and F6 move through the page;
//                 Escape goes out a level
//     page        undo and redo, copy, select all, go to a plot, the keys
//                 sheet, the next tab
//     plots       a plot command, pressed anywhere else
//     pause       Space
//
// So Escape belongs first to a drag, then to an open menu, popover or
// dialog, which closes, or a field, which keeps it; then it leaves the
// maximized view; then it deselects; then it goes out a level. A tooltip
// that closes on it takes it from none of them: what a layer over the page
// does with a key it hears at the document is not the element's doing. It
// never clears cursors.
//
// The claimants before the element hear the key on its way down to it, and
// a key one of them takes never reaches the element: not the menu, and not
// the drag library, which would start a keyboard drag of a tab or a panel.
// The claimants after it hear the key once the element has: a key the
// element keeps (keys.js `keepsKey`) is offered only to those with `kept`,
// and a key the element took, by preventing its default, only to those
// with `taken`.
//
// A claimant with a `place` is offered the keys of that place in keymap.js,
// and runs the action a key names there; one without is offered every key.
import { actionFor } from './keymap.js';
import { keepsKey, keyPressed } from './keys.js';
import { runShownKey } from './menus.js';

/** What a claimant answers to hand a key to the page: the element never hears it, and the claimants after the element take it as pressed on no element. */
export const PAGE = Symbol('page');

/** The element with focus, in ORDER. */
const ELEMENT = 'element';

/**
 * The claimants, in the order a key is offered to them: {id, place, kept,
 * taken, on}. `place` is the keymap.js place whose keys it is offered;
 * `kept` and `taken` offer it a key the element keeps or took; `on` is what
 * a claimant that is always there does, as `claim` takes it.
 */
export const ORDER = [
  { id: 'drag', place: 'drag' },
  { id: 'menu', on: (action, e, at) => runShownKey(e, at) },
  { id: 'panel', place: 'panel' },
  { id: 'tabs', place: 'tabs' },
  { id: 'signals', place: 'signals' },
  { id: 'list', place: 'list' },
  { id: 'properties', place: 'properties' },
  { id: 'slider', place: 'slider' },
  { id: 'kind', place: 'kind' },
  { id: ELEMENT },
  { id: 'palette', place: 'page', kept: true, taken: true },
  { id: 'plot', place: 'plot', kept: true, taken: true },
  { id: 'restore', place: 'page' },
  { id: 'deselect', place: 'page' },
  { id: 'navigation', kept: true },
  { id: 'page', place: 'page', taken: true },
  { id: 'plots', place: 'plot' },
  { id: 'pause', place: 'page', taken: true },
];
const BEFORE = ORDER.slice(0, ORDER.findIndex((o) => o.id === ELEMENT));
const AFTER = ORDER.slice(BEFORE.length + 1);

/** What is claimed now, by claimant id. */
const claims = new Map(ORDER.map((o) => [o.id, new Set(o.on ? [{ on: o.on }] : [])]));

/**
 * Claims keys as claimant `id` of ORDER until the function it returns is
 * called. `when(e, at)`, where given, says whether key event `e`, pressed on
 * `at`, is this claimant's to look at. `on` is what it does: an action of
 * its place to `fn(e, at)`, or one `fn(action, e, at)` for every key it is
 * offered. A function that answers false did not take the key, which goes
 * on down ORDER; PAGE hands it to the page; any other answer took it.
 */
export function claim(id, { when, on }) {
  const set = claims.get(id);
  if (!set || id === ELEMENT) throw new Error(`"${id}" is no claimant in the key router's order`);
  const claimant = { when, on };
  set.add(claimant);
  return () => set.delete(claimant);
}

/** Offers `e`, pressed on `at`, to each of `order` that `open(o)` lets hear it: the id of the claimant that took it with true, or with PAGE of the one that handed it on; else null. */
function offer(order, e, at, open = () => true) {
  for (const o of order) {
    if (!open(o)) continue;
    const action = o.place ? actionFor(o.place, e) : null;
    if (o.place && !action) continue;
    for (const c of [...claims.get(o.id)]) {
      if (c.when && !c.when(e, at)) continue;
      const fn = typeof c.on === 'function' ? c.on : c.on[action];
      if (!fn) continue;
      const answer = typeof c.on === 'function' ? fn(action, e, at) : fn(e, at);
      if (answer !== false) return [o.id, answer !== PAGE];
    }
  }
  return null;
}

/** The last key heard, as {key, by, page}: `by` the id of the claimant that took it, or null, and `page` whether the claimants after the element were offered it. */
let last = null;
/** The last key the router heard, and who took it. */
export const lastKey = () => last;

/** The key the element has heard, and whether it took it, noted as the key leaves the page's elements for the document. */
let heard = null;
function element(e) {
  heard = { e, taken: e.defaultPrevented };
}

/** The claimants after the element, for `e` pressed on `at`. */
function after(e, at) {
  const kept = keepsKey(e, at), taken = heard?.e === e && heard.taken;
  heard = null;
  const hit = offer(AFTER, e, at, (o) => (!kept || o.kept) && (!taken || o.taken));
  last = { key: e.key, by: hit?.[0] ?? null, page: true };
  if (hit) e.preventDefault();
}

/** A key on its way down: the claimants before the element. */
function down(e) {
  keyPressed();
  last = { key: e.key, by: null, page: false };
  const hit = offer(BEFORE, e, e.target);
  if (!hit) return;
  e.stopPropagation();
  if (!hit[1]) return after(e, null);
  e.preventDefault();
  last.by = hit[0];
}

/**
 * A button presses on Space's keyup, not its keydown. A Space the element
 * does not keep is the page's, to pause, and takes the keyup too, so a
 * button focused by a click presses nothing.
 */
function up(e) {
  if (e.key === ' ' && !keepsKey(e)) e.preventDefault();
}

if (typeof window !== 'undefined') {
  window.addEventListener('keydown', down, true);
  document.addEventListener('keydown', element);
  window.addEventListener('keydown', (e) => after(e, e.target));
  window.addEventListener('keyup', up);
}

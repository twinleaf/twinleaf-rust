// The page's keyboard focus as one tree (docs/twinleaf-web-focus.md).
//
// A part of the page joins the tree by its attributes:
//   data-focus="kind"       a node: a region, a panel header, a plot
//   data-focus-own          a node that moves among its own items, by its
//                           own keys; nothing inside it is a node
//   data-focus-arrows       a node that uses the arrows itself when it has focus
//   data-focus-key="k"      a node whose children also sit in the element
//   data-focus-group="k"    elsewhere, as a panel header's are in its body
//   data-focus-skip         left out of the tree, with all inside it
// Every control inside a node (a button, a field, a tab) is a node too, a
// child of the nearest node around it. Splitters are not in the tree.
//
// The cursor is the node with focus, or the node around what has it. Tab
// and Shift+Tab step through the cursor's siblings, wrapping; the arrows do
// too, but in a field or a node that uses them. Enter goes down a level, to
// the child the node last had, or its first; Escape goes up one. F6 steps
// through the page's regions, to the node each last had.

const NODE = '[data-focus]';
const CONTROL = 'button, input, select, textarea, a[href], [role=tab], [role=button], [role=checkbox], [role=switch], [role=slider], [role=combobox], [tabindex="0"]';
/** Where the keys belong to what is open: a menu, a dropdown's list, a dialog or a popover. */
const POPUP = '[role=menu], [role=dialog], [role=alertdialog], [data-slot$=-content]';
/** What keeps the arrows, to move its caret or its value. */
const FIELD = 'input, textarea, select, [contenteditable], [role=slider], [role=combobox]';
const BACK = ['ArrowLeft', 'ArrowUp'], FORWARD = ['ArrowRight', 'ArrowDown'];

// The moves, on any tree of {parent(n), children(n), last(n)}, where the
// root is null, `children(null)` the regions and `last(n)` the child `n`
// last had.

/** The sibling `d` steps from `n`, wrapping; the first sibling if `n` is not among them. */
export function step(tree, n, d) {
  const sibs = tree.children(tree.parent(n));
  if (!sibs.length) return null;
  const i = sibs.indexOf(n);
  return i < 0 ? sibs[0] : sibs[(((i + d) % sibs.length) + sibs.length) % sibs.length];
}

/** The child of `n` it last had, or its first; null for a leaf. */
export function down(tree, n) {
  const kids = tree.children(n), last = tree.last(n);
  return kids.includes(last) ? last : (kids[0] ?? null);
}

/** Where `n` was last, following the child each node last had down from it. */
export function deepest(tree, n) {
  for (let seen = new Set(); n && !seen.has(n); ) {
    seen.add(n);
    const last = tree.last(n);
    if (!last || !tree.children(n).includes(last)) break;
    n = last;
  }
  return n;
}

/** The region `d` steps from the one holding `n`, or the first or last with no `n`. */
export function region(tree, n, d) {
  const regions = tree.children(null);
  if (!n) return regions.at(d > 0 ? 0 : -1) ?? null;
  while (tree.parent(n)) n = tree.parent(n);
  return step(tree, n, d);
}

// The tree the page's elements make.

const lastChild = new WeakMap();
const ROOT = {};

/** The node around element `el`, or null at the page's top. */
function parentOf(el) {
  for (let a = el.parentElement; a; a = a.parentElement) {
    if (a.matches(NODE)) return a;
    const group = a.dataset.focusGroup;
    if (group) return document.querySelector(`[data-focus-key="${group}"]`);
  }
  return null;
}

/** Whether `el` is a node: one declared, or a control inside one, but never inside a node that keeps its own. */
function isNode(el) {
  if (el.parentElement?.closest('[data-focus-own]')) return false;
  return el.matches(NODE) || (el.matches(CONTROL) && parentOf(el) !== null);
}

/** Whether `el` can take focus now: shown, enabled and not left out, as a plot hidden behind maximized ones is. */
function usable(el) {
  return !el.disabled && !el.closest('[inert], [aria-hidden=true], [data-focus-skip], [role=separator]') && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden';
}

/** The page's tree, read from the DOM as it is when asked. */
export const page = {
  parent: (n) => parentOf(n),
  children(n) {
    const scopes = n ? [n, ...document.querySelectorAll(`[data-focus-group="${n.dataset.focusKey}"]`)] : [document.body];
    const out = [];
    for (const scope of scopes)
      for (const el of scope.querySelectorAll(`${NODE}, ${CONTROL}`)) if (isNode(el) && parentOf(el) === n && usable(el)) out.push(el);
    return out.sort((a, b) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1));
  },
  last: (n) => lastChild.get(n ?? ROOT),
};

/** The node the cursor is on, for element `el` with focus: the node itself, the one keeping its own items, or the node around it. */
export function cursorOf(el) {
  if (!(el instanceof Element) || el === document.body) return null;
  const own = el.closest('[data-focus-own]');
  if (own && isNode(own)) return own;
  return isNode(el) ? el : parentOf(el);
}

/** Puts focus on node `n`: a node that keeps its own items gives it to the item that takes it, as a tab list's chosen tab. */
export function focusNode(n) {
  const to = n.matches('[data-focus-own]') && n.tabIndex < 0 ? (n.querySelector('[tabindex="0"]') ?? n) : n;
  to.focus();
}

/** Remembers, up from where focus landed, the child each node had last. Heard on focusin. */
export function remember(e) {
  for (let c = cursorOf(e.target); c; ) {
    const p = parentOf(c);
    lastChild.set(p ?? ROOT, c);
    c = p;
  }
}

/** Goes up a level from the cursor, and says whether there was one to go to. */
export function leave() {
  const n = cursorOf(document.activeElement), to = n && parentOf(n);
  if (to) focusNode(to);
  return !!to;
}

/** Goes down a level from node `n`, if it has children. */
export function enter(n) {
  const to = down(page, n);
  if (to) focusNode(to);
  return !!to;
}

/**
 * Moves the cursor by key `e`, and says whether it did. A key a component
 * already took, a key with Alt, Ctrl or Meta, and every key in a menu or
 * dialog stay where they are. Escape on a node that `keepsEscape` says acts
 * on its own Escape first stays too.
 */
export function navigate(e, keepsEscape = () => false) {
  if (e.defaultPrevented || e.altKey || e.ctrlKey || e.metaKey) return false;
  const at = e.target instanceof Element ? e.target : null;
  if (at?.closest(POPUP)) return false;
  const n = cursorOf(document.activeElement);
  let to = null;
  if (e.key === 'Tab') to = n ? step(page, n, e.shiftKey ? -1 : 1) : deepest(page, region(page, null, e.shiftKey ? -1 : 1));
  else if (e.key === 'F6') to = deepest(page, region(page, n, e.shiftKey ? -1 : 1));
  else if (BACK.includes(e.key) || FORWARD.includes(e.key)) {
    if (!n || at?.closest(FIELD) || n.matches('[data-focus-own], [data-focus-arrows]')) return false;
    to = step(page, n, BACK.includes(e.key) ? -1 : 1);
  } else if (e.key === 'Enter') {
    if (!n || at !== n || !n.matches(NODE)) return false;
    to = down(page, n);
  } else if (e.key === 'Escape') {
    if (!n || keepsEscape(n)) return false;
    to = parentOf(n);
  }
  if (!to) return false;
  e.preventDefault();
  focusNode(to);
  return true;
}

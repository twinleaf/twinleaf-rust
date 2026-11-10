// Which keys stay where focus is, and which act on the plots the keys act
// on wherever focus is.
//
// What is typed into a field, and an open menu, list or dialog's own keys,
// stay there. A list in the page, a plot's signals or the Signals list,
// keeps every key pressed in it but Escape, which has nothing of the list's
// to close and so is the page's: it leaves a maximized view, deselects, and
// only then goes out of the list (keyrouter.js).
// A button keeps the keys that press it, Enter and Space, only
// when the keyboard moved focus to it. A button focused by a click keeps
// none, so Space still pauses. The browser's :focus-visible cannot tell
// them apart here: it turns on for a clicked button at the first key, before
// the page hears it. Every other key acts on the plots the selection gives
// it (selection.svelte.js `pageTargets`), so clicking a button in a plot's
// header, or anywhere else, never takes the plots' keys away: which plots
// they act on is the page's state, not the browser's focus.

/** What keeps every key pressed in it: a field, and what is open over the page. */
const TYPING = 'input, textarea, select, [contenteditable], [role=combobox][aria-expanded=true], [role=menu], [role=menuitem], [role=dialog], [data-slot=select-content]';
/** A list, which keeps every key pressed in it but Escape. */
const LIST = '[role=listbox]';
/** What keeps the keys that press it. */
const PRESSABLE = 'button, a, [role=button], [role=tab], [role=radio], [role=checkbox], [role=switch], [role=slider]';
const PRESS = ['Enter', ' '];

/** Whether a key was pressed since the last pointer press, so focus arriving now came from the keyboard. */
let keyboardLast = false;
/** The elements the keyboard moved focus to, while they keep it. */
const keyboardFocused = new WeakSet();
if (typeof window !== 'undefined') {
  window.addEventListener('pointerdown', () => (keyboardLast = false), true);
  window.addEventListener('focusin', (e) => (keyboardLast ? keyboardFocused.add(e.target) : keyboardFocused.delete(e.target)), true);
}

/** Notes a key pressed, as the key router does for each before anything acts on it. */
export function keyPressed() {
  keyboardLast = true;
}

/**
 * Whether key event `e`, pressed on `at`, belongs to where it was pressed,
 * rather than to the plots the keys act on. A key pressed on no element
 * belongs to none.
 */
export function keepsKey(e, at = e.target) {
  if (!(at instanceof Element)) return false;
  if (at.closest(TYPING)) return true;
  if (at.closest(LIST)) return e.key !== 'Escape';
  if (!PRESS.includes(e.key)) return false;
  const pressable = at.closest(PRESSABLE);
  return !!pressable && keyboardFocused.has(pressable);
}

/** Whether the keyboard moved focus to `el`, rather than a click. */
export function focusedByKeyboard(el) {
  return keyboardFocused.has(el);
}

/** Each plot's pickers the keys open, by plot id: `open(action)` opens the one for keymap.js plot action `action` and says whether it had one. */
export const plotPickers = new Map();

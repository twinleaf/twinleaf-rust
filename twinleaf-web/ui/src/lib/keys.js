// Which keys stay where focus is, and which act on the page's highlighted
// plot wherever focus is.
//
// What is typed into a field, and an open menu, list or dialog's own keys,
// stay there. A button with focus keeps the keys that press it, Enter and
// Space, however it got focus. Every other key acts on the highlighted plot,
// so clicking a button in a plot's header, or anywhere else, never takes the
// plot's keys away: which plot they act on is the page's state, not the
// browser's focus.

/** What keeps every key pressed in it. */
const TYPING = 'input, textarea, select, [contenteditable], [role=combobox][aria-expanded=true], [role=menu], [role=menuitem], [role=listbox], [role=dialog], [data-slot=select-content]';
/** What keeps the keys that press it. */
const PRESSABLE = 'button, a, [role=button], [role=tab], [role=radio], [role=checkbox], [role=switch], [role=slider]';
const PRESS = ['Enter', ' '];

/** Whether key event `e` belongs to where it was pressed, rather than to the highlighted plot. */
export function keepsKey(e) {
  const at = e.target instanceof Element ? e.target : null;
  if (!at) return false;
  if (at.closest(TYPING)) return true;
  return PRESS.includes(e.key) && !!at.closest(PRESSABLE);
}

/** Each plot's key handler, by plot id, which acts on that plot wherever focus is. */
export const plotKeys = new Map();

// Short notices at the foot of the page, for what an action did or why it
// could not: each a line of text and up to two buttons, gone after a while
// or when one is pressed.

const SHOW_MS = 6000;
let next = 0;

export const toasts = $state([]);

/** Shows `text`, with `actions` [{label, run}] as buttons, for SHOW_MS, or until one is pressed. */
export function toast(text, actions = []) {
  const id = next++;
  toasts.push({ id, text, actions });
  setTimeout(() => dismiss(id), SHOW_MS);
}

export function dismiss(id) {
  const i = toasts.findIndex((t) => t.id === id);
  if (i >= 0) toasts.splice(i, 1);
}

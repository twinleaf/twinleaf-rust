// The page's notices, for what an action did or why it could not: Sonner's
// toasts (components/ui/sonner), each at its level, with a button where an
// action offers one, and lines of detail under the text where it has some.
// A notice of a kind of its own, as the device writes' (WriteNotice.svelte),
// is one notice that each new one replaces, open until it is closed.
// Toasts.svelte shows them.

import { toast as sonner } from 'svelte-sonner';

/** Sonner's options for `actions` [{label, run}], a button for the first, and `detail`, lines under the text. */
const options = (actions, detail = []) => ({
  ...(actions[0] ? { action: { label: actions[0].label, onClick: actions[0].run } } : {}),
  ...(detail.length ? { description: detail.join('\n') } : {}),
});

/**
 * Shows `text` as a plain notice, with `actions` as a button and `detail`
 * under it; or, by its method, as a success, a warning or an error.
 */
export const toast = Object.assign((text, actions = [], detail = []) => sonner(text, options(actions, detail)), {
  success: (text, actions = [], detail = []) => sonner.success(text, options(actions, detail)),
  warning: (text, actions = [], detail = []) => sonner.warning(text, options(actions, detail)),
  error: (text, actions = [], detail = []) => sonner.error(text, options(actions, detail)),
});

/**
 * The notice of kind `id`: `open()` shows `component` with `props`, in place
 * of the one shown before, until `close()`; `closed` runs when the person
 * closes it.
 */
export const noticeOf = (id, component, props, closed) => ({
  open: () => sonner.custom(component, { id, duration: Number.POSITIVE_INFINITY, componentProps: props, onDismiss: closed }),
  close: () => sonner.dismiss(id),
});

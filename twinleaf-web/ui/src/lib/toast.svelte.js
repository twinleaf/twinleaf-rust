// The page's notices, for what an action did or why it could not: Sonner's
// toasts (components/ui/sonner), each at its level, with a button where an
// action offers one. Toasts.svelte shows them.

import { toast as sonner } from 'svelte-sonner';

/** Sonner's options for `actions` [{label, run}]: a button for the first. */
const options = (actions) => (actions[0] ? { action: { label: actions[0].label, onClick: actions[0].run } } : {});

/**
 * Shows `text` as a plain notice, with `actions` as a button; or, by its
 * method, as a success, a warning or an error.
 */
export const toast = Object.assign((text, actions = []) => sonner(text, options(actions)), {
  success: (text, actions = []) => sonner.success(text, options(actions)),
  warning: (text, actions = []) => sonner.warning(text, options(actions)),
  error: (text, actions = []) => sonner.error(text, options(actions)),
});

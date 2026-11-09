// What the menus' rows say in their trailing column, of what each row's
// command acts on: a count over the plots it acts on, a setting they share,
// or 'Mixed' where they differ.

/** `n` things, named `one`, or `many` when they are not one. */
export const counted = (n, one, many = `${one}s`) => `${n.toLocaleString()} ${n === 1 ? one : many}`;

/** The one value of `values`, 'Mixed' when they differ, or null when there are none. */
export const common = (values) => (!values.length ? null : values.every((v) => v === values[0]) ? values[0] : 'Mixed');

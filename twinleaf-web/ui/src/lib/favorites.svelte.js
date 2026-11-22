// The properties a person starred, by RPC name, pinned in the Favorites
// panel. Kept in localStorage under KEY, so they hold from visit to visit
// and across devices, as the same name is the same property on each.

const KEY = 'tl-favorites';

export class Favorites {
  /** The starred names. */
  names = $state(new Set());

  /** `storage` is where they are kept, as localStorage; none keeps them for this page alone. */
  constructor(storage) {
    this.storage = storage;
    try {
      const kept = JSON.parse(storage?.getItem(KEY) ?? '[]');
      if (Array.isArray(kept)) this.names = new Set(kept.filter((n) => typeof n === 'string'));
    } catch {
      // Unreadable favorites start empty.
    }
  }

  has(name) {
    return this.names.has(name);
  }

  /** Stars `name`, or takes its star away. */
  toggle(name) {
    const names = new Set(this.names);
    if (!names.delete(name)) names.add(name);
    this.set(names);
  }

  /** Stars every name of `names` when `on`, and takes their stars away when not. */
  pin(names, on) {
    const next = new Set(this.names);
    for (const n of names) on ? next.add(n) : next.delete(n);
    this.set(next);
  }

  /** Told of every change to the stars before it is made: History's check that an undo step holds it. */
  guard = null;

  /** Makes `names` the starred names. */
  set(names) {
    this.guard?.();
    this.names = new Set(names);
    try {
      this.storage?.setItem(KEY, JSON.stringify([...this.names]));
    } catch {
      // A page that cannot keep them keeps them until it closes.
    }
  }
}

export const favorites = new Favorites(typeof localStorage === 'undefined' ? null : localStorage);

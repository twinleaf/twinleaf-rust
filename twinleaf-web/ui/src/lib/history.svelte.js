// Undo and redo for what a person edits on the page: the tabs, their plots
// and layout, the cursors placed on the plots, and the starred properties. A step holds the tabs and stars
// before and after, with the plots selected at each, so undo puts the
// selection back as it was.
//
// Every edit runs inside `group`: a command that changes several things, as
// closing every selected plot, and the one write that ends a drag, are each
// one step, and nothing stays open past the call. A group that throws puts
// back what it changed and is no step. The Workspace's tabs are one frozen
// value, so a step keeps them as they were without copying them. History
// tells the Workspace and the Favorites to refuse, in development, an edit
// made outside any group, as no step would hold it.
//
// What a view shows, as a spectrum's zoom, is not an edit: a step that
// changes only that is none, and undo keeps it as it is.

import { VIEW_FIELDS } from './tabs.js';

/** How many steps undo keeps. */
const DEPTH = 200;

/** The tabs as a step compares them: without view fields. */
const edits = (tabs) => JSON.stringify(tabs, (k, v) => (VIEW_FIELDS.has(k) ? undefined : v));

export class History {
  /** The steps undo goes back through, oldest first, each {label, before, after, selBefore, selAfter}. */
  undos = $state.raw([]);
  /** The steps redo goes forward through, the next last. */
  redos = $state.raw([]);
  #grouping = false;
  #restoring = false;

  /** `ws` is the page's Workspace and `favorites` its Favorites. */
  constructor(ws, favorites) {
    this.ws = ws;
    this.favorites = favorites;
    if (import.meta.env?.DEV) {
      ws.guard = favorites.guard = () => {
        if (!this.#grouping && !this.#restoring) throw new Error('an edit outside an undo step: run it in history.group');
      };
    }
  }

  /** What undo would take back, or null. */
  get undoLabel() {
    return this.undos.at(-1)?.label ?? null;
  }
  /** What redo would do again, or null. */
  get redoLabel() {
    return this.redos.at(-1)?.label ?? null;
  }

  /**
   * Runs `fn` as one step named `label`, kept if it changed anything, and
   * returns what it returns. A group inside a group is part of the outer.
   * When `fn` throws, what it changed is put back, and the error goes on.
   */
  group(label, fn) {
    if (this.#grouping) return fn();
    const before = this.#read(), selBefore = this.#selection();
    this.#grouping = true;
    let done = false;
    try {
      const out = fn();
      done = true;
      return out;
    } finally {
      this.#grouping = false;
      if (!done) this.#write(before, selBefore);
      else {
        const after = this.#read();
        if (!this.#same(before, after)) {
          this.undos = [...this.undos.slice(-(DEPTH - 1)), { label, before, after, selBefore, selAfter: this.#selection() }];
          this.redos = [];
        }
      }
    }
  }

  /** Takes back the last step, and returns its label, or null when there is none. */
  undo() {
    const e = this.undos.at(-1);
    if (!e) return null;
    this.undos = this.undos.slice(0, -1);
    this.redos = [...this.redos, e];
    this.#write(e.before, e.selBefore);
    return e.label;
  }

  /** Does again the step undo took back last, and returns its label, or null when there is none. */
  redo() {
    const e = this.redos.at(-1);
    if (!e) return null;
    this.redos = this.redos.slice(0, -1);
    this.undos = [...this.undos, e];
    this.#write(e.after, e.selAfter);
    return e.label;
  }

  /** The ids of every tab and every plot the tabs hold, or an undo or a redo would bring back: {tabs, plots}, each a Set. */
  held() {
    const tabs = new Set(), plots = new Set();
    const add = (ts) => ts.forEach((t) => (tabs.add(t.id), Object.keys(t.plots).forEach((id) => plots.add(id))));
    add(this.ws.tabs);
    for (const e of [...this.undos, ...this.redos]) (add(e.before.tabs), add(e.after.tabs));
    return { tabs, plots };
  }

  /** What steps change: the tabs, as the frozen value they are, and the starred names. */
  #read() {
    return { tabs: this.ws.tabs, favorites: [...this.favorites.names].sort() };
  }

  /** Whether `a` and `b` hold the same edits. */
  #same(a, b) {
    return (a.tabs === b.tabs || edits(a.tabs) === edits(b.tabs)) && a.favorites.join('\n') === b.favorites.join('\n');
  }

  #selection() {
    return { tab: this.ws.activeId, ...this.ws.selection.snapshot() };
  }

  /** Puts `s` back, keeps each plot's view fields as they are now, and opens the tab of `sel` with its plots selected. */
  #write(s, sel) {
    if (s.tabs !== this.ws.tabs) {
      const now = new Map(this.ws.tabs.flatMap((t) => Object.values(t.plots)).map((p) => [p.id, p]));
      const keep = (p) => {
        const was = now.get(p.id);
        const view = was && [...VIEW_FIELDS].filter((f) => was[f] !== p[f]);
        return view?.length ? { ...p, ...Object.fromEntries(view.map((f) => [f, was[f]])) } : p;
      };
      const kept = s.tabs.map((t) => {
        const plots = Object.fromEntries(Object.entries(t.plots).map(([id, p]) => [id, keep(p)]));
        return Object.keys(plots).some((id) => plots[id] !== t.plots[id]) ? { ...t, plots } : t;
      });
      this.ws.restore(kept.some((t, i) => t !== s.tabs[i]) ? kept : s.tabs);
    }
    if ([...this.favorites.names].sort().join('\n') !== s.favorites.join('\n')) {
      this.#restoring = true;
      try {
        this.favorites.set(s.favorites);
      } finally {
        this.#restoring = false;
      }
    }
    const tab = this.ws.tabs.some((t) => t.id === sel.tab) ? sel.tab : this.ws.tabs[0].id;
    if (tab !== this.ws.activeId) this.ws.selectTab(tab);
    this.ws.selection.restore(sel);
  }
}

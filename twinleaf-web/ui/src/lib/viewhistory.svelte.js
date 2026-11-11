// Back and Forward retrace where one was looking: what the plots of a tab
// have shown, from the buttons on each plot. What was made or placed, a
// plot's cursors among it, is the edit history's, which Ctrl+Z undoes. A
// step holds, for each plot one gesture moved, what
// it showed before and after: its View `state` (live or paused and where,
// its span and window, the y ranges it holds and the spectra a
// paused spectrum holds), its view fields (tabs.js VIEW_FIELDS, as a
// spectrum's frequencies) and its kind, time or spectrum, with the time X
// cursors it had (`x`), which no step counts as a move. Back shows the before of every plot of the
// step still in the tab, so the plots one gesture moved go back together;
// Forward shows the after.
//
// Back goes to the newest step that moves a plot shown, past steps of
// plots closed, moved to another tab or hidden behind a maximized plot,
// which stay where they are until their plots show again; so Back and
// Forward can be pressed just when a press changes what shows. A view
// shown live follows the newest samples again. One shown paused holds
// where it was, ending no later than the newest samples; a time plot's
// whose start the hub has dropped since starts at the oldest time it
// holds, and a notice says so. A plot an undo brings back shows its view
// by the same rule (`returned`).
//
// Each tab has its own steps, which go when the tab is gone for good
// (`prune`). A drag, from its press to its release (`begin`, `end`), is one
// step, and so is a command (`step`), whatever they pass through on the
// way. Wheel turns and keys (`burst`) are one step while each comes less
// than IDLE_MS after the last and makes the same kind of change to the same
// plots. A gesture that moves no view, or a burst that ends where it began,
// is no step and leaves Forward as it was; a step made inside another
// gesture is part of that one.
//
// A plot's kind is the one thing both histories hold. Switching it is an
// edit, which Ctrl+Z undoes, and it changes what one is looking at, so it
// is a step here too: Back shows the plot as it was before the switch, its
// kind with its view, and goes on through the steps before it. The rule
// that keeps the two from disagreeing: the tab holds the kind, and only
// edits change it. Back and Forward change a plot's kind by an edit of
// their own (`edit`), a step undo takes back like the command's; and every
// change of a plot's kind, by the command, an undo or a redo (`edited`),
// is a step here, after the one Back is at, in place of those Forward had.
// So the newest step always ends at the kind the plot has.
//
// The other thing the two hold together is a plot's time X cursors, which
// a view that follows live never has (cursors.js): they mark moments, which
// scroll away. A view goes live through here alone: by `follow`, which
// every Follow live, Pause and Live of the page runs, by Back and
// Forward, and by `settle`, as a plot loaded paused finds the hub no
// longer holds what it shows. Each takes the time X cursors of the plots
// it makes live, by an edit (CursorEdits `followed`), Back's and Forward's
// the one their kind change makes, `settle`'s as no edit, the load's end. A step holds the time X cursors each plot had (`x`), and
// Back or Forward that shows a live plot paused again puts back those its
// view had as it last went live from there: a step's paused side takes the
// cursors a plot has as Back or Forward makes it live from that side. So
// one Back undoes going live with its cursors, and cursors removed while
// paused, an edit of their own, stay removed. An undo or a redo that
// brings time X cursors back to a plot that follows live shows the paused
// view it left as it went live (View `left`), or else pauses it where it
// is, a step after the one Back is at, as a kind switch it makes is.
// Otherwise the edit history
// (history.svelte.js) is apart: an undo moves no other view, and Back
// makes no other edit, so it moves no other cursor.

import { SvelteMap } from 'svelte/reactivity';
import { sameView } from './plot.js';
import { VIEW_FIELDS } from './tabs.js';
import { KINDS } from './constants.js';
import { counted } from './menus.js';
import { hasTimeX, withXPair, xOf } from './cursors.js';
import * as clock from './clock.js';

/** How long, in ms, wheel turns or keys rest before the next is a step of its own. */
export const IDLE_MS = 500;
/** How many steps Back keeps for each tab. */
const DEPTH = 100;
/** The notice for a view whose start the hub has dropped. */
export const GONE = 'That moment is gone: showing the oldest data held';

/** The entries of map `m` for `ids`. */
const only = (m, ids) => new Map(ids.map((id) => [id, m.get(id)]));
/** Whether plot states `a` and `b`, as `#read` gives them, show the same. */
const same = (a, b) => a.kind === b.kind && sameView(a.view, b.view) && JSON.stringify(a.fields) === JSON.stringify(b.fields);

/** One tab's steps, each {before, after}: Maps of plot id to {view, fields, kind}. */
class Steps {
  /** The steps Back goes through, oldest first. */
  backs = $state.raw([]);
  /** The steps Forward goes through, the next last. */
  forwards = $state.raw([]);
}

export class ViewHistory {
  #tabs = new SvelteMap(); // tab id -> Steps
  #held = null; // the gesture under way, from `begin` to `end`: {tab, before}
  #burst = null; // the burst open to more: {tab, kind, targets, at, before, step, forwards}, its step, if any, the newest
  #bursting = false; // whether a burst's change is running, so a step inside it is part of it

  /**
   * `ws` is the page's Workspace and `views` its Views, each View knowing
   * the oldest time its plot can show; `notice(text)` tells the person;
   * `now()` is the page time, in ms; `edit(label, fn)` runs `fn` as one
   * edit named `label`, History's `group`; `cursors` is the page's
   * CursorEdits, which writes the time X cursors views going live take.
   */
  constructor(ws, views, { notice = () => {}, now = clock.now, edit = (label, fn) => fn(), cursors } = {}) {
    Object.assign(this, { ws, views, notice, now, edit, cursors });
  }

  /** Whether Back would change what the open tab shows. */
  get canBack() {
    return this.#next('backs') >= 0;
  }
  /** Whether Forward would change what the open tab shows. */
  get canForward() {
    return this.#next('forwards') >= 0;
  }

  /** Runs `fn` as one step, and returns what it returns; inside a drag or a burst, as part of that. */
  step(fn) {
    if (this.#held || this.#bursting) return fn();
    this.begin();
    try {
      return fn();
    } finally {
      this.end();
    }
  }

  /**
   * Runs `fn`, a wheel turn or a key that makes a change of `kind` to the
   * views `targets`, and returns what it returns. It joins the burst before
   * it when that one was of the same kind on the same targets, less than
   * IDLE_MS ago, with no other step made or gone through since; else it
   * begins one. A burst's step holds what its turns moved from where they
   * were as it began, and is none while that is nothing.
   */
  burst(kind, targets, fn) {
    if (this.#held || this.#bursting) return fn();
    const tab = this.ws.activeId, at = this.now(), of = this.#idsOf(targets).join();
    const steps = this.#of(tab), b = this.#burst;
    if (!(b && b.tab === tab && b.kind === kind && b.targets === of && at - b.at < IDLE_MS))
      this.#burst = { tab, kind, targets: of, before: this.#read(this.#ids()), step: null, forwards: steps.forwards };
    const burst = this.#burst;
    burst.at = at;
    this.#bursting = true;
    let out;
    try {
      out = fn();
    } finally {
      this.#bursting = false;
    }
    const ids = [...burst.before.keys()], after = this.#read(ids);
    const moved = ids.filter((id) => after.has(id) && !same(burst.before.get(id), after.get(id)));
    const under = burst.step ? steps.backs.slice(0, -1) : steps.backs;
    burst.step = moved.length ? { before: only(burst.before, moved), after: only(after, moved) } : null;
    steps.backs = burst.step ? [...under.slice(-(DEPTH - 1)), burst.step] : under;
    steps.forwards = burst.step ? [] : burst.forwards;
    return out;
  }

  /** Starts a gesture held down, as a drag, which `end` makes one step. */
  begin() {
    this.end();
    this.#burst = null;
    this.#held = { tab: this.ws.activeId, before: this.#read(this.#ids()) };
  }

  /** Ends the gesture `begin` started, a step if it moved any view. */
  end() {
    const h = this.#held;
    if (!h) return;
    this.#held = null;
    const ids = [...h.before.keys()], after = this.#read(ids);
    const moved = ids.filter((id) => after.has(id) && !same(h.before.get(id), after.get(id)));
    if (moved.length) this.#push(h.tab, { before: only(h.before, moved), after: only(after, moved) });
  }

  /**
   * Has the views of plots `ids` follow the newest samples again, as one
   * step, each back at its window with `restore`, else on at the span it
   * shows (View `follow`), their time X cursors taken as one edit: the one
   * way the page goes live, but Back and Forward.
   */
  follow(ids, restore) {
    this.step(() =>
      this.edit('Follow live', () => {
        for (const id of ids) this.views.of(id).follow(restore);
        this.cursors.followed(ids);
      }),
    );
  }

  /**
   * Ends loading the plots kept paused, once the hub has said what it
   * holds, at page time `perf` (Views `settle`): each that still holds
   * what it shows stays paused with its cursors; each other follows live
   * at its window without its time X cursors (CursorEdits `unloaded`),
   * as part of the load, a step of neither history.
   */
  settle(perf) {
    const gone = this.views.settle(perf);
    if (!gone.length) return;
    for (const id of gone) this.views.of(id).follow(true);
    this.cursors.unloaded(gone);
  }

  /** Shows what the newest step that moves a plot shown in the open tab moved, as it was before; says whether there was one. */
  back() {
    return this.#go('backs', 'forwards', 'before');
  }

  /** Shows again what the step Back went back past last moved, of those that move a plot shown; says whether there was one. */
  forward() {
    return this.#go('forwards', 'backs', 'after');
  }

  /**
   * Plots `ids` are in the tabs again, as an undo brings back a plot closed:
   * each of the open tab's shows its view again by the rule Back shows one
   * by, as the hub, holding nothing for a plot that is gone, may have dropped
   * where it starts.
   */
  returned(ids) {
    this.#show(this.#read(ids.filter((id) => this.ws.plots[id])));
  }

  /**
   * Runs `fn`, an undo or a redo of the edit history, and returns what it
   * returns. A plot it gives time X cursors while its view follows live
   * shows the paused view it left as it went live, or pauses where it is,
   * with the notice when the hub has dropped where that starts. Each plot
   * whose kind it changed, or whose view it paused, has a step for that, in
   * its tab, which Back crosses as it does the command's.
   */
  edited(fn) {
    this.end();
    this.#burst = null;
    const before = this.ws.tabs.map((t) => [t.id, this.#read(Object.keys(t.plots))]);
    const out = fn();
    let lost = false;
    for (const t of this.ws.tabs) {
      for (const [id, plot] of Object.entries(t.plots)) {
        const v = this.views.get(id);
        if (!v?.live || !hasTimeX(plot.cursors)) continue;
        if (v.left) v.restore(v.left, this.now());
        else v.pause(this.now());
        lost = (plot.kind === 'time' && v.startHeld(this.now())) || lost;
      }
    }
    if (lost) this.notice(GONE);
    for (const [tab, was] of before) {
      const plots = this.ws.tabs.find((t) => t.id === tab)?.plots ?? {}, after = this.#read(Object.keys(plots));
      const changed = [...was.keys()].filter((id) => after.has(id) && !same(after.get(id), was.get(id)));
      if (changed.length) this.#push(tab, { before: only(was, changed), after: only(after, changed) });
    }
    return out;
  }

  /** Lets go of the steps of every tab not in `tabs`, a Set of tab ids: those the tabs or an undo or redo hold. */
  prune(tabs) {
    for (const tab of [...this.#tabs.keys()]) if (!tabs.has(tab)) this.#tabs.delete(tab);
  }

  /** Whether it keeps steps of tab `tab`. */
  holds(tab) {
    return this.#tabs.has(tab);
  }

  /** The index in the open tab's `list` of the newest step that moves a plot shown, or -1. */
  #next(list) {
    const steps = this.#tabs.get(this.ws.activeId);
    if (!steps) return -1;
    const shown = new Set(this.ws.shownIds());
    return steps[list].findLastIndex((s) => [...s.before.keys()].some((id) => shown.has(id) && this.views.get(id)));
  }

  /**
   * Moves the open tab's step `#next(from)` to `to`, and shows its plots'
   * `side`. A paused plot it shows live leaves the step's other side with
   * the time X cursors it has as it goes, so going back there puts back
   * just what going live took.
   */
  #go(from, to, side) {
    this.end();
    this.#burst = null;
    const i = this.#next(from);
    if (i < 0) return false;
    const steps = this.#of(this.ws.activeId), step = steps[from][i], other = step[side === 'before' ? 'after' : 'before'];
    steps[from] = steps[from].toSpliced(i, 1);
    steps[to] = [...steps[to], step];
    for (const [id, s] of step[side]) {
      const plot = this.ws.plots[id];
      if (plot && s.view.live && this.views.get(id)?.live === false && other.has(id)) other.set(id, { ...other.get(id), x: xOf(plot.cursors, 'time') });
    }
    this.#show(step[side]);
    return true;
  }

  /**
   * Shows `states`, by plot id, of the plots in the open tab, each as the
   * kind it was, with the notice when the hub dropped where one starts. A
   * plot shown live loses its time X cursors, and one live shown paused
   * gets back those it had then.
   */
  #show(states) {
    const here = [...states].filter(([id]) => this.ws.plots[id] && this.views.get(id));
    // One edit switches the plots it shows as the other kind, named as the
    // command's is, and sets the time X cursors of those it makes live or pauses.
    const switched = here.filter(([id, s]) => s.kind !== this.ws.plots[id].kind);
    const followed = here.filter(([id, s]) => s.view.live && hasTimeX(this.ws.plots[id].cursors)).map(([id]) => id);
    const paused = here.filter(([id, s]) => !s.view.live && this.views.get(id).live && (s.x.a !== null || s.x.b !== null));
    if (switched.length || followed.length || paused.length) {
      const name = switched.length && (KINDS.find(([kind]) => kind === switched[0][1].kind)?.[1] ?? switched[0][1].kind);
      const label = !switched.length ? (followed.length ? 'Follow live' : 'Pause') : switched.length > 1 ? `Kind ${name} (${counted(switched.length, 'plot')})` : `Kind ${name}`;
      this.edit(label, () => {
        for (const [id, s] of switched) this.ws.setPlot(id, { kind: s.kind });
        if (followed.length) this.cursors.followed(followed);
        if (paused.length) this.cursors.set(label, new Map(paused.map(([id, s]) => [id, withXPair(this.ws.plots[id].cursors, 'time', s.x)])));
      });
    }
    let lost = false;
    for (const [id, s] of here) lost = this.#put(id, s) || lost;
    if (lost) this.notice(GONE);
  }

  /**
   * Shows plot `id` of the open tab as `s` has it. A paused time plot's
   * view whose start the hub has dropped moves later to start at the oldest
   * time the plot can show (View `startHeld`); says whether it moved.
   */
  #put(id, s) {
    const view = this.views.get(id);
    view.restore(s.view, this.now());
    const lost = this.ws.plots[id].kind === 'time' && view.startHeld(this.now());
    const plot = this.ws.plots[id], patch = {};
    for (const f of VIEW_FIELDS) if (JSON.stringify(plot[f] ?? null) !== JSON.stringify(s.fields[f])) patch[f] = s.fields[f];
    if (Object.keys(patch).length) this.ws.setView(id, patch);
    return lost;
  }

  #push(tab, step) {
    const steps = this.#of(tab);
    steps.backs = [...steps.backs.slice(-(DEPTH - 1)), step];
    steps.forwards = [];
  }

  #of(tab) {
    let steps = this.#tabs.get(tab);
    if (!steps) this.#tabs.set(tab, (steps = new Steps()));
    return steps;
  }

  /** The open tab's plots, by id, in one order. */
  #ids() {
    return Object.keys(this.ws.plots ?? {}).sort();
  }

  /** The ids of the plots whose views are `targets`, in one order. */
  #idsOf(targets) {
    return [...this.views.map].filter(([, v]) => targets.includes(v)).map(([id]) => id).sort();
  }

  /** What plots `ids` that have a view show, by id: {view, fields, kind, x}, its View state, its view fields, its kind and its time X cursors. */
  #read(ids) {
    const out = new Map();
    for (const id of ids) {
      const v = this.views.get(id), plot = this.ws.tabs.find((t) => t.plots[id])?.plots[id];
      if (v && plot) out.set(id, { view: v.state(), fields: Object.fromEntries([...VIEW_FIELDS].map((f) => [f, plot[f] ?? null])), kind: plot.kind, x: xOf(plot.cursors, 'time') });
    }
    return out;
  }
}

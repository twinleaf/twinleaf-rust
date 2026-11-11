<script>
  // One plot: its header of signals and actions, the plot body the WebGL
  // layer draws behind, and its menus. The drawing is plot.js's PlotView;
  // this is the plot's controls. It is a plot of a PlotHost (plothost.js),
  // a tab's or a copy's, and reaches its surroundings through that alone.
  import { onDestroy, onMount, setContext, tick, untrack } from 'svelte';
  import { dndzone, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import ColorPicker from './ColorPicker.svelte';
  import { counted } from './menus.js';
  import RiShowOnly from '@lucide/svelte/icons/eye';
  import RiHide from '@lucide/svelte/icons/eye-off';
  import RiPalette from '@lucide/svelte/icons/palette';
  import RiMove from '@lucide/svelte/icons/square-arrow-out-up-right';
  import RiTrash from '@lucide/svelte/icons/trash-2';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import IconButton from './IconButton.svelte';
  import CommandItem from './CommandItem.svelte';
  import { AXIS_SETTINGS, commandOf, cursorPeers } from './commands.js';
  import { DIGITS, axisFormat, fromUnit, inUnit, unitChoices } from './units.js';
  import * as Select from '$lib/components/ui/select/index.js';
  import KindPicker from './KindPicker.svelte';
  import { claim } from './keyrouter.js';
  import Tip from './Tip.svelte';
  import RiDraggable from '@lucide/svelte/icons/grip-vertical';
  import RiBack from '@lucide/svelte/icons/arrow-left';
  import RiForward from '@lucide/svelte/icons/arrow-right';
  import RiPause from '@lucide/svelte/icons/pause';
  import RiPlay from '@lucide/svelte/icons/play';
  import RiAddSignal from '@lucide/svelte/icons/plus';
  import RiPeak from '@lucide/svelte/icons/mountain';
  import { PlotView, axisSlots, colorOf, monitorOf, shownUnit } from './plot.js';
  import Swatch from './Swatch.svelte';
  import { DRAG_START, kindLabel } from './constants.js';
  import { stepIn } from './keymap.js';
  import WindowField from './WindowField.svelte';
  import * as clock from './clock.js';

  /** `plot` is the plot's settings, and `host` the PlotHost it is a plot of, the same one for as long as the plot is shown. */
  let { plot, host } = $props();
  /**
   * What the host gives every plot of its: the hub, the time under the
   * pointer, the surface and the PlotViews on it, the view steps, the
   * cursors, the signals' colours, and the plot commands (commands.js),
   * which this plot's rows, buttons and keys run, made on this plot.
   */
  // svelte-ignore state_referenced_locally
  const { hub, hover, renderer, registry, viewHistory, cursors, colors, commands } = host;
  // The rows and the colour pickers within the plot are this host's too.
  setContext('commands', commands);
  setContext('colors', colors);
  /** The plot's View, and whether it is selected. */
  const view = $derived(host.view(plot.id)), selected = $derived(host.selected(plot.id));
  /** An edit of this plot alone: its fields `patch`, as one step named `what`. */
  const onEdit = (what, patch) => host.edit(plot.id, what, patch);
  /** The selection: focus or a press that lands here, a press on the plot's controls, and a click. */
  const onFocus = () => host.point(plot.id);
  const onPress = () => host.press(plot.id);
  const onSelect = (shift, clicks) => host.select(plot.id, shift, clicks);
  /** What a page that lays its plots out in tabs adds (PlotHost `place`), or null: moving the plot and its signals among the page's plots and tabs. */
  // svelte-ignore state_referenced_locally
  const place = host.place;
  const otherPlots = $derived(place?.otherPlots(plot.id) ?? []);
  const onStartMove = (e) => place?.startMove(plot.id, e);
  const onTraceDragStart = () => place?.traceDragStart();
  const onLandSignal = (item, items) => place?.landSignal(plot.id, item, items);
  const onMoveSignal = (key, to) => place?.moveSignal(plot.id, key, to);
  /** Runs plot command `id` made on this plot from its button, `how` as the command takes it. */
  const runHere = (id, how) => commands.run(id, { on: plot.id, from: 'button', how });
  /** The peak commands, as the readout's menu, the plot's menu and the x axis's popover offer them. */
  const PEAKS = ['x1ToPeak', 'x2ToPeak', 'peakLeft', 'peakRight'];
  /** How the header's commands read on this plot, for their buttons. */
  const legendRow = $derived(commands.row('legend', { on: plot.id }));
  const maximizeRow = $derived(commands.row('maximize', { on: plot.id }));
  /** How Copy image reads on this plot, for its button in the plot's corner. */
  const copyImageRow = $derived(commands.row('copyImage', { on: plot.id }));
  const closeRow = $derived(commands.row('close', { on: plot.id }));

  /** Each axis setting's buttons, as [value, what the button reads]; its tip names the value in full (commands.js AXIS_SETTINGS). */
  const CHOICES = {
    label: [['off', 'Off'], ['units', 'Units'], ['name', 'Name']],
    notation: [['auto', 'Auto'], ['plain', 'Plain'], ['exp', 'Exp']],
  };
  /** The digits a sample line's − and + step through, fewest first. */
  const STEPS = [null, ...DIGITS];

  const session = $derived(hub.session);
  /** The signal `key`'s colour, units and rate, once the hub has its trace. */
  const colorFor = (key) => {
    const t = session.trace(key);
    return t ? colorOf(t, colors) : 'var(--muted-foreground)';
  };
  const unitsOf = (key) => session.trace(key)?.meta.units ?? '';
  /** An independent plot's axes, {left, right}, each the signal keys from the plot outward. */
  const slots = $derived(plot.mode === 'independent' ? axisSlots(drawnItems().map((i) => i.key), plot.axisSides) : null);
  /** Whether signal `key` has no axis of its own on an independent plot. */
  const axisless = (key) => !!slots && !slots.left.includes(key) && !slots.right.includes(key);

  // A right-click shows what it was on: the time axis's window, or the
  // plot's own items, in a menu; a y axis's settings, which take typing, in
  // a popover at the pointer, as menus hold no fields.
  let menuOpen = $state(false);
  let region = $state({ kind: 'plot' });
  let axisOpen = $state(false);
  let xAxisOpen = $state(false);
  let readoutOpen = $state(false);
  let axisAt = $state({ x: 0, y: 0 });
  let axisAnchor = $state(null);
  let bounds = $state({ lo: '', hi: '' });
  let boundsError = $state('');
  const shown = (v) => String(+v.toPrecision(6));
  /** The one signal the axis right-clicked scales, whose colour its popover sets, or null for a shared axis. */
  const axisKey = $derived(region.kind === 'y' && region.keys.length === 1 && region.keys[0] !== 'shared' ? region.keys[0] : null);
  // What a copy of the plot would hold, asked of the hub at each right-click
  // that opens or moves the menu, since the window may have changed while it
  // was shut or open. The right-click sets the placeholder itself, so the
  // menu's first frame already has its line. A count asked again while the
  // menu stays open keeps the answer shown until its own comes: the row
  // never reads as an answer the page does not have.
  /** What Copy data says before the hub's first count since the right-click: a wait, which cannot be chosen. */
  const COUNTING = { state: 'counting', rows: null };
  let copyCheck = $state(COUNTING);
  let copyAsk = $state(0);
  function recountCopy() {
    if (plot.items.length) copyCheck = COUNTING;
    copyAsk++;
  }
  /** How long the view must rest, while the menu is open, before the count is asked again, in ms. */
  const RECOUNT_MS = 200;
  $effect(() => {
    copyAsk;
    if (!plot.items.length) return void (copyCheck = null);
    // A closing menu keeps its line while it fades, so it does not change size on the way out.
    if (!menuOpen) return;
    let current = true, asked = 0, timer;
    const ask = () => {
      const n = ++asked;
      commands.check('copyData', { on: plot.id }).then((check) => current && n === asked && (copyCheck = check), () => current && n === asked && (copyCheck = null));
    };
    // Asked when the menu opens, the plots it copies change, or a right-click asks again; never for what the count itself reads, as the hub's notes of what it holds.
    untrack(ask);
    // A wheel over a plot zooms it with the menu still open: the count
    // follows once the views of every plot a copy would hold rest, keeping
    // the last one shown meanwhile.
    const offs = commands.context('copyData', { on: plot.id }).plots.map(({ view }) =>
      view.onChange(() => {
        clock.cancel(timer);
        timer = clock.after(RECOUNT_MS, ask);
      }),
    );
    return () => {
      current = false;
      clock.cancel(timer);
      offs.forEach((off) => off());
    };
  });
  function contextAt(e) {
    // The cursors' readout has a menu of its own.
    if (e.target.closest?.('.cursor-readout')) return;
    region = pv?.regionAt(e.clientX, e.clientY) ?? { kind: 'plot' };
    if (region.kind !== 'y' && region.kind !== 'x') return;
    // The axis's popover, in place of the plot's menu.
    e.preventDefault();
    e.stopPropagation();
    const r = body.getBoundingClientRect();
    axisAt = { x: e.clientX - r.left, y: e.clientY - r.top };
    if (region.kind === 'x') {
      fBoundsError = '';
      const xd = plot.kind === 'spectrum' ? pv?.xd : null;
      fBounds = xd ? { lo: shown(xd[0]), hi: shown(xd[1]) } : { lo: '', hi: '' };
      fBoundsShown = { ...fBounds };
      sampleAt = xd ? middle(xd) : null;
      return void (xAxisOpen = true);
    }
    boundsError = '';
    readBounds();
    axisOpen = true;
  }
  /**
   * The value of range `r`, [lo, hi], a sample line writes as a value of
   * the axis: its middle, or, of a range across zero, whose middle is no
   * size of its values, its end further from zero.
   */
  const middle = (r) => (r[0] < 0 && r[1] > 0 ? (-r[0] > r[1] ? r[0] : r[1]) : (r[0] + r[1]) / 2);
  /** The value the open axis popover's sample line writes, read as it opens and as its range is set. */
  let sampleAt = $state(null);
  /** The unit the y axis right-clicked writes its numbers in, where it fixes one (PlotView `yFixed`), which its range fields read and take. */
  const fixedHere = () => (region.kind === 'y' && region.keys.length ? pv?.yFixed(region.keys[0]) : undefined);
  /** Shows the range of the y axis right-clicked in its fields, in the unit it writes, and samples its middle. */
  function readBounds() {
    const b = region.keys.length ? pv.yBounds(region.keys[0]) : null, fixed = fixedHere();
    bounds = b ? { lo: shown(inUnit(b[0], fixed)), hi: shown(inUnit(b[1], fixed)) } : { lo: '', hi: '' };
    boundsShown = { ...bounds };
    sampleAt = b ? middle(b) : null;
  }
  // The fields follow the axis's unit as it changes with the popover open.
  const fixedText = $derived(axisOpen ? (plot.yFormat, fixedHere()?.text) : null);
  $effect(() => {
    fixedText;
    if (axisOpen && region.kind === 'y') untrack(readBounds);
  });
  /**
   * What the popovers' fields showed as they opened, or last set: bounds
   * left as shown set nothing, so a field that loses focus as its popover
   * closes leaves every plot as it was.
   */
  let boundsShown = { lo: '', hi: '' }, fBoundsShown = { lo: '', hi: '' };
  const edited = (b, was) => b.lo !== was.lo || b.hi !== was.hi;
  /** Sets the bounds typed in, and says why it cannot when they do not make a range. Returns whether they hold. */
  function applyBounds() {
    if (!edited(bounds, boundsShown)) return true;
    const fixed = fixedHere(), lo = fromUnit(Number(bounds.lo), fixed), hi = fromUnit(Number(bounds.hi), fixed);
    if (!isFinite(lo) || !isFinite(hi) || bounds.lo.trim() === '' || bounds.hi.trim() === '') boundsError = 'Enter a number in each';
    else if (lo >= hi) boundsError = 'Max must be above min';
    else if (plot.kind === 'spectrum' && plot.yScale !== 'linear' && lo <= 0) boundsError = 'A log axis starts above zero';
    else {
      boundsError = '';
      runHere('yRange', { key: region.keys[0], lo, hi });
      boundsShown = { ...bounds };
      sampleAt = middle([lo, hi]);
      return true;
    }
    return false;
  }
  // A spectrum's frequency range, typed in its x axis's popover.
  let fBounds = $state({ lo: '', hi: '' });
  let fBoundsError = $state('');
  /** Sets the frequencies typed in, and says why it cannot when they do not make a range. Returns whether they hold. */
  function applyFBounds() {
    if (!edited(fBounds, fBoundsShown)) return true;
    const lo = Number(fBounds.lo), hi = Number(fBounds.hi);
    if (!isFinite(lo) || !isFinite(hi) || fBounds.lo.trim() === '' || fBounds.hi.trim() === '') fBoundsError = 'Enter a number in each';
    else if (lo >= hi) fBoundsError = 'Max must be above min';
    else if (plot.xScale !== 'linear' && lo <= 0) fBoundsError = 'A log axis starts above zero';
    else {
      fBoundsError = '';
      runHere('fRange', { range: [lo, hi] });
      fBoundsShown = { ...fBounds };
      sampleAt = middle([lo, hi]);
      return true;
    }
    return false;
  }
  /** Enter sets the bounds and closes; leaving a field sets them and stays. */
  function boundsKey(e) {
    if (e.key === 'Enter' && applyBounds()) axisOpen = false;
  }

  // The signals menu: every signal the plot does not show, filtered by a
  // substring of its name, and kept open so several can be added at once.
  // Focus stays in the filter, where the arrows move the highlight and Enter
  // adds the highlighted signal; the next one takes its place.
  let signalsOpen = $state(false);
  let signalFilter = $state('');
  let addActive = $state(0);
  const addable = $derived.by(() => {
    const has = new Set(plot.items.map((i) => i.key)), f = signalFilter.trim().toLowerCase();
    return session.signals.filter((t) => !has.has(t.key) && t.key.toLowerCase().includes(f));
  });
  /** Adds signal `key` to each plot a command here acts on that does not show it. */
  const addSignal = (key) => commands.run('addToPlots', { on: plot.id, signals: [key], from: 'menu' });
  function filterKey(e) {
    // Escape and Tab go on to the menu, which closes; every other key stays here.
    if (e.key === 'Escape' || e.key === 'Tab') return;
    e.stopPropagation();
    const to = { ArrowDown: addActive + 1, ArrowUp: addActive - 1, PageDown: addActive + 8, PageUp: addActive - 8 }[e.key];
    if (to !== undefined) addActive = Math.max(0, Math.min(addable.length - 1, to));
    else if (e.key === 'Enter' && addable[addActive]) {
      addSignal(addable[addActive].key);
      addActive = Math.max(0, Math.min(addActive, addable.length - 2));
    } else return;
    e.preventDefault();
    tick().then(() => document.getElementById(`add-${plot.id}-${addActive}`)?.scrollIntoView({ block: 'nearest' }));
  }

  // A click, not a drag, on the plot selects it; with Shift it adds the plot
  // to the selection or takes it out, and does nothing else: no pan, and no
  // pick-up from the header. The capture sees the press before the header
  // or the plot takes it for a move or a pan. A menu opened anywhere on the
  // plot, by the right button or the keys, selects it first as a press on
  // its controls does, so the menu acts on what is selected.
  // A click is a Shift+click by Shift as it was at the press: Shift pressed
  // during a press snaps a dragged cursor, and its release selects as a
  // plain click does.
  let pressedAt = null, pressedShift = false;
  /** The plot's header and controls, a press on which selects the plot first: its grip and its header start the same move. */
  const CONTROLS = '.plot-head, button, [role=button], [role=combobox], input';
  function pressed(e) {
    pressedAt = [e.clientX, e.clientY];
    pressedShift = e.shiftKey;
    if (e.button === 0 && !e.shiftKey && e.target.closest?.(CONTROLS)) onPress?.();
    else onFocus();
  }
  function clicked(e) {
    if (!pressedAt || Math.hypot(e.clientX - pressedAt[0], e.clientY - pressedAt[1]) >= DRAG_START) return;
    if (e.target.closest('button, input, [role=menuitem]')) return;
    onSelect?.(pressedShift, e.detail);
  }

  let body = $state();
  let overlay = $state();
  let pv;

  // The readouts come from the plot view each frame; the page shows them a
  // few times a second, which is as fast as anyone reads.
  const LEGEND_MS = 66;
  let legend = $state({ rows: {}, cursors: null });

  // A plot too small for its header becomes a monitor, as plot.js's
  // `monitorOf` decides from the traces it draws.
  let size = $state({ width: Infinity, height: Infinity });
  // A bare plot, as a copy is (PlotHost `bare`), has no header, and is never one.
  const monitor = $derived(!host.bare && monitorOf(size, plot.mode, plot.items.filter((i) => session.trace(i.key)).length));

  /**
   * The row of chips as last measured, for the dashes under it while it runs
   * past an edge and for its +: whether it does, where the strip of dashes
   * lies in the header (its content box, `left` and `width` in px), the ids
   * of the chips out of view, as `outOfView` decides, and where in the
   * header the + lies (`plus`, its left in px).
   */
  let chipRow = $state.raw({ over: false, left: 0, width: 0, out: new Set(), plus: 0 });
  /** A dash under the row of chips, in px: its full width, its least, and the gap between two. */
  const DASH = { w: 14, min: 4, gap: 3 };
  /** How wide the fade at an edge of the row of chips is, in px, while the row runs past that edge: its `--fade`. */
  const FADE = 24;

  /** The + of the header that row `node` is in. */
  const plusOf = (node) => node?.parentElement?.querySelector(':scope > .add-signal') ?? null;

  /**
   * The part of row `node`'s client box that shows chips: all of it but the
   * room it keeps at its right end for the + (`--add`).
   */
  function rowView(node) {
    const b = node.getBoundingClientRect(), add = parseFloat(node.style.getPropertyValue('--add')) || 0;
    return { left: b.left, right: b.right - add, top: b.top, bottom: b.bottom, width: b.width - add };
  }

  /**
   * Marks a row of chips that runs past either edge, for the fade there, and
   * turns the wheel into a sideways scroll over it. The row is one line high
   * however many signals it holds, and spans the header's free room, so a
   * drop, a right-click or a press there is the row's. It keeps room at its
   * right end for the + (`--add`: the +'s width and the chips' gap), and the
   * + lies after its last chip, or the text an empty row shows, at the
   * chips' gap, or in that room while the row runs past its end. `chipRow`
   * follows it as it scrolls, resizes and its chips change.
   */
  function overflowEdges(node) {
    node.style.setProperty('--fade', `${FADE}px`);
    const head = node.parentElement;
    const mark = () => {
      const plus = plusOf(node), style = getComputedStyle(node), gap = parseFloat(style.columnGap) || 0;
      const add = `${gap + (plus?.offsetWidth ?? 0)}px`;
      if (node.style.getPropertyValue('--add') !== add) node.style.setProperty('--add', add);
      const left = node.scrollLeft > 1, right = node.scrollLeft + node.clientWidth < node.scrollWidth - 1;
      node.classList.toggle('more-left', left);
      node.classList.toggle('more-right', right);
      const over = node.scrollWidth > node.clientWidth + 1, padL = parseFloat(style.paddingLeft) || 0, padR = parseFloat(style.paddingRight) || 0;
      const chips = over ? [...node.querySelectorAll(':scope > .trace-chip')].map((c) => [c.id, c.getBoundingClientRect()]) : [];
      const out = outOfView(chips, rowView(node), { left: left ? FADE : 0, right: right ? FADE : 0 });
      const b = node.getBoundingClientRect(), last = [...node.querySelectorAll(':scope > .trace-chip, :scope > .trace-empty')].at(-1)?.getBoundingClientRect();
      const at = Math.min(last ? last.right + gap : b.left + padL, b.right - padL - (plus?.offsetWidth ?? 0));
      chipRow = { over, left: node.offsetLeft + padL, width: node.clientWidth - padL - padR, out, plus: at - head.getBoundingClientRect().left };
    };
    const wheel = (e) => {
      if (node.scrollWidth <= node.clientWidth || Math.abs(e.deltaX) > Math.abs(e.deltaY)) return;
      e.preventDefault();
      node.scrollLeft += e.deltaY;
    };
    // The row's own size, its header's, the +'s, and each chip's, as a value or a name changes its width.
    const sizing = new ResizeObserver(mark);
    const watch = () => [node, head, plusOf(node), ...node.children].forEach((el) => el && sizing.observe(el));
    watch();
    const chips = new MutationObserver(() => (watch(), mark()));
    chips.observe(node, { childList: true });
    node.addEventListener('scroll', mark, { passive: true });
    node.addEventListener('wheel', wheel, { passive: false });
    return {
      destroy() {
        sizing.disconnect();
        chips.disconnect();
        node.removeEventListener('scroll', mark);
        node.removeEventListener('wheel', wheel);
      },
    };
  }

  /**
   * The ids of the chips out of view in a row at client box `box` that fades
   * `fade.left` and `fade.right` px in from its edges, of `chips`, each
   * [id, client box]. A chip is in view when most of its width lies clear of
   * the fades, or when it holds as much of the row as any chip does, as one
   * covering most of the row does: while the row shows any chip, one is in view.
   */
  function outOfView(chips, box, fade) {
    const span = (r, a, b) => Math.max(0, Math.min(r.right, b) - Math.max(r.left, a));
    const most = Math.max(0, ...chips.map(([, r]) => span(r, box.left, box.right)));
    const shows = (r) => span(r, box.left + fade.left, box.right - fade.right) >= r.width / 2 || (most > 0 && span(r, box.left, box.right) >= most - 0.5);
    return new Set(chips.filter(([, r]) => !shows(r)).map(([id]) => id));
  }

  /**
   * Scrolls the row of chips just far enough that chip `id` is wholly in
   * view, clear of the fade at the edge when the row has room; a chip wider
   * than the row comes to its start.
   */
  function reveal(id) {
    const c = document.getElementById(`chip-${id}`);
    if (!c || !zone) return;
    const r = c.getBoundingClientRect(), box = rowView(zone);
    const clear = Math.max(0, Math.min(FADE, (box.width - r.width) / 2));
    if (r.width >= box.width) zone.scrollLeft += r.left - box.left;
    else if (r.left < box.left + clear) zone.scrollLeft -= box.left + clear - r.left;
    else if (r.right > box.right - clear) zone.scrollLeft += r.right - (box.right - clear);
  }

  // Whether the plot's view streams, for its play and pause button, and its window, for the time axis's window field.
  let streaming = $state(true);
  let windowSpan = $state(10);
  /** The widest window the plot's view can show now. */
  const widestWindow = () => view.spans(clock.now())[1];

  onMount(() => {
    // The window read is what the view can show of it, which grows as a hub fills.
    const readWindow = () => (windowSpan = view.windowShown(clock.now()));
    const poll = clock.every(LEGEND_MS, () => (readWindow(), pv?.legend && (legend = pv.legend)));
    pv = new PlotView({
      body,
      overlay,
      hub,
      view,
      // What a gesture on the plot moves: the views, and for a cursor the PlotViews, of the plots a command here acts on.
      targets: () => host.targets(plot.id).map((id) => host.view(id)),
      cursorTargets: (key) => cursorPeers(pv, host.targets(plot.id).map((id) => host.pv(id)).filter(Boolean), key),
      cursors,
      colors,
      onFollow: () => runHere('follow'),
      hoverTime: hover,
      plotId: plot.id,
      renderer,
      onFocus,
      onFrequencies: (lo, hi) => host.setView(plot.id, { fRange: [lo, hi] }),
      onFitY: (key) => runHere('fitYAxis', key),
      viewHistory,
      config: () => ({ kind: plot.kind, mode: plot.mode, sides: plot.axisSides, keys: drawnItems().map((i) => i.key), floorsHidden: drawnItems().filter((i) => i.floorHidden).map((i) => i.key), xLog: plot.xScale !== 'linear', yLog: plot.yScale !== 'linear', fRange: plot.fRange ?? null, yFormat: plot.yFormat ?? null, xFormat: plot.xFormat ?? null, grid: plot.grid ?? true, xTime: plot.xTime ?? null, readoutAt: overlayDrag?.place === 'readoutAt' ? null : atOf('readoutAt', READOUT_AT) }),
    });
    registry.add(pv);
    host.pickers.set(plot.id, openPicker);
    const sizing = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      size = { width, height };
    });
    sizing.observe(card);
    streaming = view.live;
    readWindow();
    const unwatch = view.onChange(() => ((streaming = view.live), readWindow()));
    return () => {
      pv.letGo();
      clock.cancel(poll);
      unwatch();
      sizing.disconnect();
      registry.delete(pv);
      if (host.pickers.get(plot.id) === openPicker) host.pickers.delete(plot.id);
    };
  });

  // While a chip is dragged over the header, the chips it shows, the drag
  // library's shadow among them; the plot's own stay as they are until the
  // drop, which the page lands as one step. dragOrigin: this plot's chips as
  // they were when one of them was picked up.
  let chips = $state.raw(null);
  let dragOrigin = $state.raw(null);
  const zoneItems = $derived(chips ?? plot.items);
  /** The signals the plot draws and gives axes to. */
  const drawnItems = () => plot.items.filter((i) => !i.hidden);
  // A chip being dragged is the drag library's shadow in the row it is over,
  // drawn as the chip faded and outlined at the place it would land, the
  // other chips trading places with it as the pointer crosses them. In its
  // own row the shadow takes the chip's own room, so the row keeps its width
  // and only its order changes. While the pointer is anywhere else, a shadow
  // of the row's own stands where the chip was, so the row keeps its chips in
  // place until the chip lands on another plot. That one has an id of its
  // own, so the chip coming back gets a new element, not the one the library
  // hid. Where the library adds its shadow beside that one, the row keeps the
  // library's.
  const HOME = 'home:'; // the id prefix of that shadow
  function consider(e) {
    const { trigger, id } = e.detail.info;
    if (trigger === TRIGGERS.DRAG_STARTED) {
      dragOrigin = [...plot.items];
      onTraceDragStart?.();
    }
    const items = e.detail.items;
    const shadows = items.filter((i) => i[SHADOW]);
    const shadow = shadows.find((i) => !i.id.startsWith(HOME)) ?? shadows[0];
    // A row the chip only passed over shows the plot's own chips again.
    if (!shadow) chips = dragOrigin ? dragOrigin.map((i) => (i.id === id ? { ...i, id: HOME + i.id, [SHADOW]: true } : i)) : null;
    else chips = items.filter((i) => !i[SHADOW] || i === shadow);
  }
  // A pointer drop ends in one finalize that is not DROPPED_INTO_ANOTHER:
  // the zone the chip landed in, or the one it came from when it landed in
  // none. That one lands it, with the chips as dropped when it landed among
  // them; the zone it left only lets go of what it showed.
  function finalize(e) {
    const { trigger, id } = e.detail.info;
    const items = e.detail.items.filter((i) => !i[SHADOW]);
    chips = null;
    dragOrigin = null;
    if (trigger === TRIGGERS.DROPPED_INTO_ANOTHER) return;
    const dropped = items.find((i) => i.id === id);
    if (dropped) onLandSignal?.(dropped, trigger === TRIGGERS.DROPPED_INTO_ZONE ? items : null);
  }
  // The plot body is a drop target of its own, which shows nothing: a signal
  // dropped there joins the header's chips, or a new plot when on an edge.
  let bodyItems = $state([]);
  function bodyConsider(e) {
    bodyItems = e.detail.items;
  }
  function bodyFinalize(e) {
    const dropped = e.detail.items.filter((i) => !i[SHADOW]);
    bodyItems = [];
    for (const item of dropped) onLandSignal?.(item, [...plot.items, item]);
  }
  function remove(id) {
    const item = plot.items.find((i) => i.id === id);
    if (item) onEdit(`Remove ${item.key}`, { items: plot.items.filter((i) => i !== item) });
  }
  // A signal the plot keeps but does not draw, as a legend hides a series.
  let menuChip = $state(null); // the signal a right-click on its chip opened the menu for
  let legendMenuOpen = $state(false);
  let legendKey = $state(null); // the signal a right-click on its legend row opened the legend's menu for
  const toggleHidden = (key) => onEdit(`${plot.items.find((i) => i.key === key)?.hidden ? 'Show' : 'Hide'} ${key}`, { items: plot.items.map((i) => (i.key === key ? { ...i, hidden: !i.hidden } : i)) });
  // Whether a spectrum draws the signal's noise floor line, which its chip's eye turns on and off.
  const toggleFloor = (key) => onEdit(`${plot.items.find((i) => i.key === key)?.floorHidden ? 'Show' : 'Hide'} ${key}'s noise floor`, { items: plot.items.map((i) => (i.key === key ? { ...i, floorHidden: !i.floorHidden } : i)) });
  /** The unit this plot shows signal `key` in (plot.js `shownUnit`), which every place in it that names the signal's unit names. */
  const unitsShown = (key) => shownUnit({ kind: plot.kind, mode: plot.mode, keys: drawnItems().map((i) => i.key), yFormat: plot.yFormat }, key, unitsOf);
  const showOnly = (key) => onEdit(`Show only ${key}`, { items: plot.items.map((i) => ({ ...i, hidden: i.key !== key })) });
  const showAll = () => onEdit('Show every signal', { items: plot.items.map((i) => ({ ...i, hidden: false })) });
  /** Whether `e` happened on the header itself or the empty part of its chips. */
  const empty = (e) => e.target === e.currentTarget || e.target.classList?.contains('trace-zone');

  // The keyboard. The plot is one stop in the page's Tab order; nothing in
  // it takes a stop of its own. Keys pressed while it has focus act on it,
  // and Enter moves into its signals, where the arrows move between them.
  // Which key does what is keymap.js's.
  let card = $state();
  let zone = $state();
  let kindOpen = $state(false);
  let kindFromKeys = false;
  /** Opens the kind picker, which takes the keys until it closes. */
  function openKind() {
    kindFromKeys = true;
    kindOpen = true;
  }
  /** Gives focus back to the plot when a kind menu opened from the keys closes, after the closing key is done. */
  function kindClosed(open) {
    if (open || !kindFromKeys) return;
    kindFromKeys = false;
    clock.after(0, () => card?.focus());
  }
  let active = $state(0); // the signal the arrows are on, while the signals have focus
  /**
   * Makes signal `key` the one the signals' keys act on, the signals
   * focused: what a right-click on its chip or its legend row does, so the
   * keys its menu shows act on it once the menu has closed. Returns `key`.
   */
  function makeCurrent(key) {
    const at = plot.items.findIndex((i) => i.key === key);
    if (at < 0) return key;
    active = at;
    zoneFocused = true;
    zone?.focus();
    return key;
  }
  let zoneFocused = $state(false);
  let addFromKeys = false;
  /**
   * Opens the add-signal menu under `below`, the header's + by default, as
   * a right-click on the row does. From the keys, closing it gives focus back
   * to the plot.
   */
  function openAdd(below = plusOf(zone) ?? zone, fromKeys = true) {
    const r = below.getBoundingClientRect();
    addFromKeys = fromKeys;
    zone.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: r.left + 4, clientY: r.bottom }));
  }
  let addInput = $state(null);
  /** Keeps the add-signal menu open on its filter once every signal is removed, as adding one does, to add others. */
  function keepAdding(e) {
    e.preventDefault();
    tick().then(() => addInput?.focus());
  }
  /** Gives focus back to the plot when a menu opened from the keys closes. */
  function signalsClosed(open) {
    if (open || !addFromKeys) return;
    addFromKeys = false;
    queueMicrotask(() => card?.focus());
  }

  /** Where a legend sits before it is dragged: the top right corner. */
  const LEGEND_AT = { x: 1, y: 0 };
  /** Where the cursors' readout sits before it is dragged: the top left corner. */
  const READOUT_AT = { x: 0, y: 0 };
  /** The space between the readout and the plot's edge, in px, as plot.css keeps it. */
  const READOUT_INSET = 4;
  /**
   * The space between the legend and the plot area's edges, in px, as
   * plot.css keeps it: its top clears the LIVE tag, and a monitor's header
   * while it shows.
   */
  const LEGEND_INSET = { side: 8, top: 22, monitorTop: 32 };
  /** How near, in px, a dragged legend or readout comes to an edge or the middle before it snaps there. */
  const LEGEND_SNAP = 12;
  /** The legend or readout being dragged, {place, at}: shown where the pointer has it, and kept once let go. */
  let overlayDrag = $state.raw(null);
  /** Where the legend or readout `place` sits: `plot[place]`, or where a drag has it, or else `fallback`. */
  const atOf = (place, fallback) => (overlayDrag?.place === place ? overlayDrag.at : plot[place]) ?? fallback;
  /**
   * Drags the legend or the cursors' readout within the plot, keeping where
   * it sits in `plot[place]`, as one edit when it is let go. That is a share
   * of the room around it, 0 to 1 across and down, so it stays in its place,
   * a corner or between, as the plot resizes. Across and down, it snaps to
   * either edge and the middle: the nine places of a corner, an edge's
   * middle or the centre.
   */
  function dragOverlay(e, place) {
    // Its drag is its own, not the plot's pan.
    e.stopPropagation();
    if (e.button !== 0 || e.target.closest('.x, .swatch')) return;
    e.preventDefault();
    // A menu opened on it stays where it opened, so picking it up closes that.
    readoutOpen = false;
    legendMenuOpen = false;
    menuOpen = false;
    const own = e.currentTarget.getBoundingClientRect();
    const [dx, dy] = [e.clientX - own.left, e.clientY - own.top];
    // The room it moves in, in client px: the legend's within the plot area, the readout's within the body.
    const b = body.getBoundingClientRect(), P = pv.geo?.plot;
    const room = place === 'legendAt' && P
      ? { l: P.x + LEGEND_INSET.side, t: Math.max(P.y + LEGEND_INSET.top, b.top + (monitor ? LEGEND_INSET.monitorTop : 0)), r: P.x + P.w - LEGEND_INSET.side, b: P.y + P.h - LEGEND_INSET.side }
      : { l: b.left + READOUT_INSET, t: b.top + READOUT_INSET, r: b.right - READOUT_INSET, b: b.bottom - READOUT_INSET };
    const share = (offset, room) => {
      if (room <= 0) return 0;
      const at = Math.min(1, Math.max(0, offset / room));
      return [0, 0.5, 1].find((a) => Math.abs(a - at) * room < LEGEND_SNAP) ?? at;
    };
    // The pointer is held to it, so the drag ends where it was let go, or
    // with nothing kept when the pointer is lost or Escape is pressed.
    const el = e.currentTarget, pointer = e.pointerId;
    // A press made by script has no pointer to hold.
    if (e.isTrusted) el.setPointerCapture(pointer);
    const move = (ev) => {
      const at = { x: share(ev.clientX - dx - room.l, room.r - room.l - own.width), y: share(ev.clientY - dy - room.t, room.b - room.t - own.height) };
      overlayDrag = { place, at };
    };
    const end = (keep) => {
      el.removeEventListener('pointermove', move);
      el.removeEventListener('pointerup', up);
      el.removeEventListener('lostpointercapture', lost);
      unclaim();
      if (el.hasPointerCapture(pointer)) el.releasePointerCapture(pointer);
      endOverlayDrag = null;
      const d = overlayDrag;
      overlayDrag = null;
      if (d && keep) onEdit(place === 'legendAt' ? 'Move the legend' : 'Move the readout', { [place]: d.at });
    };
    const up = () => end(true);
    const lost = () => end(false);
    const unclaim = claim('drag', { on: { cancel: () => end(false) } });
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerup', up);
    el.addEventListener('lostpointercapture', lost);
    endOverlayDrag = () => end(false);
  }
  /** Ends a legend or readout drag with nothing kept: the plot going away mid-drag. */
  let endOverlayDrag = null;
  onDestroy(() => endOverlayDrag?.());
  /** Opens this plot's picker for keymap.js plot action `action`, as the page asks when the keys act on this plot; says whether it has one. */
  function openPicker(action) {
    const open = { enterSignals: () => (plot.items.length ? ((active = 0), zone.focus()) : openAdd()), addSignals: () => openAdd(), kind: openKind }[action];
    open?.();
    return !!open;
  }
  // The keys of the plot's signals while they have focus (keymap.js, place
  // 'signals'); the rest go on to the plot. A key of theirs shows the
  // signal the arrows are on, however the row got focus.
  onDestroy(
    claim('signals', {
      when: (e, at) => at === zone,
      on: {
        move(e) {
          zoneFocused = true;
          active = stepIn(e, active, plot.items.length);
          document.getElementById(`chip-${plot.items[active]?.id}`)?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
        },
        remove() {
          zoneFocused = true;
          remove(plot.items[active]?.id);
          active = Math.min(active, plot.items.length - 1);
          if (!plot.items.length) card.focus();
        },
        leave: () => card.focus(),
      },
    }),
  );
  const description = $derived(`${kindLabel(plot.kind, plot.mode)} plot: ${plot.items.map((i) => i.key).join(', ') || 'no signals'}`);
</script>

<!-- A row's value in its trailing column, when it has one. -->
{#snippet value(text)}{#if text}<span class="menu-value">{text}</span>{/if}{/snippet}
<!-- An axis setting's row of an axis popover, as command `id` reads, made
     with `how`: a button for each value, pressed at `current`, this plot's. -->
{#snippet axisSetting(id, field, how, current)}
  {@const row = commands.row(id, { on: plot.id, how })}
  <div class="axis-row fill" role="group" aria-label={row.label}>
    <span>{row.label}</span>
    <div class="seg">
      {#each CHOICES[field] as [to, text]}
        <button class:on={current === to} aria-pressed={current === to} aria-label={AXIS_SETTINGS[field].names.get(to)} data-command={id} onclick={() => runHere(id, { ...how, to })}>{text}</button>
      {/each}
    </div>
  </div>
{/snippet}
<!-- An axis's sample line: `text`, a value of the axis as it writes its
     values, and − and + that step digits command `id`, made with `how`,
     from this plot's `current` through STEPS, stopping at either end. -->
{#snippet sampleLine(id, how, current, text)}
  {@const at = STEPS.indexOf(current)}
  <div class="axis-sample" role="group" aria-label="Digits">
    <output aria-label="Sample value">{text}</output>
    <span class="axis-step">
      <button aria-label="Fewer digits" data-command={id} disabled={at <= 0} onclick={() => runHere(id, { ...how, to: STEPS[at - 1] })}>−</button>
      <span>{current === null ? 'auto' : `${current} fig`}</span>
      <button aria-label="More digits" data-command={id} disabled={at >= STEPS.length - 1} onclick={() => runHere(id, { ...how, to: STEPS[at + 1] })}>+</button>
    </span>
  </div>
{/snippet}
<!-- A scale's two buttons in an axis popover, which run command `id` with
     `how`; the one of a log scale is pressed while `log`. -->
{#snippet scale(id, how, log)}
  <div class="seg">
    <button class:on={log} aria-pressed={log} data-command={id} onclick={() => runHere(id, { ...how, to: 'log' })}>Log</button>
    <button class:on={!log} aria-pressed={!log} data-command={id} onclick={() => runHere(id, { ...how, to: 'linear' })}>Linear</button>
  </div>
{/snippet}
<!-- A button of an axis popover that runs command `id` with `how` and
     closes the popover: its picture, its name and its tip, with the pointer
     gesture or the keys that run it too, as the command reads. -->
{#snippet tool(id, how)}
  {@const row = commands.row(id, { on: plot.id, how })}
  <Tip text={row.label} keys={row.gesture ?? row.show ?? undefined}>
    {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label={row.label} data-command={id} disabled={row.disabled} onclick={() => (runHere(id, how), (axisOpen = xAxisOpen = false))}><row.icon /></button>{/snippet}
  </Tip>
{/snippet}
<!-- The peak commands' rows, on a plot that shows a signal: each says where its X cursor would go. -->
{#snippet peakItems()}{#if plot.items.length}{#each PEAKS as id}<CommandItem {id} on={plot.id} />{/each}{/if}{/snippet}
<!-- A signal's own menu, on its chip and on its row of the legend: what
     it shows, how it looks, where it goes. `close` closes the menu it is in. -->
{#snippet signalMenu(key, close)}
  {@const item = plot.items.find((i) => i.key === key)}
  {@const hides = plot.items.filter((i) => i.key !== key && !i.hidden).length}
  <!-- Two rows: this signal alone or every signal, whichever it is not showing; and this signal hidden or shown. -->
  {#if !hides && !item?.hidden && plot.items.some((i) => i.hidden)}
    <ContextMenu.Item onSelect={showAll}><RiShowOnly class="menu-icon" />Show all</ContextMenu.Item>
  {:else}
    <ContextMenu.Item onSelect={() => showOnly(key)}><RiShowOnly class="menu-icon" />Show only this{@render value(hides ? `Hides ${hides}` : null)}</ContextMenu.Item>
  {/if}
  <ContextMenu.Item onSelect={() => toggleHidden(key)}>{#if item?.hidden}<RiShowOnly class="menu-icon" />Show{:else}<RiHide class="menu-icon" />Hide{/if}</ContextMenu.Item>
  <ContextMenu.Separator />
  <ContextMenu.Sub>
    <ContextMenu.SubTrigger><RiPalette class="menu-icon" />Colour<span class="menu-value"><i style="--c: {colorFor(key)}"></i>{#if colors.picked[key] === undefined}Auto{/if}</span></ContextMenu.SubTrigger>
    <ContextMenu.SubContent class="p-1.5"><ColorPicker {key} ondone={close} /></ContextMenu.SubContent>
  </ContextMenu.Sub>
  <CommandItem id="axisSide" on={plot.id} how={{ key }} />
  {#if place}
  <ContextMenu.Sub>
    <ContextMenu.SubTrigger><RiMove class="menu-icon" />Move to{@render value(counted(otherPlots.length, 'plot'))}</ContextMenu.SubTrigger>
    <ContextMenu.SubContent class="max-w-72">
      <ContextMenu.Item onSelect={() => onMoveSignal?.(key, null)}>New plot</ContextMenu.Item>
      {#if otherPlots.length}<ContextMenu.Separator />{/if}
      {#each otherPlots as p (p.id)}<ContextMenu.Item onSelect={() => onMoveSignal?.(key, p.id)}><span class="truncate">{p.label}</span></ContextMenu.Item>{/each}
    </ContextMenu.SubContent>
  </ContextMenu.Sub>
  {/if}
  <ContextMenu.Separator />
  <ContextMenu.Item variant="destructive" onSelect={() => item && remove(item.id)}><RiTrash class="menu-icon" />Remove<ContextMenu.Shortcut><kbd>Delete</kbd></ContextMenu.Shortcut></ContextMenu.Item>
{/snippet}
<div
  class:monitor
  class="plot-card flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden border bg-transparent {selected ? 'selected border-primary' : 'border-border'}"
  bind:this={card}
  onpointerdowncapture={pressed}
  oncontextmenucapture={() => onPress?.()}
  onclick={clicked}
  onfocusin={onFocus}
  role="group"
  tabindex="0"
  data-focus="plot"
  data-focus-arrows
  aria-label={description}
>
  <!-- The header picks the plot up from anywhere empty, as a title bar does. -->
  <div
    class="plot-head flex h-7 shrink-0 cursor-grab items-center gap-1.5 border-b bg-card px-1.5"
    ondblclick={(e) => empty(e) && runHere('maximize')}
    onpointerdown={(e) => !e.shiftKey && empty(e) && onStartMove(e)}
    role="toolbar"
    tabindex="-1"
  >
    <IconButton icon={RiDraggable} label="Drag onto a plot's edge to dock, its middle to swap, or a tab to move" class="grip cursor-grab touch-none text-muted-foreground" tabindex={-1} data-focus-skip onpointerdown={(e) => !e.shiftKey && onStartMove(e)} />
    <KindPicker {plot} bind:open={kindOpen} onOpenChange={kindClosed} />
    <!-- The plot's signals, each with its value at the hover, or its newest
         while live, unless the legend shows the values. Right-clicking them
         adds or removes signals. -->
    <ContextMenu.Root bind:open={signalsOpen} onOpenChange={signalsClosed}>
      <ContextMenu.Trigger>
        {#snippet child({ props })}
          <div
            {...props}
            bind:this={zone}
            class="trace-zone"
            role="listbox"
            data-focus="signals"
            data-focus-own
            aria-label="Signals in this plot"
            aria-activedescendant={zoneFocused && plot.items[active] ? `chip-${plot.items[active].id}` : undefined}
            use:overflowEdges
            use:dndzone={{ items: zoneItems, type: 'trace', flipDurationMs: 0, dropTargetStyle: {}, dropTargetClasses: [], delayTouchStart: true, zoneTabIndex: -1, zoneItemTabIndex: -1, autoAriaDisabled: true, useCursorForDetection: true, morphDisabled: true }}
            onconsider={consider}
            onfinalize={finalize}
            oncontextmenucapture={(e) => ((signalFilter = ''), (addActive = 0), (menuChip = makeCurrent(e.target.closest?.('.trace-chip')?.dataset.key ?? null)))}
            onfocus={(e) => (zoneFocused = e.currentTarget.matches(':focus-visible'))}
            onblur={() => (zoneFocused = false)}
          >
            {#each zoneItems as item, i (item.id)}
              {@const r = legend.rows[item.key]}
              <div id="chip-{item.id}" data-key={item.key} class:hidden-signal={item.hidden} class="trace-chip" class:ghost={item[SHADOW]} data-menu-for={signalsOpen && menuChip === item.key ? '' : undefined} class:unscaled={axisless(item.key)} role="option" aria-selected={zoneFocused && i === active} style="--c:{colorFor(item.key)}" title={axisless(item.key) ? 'No axis of its own: drag it beside the plot\'s left or right edge to give it one' : r?.info}>
                <Swatch key={item.key} />
                <span class="name">{item.key}</span>
                {#if unitsShown(item.key)}<span class="units" data-unit-of={item.key}>{unitsShown(item.key)}</span>{/if}
                {#if plot.kind === 'spectrum'}
                  <!-- A spectrum's chip shows its white noise floor, always,
                       the hover's values being in the legend and the tip; its
                       eye turns the floor's line on the plot on and off. -->
                  <span class="val floor" title={r?.floor === '—' ? 'No flat noise floor in this spectrum yet' : `White noise floor: ${r?.floor ?? ''} ${unitsShown(item.key)}`}>
                    <button class="eye" tabindex="-1" aria-pressed={!item.floorHidden} aria-label="{item.floorHidden ? 'Show' : 'Hide'} the noise floor of {item.key}" onclick={() => toggleFloor(item.key)}>
                      {#if item.floorHidden}<RiHide />{:else}<RiShowOnly />{/if}
                    </button>{r?.floor ?? ''}
                  </span>
                {:else if !plot.legend}
                  <!-- Always there, and as wide as the longest value, so a value that comes and goes on hover never moves the header. The legend, when shown, holds the values instead. -->
                  <span class="val">{r?.value ?? ''}</span>
                {/if}
                <button class="x" tabindex="-1" aria-label="Remove {item.key}" onclick={() => remove(item.id)}>×</button>
              </div>
            {/each}
            {#if !plot.items.length}<span class="trace-empty">Drop signals here, or right-click to add</span>{/if}
          </div>
        {/snippet}
      </ContextMenu.Trigger>
      <!-- While a row of signals runs past an edge: a dash per signal under it, in its
           colour and in its order, dim while its chip is out of view, as many
           as there are signals. The dashes share the row's width, down to a
           least width each, past which the strip fades at its end. A click
           brings the dash's chip into view. It lies over the header's foot,
           so it takes no room. -->
      {#if chipRow.over && zoneItems.length}
        {@const crowded = zoneItems.length * (DASH.min + DASH.gap) - DASH.gap > chipRow.width}
        <div class="chip-dashes" class:crowded role="group" aria-label="Signals in this plot, in view or not" style="left:{chipRow.left}px;width:{chipRow.width}px;gap:{DASH.gap}px">
          {#each zoneItems as item (item.id)}
            <button
              class="chip-dash"
              class:out={chipRow.out.has(`chip-${item.id}`)}
              class:hidden-signal={item.hidden}
              data-key={item.key}
              tabindex="-1"
              aria-label="Show {item.key}"
              style="--c:{colorFor(item.key)};flex-basis:{DASH.w}px;min-width:{DASH.min}px"
              onclick={() => reveal(item.id)}
            ></button>
          {/each}
        </div>
      {/if}
      <ContextMenu.Content class={menuChip ? 'menu-icons' : 'menu-icons w-64'}>
        {#if menuChip}
          {@render signalMenu(menuChip, () => (signalsOpen = false))}
        {:else}
        <div class="add-filter">
          <!-- svelte-ignore a11y_autofocus -->
          <input
            type="text"
            placeholder="Add a signal…"
            aria-label="Add a signal: type to filter"
            autofocus
            role="combobox"
            aria-expanded="true"
            aria-controls="add-{plot.id}"
            aria-activedescendant="add-{plot.id}-{addActive}"
            bind:this={addInput}
            bind:value={signalFilter}
            oninput={() => (addActive = 0)}
            onkeydown={filterKey}
          />
        </div>
        <ScrollArea class="signal-list">
        <div id="add-{plot.id}" role="listbox" aria-label="Signals to add">
          {#each addable as t, i (t.key)}
            <!-- The filter takes the keys for the whole list. -->
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <div
              id="add-{plot.id}-{i}"
              class="signal-row"
              class:active={i === addActive}
              role="option"
              aria-selected={i === addActive}
              tabindex="-1"
              data-command="addToPlots"
              onpointermove={() => (addActive = i)}
              onpointerdown={(e) => e.preventDefault()}
              onclick={() => addSignal(t.key)}
            >
              <span class="signal-option" style="--c:{colorOf(t, colors)}"><i></i>{t.key}<small>{t.meta.units}</small></span>
              <span class="rate">{t.meta.rate} Hz</span>
            </div>
          {:else}
            <div class="signal-none">{signalFilter ? 'No signal matches' : 'Every signal is shown'}</div>
          {/each}
        </div>
        </ScrollArea>
        {#if plot.items.length}
          <ContextMenu.Separator />
          <CommandItem id="clearSignals" on={plot.id} onran={keepAdding} />
        {/if}
        {/if}
      </ContextMenu.Content>
    </ContextMenu.Root>
    <!-- After the row's last chip, as the row places it. -->
    <IconButton icon={RiAddSignal} label="Add a signal" keys={commandOf('addSignals').show} class="add-signal" tabindex={-1} style="left:{chipRow.plus}px" onclick={(e) => openAdd(e.currentTarget, false)} />
    <div class="plot-actions flex shrink-0 items-center">
      <!-- Copy image, shown while the pointer is on the plot. -->
      {#if copyImageRow.offered}<IconButton icon={copyImageRow.icon} label={copyImageRow.label} class="copy-image" tabindex={-1} data-command="copyImage" onclick={() => runHere('copyImage')} />{/if}
      <IconButton
        icon={legendRow.icon}
        label={legendRow.label}
        keys={legendRow.show}
        class={plot.legend ? 'text-primary' : ''}
        aria-pressed={!!plot.legend}
        tabindex={-1}
        data-command="legend"
        onclick={() => runHere('legend')}
      />
      <IconButton
        icon={streaming ? RiPause : RiPlay}
        label={streaming ? 'Pause' : 'Follow live'}
        keys={commandOf('pause').show}
        class={streaming ? '' : 'text-primary'}
        tabindex={-1}
        data-command="pause"
        onclick={() => runHere('pause')}
      />
      <!-- Back and Forward through the views of the tab's plots (viewhistory.svelte.js). -->
      <IconButton icon={RiBack} label="Back to the view before" class="view-step" disabled={!viewHistory.canBack} tabindex={-1} onclick={() => viewHistory.back()} />
      <IconButton icon={RiForward} label="Forward to the view after" class="view-step" disabled={!viewHistory.canForward} tabindex={-1} onclick={() => viewHistory.forward()} />
      <IconButton icon={maximizeRow.icon} label={maximizeRow.label} keys={maximizeRow.show} class="maximize" tabindex={-1} data-command="maximize" onclick={() => runHere('maximize')} />
      <IconButton icon={closeRow.icon} label={closeRow.label} keys={closeRow.show} tabindex={-1} data-command="close" onclick={() => runHere('close')} />
    </div>
  </div>

  <!-- The x axis's settings, as a y axis's: a time axis's window and
       stream, a frequency axis's scale, and its cursors. -->
  <Popover.Root bind:open={xAxisOpen}>
    <Popover.Content customAnchor={axisAnchor} side="top" align="start" class="axis-pop w-auto gap-0 p-1.5">
      <div class="axis-head">
        {plot.kind === 'time' ? 'Time' : 'Frequency'}<span class="units">{plot.kind === 'time' ? 's' : 'Hz'}</span>
        {#if plot.kind === 'time' && host.live}
          <span class="axis-tools">
            <Tip text={view.live ? 'Pause' : 'Follow live'} keys={commandOf('pause').show}>
              {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label={view.live ? 'Pause' : 'Follow live'} data-command="pause" onclick={() => runHere('pause')}>{#if view.live}<RiPause />{:else}<RiPlay />{/if}</button>{/snippet}
            </Tip>
          </span>
        {/if}
      </div>
      {#if plot.kind === 'time'}
        <div class="axis-row">
          <span>Window</span>
          <WindowField data-command="window" value={windowSpan} onchange={(s, typed) => runHere('window', { s, typed })} widest={widestWindow} />
        </div>
      {:else}
        <!-- The range as a phrase, low to high, in Hz, with the fit to every bin beside it. -->
        <div class="axis-range" role="group" aria-label="Frequency range" data-command="fRange">
          <input aria-label="Lowest frequency shown" type="text" inputmode="decimal" bind:value={fBounds.lo} onkeydown={(e) => e.key === 'Enter' && applyFBounds() && (xAxisOpen = false)} onblur={applyFBounds} />
          <span>to</span>
          <input aria-label="Highest frequency shown" type="text" inputmode="decimal" bind:value={fBounds.hi} onkeydown={(e) => e.key === 'Enter' && applyFBounds() && (xAxisOpen = false)} onblur={applyFBounds} />
          {@render tool('fRange', { range: null })}
        </div>
        {#if fBoundsError}<div class="axis-error">{fBoundsError}</div>{/if}
        <div class="axis-row" role="group" aria-label="Frequency scale">
          <span>Scale</span>
          {@render scale('xScale', {}, plot.xScale !== 'linear')}
        </div>
      {/if}
      <div class="axis-row" role="group" aria-label="X cursors">
        <span>Cursors</span>
        {@render tool('addXCursors')}
        {@render tool('zoomToCursors')}
        {@render tool('clearXCursors')}
      </div>
      {#if plot.items.length}
        <!-- Each peak command's button, its tip saying where its X cursor would go, or why it goes nowhere. -->
        <div class="axis-row" role="group" aria-label="Peaks">
          <span>Peaks</span>
          {#each PEAKS as id}
            {@const row = commands.row(id, { on: plot.id })}
            <Tip text="{row.label}: {row.value}" keys={row.show}>
              {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label={row.label} data-command={id} disabled={row.disabled} onclick={() => (runHere(id), (xAxisOpen = false))}><row.icon /></button>{/snippet}
            </Tip>
          {/each}
        </div>
      {/if}
      {#if plot.kind === 'spectrum'}
        <!-- How the frequencies are written: a sample of them, and their notation. -->
        {@const set = axisFormat(plot.xFormat)}
        <div class="axis-sep"></div>
        {@render sampleLine('xDigits', {}, set.digits, (plot.xFormat, sampleAt === null ? '—' : pv?.fText()(sampleAt)))}
        {@render axisSetting('xNotation', 'notation', {}, set.notation)}
      {/if}
    </Popover.Content>
  </Popover.Root>

  <!-- A y axis's settings: the signal it scales, its bounds, a fit, and the
       signal's colour when the axis is one signal's. -->
  <Popover.Root bind:open={axisOpen}>
    <Popover.Content customAnchor={axisAnchor} side="right" align="start" class="axis-pop w-auto gap-0 p-1.5">
      <div class="axis-head">
        {#if axisKey}<i style="--c: {colorFor(axisKey)}"></i>{axisKey}{#if unitsShown(axisKey)}<span class="units" data-unit-of={axisKey}>{unitsShown(axisKey)}</span>{/if}{:else}Y axis{/if}
        {#if axisKey && plot.mode === 'independent'}
          {@const other = region.side === 'left' ? 'right' : 'left'}
          <span class="axis-tools">
            {@render tool('axisSide', { key: axisKey, to: other })}
            {@render tool('axisSide', { key: axisKey, to: 'none' })}
          </span>
        {/if}
      </div>
      <!-- The range as a phrase, low to high, with the fit beside it. -->
      <div class="axis-range" role="group" aria-label="Y axis range" data-command="yRange">
        <input aria-label="Lowest value shown" type="text" inputmode="decimal" bind:value={bounds.lo} onkeydown={boundsKey} onblur={applyBounds} />
        <span>to</span>
        <input aria-label="Highest value shown" type="text" inputmode="decimal" bind:value={bounds.hi} onkeydown={boundsKey} onblur={applyBounds} />
        {@render tool('fitYAxis', region.keys[0])}
      </div>
      {#if plot.kind === 'spectrum'}
        <div class="axis-row" role="group" aria-label="Density scale">
          <span>Scale</span>
          {@render scale('yScale', { key: region.keys[0] }, plot.yScale !== 'linear')}
        </div>
      {/if}
      <!-- This axis's Y cursors, which a pull up off the x axis also draws out. -->
      <div class="axis-row" role="group" aria-label="Y cursors">
        <span>Cursors</span>
        {@render tool('addYCursors', region.keys[0])}
        {@render tool('clearYCursors', { key: region.keys[0] })}
      </div>
      {#if boundsError}<div class="axis-error">{boundsError}</div>{/if}
      <!-- How the axis writes its numbers, its ticks, tags and readout alike, and is labelled. -->
      {@const key = region.keys?.[0]}
      {@const set = axisFormat(plot.yFormat?.[key])}
      {@const units = axisOpen && key !== undefined ? (pv?.axisUnits(key) ?? '') : ''}
      <div class="axis-sep"></div>
      {@render sampleLine('yDigits', { key }, set.digits, (plot.yFormat, sampleAt === null || key === undefined ? '—' : pv?.yValueText(key)(sampleAt)))}
      {@render axisSetting('yNotation', 'notation', { key }, set.notation)}
      {#if unitChoices(units).length}
        {@const row = commands.row('yUnit', { on: plot.id, how: { key } })}
        <div class="axis-row fill" role="group" aria-label={row.label}>
          <span>{row.label}</span>
          <Select.Root type="single" value={pv?.yFixed(key) ? String(set.unit) : 'auto'} onValueChange={(v) => runHere('yUnit', { key, to: v === 'auto' ? null : Number(v) })}>
            <Select.Trigger size="sm" class="axis-unit" data-command="yUnit" aria-label="Unit: {row.value}">{row.value}</Select.Trigger>
            <Select.Content>
              <Select.Item value="auto" label="Auto" />
              {#each unitChoices(units) as c}<Select.Item value={String(c.e)} label={c.text} />{/each}
            </Select.Content>
          </Select.Root>
        </div>
      {/if}
      {@render axisSetting('yLabel', 'label', { key }, set.label)}
      {#if axisKey}
        <div class="axis-sep"></div>
        <ColorPicker key={axisKey} ondone={() => (axisOpen = false)} />
      {/if}
    </Popover.Content>
  </Popover.Root>
  <ContextMenu.Root bind:open={menuOpen}>
    <ContextMenu.Trigger>
      {#snippet child({ props })}
        <div {...props} class="plot-body" bind:this={body} oncontextmenucapture={(e) => (recountCopy(), contextAt(e))}>
          <canvas bind:this={overlay}></canvas>
          <div class="axis-anchor" bind:this={axisAnchor} style="left: {axisAt.x}px; top: {axisAt.y}px"></div>
          <!-- The legend: each signal and its value, one short row apiece,
               no taller than part of the plot, where it was dragged to. A
               row's menu is its chip's. -->
          {#if plot.legend && plot.items.length}
            {@const at = atOf('legendAt', LEGEND_AT)}
            <div class="legend-layer" oncontextmenu={(e) => e.stopPropagation()}>
              <ContextMenu.Root bind:open={legendMenuOpen}>
                <ContextMenu.Trigger>
                  {#snippet child({ props })}
                    <div {...props} class="pinned-legend" data-focus-skip role="list" aria-label="Legend: drag to move" style="--x: {at.x}; --y: {at.y}" onpointerdowncapture={(e) => dragOverlay(e, 'legendAt')} oncontextmenucapture={(e) => (legendKey = makeCurrent(e.target.closest?.('.row')?.dataset.key ?? null))}>
                      {#each plot.items as item (item.id)}
                        <div class="row" role="listitem" data-key={item.key} style="--c:{colorFor(item.key)}" title={item.key}>
                          <!-- A bare plot's legend names its signals, by the name given them where there is one, as a picture of it holds no values. -->
                          <Swatch key={item.key} /><span class="name">{item.label || item.key}</span>{#if !host.bare}<span class="val">{legend.rows[item.key]?.value ?? ''}</span>{/if}<span class="units" data-unit-of={item.key}>{unitsShown(item.key)}</span>
                          {#if !host.bare}
                            <Tip text="Remove from this plot" keys={['Delete']}>
                              {#snippet children(tip)}<button {...tip} class="x" tabindex="-1" aria-label="Remove {item.key}" onclick={() => remove(item.id)}>×</button>{/snippet}
                            </Tip>
                          {/if}
                        </div>
                      {/each}
                    </div>
                  {/snippet}
                </ContextMenu.Trigger>
                <ContextMenu.Content class="menu-icons">
                  {#if legendKey}
                    {@render signalMenu(legendKey, () => (legendMenuOpen = false))}
                  {:else}
                    <CommandItem id="legend" on={plot.id} />
                  {/if}
                </ContextMenu.Content>
              </ContextMenu.Root>
            </div>
          {/if}
          <!-- The cursors' readout, a scope's: where X1 and X2 are, then each
               signal there; then each y axis's Y1 and Y2. Column 1 is X1 or
               Y1, 2 is X2 or Y2. It floats in the plot where it was dragged
               to, as the legend does, taking none of the plot's height, and
               its menu copies it or clears the cursors. It shows unless the
               plot hides it, `readout` false. -->
          {#if legend.cursors && plot.readout !== false}
            {@const c = legend.cursors}
            {@const extra = !!c.x?.rate || c.ys.some((y) => y.ratio)}
            {@const at = atOf('readoutAt', READOUT_AT)}
            <div class="readout-layer" oncontextmenu={(e) => e.stopPropagation()}>
              <ContextMenu.Root bind:open={readoutOpen}>
                <ContextMenu.Trigger>
                  {#snippet child({ props })}
                    <div {...props} class="cursor-readout" data-focus-skip role="group" aria-label="Cursor readout: drag to move" style="--x: {at.x}; --y: {at.y}" onpointerdowncapture={(e) => dragOverlay(e, 'readoutAt')}>
                      <table>
                        <thead>
                          <tr>
                            <th></th>
                            <th class="a">1</th>
                            <th class="b">2</th>
                            <th>Δ = 2 − 1</th>
                            {#if extra}<th></th>{/if}
                          </tr>
                        </thead>
                        <tbody>
                          {#if c.x}
                            <tr class="position">
                              <th>X <var>{c.x.v}</var></th>
                              <td>{c.x.a}</td>
                              <td>{c.x.b}</td>
                              <td>{c.x.d}</td>
                              {#if extra}<td class="rate">{#if c.x.rate}<span>1/Δ<var>t</var></span> {c.x.rate}{/if}</td>{/if}
                            </tr>
                            {#each plot.items as item (item.id)}
                              {@const r = legend.rows[item.key]}
                              <tr>
                                <th style="--c:{colorFor(item.key)}"><i></i>{item.key}</th>
                                <td>{r?.a ?? '—'}</td>
                                <td>{r?.b ?? '—'}</td>
                                <td>{r?.d ?? '—'}</td>
                                {#if extra}<td></td>{/if}
                              </tr>
                            {/each}
                          {/if}
                          {#each c.ys as y (y.key)}
                            <tr class="position">
                              <th style={y.color ? `--c:${y.color}` : ''}>Y {#if y.color}<i></i>{/if}{y.name}</th>
                              <td>{y.a}</td>
                              <td>{y.b}</td>
                              <td>{y.d}</td>
                              {#if extra}<td class="rate">{y.ratio}</td>{/if}
                            </tr>
                          {/each}
                        </tbody>
                      </table>
                    </div>
                  {/snippet}
                </ContextMenu.Trigger>
                <ContextMenu.Content class="menu-icons">
                  <CommandItem id="copyReadout" on={plot.id} />
                  <CommandItem id="zoomToCursors" on={plot.id} />
                  {@render peakItems()}
                  <ContextMenu.Separator />
                  <CommandItem id="clearXCursors" on={plot.id} />
                  <CommandItem id="clearYCursors" on={plot.id} />
                  <CommandItem id="clearCursors" on={plot.id} />
                </ContextMenu.Content>
              </ContextMenu.Root>
            </div>
          {/if}
          <div
            class="legend-zone"
            use:dndzone={{ items: bodyItems, type: 'trace', flipDurationMs: 0, dropTargetStyle: {}, dropTargetClasses: [], dropAnimationDisabled: true, morphDisabled: true, zoneTabIndex: -1, zoneItemTabIndex: -1 }}
            onconsider={bodyConsider}
            onfinalize={bodyFinalize}
          >
            {#each bodyItems as item (item.id)}<div class="body-ghost"></div>{/each}
          </div>
        </div>
      {/snippet}
    </ContextMenu.Trigger>
    <ContextMenu.Content class="menu-icons">
      <!-- Four groups, each under its name, the most asked for first: the
           plot's data, its cursors, how it shows its signals, and where it
           is; then closing it. Copy data says first when a copy would hold
           too much or come back short, from the hub's count of the rows. -->
      {#if plot.items.length && place}
        <ContextMenu.Label>Data</ContextMenu.Label>
        <CommandItem id="copyData" on={plot.id} how={copyCheck} />
        <CommandItem id="copyImage" on={plot.id} />
        <CommandItem id="exportData" on={plot.id} />
      {/if}
      <ContextMenu.Label>Cursors</ContextMenu.Label>
      <!-- Cursors are dragged out of the axes; these place a pair in view, Add Y cursors on the axis under the right-click. -->
      <CommandItem id="addXCursors" on={plot.id} />
      {#if plot.items.length}
        <!-- The peak commands in a submenu of their own, so the plot's menu stays short. -->
        <ContextMenu.Sub>
          <ContextMenu.SubTrigger><RiPeak class="menu-icon" />X cursors to peaks</ContextMenu.SubTrigger>
          <ContextMenu.SubContent class="menu-icons">{@render peakItems()}</ContextMenu.SubContent>
        </ContextMenu.Sub>
      {/if}
      <CommandItem id="addYCursors" on={plot.id} how={region.keys?.[0]} />
      <CommandItem id="clearCursors" on={plot.id} />
      <CommandItem id="readout" on={plot.id} />
      <ContextMenu.Label>Show</ContextMenu.Label>
      <CommandItem id="setKind" on={plot.id} />
      <CommandItem id="setLayout" on={plot.id} />
      <!-- Where the plot is among the page's plots and tabs, on a page that lays them out. -->
      {#if place}
      <ContextMenu.Label>Arrange</ContextMenu.Label>
      <CommandItem id="splitRight" on={plot.id} />
      <CommandItem id="splitDown" on={plot.id} />
      <CommandItem id="maximize" on={plot.id} />
      <CommandItem id="moveToTab" on={plot.id} />
      <ContextMenu.Separator />
      <CommandItem id="close" on={plot.id} />
      {/if}
    </ContextMenu.Content>
  </ContextMenu.Root>
</div>

<script>
  // Exporting plots, as they were when the dialog opened: their data, every
  // raw sample of their signals as the hub writes it (CSV or HDF5), or an
  // image of each, PNG or JPEG. Each plot exported is a paused copy of the
  // page's (PlotHost `copy`), drawn by the plot's own code on a stage
  // (PlotStage.svelte) and edited by its own commands, so the figure is the
  // plot and what is done to it here leaves the page's as it was.
  //
  // The plots are at the left, each ticked to be exported and picked to be
  // shown; the figure in the middle, as its image will be, with the files
  // that will be written under it; and an inspector at the right: on the
  // Data tab, the time and the file; on the Image tab, what every image
  // shares (All: the time, the size, the x axis, what shows and how the
  // files are saved) and what belongs to the plot picked (Plot,
  // ExportPlotPane.svelte). Save asks where the files go at the press, as a
  // browser asks only shortly after one, then makes them, a bar showing how
  // far, with Cancel, and writes each there once it is whole.
  import { getContext, untrack } from 'svelte';
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import * as Tooltip from '$lib/components/ui/tooltip/index.js';
  import PlotStage from './PlotStage.svelte';
  import ExportPlotPane from './ExportPlotPane.svelte';
  import { FORMATS, exportName, exportStamp, fetchExport, filesText, askWhere, giveUp, putFiles } from './exportfile.js';
  import { IMAGE_FORMATS, LEAST_SIDE, SHAPES, UNITS, bandsOf, canvasFits, devicesText, fileSafe, pictureSize, stampText } from './figure.js';
  import { xOf } from './cursors.js';
  import { timeAxis, titleOf } from './plot.js';
  import { prefs } from './prefs.svelte.js';
  import { api } from './protocol.js';
  import { bytesEstimate, bytesText, spanText } from './units.js';
  import { toast } from './toast.svelte.js';
  import * as clock from './clock.js';

  /** `what` is the export to offer, {ids}, the plots of the page's open tab to export, or null. */
  let { what = $bindable(null) } = $props();
  const { hub, plots } = getContext('page');

  /** The stretches of time the figure and the files can hold, by value: their names. */
  const RANGES = { shows: 'As the plot shows', all: 'All captured', cursors: 'Between X cursors' };
  /** About how many bytes a sample takes in each data format, for the files' sizes. */
  const SAMPLE_BYTES = { csv: 14, h5: 8 };
  /** About how many bytes a pixel takes in each image format. */
  const PIXEL_BYTES = { png: 0.15, jpg: 0.09 };
  /** The tallest the figure shows, in CSS px. */
  const PREVIEW_HEIGHT = 340;
  /**
   * The most room a thumbnail takes, in CSS px. A thumbnail is its plot's
   * picture drawn at the size it shows, of its plot's shape, not a larger
   * one scaled down, which thins its lines away.
   */
  const THUMB = { w: 116, h: 96 };

  // What one opening exports: each plot, its copy, and what the page showed.
  let entries = $state([]);
  let openedAt = $state(0), openedWall = $state(0);
  let at = $state(0);
  const open = $derived(!!what);
  const current = $derived(entries[at] ?? null);
  const chosen = $derived(entries.filter((e) => e.on));
  const several = $derived(entries.length > 1);

  // The choices, as last left while the page is open; each opening starts on the Data tab.
  let tab = $state('data');
  let pane = $state('all');
  let range = $state('shows');
  let format = $state('csv');
  let files = $state('each');
  let imageFormat = $state('png');
  let shape = $state('screen');
  let unit = $state('px');
  let dpi = $state(300);
  let width = $state(0), height = $state(0);
  let background = $state(prefs.dark ? 'dark' : 'white');
  let xTime = $state('clock');
  let xLabel = $state('Time');
  let shows = $state({ grid: true, stamp: true, device: true });

  $effect(() => {
    const ids = what?.ids;
    if (ids) untrack(() => begin(ids));
    return () => untrack(end);
  });

  /** Makes the copies of plots `ids` and reads what the page shows of them now. */
  function begin(ids) {
    const session = hub.session, perf = clock.now();
    openedAt = hub.now(perf);
    openedWall = clock.wall();
    entries = ids.flatMap((id) => {
      const plot = plots.plot(id);
      if (!plot) return [];
      const copy = plots.copy(id), cid = copy.id, pv = plots.pv(id);
      const traces = plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean);
      const oldest = Math.min(...traces.map((t) => session.memory?.oldest?.[t.meta.id] ?? Infinity));
      const { a, b } = xOf(plot.cursors, 'time');
      return [{
        id, copy, cid, traces, kind: plot.kind, on: true, title: '', yRanges: {},
        thumb: pv?.drawnOn ? thumbOf(id, pv.drawnOn) : null,
        screen: pv?.drawnOn ? { w: Math.round(pv.drawnOn.w), h: Math.round(pv.drawnOn.h) } : { w: 640, h: 300 },
        shows: plot.kind === 'time' ? copy.view(cid).range(openedAt) : null,
        captured: plot.kind === 'time' ? copy.view(cid).limits(perf) : null,
        oldest: isFinite(oldest) ? oldest : -Infinity,
        cursors: a !== null && b !== null && a !== b ? [Math.min(a, b), Math.max(a, b)] : null,
      }];
    });
    at = 0;
    tab = 'data';
    pane = 'all';
    progress = { busy: null };
    const s = entries[0]?.screen ?? { w: 640, h: 300 };
    if (shape === 'screen' || !width) [width, height] = [s.w, s.h];
    if (!entries.some((e) => e.cursors) && range === 'cursors') range = 'shows';
  }

  /** Ends the opening: any export under way stops, and the copies go. */
  function end() {
    stop();
    for (const e of entries) e.copy.dispose();
    entries = [];
  }

  /** Page plot `id`'s thumbnail, its body `body` in CSS px: {src, w, h}, its picture at the size it shows, w by h CSS px within THUMB. */
  function thumbOf(id, body) {
    const k = Math.min(THUMB.w / body.w, THUMB.h / body.h);
    return { src: plots.picture(id, { ratio: k * screenRatio }).toDataURL('image/png'), w: body.w * k, h: body.h * k };
  }

  /**
   * The stretch of time plot `e` shows as `range` picks, or with `data`,
   * asks the hub for, or null for a spectrum, which shows no stretch: all
   * captured is all the plot can show, and of the data, from the oldest the
   * hub holds to when the dialog opened, as the export always asked.
   */
  function stretchOf(e, which = range, data = false) {
    if (e.kind !== 'time') return null;
    if (which === 'all') return data || !e.captured ? [e.oldest, openedAt] : e.captured;
    if (which === 'cursors' && e.cursors) return e.cursors;
    return e.shows;
  }
  // The copies show the stretch picked, the whole capture from where the history begins.
  $effect(() => {
    for (const e of entries) {
      const s = stretchOf(e);
      if (s) untrack(() => e.copy.view(e.cid).show(isFinite(s[0]) ? s[0] : (e.shows?.[0] ?? s[1] - 10), s[1], clock.now()));
    }
  });
  // The copies look as the tab shows them: as the page does on Data, as the image asks on Image.
  $effect(() => {
    const image = tab === 'image';
    const look = { dark: image ? background === 'dark' : null, grid: image ? shows.grid : true, xTime: image && xTime === 'secs' ? 'seconds' : null };
    untrack(() => {
      for (const e of entries) {
        e.copy.colors.dark = look.dark;
        const plot = e.copy.plot(e.cid);
        if ((plot.grid ?? true) !== look.grid || (plot.xTime ?? null) !== look.xTime) e.copy.edit(e.cid, 'The image’s look', { grid: look.grid, xTime: look.xTime });
      }
    });
  });

  /** The device line of plot `e`'s caption: each device of its signals by name and serial, or route. */
  function deviceOf(e) {
    const devices = hub.session.devices;
    return devicesText(e.traces.map((t) => {
      const d = devices?.get(t.meta.route);
      return d?.name ? [d.name, d.serial].filter(Boolean).join(' ') : t.meta.route;
    }));
  }
  /** Plot `e`'s figure: its title, the x label and the caption, as the image's settings ask. */
  function figureOf(e) {
    const s = stretchOf(e);
    const right = shows.stamp && s && isFinite(s[0]) ? stampText(s[0], s[1], e.traces[0]?.meta.epoch) : '';
    const left = shows.device ? deviceOf(e) : '';
    return { title: e.title.trim(), xLabel: xLabel.trim(), caption: left || right ? { left, right } : null };
  }

  // The image's size: `width` by `height` CSS px, shown in `unit`.
  const sized = $derived(pictureSize({ width, height, unit, dpi }));
  const aspectOf = (s) => (s === 'screen' ? (current ? current.screen.w / current.screen.h : 2) : SHAPES.find((x) => x[0] === s)[2]);
  const inUnit = (v) => (unit === 'px' ? Math.round(v) : +(v / UNITS[unit]).toFixed(2));
  function setSide(side, typed) {
    const v = Math.max(LEAST_SIDE, Math.round(Number(typed) * UNITS[unit] * 100) / 100);
    if (!isFinite(v)) return;
    if (side === 'w') {
      width = v;
      const a = aspectOf(shape);
      if (a) height = Math.round((width / a) * 100) / 100;
    } else {
      height = v;
      shape = 'custom';
    }
  }
  function setShape(s) {
    shape = s;
    const a = aspectOf(s);
    if (s === 'screen' && current) [width, height] = [current.screen.w, current.screen.h];
    else if (a) height = Math.round((width / a) * 100) / 100;
  }
  // As on screen, the size is the picked plot's on the page.
  $effect(() => {
    const s = current?.screen;
    if (s && shape === 'screen') untrack(() => ([width, height] = [s.w, s.h]));
  });

  // What the figure shows: the plot picked, at its image's size on Image, at its size on the page on Data.
  let mainWidth = $state(600);
  const layout = $derived(tab === 'image' ? { width, height, ratio: sized.ratio } : { width: current?.screen.w ?? 640, height: current?.screen.h ?? 300, ratio: 1 });
  const figure = $derived(tab === 'image' && current ? figureOf(current) : null);
  const scale = $derived(Math.min(1, (mainWidth - 30) / layout.width, PREVIEW_HEIGHT / layout.height));
  const screenRatio = typeof window === 'undefined' ? 1 : window.devicePixelRatio || 1;

  /** Why the image cannot be made at its size, or null: past what the browser draws. */
  let refused = $state(null);
  $effect(() => {
    void [width, height, sized.ratio, figure, files, chosen.length, tab];
    if (tab !== 'image' || !current) return void (refused = null);
    const ask = () => {
      const cut = current.copy.pictureRefused(current.cid, { width, height, ratio: sized.ratio, figure });
      const stack = files === 'one' && chosen.length > 1 && !canvasFits(sized.pixels[0], sized.pixels[1] * chosen.length) ? 'Too large for this browser' : null;
      refused = current.copy.renderer ? (cut ?? stack) : null;
    };
    const handle = clock.after(0, ask);
    return () => clock.cancel(handle);
  });

  // How many samples each plot's signals hold in its stretch, as the hub counts them, by entry and range.
  let counts = $state({});
  /** The counts asked for, by key, so each is asked once. */
  const asked = new Set();
  $effect(() => {
    const asks = chosen.map((e) => [e, stretchOf(e, range, true) ?? [e.oldest, openedAt]]);
    for (const [e, [t0, t1]] of asks) {
      const key = `${e.cid}|${t0}|${t1}`;
      if (asked.has(key)) continue;
      asked.add(key);
      Promise.all(e.traces.filter(streams).map((t) => api.rows({ hub: hub.session.id, id: t.meta.id, t0, t1, limit: 0 }))).then(
        (replies) => (counts = { ...counts, [key]: replies.reduce((n, r) => n + r.count, 0) }),
        () => {},
      );
    }
  });
  const countOf = (e, t0, t1) => counts[`${e.cid}|${t0}|${t1}`] ?? null;
  /** Whether trace `t` is a signal that streams, whose samples the hub holds and writes to a file: a capture is none. */
  const streams = (t) => !t.shots;

  /**
   * The data files the choices make, each {name, request, samples, holds}:
   * one of every signal of the plots ticked over the time they span, or one
   * each. One file of them is the request the hub always had for them: each
   * signal once, from the earliest to the latest time the plots show, or the
   * whole capture with no time plot among them. A plot of captures has no
   * part in them, as the hub holds no capture.
   */
  const dataFiles = $derived.by(() => {
    const held = chosen.filter((e) => e.traces.some(streams));
    if (!held.length) return [];
    const stamp = exportStamp(new Date(openedWall)), ext = FORMATS[format].ext;
    const ask = (es) => {
      const traces = [...new Map(es.flatMap((e) => e.traces.filter(streams)).map((t) => [t.key, t])).values()];
      const spans = es.map((e) => stretchOf(e, range, true)).filter(Boolean);
      const whole = [Math.min(...es.map((e) => e.oldest)), openedAt];
      const [t0, t1] = spans.length ? [Math.min(...spans.map((s) => s[0])), Math.max(...spans.map((s) => s[1]))] : whole;
      const samples = es.reduce((n, e) => (n === null || countOf(e, ...(stretchOf(e, range, true) ?? whole)) === null ? null : n + countOf(e, ...(stretchOf(e, range, true) ?? whole))), 0);
      return { request: { hub: hub.session.id, ids: traces.map((t) => t.meta.id), t0, t1 }, samples, holds: traces.length === 1 ? traces[0].key : `${traces.length} signals`, keys: traces.map((t) => t.key) };
    };
    if (files === 'one' || held.length === 1) return [{ name: exportName(format, new Date(openedWall)), ...ask(held) }];
    return unique(held.map((e) => ({ name: `twinleaf-${stamp}-${fileSafe(e.traces.map((t) => t.key).join('+'))}.${ext}`, ...ask([e]) })));
  });
  /** The image files the choices make, each {name, holds, entries}: one image of the plots ticked, one above another, or one each. */
  const imageFiles = $derived.by(() => {
    if (!chosen.length) return [];
    const stamp = exportStamp(new Date(openedWall)), ext = IMAGE_FORMATS[imageFormat].ext;
    if (files === 'one' || chosen.length === 1) return [{ name: `twinleaf-${stamp}.${ext}`, holds: chosen.length > 1 ? `${chosen.length} plots` : signalsText(chosen[0]), entries: chosen }];
    return unique(chosen.map((e) => ({ name: `twinleaf-${stamp}-${fileSafe(e.traces.map((t) => t.key).join('+'))}.${ext}`, holds: signalsText(e), entries: [e] })));
  });
  const signalsText = (e) => (e.traces.length === 1 ? '1 signal' : `${e.traces.length} signals`);
  /** `list` with each name made unique by a count. */
  function unique(list) {
    const seen = new Map();
    return list.map((f) => {
      const n = (seen.get(f.name) ?? 0) + 1;
      seen.set(f.name, n);
      return n === 1 ? f : { ...f, name: f.name.replace(/(\.\w+)$/, `-${n}$1`) };
    });
  }
  const filesShown = $derived(tab === 'image' ? imageFiles : dataFiles);
  const sizeOf = (f) => (tab === 'image' ? sized.pixels[0] * sized.pixels[1] * f.entries.length * PIXEL_BYTES[imageFormat] : f.samples === null ? null : f.samples * SAMPLE_BYTES[format]);

  /** The span the figure's plot shows, as the inspector reads it. */
  const span = $derived.by(() => {
    const s = current && stretchOf(current);
    if (!s || !isFinite(s[0])) return null;
    const time = timeAxis(current.traces[0]?.meta.epoch);
    return { from: time.readout(s[0]), to: time.readout(s[1]), long: spanText(s[1] - s[0]) };
  });
  const madeAt = $derived(openedWall ? timeAxis('unix').readout(openedWall / 1000).slice(0, 8) : '');

  // Saving: {busy} while the files are made, `busy` how far, 0 to 1, with
  // `said` what is being made.
  let progress = $state({ busy: null });
  let abort = null;
  /** Stops an export under way; the files made so far go, and the place picked for them is left as it was. */
  function stop() {
    abort?.abort();
    abort = null;
    progress = { busy: null };
  }
  /** Waits for the next frame. */
  const nextFrame = () => new Promise((done) => clock.frame(done));

  /**
   * Saves the files the choices make, called by the press: it asks where
   * first, while the press lets the browser ask, then makes them, the bar
   * showing how far, and writes each there once it is whole, then closes.
   * Nothing is made when nothing is picked; when the making stops or fails,
   * the place picked is left as it was.
   */
  async function save() {
    const tabNow = tab, saving = tabNow === 'image' ? imageFiles : dataFiles;
    const type = tabNow === 'image' ? IMAGE_FORMATS[imageFormat] : FORMATS[format];
    let where;
    try {
      where = await askWhere(saving.map((f) => ({ name: f.name, type })));
    } catch (error) {
      return void toast.error(`Could not save: ${error.message}`);
    }
    if (!where) return;
    const job = (abort = new AbortController());
    progress = { busy: 0, said: '' };
    try {
      const made = tabNow === 'image' ? await makeImages(saving, job) : await makeData(saving, job);
      if (job.signal.aborted) return void (await giveUp(where));
      const saved = await putFiles(where, made);
      progress = { busy: null };
      what = null;
      const bytes = saved.reduce((n, f) => n + f.bytes, 0);
      toast.success(`Saved ${saved.length === 1 ? saved[0].name : filesText(saved.length)}, ${bytesText(bytes)}`);
    } catch (error) {
      await giveUp(where);
      if (job.signal.aborted) return;
      progress = { busy: null };
      toast.error(`Could not export: ${error.message}`);
    }
  }

  /** Fetches each data file of `list` from the hub, the bar following the bytes against the size each is about. */
  async function makeData(list, job) {
    const type = FORMATS[format], made = [], about = list.reduce((n, f) => n + Math.max(1, sizeOf(f) ?? 1), 0);
    let before = 0;
    for (const f of list) {
      const blob = await fetchExport(f.request, format, f.name, {
        signal: job.signal,
        progress: (bytes) => !job.signal.aborted && (progress = { busy: Math.min(0.97, (before + bytes) / about), said: bytesText(before + bytes) }),
      });
      before += blob.size;
      made.push({ name: f.name, blob, type });
    }
    return made;
  }

  /** Draws each image file of `list`, plot by plot, each on the stage at its full size, one above another in a file of several. */
  async function makeImages(list, job) {
    const type = IMAGE_FORMATS[imageFormat], made = [], all = list.flatMap((f) => f.entries), back = at;
    let done = 0;
    try {
      for (const f of list) {
        const pictures = [];
        for (const e of f.entries) {
          progress = { busy: done / all.length, said: `Image ${done + 1} of ${all.length}` };
          at = entries.indexOf(e);
          pictures.push(await pictureOf(e, job));
          if (job.signal.aborted) return made;
          done++;
        }
        const out = pictures.length === 1 ? pictures[0] : stacked(pictures);
        const blob = await new Promise((done) => out.toBlob(done, type.mime, type.quality));
        if (!blob) throw new Error('the browser made no image');
        made.push({ name: f.name, blob, type });
      }
    } finally {
      at = back;
    }
    progress = { busy: 1, said: '' };
    return made;
  }
  /** Plot `e`'s image at its full size, once its stage shows it and its windows of the hub's history have come. */
  async function pictureOf(e, job) {
    for (let n = 0; n < 600 && !job.signal.aborted; n++) {
      await nextFrame();
      if (e.copy.pv(e.cid) && n >= 2 && !hub.session.reading(e.cid)) break;
    }
    const fig = figureOf(e), why = e.copy.pictureRefused(e.cid, { width, height, ratio: sized.ratio, figure: fig });
    if (why) throw new Error(why.toLowerCase());
    // A figure lies on the plot's background, which a JPEG, holding no transparency, needs.
    return e.copy.picture(e.cid, { ratio: sized.ratio, figure: fig });
  }
  /** Pictures `list`, one above another, on one canvas. */
  function stacked(list) {
    const c = document.createElement('canvas');
    c.width = Math.max(...list.map((p) => p.width));
    c.height = list.reduce((n, p) => n + p.height, 0);
    const ctx = c.getContext('2d');
    let y = 0;
    for (const p of list) {
      ctx.drawImage(p, 0, y);
      y += p.height;
    }
    return c;
  }

  // The legend and the cursors' readout drag on the figure as on the plot:
  // where each shows on the figure there is a handle, whose presses go on
  // to that element of the plot the figure is drawn from, at the place on
  // that plot they land, so the plot's own drag moves it, snapping as it
  // does there.
  let fig = $state(null);
  /** What a handle stands over, by name: the plot's element it forwards to. */
  const LAID_OVER = { legend: '.pinned-legend', readout: '.cursor-readout' };
  /** Each handle's box on the figure, by name, {left, top, width, height} in CSS px, for those that show. */
  let handles = $state({});
  $effect(() => {
    if (tab !== 'image' || !fig) return void (handles = {});
    const handle = clock.every(50, () => {
      const shown = fig.querySelector('.stage-shown'), next = {};
      for (const name of Object.keys(LAID_OVER)) {
        const el = laidOver(name);
        if (!el || !shown) continue;
        const [r, s, f, at] = [el.getBoundingClientRect(), shown.getBoundingClientRect(), fig.getBoundingClientRect(), placeOf()];
        next[name] = { left: s.left - f.left + (r.left - at.x) * scale, top: s.top - f.top + (r.top - at.y + at.top) * scale, width: r.width * scale, height: r.height * scale };
      }
      if (JSON.stringify(next) !== JSON.stringify(handles)) handles = next;
    });
    return () => clock.cancel(handle);
  });
  /** The element `name` of LAID_OVER of the plot the figure is drawn from, or null. */
  const laidOver = (name) => fig?.querySelector(`.stage-box ${LAID_OVER[name]}`) ?? null;
  /** Where the figure's plot lies: its body's top left in client px, and the room the figure's title takes above it, in CSS px. */
  const placeOf = () => {
    const body = fig.querySelector('.stage-box').getBoundingClientRect();
    return { x: body.left, y: body.top, top: figure ? bandsOf(figure, layout.width).top : 0 };
  };
  /** Sends pointer event `e` on handle `name` on to that element of the plot, at the place on the plot it lands. */
  function forward(name, e) {
    const el = laidOver(name), shown = fig.querySelector('.stage-shown');
    if (!el || !shown) return;
    const s = shown.getBoundingClientRect(), at = placeOf();
    const clientX = at.x + (e.clientX - s.left) / scale, clientY = at.y + (e.clientY - s.top) / scale - at.top;
    if (e.type === 'pointerdown') e.currentTarget.setPointerCapture(e.pointerId);
    el.dispatchEvent(new PointerEvent(e.type, { bubbles: true, cancelable: true, clientX, clientY, button: e.button, buttons: e.buttons, pointerId: e.pointerId }));
  }

  const close = () => (what = null);
  const saveLabel = $derived(tab === 'image' ? 'Save image…' : 'Save data…');
  /** `name` as the table of files shows it: its start and its end about an ellipsis, past MIDDLE characters. */
  const MIDDLE = 34;
  const shortName = (name) => (name.length > MIDDLE ? `${name.slice(0, 20)}…${name.slice(-12)}` : name);
</script>

<!-- The figure's plot has the tooltips a plot has, as on the page. -->
<Tooltip.Provider delayDuration={300} ignoreNonKeyboardFocus>
<Dialog.Root {open} onOpenChange={(o) => !o && close()}>
  <Dialog.Content class="export-dialog" showCloseButton={false}>
    <div class="dhead">
      <Dialog.Title class="dtitle">Export</Dialog.Title>
      <Dialog.Description class="dsub">as the plots were at {madeAt}</Dialog.Description>
      <span class="seg tabs" role="group" aria-label="Export as">
        <button aria-pressed={tab === 'data'} onclick={() => (tab = 'data')}>Data</button>
        <button aria-pressed={tab === 'image'} onclick={() => (tab = 'image')}>Image</button>
      </span>
    </div>
    <div class="dbody" inert={progress.busy !== null}>
      <div class="plots" role="group" aria-label="Plots">
        {#each entries as e, i (e.cid)}
          <div class="thumb" class:off={!e.on} class:at={i === at}>
            <button class="pick" aria-pressed={i === at} aria-label="Show plot {i + 1}" onclick={() => (at = i)}>
              {#if e.thumb}<img src={e.thumb.src} alt="" style="width: {e.thumb.w}px; height: {e.thumb.h}px" />{:else}<span class="blank"></span>{/if}
              <small>{e.traces.map(titleOf).join(', ') || 'Empty plot'}</small>
            </button>
            <label><input type="checkbox" bind:checked={e.on} aria-label="Include plot {i + 1}" /> Plot {i + 1}</label>
          </div>
        {/each}
      </div>
      <div class="main" bind:clientWidth={mainWidth}>
        <!-- The stage keeps the plot it shows until it goes, a new one for another plot or tab. -->
        {#each current ? [{ e: current, tab }] : [] as shown (`${shown.e.cid}|${shown.tab}`)}
          <div class="fig" bind:this={fig}>
            <PlotStage host={shown.e.copy} id={shown.e.cid} width={layout.width} height={layout.height} pixelRatio={layout.ratio} {figure} {scale} {screenRatio} />
            {#each Object.entries(handles) as [name, box] (name)}
              <div class="handle" data-handle={name} role="presentation" style="left: {box.left}px; top: {box.top}px; width: {box.width}px; height: {box.height}px" onpointerdown={(e) => forward(name, e)} onpointermove={(e) => forward(name, e)} onpointerup={(e) => forward(name, e)} onlostpointercapture={(e) => laidOver(name)?.dispatchEvent(new PointerEvent('lostpointercapture', { pointerId: e.pointerId }))}></div>
            {/each}
          </div>
        {/each}
        <div class="will">
          <table>
            <thead><tr><th>File</th><th>Holds</th><th class="n">{tab === 'image' ? 'Pixels' : 'Samples'}</th><th class="n">About</th></tr></thead>
            <tbody>
              {#each filesShown as f (f.name)}
                <tr>
                  <td title={f.name}>{shortName(f.name)}</td>
                  <td class="c">{f.holds}</td>
                  <td class="n">{tab === 'image' ? `${sized.pixels[0]} × ${sized.pixels[1] * f.entries.length}` : f.samples === null ? '…' : f.samples.toLocaleString()}</td>
                  <td class="n">{sizeOf(f) === null ? '…' : bytesEstimate(sizeOf(f))}</td>
                </tr>
              {:else}
                <tr><td class="c" colspan="4">{chosen.length ? 'No data file of a capture: copy its data from the plot.' : 'Nothing: tick a plot at the side.'}</td></tr>
              {/each}
            </tbody>
          </table>
        </div>
      </div>
      <div class="insp">
        {#snippet rangeSection()}
          <div class="sec">
            <h3>Range</h3>
            <div class="r">
              <span>Time</span>
              <select aria-label="Time" bind:value={range}>
                {#each Object.entries(RANGES) as [v, name] (v)}<option value={v} disabled={v === 'cursors' && !chosen.some((e) => e.cursors)}>{name}</option>{/each}
              </select>
            </div>
            {#if span}<div class="span" data-span><span>{span.from} – {span.to}</span><span>{span.long}</span></div>{/if}
          </div>
        {/snippet}
        {#if tab === 'data'}
          <div class="grp">{several ? `All ${chosen.length} plots` : 'The file'}</div>
          {@render rangeSection()}
          <div class="sec">
            <h3>File</h3>
            <div class="r">
              <span>Format <button class="info" aria-label="About the formats" title="HDF5 keeps the devices' details. CSV is a plain table.">i</button></span>
              <select aria-label="Format" bind:value={format}>
                <option value="h5">HDF5</option>
                <option value="csv">CSV</option>
              </select>
            </div>
            {#if several}
              <div class="r">
                <span>Saved as</span>
                <select aria-label="Saved as" bind:value={files}><option value="each">A file each</option><option value="one">One file</option></select>
              </div>
            {/if}
          </div>
        {:else}
          <div class="grp">
            <span class="seg tabs" role="group" aria-label="Settings for">
              <button aria-pressed={pane === 'all'} onclick={() => (pane = 'all')}>All</button>
              <button aria-pressed={pane === 'plot'} onclick={() => (pane = 'plot')}>Plot {at + 1}</button>
            </span>
          </div>
          {#if pane === 'all'}
            {@render rangeSection()}
            <div class="sec">
              <h3>Size</h3>
              <div class="r">
                <span>Format</span>
                <span class="seg" role="group" aria-label="Image format">
                  {#each Object.entries(IMAGE_FORMATS) as [v, f] (v)}<button aria-pressed={imageFormat === v} onclick={() => (imageFormat = v)}>{f.name}</button>{/each}
                </span>
              </div>
              <div class="r">
                <span>Shape</span>
                <select aria-label="Shape" value={shape} onchange={(e) => setShape(e.currentTarget.value)}>
                  {#each SHAPES as [v, name] (v)}<option value={v}>{name}</option>{/each}
                </select>
              </div>
              <div class="r">
                <span>Units</span>
                <span class="seg" role="group" aria-label="Units">
                  {#each Object.keys(UNITS) as u (u)}<button aria-pressed={unit === u} onclick={() => (unit = u)}>{u}</button>{/each}
                </span>
              </div>
              <div class="r"><span>Width</span><span class="unit" data-slot="input-group"><input type="number" step="any" aria-label="Width" value={inUnit(width)} onchange={(e) => setSide('w', e.currentTarget.value)} />{unit}</span></div>
              <div class="r"><span>Height</span><span class="unit" data-slot="input-group"><input type="number" step="any" aria-label="Height" value={inUnit(height)} onchange={(e) => setSide('h', e.currentTarget.value)} />{unit}</span></div>
              {#if unit !== 'px'}
                <div class="r"><span>Resolution</span><span class="unit" data-slot="input-group"><input type="number" min="72" max="1200" aria-label="Resolution" value={dpi} onchange={(e) => (dpi = Math.min(1200, Math.max(72, Number(e.currentTarget.value) || dpi)))} />dpi</span></div>
              {/if}
              {#if refused}<div class="refused" role="alert">{refused}</div>{/if}
              <div class="r">
                <span>Background</span>
                <span class="seg" role="group" aria-label="Background">
                  <button aria-pressed={background === 'dark'} onclick={() => (background = 'dark')}>Dark</button>
                  <button aria-pressed={background === 'white'} onclick={() => (background = 'white')}>White</button>
                </span>
              </div>
            </div>
            <div class="sec">
              <h3>X axis</h3>
              <div class="r">
                <span>Shows</span>
                <select aria-label="X axis shows" bind:value={xTime} disabled={current?.kind !== 'time'}><option value="clock">Clock time</option><option value="secs">Seconds</option></select>
              </div>
              <div class="r"><span>Label</span><input type="text" aria-label="X label" placeholder="None" bind:value={xLabel} /></div>
            </div>
            <div class="sec">
              <h3>Shows</h3>
              <span class="chips">
                {#each [['grid', 'Grid'], ['stamp', 'Date and time'], ['device', 'Device']] as [k, name] (k)}
                  <button aria-pressed={shows[k]} onclick={() => (shows = { ...shows, [k]: !shows[k] })}>{name}</button>
                {/each}
              </span>
            </div>
            {#if several}
              <div class="sec">
                <h3>Files</h3>
                <div class="r">
                  <span>Saved as</span>
                  <select aria-label="Saved as" bind:value={files}><option value="each">An image each</option><option value="one">One image</option></select>
                </div>
              </div>
            {/if}
          {:else if current}
            {#key current.cid}<ExportPlotPane bind:entry={entries[at]} />{/key}
          {/if}
        {/if}
      </div>
    </div>
    <div class="dfoot">
      {#if progress.busy !== null}
        <div class="prog">
          <div class="bar" role="progressbar" aria-label="Making the files" aria-valuemin="0" aria-valuemax="100" aria-valuenow={Math.round(progress.busy * 100)}><i style="width: {progress.busy * 100}%"></i></div>
          {#if progress.said}<span>{progress.said}</span>{/if}
        </div>
        <button class="btn" onclick={stop}>Cancel</button>
      {:else}
        <button class="btn" onclick={close}>Cancel</button>
        <button class="btn go" disabled={!filesShown.length || (tab === 'image' && !!refused)} onclick={save}>{saveLabel}</button>
      {/if}
    </div>
  </Dialog.Content>
</Dialog.Root>
</Tooltip.Provider>

<style>
  :global(.export-dialog[data-slot='dialog-content']) {
    display: flex;
    flex-direction: column;
    gap: 0;
    padding: 0;
    width: min(1080px, calc(100vw - 2rem));
    max-width: calc(100vw - 2rem);
    max-height: calc(100dvh - 2rem);
    overflow: hidden;
  }
  .dhead { display: flex; align-items: center; gap: 10px; padding: 10px 16px; border-bottom: 1px solid var(--border); flex-wrap: wrap; }
  .dhead :global(.dtitle) { font: 600 16px var(--font-ui); }
  .dhead :global(.dsub) { color: var(--muted-foreground); font-size: 13px; }
  .dhead .tabs { margin-left: auto; }
  .dbody { display: grid; grid-template-columns: 150px minmax(0, 1fr) 272px; min-height: 0; flex: 1; overflow: hidden; }
  .plots { border-right: 1px solid var(--border); padding: 10px; display: grid; gap: 8px; align-content: start; overflow-y: auto; scrollbar-width: thin; }
  .insp { border-left: 1px solid var(--border); padding: 0 8px 12px 12px; overflow-y: auto; scrollbar-width: thin; scrollbar-gutter: stable; }
  @media (max-width: 860px) {
    .dbody { grid-template-columns: 1fr; overflow-y: auto; }
    .plots { border-right: 0; border-bottom: 1px solid var(--border); grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); }
    .insp { border-left: 0; border-top: 1px solid var(--border); overflow-y: visible; }
  }
  .thumb { display: grid; gap: 4px; padding: 6px; border: 1px solid var(--border); border-radius: 6px; background: var(--secondary); min-width: 0; }
  .thumb.at { border-color: var(--primary); }
  .thumb.off { opacity: 0.45; }
  .thumb .pick { display: grid; gap: 4px; padding: 0; border: 0; background: none; color: inherit; text-align: left; cursor: pointer; min-width: 0; outline: none; }
  .thumb img { display: block; margin: 0 auto; border-radius: 3px; }
  .thumb .blank { display: block; width: 100%; height: 44px; border-radius: 3px; background: var(--plot-bg); }
  .thumb label { font-size: 12px; display: flex; gap: 6px; align-items: center; }
  .thumb small { font: 11px var(--font-data); color: var(--muted-foreground); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .main { padding: 12px 14px 14px; display: grid; gap: 12px; min-width: 0; align-content: start; overflow-y: auto; }
  .fig { position: relative; margin: 0 auto; border: 1px solid var(--border); border-radius: 4px; overflow: hidden; line-height: 0; max-width: 100%; }
  .handle { position: absolute; cursor: grab; touch-action: none; }
  .insp :global(.sec.overlays) { grid-template-columns: repeat(2, minmax(0, 1fr)); column-gap: 10px; }
  .insp :global(.overlay) { display: grid; gap: 6px; align-content: start; min-width: 0; }
  .insp :global(.overlay .c) { display: flex; gap: 5px; align-items: center; flex-wrap: nowrap; }
  .insp :global(.overlay .chips button) { padding: 2px 6px; }
  .insp :global(.overlay .nine) { grid-template-columns: repeat(3, 12px); }
  .insp :global(.overlay .nine button) { width: 12px; }
  .will { border: 1px solid var(--border); border-radius: 6px; overflow: auto; max-height: 172px; scrollbar-width: thin; }
  .will thead th { position: sticky; top: 0; background: var(--popover); }
  table { border-collapse: collapse; width: 100%; font-size: 12.5px; }
  th, td { text-align: left; padding: 5px 10px; white-space: nowrap; }
  th { font: 600 11px var(--font-ui); letter-spacing: 0.06em; text-transform: uppercase; color: var(--muted-foreground); border-bottom: 1px solid var(--border); }
  td { font-family: var(--font-data); border-bottom: 1px solid color-mix(in oklch, var(--border) 55%, transparent); max-width: 260px; overflow: hidden; text-overflow: ellipsis; }
  td.n, th.n { text-align: right; }
  td.c { font-family: var(--font-ui); color: var(--muted-foreground); }
  tr:last-child td { border-bottom: 0; }
  .grp { display: flex; gap: 6px; align-items: center; font: 600 12px var(--font-ui); padding: 10px 0 8px; position: sticky; top: 0; background: var(--popover); z-index: 3; border-bottom: 1px solid var(--border); }
  .insp :global(.sec) { padding: 9px 0 8px; display: grid; gap: 6px; border-bottom: 1px solid color-mix(in oklch, var(--border) 60%, transparent); }
  .insp :global(.sec h3) { font: 600 11px var(--font-ui); letter-spacing: 0.07em; text-transform: uppercase; color: var(--muted-foreground); margin: 0 0 1px; }
  .insp :global(.r) { display: grid; grid-template-columns: 74px minmax(0, 1fr); gap: 8px; align-items: center; font-size: 13px; min-height: 26px; }
  .insp :global(.r > span:first-child) { color: var(--muted-foreground); }
  .insp :global(.r .c) { display: flex; gap: 6px; align-items: center; min-width: 0; flex-wrap: wrap; }
  .span { display: flex; justify-content: space-between; gap: 8px; font: 12px var(--font-data); color: var(--muted-foreground); white-space: nowrap; font-variant-numeric: tabular-nums; }
  .insp :global(select), .insp :global(input[type='text']), .insp :global(input[type='number']) { background: var(--secondary); color: var(--foreground); border: 1px solid var(--border); border-radius: 5px; padding: 3px 6px; font: 13px var(--font-ui); width: 100%; min-width: 0; height: 26px; box-sizing: border-box; outline: none; }
  .insp :global(input[type='number']) { font-family: var(--font-data); }
  .insp :global(input::placeholder) { color: var(--muted-foreground); }
  .insp :global(.pairin) { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; width: 100%; }
  .unit { display: flex; align-items: center; width: 100%; min-width: 0; height: 26px; box-sizing: border-box; background: var(--secondary); border: 1px solid var(--border); border-radius: 5px; padding-right: 7px; font: 12px var(--font-data); color: var(--muted-foreground); }
  .insp .unit input[type='number'] { border: 0; background: none; height: 24px; }
  .refused { font-size: 12px; color: var(--destructive); }
  input[type='checkbox'] { accent-color: var(--primary); width: 15px; height: 15px; margin: 0; outline: none; }
  :global(.export-dialog) :global(.seg) { display: inline-flex; border: 1px solid var(--border); border-radius: 5px; overflow: hidden; flex-wrap: wrap; }
  :global(.export-dialog) :global(.seg button) { background: none; border: 0; color: var(--muted-foreground); font: 13px var(--font-ui); padding: 3px 9px; cursor: pointer; height: 24px; outline: none; }
  :global(.export-dialog) :global(.seg button[aria-pressed='true']) { background: var(--secondary); color: var(--foreground); }
  .seg.tabs { flex-wrap: nowrap; }
  .insp :global(.chips) { display: flex; flex-wrap: wrap; gap: 4px; }
  .insp :global(.chips button) { background: none; border: 1px solid var(--border); border-radius: 12px; color: var(--muted-foreground); font: 12px var(--font-ui); padding: 2px 9px; cursor: pointer; outline: none; }
  .insp :global(.chips button[aria-pressed='true']) { background: var(--secondary); color: var(--foreground); border-color: color-mix(in srgb, var(--primary) 55%, var(--border)); }
  .insp :global(.r.sig) { grid-template-columns: 12px 26px minmax(0, 1fr) auto; gap: 6px; padding: 2px 4px 2px 2px; margin: 0 -4px 0 -2px; border-radius: 6px; }
  .insp :global(.r.sig[draggable='true']) { cursor: grab; }
  .insp :global(.r.sig.on) { background: var(--secondary); }
  .insp :global(.r.sig.land) { box-shadow: 0 -2px 0 var(--primary); }
  .insp :global(.r.sig.land.after) { box-shadow: 0 2px 0 var(--primary); }
  .insp :global(.colour) { display: grid; place-items: center; width: 26px; height: 26px; padding: 0; border: 1px solid var(--border); border-radius: 5px; background: none; cursor: pointer; outline: none; }
  .insp :global(.colour i) { width: 14px; height: 4px; border-radius: 2px; background: var(--c); }
  .insp :global(.axis) { min-width: 34px; height: 24px; padding: 0 6px; border: 1px solid var(--border); border-radius: 5px; background: none; color: var(--muted-foreground); font: 12px var(--font-data); cursor: pointer; outline: none; }
  .insp :global(.axis[aria-pressed='true']) { color: var(--foreground); border-color: color-mix(in srgb, var(--primary) 55%, var(--border)); }
  .insp :global(.axis-of) { font: 600 11px var(--font-ui); letter-spacing: 0.07em; text-transform: uppercase; color: var(--muted-foreground); padding-top: 6px; }
  .insp :global(.grip) { color: var(--muted-foreground); font-size: 10px; letter-spacing: -2px; }
  .insp :global(.nine) { display: inline-grid; grid-template-columns: repeat(3, 14px); gap: 2px; padding: 3px; border: 1px solid var(--border); border-radius: 5px; }
  .insp :global(.nine button) { width: 14px; height: 10px; padding: 0; border: 0; border-radius: 2px; background: var(--border); cursor: pointer; outline: none; }
  .insp :global(.nine button[aria-pressed='true']) { background: var(--primary); }
  .info { width: 16px; height: 16px; padding: 0; margin-left: 2px; border: 1px solid var(--border); border-radius: 50%; background: none; color: var(--muted-foreground); font: italic 600 11px/1 Georgia, serif; cursor: help; vertical-align: 1px; outline: none; }
  .dfoot { display: flex; justify-content: flex-end; gap: 8px; padding: 10px 16px; border-top: 1px solid var(--border); flex-wrap: wrap; align-items: center; }
  .btn { background: var(--secondary); color: var(--foreground); border: 1px solid var(--border); border-radius: 6px; padding: 6px 14px; font: 500 13px var(--font-ui); cursor: pointer; outline: none; }
  .btn.go { background: var(--primary); color: var(--primary-foreground); border-color: transparent; }
  .btn:disabled { opacity: 0.45; cursor: default; }
  .prog { flex: 1; min-width: 180px; display: grid; gap: 5px; align-content: center; font: 12px var(--font-data); color: var(--muted-foreground); }
  .bar { height: 6px; background: var(--secondary); border-radius: 3px; overflow: hidden; }
  .bar i { display: block; height: 100%; background: var(--primary); }
</style>

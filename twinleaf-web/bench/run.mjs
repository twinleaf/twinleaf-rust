// Load and latency profiles of the `tio proxy` web page.
//
// Each scenario starts a simulator with some number of devices at some
// sample rate, a release `tio proxy` in front of it serving the page on
// 127.0.0.1:7855, and a headless Edge on a fresh profile whose saved layout
// holds some number of plots. It measures the page while it streams, then
// types into the page's searches and moves its plots, and times each key to
// the frame after it. Everything binds loopback only.
//
//   node run.mjs                 the full matrix
//   node run.mjs --quick         three small scenarios, to check the harness
//   node run.mjs --only search   scenarios whose name matches a pattern
//   node run.mjs --headed        a visible browser window
//   node run.mjs --tio <path>    the tio binary (default: target/bench/release)
//   node run.mjs --page <url>    the page from another server, as Vite's
//   node run.mjs --profile       a CPU profile of each interaction, and its
//                                hottest functions
//
// Port 7855 must be free: the page is served only there. Results go to
// results/<time>.json and a table on stdout.

import { spawn, execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { cpus, totalmem } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright-core';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '..', '..');
// The proxy serves the page; `--page` may name another server of it, such as
// Vite's (`http://localhost:5173/`), whose unminified code names the
// functions in a profile.
const PROXY = 'http://127.0.0.1:7855/';
const SIM_PORT = 7870;

// The simulator's columns, by stream. The waveform stream runs at the
// simulator's sample rate; the auxiliary one at its own slow rate.
const FAST = ['sine.sine', 'sine.cosine'];
const SLOW = ['aux.triangle', 'aux.sawtooth'];

// --- Options --------------------------------------------------------------

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(name);
const option = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const TIO = resolve(option('--tio', join(REPO, 'target', 'bench', 'release', 'tio.exe')));
const HEADED = flag('--headed');
const ONLY = option('--only') ? new RegExp(option('--only')) : null;
const WARM_MS = 3000;
const MEASURE_MS = flag('--quick') ? 4000 : 8000;
const PAGE = option('--page', PROXY);
const PROFILE = flag('--profile');

// --- Scenarios ------------------------------------------------------------

// Throughput: what the stream costs as devices, sample rate and plots grow.
// Every fast signal is shown, spread over the plots.
const load = (devices, rate, plots) => ({ name: `load d${devices} r${rate} p${plots}`, devices, rate, plots, shown: 'fast', interact: ['zoom'] });
// Search: how the page's lists answer as the number of signals grows, at a
// slow rate so the stream costs little.
const search = (devices) => ({ name: `search d${devices}`, devices, rate: 10, plots: 1, shown: 'one', interact: ['palette', 'add', 'zoom'] });

const FULL = [
  ...[1, 4, 16].flatMap((d) => [1000, 10000].flatMap((r) => [1, 4, 12].map((p) => load(d, r, p)))),
  load(1, 50000, 1),
  load(4, 50000, 4),
  ...[1, 16, 64, 128].map(search),
];
const QUICK = [load(1, 1000, 1), load(4, 5000, 4), search(16)];
const SCENARIOS = (flag('--quick') ? QUICK : FULL).filter((s) => !ONLY || ONLY.test(s.name));

// --- Processes ------------------------------------------------------------

const children = new Set();
function start(args, label) {
  const child = spawn(TIO, args, { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  let log = '';
  const keep = (chunk) => (log = (log + chunk).slice(-4000));
  child.stdout.on('data', keep);
  child.stderr.on('data', keep);
  child.log = () => `${label}: ${log}`;
  children.add(child);
  child.on('exit', () => children.delete(child));
  return child;
}
function stopAll() {
  for (const child of children) child.kill();
}
process.on('exit', stopAll);
process.on('SIGINT', () => process.exit(130));

async function stop(child) {
  if (child.exitCode !== null) return;
  const gone = new Promise((done) => child.once('exit', done));
  child.kill();
  await gone;
}

/** CPU milliseconds and working set of a process, from PowerShell. */
function usage(pid) {
  try {
    const out = execFileSync('powershell', ['-NoProfile', '-Command', `$p = Get-Process -Id ${pid}; "$($p.TotalProcessorTime.TotalMilliseconds) $($p.WorkingSet64)"`], { encoding: 'utf8' });
    const [cpu, ws] = out.trim().split(/\s+/).map(Number);
    return { cpu, ws };
  } catch {
    return { cpu: NaN, ws: NaN };
  }
}

/**
 * Keeps the simulator on the last two cores, so the device being simulated
 * takes as little as it can from the proxy and the browser on the same host.
 */
function pin(pid) {
  const n = cpus().length;
  const mask = (1n << BigInt(n - 1)) | (1n << BigInt(n - 2));
  try {
    execFileSync("powershell", ["-NoProfile", "-Command", `(Get-Process -Id ${pid}).ProcessorAffinity = ${mask}`]);
  } catch {}
}

/** Busy and total CPU milliseconds of the whole host, over every core. */
function hostTimes() {
  let busy = 0, total = 0;
  for (const { times } of cpus()) {
    const sum = times.user + times.nice + times.sys + times.irq + times.idle;
    total += sum;
    busy += sum - times.idle;
  }
  return { busy, total };
}

async function portFree() {
  try {
    await fetch(PROXY, { signal: AbortSignal.timeout(500) });
    return false;
  } catch {
    return true;
  }
}

async function waitForPage(proxy) {
  const until = Date.now() + 30000;
  while (Date.now() < until) {
    if (proxy.exitCode !== null) throw new Error(`the proxy exited\n${proxy.log()}`);
    try {
      const r = await fetch(PROXY, { signal: AbortSignal.timeout(1000) });
      if (r.ok) return;
    } catch {}
    await sleep(250);
  }
  throw new Error(`the page did not come up\n${proxy.log()}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- The page's layout ----------------------------------------------------

/** The key a signal has on the page: the route without its slash, then the column. */
const keyOf = (route, column) => (route === '/' ? column : `${route.slice(1)}/${column}`);

function routesOf(devices) {
  return devices === 1 ? ['/'] : Array.from({ length: devices }, (_, i) => `/${i}`);
}

/** A split tree of `ids` as near square as it goes, rows of columns. */
function grid(ids) {
  const cols = Math.ceil(Math.sqrt(ids.length));
  const rows = [];
  for (let i = 0; i < ids.length; i += cols) rows.push(ids.slice(i, i + cols));
  let n = 0;
  const leaf = (id) => ({ type: 'leaf', id: `n-leaf-${id}`, plotId: id });
  const split = (dir, items, make) => {
    if (items.length === 1) return make(items[0]);
    const half = Math.ceil(items.length / 2);
    return { type: 'split', id: `n-split-${n++}`, dir, ratio: half / items.length, a: split(dir, items.slice(0, half), make), b: split(dir, items.slice(half), make) };
  };
  return split('col', rows, (row) => split('row', row, leaf));
}

function layoutFor(s) {
  const routes = routesOf(s.devices);
  const keys = s.shown === 'fast' ? routes.flatMap((r) => FAST.map((c) => keyOf(r, c))) : [keyOf(routes[0], FAST[0])];
  const ids = Array.from({ length: s.plots }, (_, i) => `p-${i}`);
  const plots = {};
  ids.forEach((id) => (plots[id] = { id, kind: 'time', mode: 'overlay', items: [] }));
  keys.forEach((key, i) => plots[ids[i % ids.length]].items.push({ id: `c-${i}`, key }));
  const tab = { id: 't-bench', name: 'Bench', plots, layout: grid(ids) };
  return { tabs: [tab], activeId: tab.id, signals: routes.length * (FAST.length + SLOW.length), shown: keys.length };
}

// --- Measuring in the page ------------------------------------------------

// Runs in the page before its own scripts: the saved layout, and the
// counters the bench reads back.
function instrument({ saved }) {
  localStorage.setItem('tl-plot-tabs-v2', saved);
  localStorage.setItem('tl-picker-network', 'false');
  const b = (window.__bench = { frames: [], long: [], events: [], keys: [], phase: '', wsFrames: 0, wsBytes: 0, wsKinds: {} });
  b.reset = () => {
    b.frames.length = b.long.length = b.events.length = 0;
    b.wsFrames = b.wsBytes = 0;
    b.wsKinds = {};
  };
  const loop = (t) => {
    b.frames.push(t);
    requestAnimationFrame(loop);
  };
  requestAnimationFrame(loop);
  try {
    new PerformanceObserver((list) => list.getEntries().forEach((e) => b.long.push(e.duration))).observe({ type: 'longtask' });
    new PerformanceObserver((list) => list.getEntries().forEach((e) => b.events.push({ name: e.name, ms: e.duration, phase: b.phase }))).observe({ type: 'event', durationThreshold: 16 });
  } catch {}
  // A key's latency: from its keydown to the task after the next frame,
  // which is when what it changed has been painted.
  addEventListener(
    'keydown',
    (e) => {
      const t = performance.now(), phase = b.phase, key = e.key;
      requestAnimationFrame(() => setTimeout(() => b.keys.push({ phase, key, ms: performance.now() - t })));
    },
    true,
  );
  const Native = window.WebSocket;
  window.WebSocket = class extends Native {
    constructor(...args) {
      super(...args);
      // Each message by kind: a text message's `type`, a binary one's first byte.
      this.addEventListener('message', (e) => {
        let kind, bytes;
        if (typeof e.data === 'string') {
          bytes = e.data.length;
          kind = /"type"\s*:\s*"([^"]+)"/.exec(e.data)?.[1] ?? 'text';
        } else {
          bytes = e.data.byteLength ?? e.data.size ?? 0;
          kind = e.data instanceof ArrayBuffer && bytes ? `bin${new Uint8Array(e.data, 0, 1)[0]}` : 'bin';
        }
        b.wsFrames++;
        b.wsBytes += bytes;
        const k = (b.wsKinds[kind] ??= { frames: 0, bytes: 0 });
        k.frames++;
        k.bytes += bytes;
      });
    }
  };
}

function quantiles(values) {
  if (!values.length) return { n: 0 };
  const v = [...values].sort((a, b) => a - b);
  const q = (p) => v[Math.min(v.length - 1, Math.floor(p * v.length))];
  return { n: v.length, p50: q(0.5), p95: q(0.95), p99: q(0.99), max: v[v.length - 1] };
}

async function metrics(cdp) {
  const { metrics } = await cdp.send('Performance.getMetrics');
  return Object.fromEntries(metrics.map((m) => [m.name, m.value]));
}

async function profileStart(cdp) {
  await cdp.send('Profiler.enable');
  await cdp.send('Profiler.setSamplingInterval', { interval: 200 });
  await cdp.send('Profiler.start');
}

/**
 * Stops the profile, saves it beside the results for DevTools, and returns
 * the functions that spent the most time themselves, in milliseconds.
 */
async function profileStop(cdp, label) {
  const { profile } = await cdp.send('Profiler.stop');
  mkdirSync(join(HERE, 'results', 'profiles'), { recursive: true });
  const file = join(HERE, 'results', 'profiles', `${label.replace(/[^\w-]+/g, '_')}.cpuprofile`);
  writeFileSync(file, JSON.stringify(profile));
  const byId = new Map(profile.nodes.map((n) => [n.id, n]));
  const self = new Map();
  profile.samples.forEach((id, i) => {
    const { functionName, url, lineNumber } = byId.get(id).callFrame;
    const where = `${functionName || '(anonymous)'} ${url.split('/').pop().split('?')[0]}:${lineNumber + 1}`;
    self.set(where, (self.get(where) ?? 0) + (profile.timeDeltas[i] ?? 0) / 1000);
  });
  const top = [...self].filter(([where]) => !/^\((idle|program|garbage collector)\)/.test(where)).sort((a, b) => b[1] - a[1]).slice(0, 12);
  return { file, top: top.map(([where, ms]) => ({ where, ms: Math.round(ms) })), gcMs: Math.round(self.get('(garbage collector) :0') ?? 0) };
}

// The page's interactions, each a phase its keys are timed under.
const INTERACTIONS = {
  async palette(page) {
    await page.keyboard.press('Control+k');
    await sleep(400);
    await page.keyboard.type('cosine', { delay: 150 });
    for (let i = 0; i < 6; i++) await page.keyboard.press('Backspace', { delay: 150 });
    await page.keyboard.press('Escape');
  },
  async add(page) {
    await page.keyboard.press('1');
    await sleep(200);
    await page.keyboard.press('a');
    await sleep(400);
    await page.keyboard.type('tri', { delay: 150 });
    for (let i = 0; i < 3; i++) await page.keyboard.press('ArrowDown', { delay: 150 });
    await page.keyboard.press('Escape');
  },
  async zoom(page) {
    await page.keyboard.press('1');
    await sleep(200);
    for (const key of ['ArrowUp', 'ArrowUp', 'ArrowUp', 'ArrowDown', 'ArrowDown', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'End']) {
      await page.keyboard.press(key);
      await sleep(150);
    }
  },
};

// --- One scenario ---------------------------------------------------------

async function run(browser, s) {
  const layout = layoutFor(s);
  const sim = start(['simulate', '--samplerate', String(s.rate), '--port', String(SIM_PORT), '--listen', 'loopback', '--no-drop', '--layout', routesOf(s.devices).join(',')], 'simulator');
  pin(sim.pid);
  await sleep(500);
  const proxy = start(['proxy', `udp://127.0.0.1:${SIM_PORT}`, '--listen', 'loopback'], 'proxy');
  try {
    await waitForPage(proxy);
    const context = await browser.newContext({ viewport: { width: 1600, height: 900 } });
    const saved = JSON.stringify({ tabs: layout.tabs, activeId: layout.activeId });
    await context.addInitScript(instrument, { saved });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e.stack ?? e)));
    const cdp = await context.newCDPSession(page);
    await cdp.send('Performance.enable');
    const opened = performance.now();
    if (PROFILE) await profileStart(cdp);
    await page.goto(PAGE);
    // Streaming has started once binary frames arrive.
    await page.waitForFunction(() => window.__bench.wsFrames > 20, null, { timeout: 30000 });
    const firstData = performance.now() - opened;
    await sleep(WARM_MS);
    const loading = PROFILE ? await profileStop(cdp, `${s.name} load`) : null;

    await page.evaluate(() => window.__bench.reset());
    if (PROFILE) await profileStart(cdp);
    const m0 = await metrics(cdp), u0 = usage(proxy.pid), s0 = usage(sim.pid), h0 = hostTimes(), t0 = performance.now();
    await sleep(MEASURE_MS);
    const m1 = await metrics(cdp), u1 = usage(proxy.pid), s1 = usage(sim.pid), h1 = hostTimes(), wall = performance.now() - t0;
    const streaming = PROFILE ? await profileStop(cdp, `${s.name} stream`) : null;
    const b = await page.evaluate(() => ({ frames: window.__bench.frames, long: window.__bench.long, wsFrames: window.__bench.wsFrames, wsBytes: window.__bench.wsBytes, wsKinds: window.__bench.wsKinds }));
    const intervals = b.frames.slice(1).map((t, i) => t - b.frames[i]);

    const latency = {}, hotspots = PROFILE ? { load: loading, stream: streaming } : {};
    for (const name of s.interact) {
      await page.evaluate((phase) => (window.__bench.phase = phase), name);
      if (PROFILE) await profileStart(cdp);
      try {
        await INTERACTIONS[name](page);
      } catch (e) {
        latency[name] = { error: String(e) };
      }
      await sleep(400);
      if (PROFILE) hotspots[name] = await profileStop(cdp, `${s.name} ${name}`);
    }
    const keyed = await page.evaluate(() => ({ keys: window.__bench.keys, events: window.__bench.events }));
    for (const name of s.interact) {
      if (latency[name]) continue;
      latency[name] = { ...quantiles(keyed.keys.filter((k) => k.phase === name).map((k) => k.ms)), slowEvents: keyed.events.filter((e) => e.phase === name).length };
    }
    const renderer = await page.evaluate(() => {
      const gl = document.createElement('canvas').getContext('webgl2');
      const info = gl?.getExtension('WEBGL_debug_renderer_info');
      return info ? gl.getParameter(info.UNMASKED_RENDERER_WEBGL) : 'unknown';
    });
    await context.close();

    const seconds = wall / 1000;
    return {
      ...s,
      signals: layout.signals,
      shownSignals: layout.shown,
      pointsPerSecond: s.devices * FAST.length * s.rate,
      firstDataMs: Math.round(firstData),
      frame: { fps: intervals.length / seconds, ...quantiles(intervals), over25: intervals.filter((d) => d > 25).length / Math.max(1, intervals.length) },
      longTasks: { count: b.long.length, ms: b.long.reduce((a, d) => a + d, 0) },
      mainThreadBusy: (m1.TaskDuration - m0.TaskDuration) / seconds,
      scriptShare: (m1.ScriptDuration - m0.ScriptDuration) / seconds,
      heapMB: m1.JSHeapUsedSize / 2 ** 20,
      ws: { framesPerSecond: b.wsFrames / seconds, kBPerSecond: b.wsBytes / 1024 / seconds, byKind: Object.fromEntries(Object.entries(b.wsKinds).map(([k, v]) => [k, { perSecond: v.frames / seconds, kBPerSecond: v.bytes / 1024 / seconds }])) },
      proxy: { cpu: (u1.cpu - u0.cpu) / wall, workingSetMB: u1.ws / 2 ** 20 },
      simulator: { cpu: (s1.cpu - s0.cpu) / wall },
      host: { busy: (h1.busy - h0.busy) / (h1.total - h0.total), cores: cpus().length },
      latency,
      hotspots,
      errors,
      renderer,
    };
  } finally {
    await stop(proxy);
    await stop(sim);
  }
}

// --- Report ---------------------------------------------------------------

const f = (v, digits = 1) => (Number.isFinite(v) ? v.toFixed(digits) : '—');
function table(results) {
  const rows = results.map((r) =>
    r.error
      ? [r.name, 'error: ' + r.error.split('\n')[0]]
      : [
          r.name,
          String(r.signals),
          f(r.pointsPerSecond / 1000, 0) + 'k',
          f(r.frame.fps, 0),
          f(r.frame.p95),
          f(r.frame.p99),
          f(r.frame.over25 * 100, 0) + '%',
          f(r.mainThreadBusy * 100, 0) + '%',
          f(r.longTasks.ms, 0),
          f(r.heapMB, 0),
          f(r.ws.kBPerSecond, 0),
          f(r.proxy.cpu * 100, 0) + '%',
          f(r.simulator.cpu * 100, 0) + '%',
          f(r.host.busy * 100, 0) + '%',
          ...['palette', 'add', 'zoom'].map((p) => (r.latency[p] ? (r.latency[p].error ? 'err' : `${f(r.latency[p].p50, 0)}/${f(r.latency[p].p95, 0)}`) : '')),
        ],
  );
  const head = ['scenario', 'sigs', 'pts/s', 'fps', 'f p95', 'f p99', '>25ms', 'main', 'long ms', 'heap', 'ws kB/s', 'proxy', 'sim', 'host', 'palette', 'add', 'zoom'];
  const all = [head, ...rows];
  const widths = head.map((_, i) => Math.max(...all.map((r) => (r[i] ?? '').length)));
  return all.map((r) => r.map((c, i) => (c ?? '').padEnd(widths[i])).join('  ')).join('\n');
}

// --- Main -----------------------------------------------------------------

if (!existsSync(TIO)) {
  console.error(`No tio at ${TIO}. Build one:\n  CARGO_TARGET_DIR=target/bench cargo build --release -p twinleaf-tools --bin tio`);
  process.exit(1);
}
if (!(await portFree())) {
  console.error(`Something already serves ${PROXY}. Stop that proxy first: the page is served only on 7855.`);
  process.exit(1);
}

const browser = await chromium.launch({ channel: 'msedge', headless: !HEADED, args: ['--ignore-gpu-blocklist', '--enable-gpu-rasterization'] });
const results = [];
for (const s of SCENARIOS) {
  process.stdout.write(`${s.name} … `);
  try {
    const r = await run(browser, s);
    results.push(r);
    console.log(`${f(r.frame.fps, 0)} fps, frame p95 ${f(r.frame.p95)} ms, main ${f(r.mainThreadBusy * 100, 0)}%${r.errors.length ? `, ${r.errors.length} page errors` : ''}`);
  } catch (e) {
    results.push({ ...s, error: String(e?.stack ?? e) });
    console.log(`error: ${String(e).split('\n')[0]}`);
  }
}
await browser.close();

const git = (args) => {
  try {
    return execFileSync('git', args, { cwd: REPO, encoding: 'utf8' }).trim();
  } catch {
    return '';
  }
};
const env = {
  at: new Date().toISOString(),
  cpu: cpus()[0]?.model,
  cores: cpus().length,
  memoryGB: totalmem() / 2 ** 30,
  headed: HEADED,
  renderer: results.find((r) => r.renderer)?.renderer,
  commit: git(['rev-parse', '--short', 'HEAD']),
  uncommitted: git(['diff', '--shortstat']),
  measureMs: MEASURE_MS,
};
mkdirSync(join(HERE, 'results'), { recursive: true });
const file = join(HERE, 'results', `${env.at.replace(/[:.]/g, '-')}.json`);
writeFileSync(file, JSON.stringify({ env, results }, null, 2));
console.log(`\n${env.cpu} · ${env.renderer} · ${HEADED ? 'headed' : 'headless'}\n`);
console.log(table(results));
console.log(`\nfps and frame p95/p99 are the page's frames; main is its main thread busy; latency columns are p50/p95 ms from keydown to the frame after.\n${file}`);

// A stub hub's firmware: what each device runs, the catalog a check
// answers from, and upgrades taken, queued, run a step at a time, refused,
// cancelled and retried as the hub does (twinleaf-web/src/firmware.rs).
import * as clock from '../../src/lib/clock.js';

/** The most bytes of an image the hub takes (twinleaf-web/src/firmware.rs MAX_IMAGE). */
const MAX_IMAGE = 16 * 1024 * 1024;

/** What a device runs, as the hub's `firmware` message says it: `name` `revision`, built on `date` as `build`. */
export const installed = (name, revision, date, build, serial = null) => ({ name, revision, date, build, serial, development: /dev/i.test(build) });
/** A release of `name` `revision` as the catalog lists it. */
export const release = (name, revision, date, build) => ({ date, build, file: `${name}-${revision}-firmware-${date}-${build}.bin` });

/**
 * Has stub hub `hub` (page.js `stubHub`) list `devices`, each {route, name,
 * serial, installed, releases}: alive, running `installed`, with `releases`
 * what the catalog lists for it, newest first, none unless given. The hub
 * then takes the page's firmware asks, as `hub.firmware`:
 *
 * - `posted` lists every message the page sent, parsed, a binary frame as
 *   {bytes}; `images` each file the hub received whole, {route, name, bytes}.
 * - `reachable` false has a check end `unreachable`; a check says
 *   `checking` and, `latency` ms of the page's time later, each device's
 *   status by its build and date against its releases, as the library
 *   compares them.
 * - An upgrade runs as the hub's: one at a time in the order asked, the
 *   others `queued`; `downloading` for `times.download` ms unless a file,
 *   `uploading` in `times.chunks` equal parts `times.chunk` ms apart, the
 *   last shown as long,
 *   `finalizing` for `times.finalize` ms, `restarting` for `times.restart`
 *   ms, then `done`, the device running the target: a release's date and
 *   build, or for a file `afterFile`. `failing`, by route, has the next
 *   upgrade of that device fail at {step, error}: check, download, upload
 *   or finalize. An ask the hub refuses is answered `refused`.
 * - `tell(route, state, more)` says a device's upgrade stands so, whatever
 *   runs, `say(route, catalog)` its catalog, and `reads(route, installed)`
 *   what it runs, as the hub does once it has read it, for a case that
 *   shows one state.
 */
export function serveFirmware(hub, devices) {
  const send = (msg) => hub.text({ ...msg, seq: hub.seq + 1 });
  const slots = new Map(devices.map((d) => [d.route, { ...d, releases: d.releases ?? [], catalog: { state: 'unchecked' }, upgrade: null, job: null, timer: null }]));
  const api = { posted: [], images: [], reachable: true, latency: 50, failing: {}, times: { download: 400, chunks: 10, chunk: 100, finalize: 500, restart: 600 }, afterFile: { date: '2026-06-08', build: '000002' } };
  const firmware = (slot) => send({ type: 'firmware', route: slot.route, installed: slot.installed, catalog: slot.catalog });
  const tell = (slot, state, more = {}) => {
    slot.upgrade = state === 'idle' ? null : { state, source: slot.job?.source, target: slot.job?.target, ...more };
    send({ type: 'upgrade', route: slot.route, ...(slot.upgrade ?? { state }) });
  };
  const refuse = (ask, route, error) => hub.text({ type: 'refused', ask, route, error });
  /** How `slot`'s releases stand to what it runs, as `twinleaf::firmware::check_for_update` says. */
  const checked = (slot) => {
    const latest = slot.releases[0], has = slot.installed;
    const status = has.development ? 'development' : !latest ? 'none' : has.build?.toLowerCase() === latest.build.toLowerCase() ? 'current' : !has.date ? 'unknown' : latest.date > has.date ? 'update' : 'current';
    return { state: 'checked', status, releases: has.development ? [] : slot.releases };
  };
  let checking = false;
  function check() {
    if (checking) return;
    checking = true;
    for (const slot of slots.values()) firmware(Object.assign(slot, { catalog: { state: 'checking' } }));
    clock.after(api.latency, () => {
      checking = false;
      for (const slot of slots.values()) firmware(Object.assign(slot, { catalog: api.reachable ? checked(slot) : { state: 'unreachable', error: 'firmware catalog error: listing request failed: no network' } }));
    });
  }

  /** The upgrades asked for and not ended, the first under way. */
  const queue = [];
  /** Ends the upgrade under way and starts the next. */
  const next = () => {
    queue.shift();
    if (queue.length) run(queue[0]);
  };
  const fail = (slot, step, error) => (tell(slot, 'failed', { step, error }), next());
  /** Runs `slot`'s job a step at a time, each after its time, failing where `failing` says. */
  function run(slot) {
    const job = slot.job, failing = api.failing[slot.route];
    delete api.failing[slot.route];
    const fails = (step) => failing?.step === step && (fail(slot, step, failing.error), true);
    const after = (ms, then) => (slot.timer = clock.after(ms, then));
    const finalize = () => {
      if (fails('finalize')) return;
      tell(slot, 'finalizing');
      after(api.times.finalize, () => {
        tell(slot, 'restarting');
        after(api.times.restart, () => {
          slot.installed = { ...slot.installed, ...job.becomes, development: false };
          if (slot.catalog.state === 'checked') slot.catalog = checked(slot);
          tell(slot, 'done', { installed: slot.installed });
          firmware(slot);
          next();
        });
      });
    };
    const upload = (part = 0) => {
      tell(slot, 'uploading', { sent: Math.floor((job.size * part) / api.times.chunks), size: job.size });
      if (part === 0 && fails('check')) return;
      if (part === Math.ceil(api.times.chunks / 2) && fails('upload')) return;
      if (part === api.times.chunks) return void after(api.times.chunk, finalize);
      after(api.times.chunk, () => upload(part + 1));
    };
    if (job.source === 'file') return upload();
    tell(slot, 'downloading');
    after(api.times.download, () => (fails('download') ? null : upload()));
  }
  /** Queues `job` for `slot`, and runs it when none is under way. */
  function start(slot, job) {
    slot.job = job;
    queue.push(slot);
    if (queue.length === 1) run(slot);
    else tell(slot, 'queued');
  }

  /** The file whose bytes are still to come: {slot, name, size, got}. */
  let pending = null;
  /** Takes one message of the page's, as the hub does once it has arrived. */
  function take(msg) {
    if (msg.bytes !== undefined) {
      if (!pending) return;
      pending.got += msg.bytes;
      if (pending.got < pending.size) return;
      const { slot, name, size, got } = pending;
      pending = null;
      if (got > size) return refuse('upgrade', slot.route, `more than the ${size} bytes the upgrade of ${slot.route} named came`);
      api.images.push({ route: slot.route, name, bytes: got });
      return start(slot, { source: 'file', target: name, size, becomes: api.afterFile });
    }
    pending = null;
    if (msg.type === 'check') return check();
    if (!['upgrade', 'cancel', 'retry'].includes(msg.type)) return;
    const slot = slots.get(msg.route), at = slot?.upgrade?.state;
    if (!slot) return refuse(msg.type, msg.route, `there is no device at ${msg.route}`);
    if (msg.type === 'cancel') {
      if (['uploading', 'finalizing', 'restarting'].includes(at)) return refuse('cancel', msg.route, `the upgrade of ${msg.route} is ${at} and cannot be cancelled`);
      if (!at) return;
      clock.cancel(slot.timer);
      const ran = queue[0] === slot;
      if (queue.includes(slot)) queue.splice(queue.indexOf(slot), 1);
      tell(slot, 'idle');
      if (ran && queue.length) run(queue[0]);
      return;
    }
    if (msg.type === 'retry') return at === 'failed' ? start(slot, slot.job) : refuse('retry', msg.route, `no upgrade of ${msg.route} has failed`);
    if (queue.includes(slot)) return refuse('upgrade', msg.route, `an upgrade of ${msg.route} is already running`);
    const { kind, file, name, size } = msg.source;
    if (kind === 'file') {
      if (size === 0) return refuse('upgrade', msg.route, `the image ${name} is empty`);
      if (size > MAX_IMAGE) return refuse('upgrade', msg.route, `the image ${name} is ${size} bytes, more than the ${MAX_IMAGE} an upgrade takes`);
      return void (pending = { slot, name, size, got: 0 });
    }
    const listed = slot.catalog.state === 'checked' ? slot.catalog.releases : [];
    const found = kind === 'latest' ? listed[0] : listed.find((r) => r.file === file);
    if (!found) return refuse('upgrade', msg.route, `the catalog lists no such firmware for ${msg.route}`);
    start(slot, { source: kind, target: `${found.date} ${found.build}`, size: 412000, becomes: { date: found.date, build: found.build } });
  }
  // A message is on its way as the page's send returns, and the hub answers after, as over a socket.
  const before = hub.post;
  hub.post = (message) => {
    if (typeof message !== 'string') {
      const got = { bytes: message.byteLength };
      api.posted.push(got);
      Promise.resolve().then(() => take(got));
      return true;
    }
    const msg = JSON.parse(message);
    if (!['check', 'upgrade', 'cancel', 'retry'].includes(msg.type)) {
      // Any text frame drops an image whose bytes are still to come.
      pending = null;
      return before.call(hub, message);
    }
    api.posted.push(msg);
    Promise.resolve().then(() => take(msg));
    return true;
  };
  Object.assign(api, {
    tell: (route, state, more = {}) => {
      const slot = slots.get(route);
      slot.job ??= { source: more.source ?? 'latest', target: more.target ?? '2026-07-01 5d1494' };
      tell(slot, state, more);
    },
    say: (route, catalog) => firmware(Object.assign(slots.get(route), { catalog: catalog ?? checked(slots.get(route)) })),
    asks: (type) => api.posted.filter((m) => m.type === type),
    reads: (route, installed) => firmware(Object.assign(slots.get(route), { installed })),
  });
  send({ type: 'routes', mounts: null, routes: [...new Set([...hub.session.devices.keys(), ...slots.keys()])] });
  for (const slot of slots.values()) {
    send({ type: 'device', route: slot.route, name: slot.name, serial: slot.serial ?? null, firmware: 'fw', presence: 'alive' });
    // A device with no `installed` is one the hub has not read: it has no firmware message.
    if (slot.installed) firmware(slot);
  }
  hub.firmware = api;
  return api;
}

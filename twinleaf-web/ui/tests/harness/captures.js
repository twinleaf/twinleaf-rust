// A stub hub's captures: the frame the hub sends of one, byte for byte
// (twinleaf-web/src/wire.rs `encode_capture`), and a hub that lists capture
// RPCs, takes the page's asks and tells where each capture stands as the
// hub does (twinleaf-web/src/captures.rs).
import { captureKey } from '../../src/lib/protocol.js';
import { chip } from '../../src/lib/tabs.js';
import * as clock from '../../src/lib/clock.js';

/** The typed array of each element type a capture frame carries, by the protocol's code. */
export const ELEMENTS = { 0x10: Uint8Array, 0x11: Int8Array, 0x20: Uint16Array, 0x21: Int16Array, 0x40: Uint32Array, 0x41: Int32Array, 0x42: Float32Array, 0x80: BigUint64Array, 0x81: BigInt64Array, 0x82: Float64Array };

/**
 * The frame of capture `number` of `rpc` of the device at `route`, as the
 * hub encodes it: `elements` the device's own, of element type `type`,
 * each times `calibration` in `units`, element i at `x0 + i * dx` in
 * `xUnits`, and `spectrum`, {f, a}, the hub's estimate of their spectrum
 * along the reciprocal of x, none unless given. Returns its ArrayBuffer.
 */
export function encodeCapture({ route, rpc, number, type, calibration, x0, dx, name, units, xName, xUnits, elements, spectrum = { f: [], a: [] } }) {
  const utf8 = new TextEncoder(), strings = [route, rpc, name, units, xName, xUnits].map((s) => utf8.encode(s));
  const text = strings.reduce((n, s) => n + s.length, 0), padded = Math.ceil(text / 8) * 8;
  const data = new Uint8Array(elements.buffer, elements.byteOffset, elements.byteLength);
  const bins = 64 + padded + Math.ceil(data.length / 8) * 8, m = spectrum.f.length;
  const bytes = new Uint8Array(bins + 8 + 8 * m), dv = new DataView(bytes.buffer);
  dv.setUint32(bins, m, true);
  for (let i = 0; i < m; i++) {
    dv.setFloat32(bins + 8 + 4 * i, spectrum.f[i], true);
    dv.setFloat32(bins + 8 + 4 * (m + i), spectrum.a[i], true);
  }
  dv.setUint8(0, 3);
  dv.setBigUint64(16, BigInt(number), true);
  dv.setUint32(24, elements.length, true);
  dv.setUint8(28, type);
  dv.setUint8(29, type >> 4);
  dv.setFloat32(32, calibration, true);
  dv.setFloat32(36, x0, true);
  dv.setFloat32(40, dx, true);
  strings.forEach((s, i) => dv.setUint16(48 + 2 * i, s.length, true));
  let at = 64;
  for (const s of strings) {
    bytes.set(s, at);
    at += s.length;
  }
  bytes.set(data, 64 + padded);
  return bytes.buffer;
}

/** The capture key of capture RPC `rpc` of the device at `route`, the root's unless given. */
export const keyOf = (rpc, route = '/') => captureKey({ route, rpc });

/**
 * Makes plot `id` of `page`'s open tab a plot of `kind`, with the fields of
 * `more`, as a case prepares one before it runs: a time plot or a spectrum
 * of its signals by its kind alone; a capture plot of a capture for each
 * signal it showed, `<signal>.capture`, named as the signal, which the hub
 * lists in place of any it listed and has sent one shot of: 400 values of
 * a sine of three turns, 1 ms apart, in V.
 */
export function asKind(page, id, kind, more = {}) {
  if (kind !== 'capture') return page.history.group('Prepare', () => page.ws.setPlot(id, { kind, ...more }));
  const captures = page.ws.plots[id].items.map(({ key }) => ({ rpc: `${key}.capture`, label: { name: key, units: 'V', x_name: 'Time', x_units: 's' }, shot: () => ({ type: 0x42, calibration: 1, x0: 0, dx: 1e-3, elements: Float32Array.from({ length: 400 }, (_, i) => Math.sin((i / 400) * 6 * Math.PI)) }) }));
  serveCaptures(page.hub, captures);
  for (const c of captures) page.hub.binary(encodeCapture({ route: '/', rpc: c.rpc, number: 1, name: c.label.name, units: 'V', xName: 'Time', xUnits: 's', ...c.shot(1) }));
  page.history.group('Prepare', () => page.ws.setPlot(id, { kind, items: captures.map((c) => chip(keyOf(c.rpc))), ...more }));
}

/** Where the spectrum `fid` gives a shot peaks, as a bin of FID_BINS, near the sine's 2 MHz / (24π), 26.5 kHz. */
export const FID_PEAK = 14;
/** How many bins the spectrum `fid` gives a shot has, one above zero to one below its Nyquist of 1 MHz, 2 MHz / 1024 apart, as the hub's estimate of 1500 samples has. */
export const FID_BINS = 511;
/**
 * A capture an FID magnetometer's ADC reads out: `n` 16-bit samples 0.5 µs
 * apart, a decaying sine of `amplitude` counts, 1e-4 V a count, as shot
 * `number`, each shot a little larger than the last; with a spectrum as
 * the hub would send it, in cycles a second of its x: FID_BINS bins
 * 2 MHz / 1024 apart, a line at bin FID_PEAK over a floor, as large as the
 * shot.
 */
export const fid = (n = 1500, amplitude = 20000) => (number) => ({
  type: 0x21, calibration: 1e-4, x0: 0, dx: 5e-7,
  elements: Int16Array.from({ length: n }, (_, i) => Math.round(amplitude * (1 + number / 100) * Math.exp(-i / (n / 3)) * Math.sin(i / 12))),
  spectrum: {
    f: Float32Array.from({ length: FID_BINS }, (_, k) => ((k + 1) * 2e6) / 1024),
    a: Float32Array.from({ length: FID_BINS }, (_, k) => (1 + number / 100) * (1e-3 / (1 + ((k + 1 - FID_PEAK) / 1.5) ** 2) + 2e-6)),
  },
});

/**
 * Has stub hub `hub` (page.js `stubHub`) list `captures` as the capture
 * RPCs of the device at `route`, alive, each {rpc, label, shot}: `label`
 * {name, units, x_name, x_units} or null, and `shot(number)` what capture
 * `number` of it holds, as `encodeCapture` takes it. The hub then takes
 * what the page asks of them, as `hub.captures`:
 *
 * - `posted` lists every message the page sent, parsed, and `asks(rpc)` is
 *   what the page asks of `rpc` now: {once, repeat}.
 * - A capture asked for is taken as the hub takes it: it says `capturing`,
 *   and `latency` ms of the page's time later (100 unless set) sends the
 *   frame to the page, while it still asks, and says `idle`, or `waiting`
 *   and takes the next after the delay asked. `failing`, by rpc, has the
 *   next capture of one end as that state instead, `busy`, `timeout` or
 *   {error}, a repeating one tried again after its delay and 1 s at least.
 * - `reading(rpc, read, size)` says a capture in flight is being read.
 * - `lose()` loses the device: its presence lost, every ask of its
 *   captures dropped, each capture in flight ended as the hub ends it.
 *   `find()` brings it back, alive, its captures listed again.
 */
export function serveCaptures(hub, captures, route = '/') {
  const send = (msg) => hub.text({ ...msg, seq: hub.seq + 1 });
  const slots = new Map(captures.map((c) => [c.rpc, { ...c, once: false, repeat: null, number: 0, running: false, timer: null, told: null }]));
  const list = () => send({ type: 'captures', route, captures: captures.map(({ rpc }) => ({ rpc, label: slots.get(rpc).label ?? null })) });
  const tell = (slot, state, more = {}) => {
    const told = JSON.stringify([state, more, slot.repeat, slot.number]);
    if (slot.told === told) return;
    slot.told = told;
    send({ type: 'capture', route, rpc: slot.rpc, state, number: slot.number, repeat: slot.repeat, ...more });
  };
  const wanted = (slot) => slot.once || slot.repeat !== null;
  const api = { posted: [], latency: 100, failing: {}, lost: false };
  /** Takes one capture of `slot`, and the next while the page asks for them again and again. */
  const run = (slot) => {
    slot.running = true;
    tell(slot, 'capturing');
    slot.timer = clock.after(api.latency, () => {
      const failed = api.failing[slot.rpc];
      delete api.failing[slot.rpc];
      if (failed) {
        slot.once = false;
        tell(slot, failed.error ? 'error' : failed, failed.error ? { error: failed.error } : {});
      } else if (wanted(slot)) {
        slot.number++;
        const label = slot.label ?? { name: '', units: '', x_name: '', x_units: '' };
        const frame = encodeCapture({ route, rpc: slot.rpc, number: slot.number, name: label.name, units: label.units, xName: label.x_name, xUnits: label.x_units, ...slot.shot(slot.number) });
        slot.once = false;
        hub.binary(frame);
        tell(slot, slot.repeat !== null ? 'waiting' : 'idle');
      } else tell(slot, 'idle');
      if (slot.repeat === null) return void (slot.running = false);
      slot.timer = clock.after(Math.max(slot.repeat * 1000, failed ? 1000 : 0), () => (wanted(slot) ? run(slot) : ((slot.running = false), tell(slot, 'idle'))));
    });
  };
  /** Takes one message of the page's, as the hub does once it has arrived. */
  const take = (msg) => {
    const slot = msg.route === route && !api.lost ? slots.get(msg.rpc) : undefined;
    if (!slot) return;
    if (msg.type === 'capture') slot.once = true;
    else if (msg.type === 'repeat') slot.repeat = Math.min(10, Math.max(0.05, msg.delay));
    else if (msg.type === 'stop') [slot.once, slot.repeat] = [false, null];
    else return;
    if (wanted(slot) && !slot.running) run(slot);
    else if (!wanted(slot) && !slot.running) tell(slot, 'idle');
  };
  // A message is on its way as the page's send returns, and the hub answers after, as over a socket.
  hub.post = (message) => {
    const msg = JSON.parse(message);
    api.posted.push(msg);
    Promise.resolve().then(() => take(msg));
    return true;
  };
  const presence = (state) => send({ type: 'device', route, name: 'test', serial: route, firmware: '1', presence: state });
  Object.assign(api, {
    asks: (rpc) => ({ once: slots.get(rpc).once, repeat: slots.get(rpc).repeat }),
    number: (rpc) => slots.get(rpc).number,
    reading: (rpc, read, size) => tell(slots.get(rpc), 'reading', { read, size }),
    lose() {
      api.lost = true;
      presence('lost');
      for (const slot of slots.values()) {
        const had = wanted(slot) || slot.running;
        clock.cancel(slot.timer);
        Object.assign(slot, { once: false, repeat: null, running: false });
        if (had) tell(slot, 'error', { error: 'the device is gone' });
      }
    },
    find() {
      api.lost = false;
      presence('alive');
      list();
    },
  });
  if (!hub.session.devices.has(route)) {
    send({ type: 'routes', mounts: {}, routes: [...hub.session.devices.keys(), route] });
    presence('alive');
  }
  list();
  hub.captures = api;
  // What the page's plots already show is asked as the captures are listed.
  hub.sendCaptures();
  return api;
}

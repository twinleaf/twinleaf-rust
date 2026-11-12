// The page's connection to the hub: the socket, opened again whenever it
// closes, and the Session of the hub it reached. Each `hub` message, the
// start of a snapshot, replaces the session whole, and each change after it
// must be numbered one more than the last. A gap closes the socket, and the
// snapshot of the socket opened in its place fills it, the one way the page
// comes to hold a hub's state (see protocol.js). The plots' views, which
// outlive any hub, read the time through `now`. The devices' RPCs go
// through `call` and `read`. What the page lost (loss.svelte.js) and the
// hub's log (log.svelte.js) outlive the sessions too: every snapshot and
// every `loss` and `log` message is handed to them.
//
// The page asks the hub for what its plots show, and says so again to each
// hub it reaches: the spectra (`showSpectra`), whether a plot is held
// paused (`hold`), and the captures (`showCaptures`, `trigger`). Each is
// sent when it differs from what the hub was last told (`post`).

import { Session } from './session.svelte.js';
import { Losses } from './loss.svelte.js';
import { Log } from './log.svelte.js';
import { DEVICE_GONE, api, captureKey, captureMessage, decodeFrame, holdMessage, spectraMessage } from './protocol.js';
import { WINDOW_LIMITS } from './window.js';
import * as clock from './clock.js';

/** How long the page waits before it opens a socket that closed on its own. */
const RECONNECT_MS = 1000;
/** The messages after which what the hub takes of the page's capture asks may differ: a device listed, lost or back, its captures listed, a capture ended. */
const CAPTURE_NEWS = ['routes', 'device', 'captures', 'capture'];

export class Hub {
  /** The state of the hub the socket last reached, replaced by every snapshot. */
  session = $state.raw(new Session());
  /** Whether the socket reached a hub and is open. */
  connected = $state(false);
  /** What the page lost of its data, and where, since it opened. */
  loss = new Losses(clock.wall());
  /** The hub's log, and the drawer that shows it. */
  log = new Log(typeof localStorage === 'undefined' ? null : localStorage);
  /** Bytes of binary frames received, for the page's rate readout. */
  bytes = 0;
  /** Whether the page holds a plot paused, for which the hub keeps a capture of its history. */
  holding = false;
  /** Who holds a plot paused now: each that `hold` heard so from last. */
  holders = new Set();
  /** The number of the last change applied. */
  seq = 0;
  /** The signal keys the page shows as spectra, and the ids last sent for them. */
  spectrumKeys = [];
  sentSpectra = null;
  /** Whether the hub was last told the page holds a plot paused: a new connection holds nothing. */
  sentHold = false;
  /** The captures the page's plots show, by key: the seconds between captures each is asked for again and again, or null for none. */
  captureWants = new Map();
  /** The captures the hub holds an ask of from this page, by key, as {repeat}: the delay it was asked to repeat at, or null for one capture alone. */
  asked = new Map();

  /** Applies one text message: a snapshot's `hub`, a snapshot's state, or a change. */
  text(msg) {
    if (msg.type === 'hub') {
      this.session.clearAllWindows();
      this.session = new Session(msg.id);
      this.loss.snapshot(msg.id, msg.behind ?? 0, clock.now());
      this.log.snapshot(msg.id);
      this.connected = true;
      this.seq = msg.seq;
      this.sentSpectra = null;
      this.sentHold = false;
      this.asked = new Map();
      this.sendHold();
      return;
    }
    if (msg.seq !== undefined && !this.next(msg.seq)) return this.resync();
    if (msg.type === 'loss') return this.loss.apply(msg, msg.seq === undefined, (stream) => this.session.rateOf(msg.route, stream), clock.now());
    if (msg.type === 'log') return this.log.take(msg, msg.seq === undefined);
    this.session.text(msg);
    if (msg.type === 'trace') this.sendSpectra();
    // The hub drops every page's ask of a capture as it loses the device.
    if (msg.type === 'capture' && msg.state === 'error' && msg.error === DEVICE_GONE) this.asked.delete(captureKey(msg));
    if (CAPTURE_NEWS.includes(msg.type)) this.sendCaptures();
  }

  /** Sends `message` to the hub, and says whether it went: nothing goes before the socket has reached one. */
  post(message) {
    if (this.ws?.readyState !== WebSocket.OPEN || this.session.id === null) return false;
    this.ws.send(message);
    return true;
  }

  binary(buf) {
    this.bytes += buf.byteLength;
    const frame = decodeFrame(buf);
    if (!frame) return;
    if (frame.seq !== 0 && !this.next(frame.seq)) return this.resync();
    if (!this.session.apply(frame)) this.resync();
  }

  /** Whether change `seq` is the next one, which it then is the last of. */
  next(seq) {
    if (seq !== this.seq + 1) return false;
    this.seq = seq;
    return true;
  }

  /** Drops the socket and opens another at once, for a snapshot to fill a gap. */
  resync() {
    const ws = this.ws;
    if (!ws) return;
    ws.onclose = null;
    ws.onmessage = null;
    ws.close();
    this.connect();
  }

  /** Shows the signals `keys` as spectra: the hub estimates only the spectra some page shows. */
  showSpectra(keys) {
    this.spectrumKeys = keys;
    this.sendSpectra();
  }

  /** Tells the hub which of its traces the page shows as spectra, when that changed. */
  sendSpectra() {
    const ids = this.spectrumKeys.map((k) => this.session.byKey.get(k)?.meta.id).filter((id) => id !== undefined);
    const message = spectraMessage([...new Set(ids)].sort((a, b) => a - b));
    if (message !== this.sentSpectra && this.post(message)) this.sentSpectra = message;
  }

  /**
   * Shows the captures of `wants`, a Map of capture key to the seconds
   * between captures it is asked for again and again, or null for none:
   * the hub takes the captures some page asks for, and sends each to the
   * pages that asked.
   */
  showCaptures(wants) {
    this.captureWants = wants;
    this.sendCaptures();
  }

  /** The Capture of `key` while the hub takes an ask of it: one a device lists that is alive. */
  #asks(key) {
    const c = this.session.byKey.get(key);
    return c && this.session.devices.get(c.route)?.presence === 'alive' ? c : null;
  }

  /**
   * Tells the hub what the page wants of each capture, where that differs
   * from what it holds of the page's: `stop` for one no plot shows any
   * more, or shows without repeating, and `repeat` for one to repeat at a
   * delay it was not asked at. An ask of a capture whose device is lost is
   * forgotten, as the hub dropped it, and made again once the device is
   * back.
   */
  sendCaptures() {
    for (const [key, ask] of this.asked) {
      const c = this.#asks(key), want = this.captureWants.get(key);
      if (!c) this.asked.delete(key);
      else if ((want === undefined || (want === null && ask.repeat !== null)) && this.post(captureMessage('stop', c.route, c.rpc))) this.asked.delete(key);
    }
    for (const [key, delay] of this.captureWants) {
      const c = this.#asks(key);
      if (!c || delay === null || this.asked.get(key)?.repeat === delay) continue;
      if (this.post(captureMessage('repeat', c.route, c.rpc, delay))) this.asked.set(key, { repeat: delay });
    }
  }

  /** Asks for one capture now of each of `keys` that the page's plots show. */
  trigger(keys) {
    for (const key of keys) {
      const c = this.captureWants.has(key) ? this.#asks(key) : null;
      if (c && this.post(captureMessage('capture', c.route, c.rpc)) && !this.asked.has(key)) this.asked.set(key, { repeat: null });
    }
  }

  /**
   * Notes whether `who`, anything that shows plots, holds one paused: `on`.
   * The hub keeps a capture of its history, as a scope's Stop does, while
   * anyone does.
   */
  hold(who, on) {
    if (on) this.holders.add(who);
    else this.holders.delete(who);
    this.holding = this.holders.size > 0;
    this.sendHold();
  }

  /** Tells the hub whether the page holds a plot paused, when that changed. */
  sendHold() {
    const on = this.holding;
    if (on !== this.sentHold && this.post(holdMessage(on))) this.sentHold = on;
  }

  /** The widest a live window goes, in seconds: what the coarsest level of decimation keeps. */
  liveSeconds() {
    return this.session.levels.at(-1) ?? WINDOW_LIMITS[1];
  }

  /** Device time that page time `perfMs` corresponds to. */
  now(perfMs) {
    return this.session.now(perfMs);
  }

  /** Calls RPC `name` with `value`, as typed, on the devices at `routes`: protocol.js `api.call`. */
  call(name, value, routes) {
    return api.call(name, value, routes);
  }

  /** Reads `names` of the device at `route`: protocol.js `api.read`. */
  read(route, names) {
    return api.read(route, names);
  }

  /** Opens the socket, and opens it again whenever it closes, until `close`. */
  connect() {
    if (this.closed) return;
    const ws = new WebSocket(`ws://${location.host}/ws`);
    ws.binaryType = 'arraybuffer';
    ws.onmessage = (ev) => (typeof ev.data === 'string' ? this.text(JSON.parse(ev.data)) : this.binary(ev.data));
    ws.onclose = () => {
      this.connected = false;
      clock.after(RECONNECT_MS, () => this.connect());
    };
    this.ws = ws;
  }

  /** Closes the socket for good, and ends the session's window reads, as the page goes away. */
  close() {
    this.closed = true;
    this.session.clearAllWindows();
    if (this.ws) this.ws.onclose = null;
    this.ws?.close();
  }
}

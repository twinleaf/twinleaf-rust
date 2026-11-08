// The page's connection to the hub: the socket, opened again whenever it
// closes, and the Session of the hub it reached. Each `hub` message, the
// start of a snapshot, replaces the session whole, and each change after it
// must be numbered one more than the last. A gap closes the socket, and the
// snapshot of the socket opened in its place fills it, the one way the page
// comes to hold a hub's state (see protocol.js). The plots' views, which
// outlive any hub, read the time through `now`.

import { Session } from './session.svelte.js';
import { decodeFrame, holdMessage, spectraMessage } from './protocol.js';

/** How long the page waits before it opens a socket that closed on its own. */
const RECONNECT_MS = 1000;

export class Hub {
  /** The state of the hub the socket last reached, replaced by every snapshot. */
  session = $state.raw(new Session());
  /** Whether the socket reached a hub and is open. */
  connected = $state(false);
  /** Bytes of binary frames received, for the page's rate readout. */
  bytes = 0;
  /** Whether the page holds a plot paused, for which the hub keeps a capture of its history. */
  holding = false;
  /** The number of the last change applied. */
  seq = 0;
  /** The signal keys the page shows as spectra, and the ids last sent for them. */
  spectrumKeys = [];
  sentSpectra = null;
  /** Whether the hub was last told the page holds a plot paused: a new connection holds nothing. */
  sentHold = false;

  /** Applies one text message: a snapshot's `hub`, a snapshot's state, or a change. */
  text(msg) {
    if (msg.type === 'hub') {
      this.session = new Session(msg.id);
      this.connected = true;
      this.seq = msg.seq;
      this.sentSpectra = null;
      this.sentHold = false;
      this.sendHold();
      return;
    }
    if (msg.seq !== undefined && !this.next(msg.seq)) return this.resync();
    this.session.text(msg);
    if (msg.type === 'trace') this.sendSpectra();
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
    if (message === this.sentSpectra || this.ws?.readyState !== WebSocket.OPEN || this.session.id === null) return;
    this.ws.send(message);
    this.sentSpectra = message;
  }

  /** Has the hub keep a capture of its history while `on`, as a scope's Stop does, for the page's paused plots. */
  hold(on) {
    this.holding = on;
    this.sendHold();
  }

  /** Tells the hub whether the page holds a plot paused, when that changed. */
  sendHold() {
    const on = this.holding;
    if (on === this.sentHold || this.ws?.readyState !== WebSocket.OPEN || this.session.id === null) return;
    this.ws.send(holdMessage(on));
    this.sentHold = on;
  }

  /** The seconds of history the hub stores at page time `perfMs`, back to the oldest time it holds of any trace. */
  historySeconds(perfMs) {
    const m = this.session.memory;
    if (!m) return 0;
    const oldest = Math.min(...Object.values(m.oldest ?? {}));
    return Math.max(m.seconds, isFinite(oldest) ? this.now(perfMs) - oldest : 0);
  }

  /** Device time that page time `perfMs` corresponds to. */
  now(perfMs) {
    return this.session.now(perfMs);
  }

  /** Opens the socket, and opens it again whenever it closes, until `close`. */
  connect() {
    if (this.closed) return;
    const ws = new WebSocket(`ws://${location.host}/ws`);
    ws.binaryType = 'arraybuffer';
    ws.onmessage = (ev) => (typeof ev.data === 'string' ? this.text(JSON.parse(ev.data)) : this.binary(ev.data));
    ws.onclose = () => {
      this.connected = false;
      setTimeout(() => this.connect(), RECONNECT_MS);
    };
    this.ws = ws;
  }

  /** Closes the socket for good, as the page goes away. */
  close() {
    this.closed = true;
    if (this.ws) this.ws.onclose = null;
    this.ws?.close();
  }
}

import { describe, expect, it } from 'vitest';
import { Session, Trace } from '../src/lib/session.svelte.js';
import { Hub } from '../src/lib/hub.svelte.js';
import { decodeFrame, holdMessage, signalKey, spectraMessage } from '../src/lib/protocol.js';

const meta = (id, route = '/', stream = 'sine', column = 'sine') => ({ id, route, stream, column, units: 'V', rate: 100 });

/** A points frame as wire.rs encodes it: chunks of [id, brk, xs, ys], change `seq`. */
function pointsFrame(seq, chunks) {
  const size = 16 + 2 + chunks.reduce((n, [, , xs]) => n + 8 + xs.length * 12, 0);
  const dv = new DataView(new ArrayBuffer(size));
  dv.setUint8(0, 1);
  dv.setBigUint64(8, BigInt(seq), true);
  dv.setUint16(16, chunks.length, true);
  let off = 18;
  for (const [id, brk, xs, ys, level = 0] of chunks) {
    dv.setUint16(off, id, true);
    dv.setUint8(off + 2, brk ? 1 : 0);
    dv.setUint8(off + 3, level);
    dv.setUint32(off + 4, xs.length, true);
    off += 8;
    xs.forEach((x, i) => dv.setFloat64(off + i * 8, x, true));
    off += xs.length * 8;
    ys.forEach((y, i) => dv.setFloat32(off + i * 4, y, true));
    off += ys.length * 4;
  }
  return dv.buffer;
}

/** A Hub on a fake socket, which records what it sends and whether it was closed. */
function hubOnFakeSocket() {
  const hub = new Hub();
  const socket = { readyState: 1, sent: [], closed: false, send(m) { this.sent.push(m); }, close() { this.closed = true; } };
  hub.ws = socket;
  hub.connect = () => (hub.reconnected = (hub.reconnected ?? 0) + 1);
  globalThis.WebSocket ??= { OPEN: 1 };
  return { hub, socket };
}

describe('signal keys', () => {
  it('name a root signal by stream and column, and another by its route too', () => {
    expect(signalKey(meta(0))).toBe('sine.sine');
    expect(signalKey(meta(0, '/1', 'aux', 'triangle'))).toBe('1/aux.triangle');
  });
});

describe('frames', () => {
  it('decode as wire.rs lays them out', () => {
    const frame = decodeFrame(pointsFrame(7, [[3, true, [1.5, 2.5], [2, 3]]]));
    expect(frame.kind).toBe('points');
    expect(frame.seq).toBe(7);
    expect(frame.chunks[0].id).toBe(3);
    expect(frame.chunks[0].brk).toBe(true);
    expect([...frame.chunks[0].xs]).toEqual([1.5, 2.5]);
  });
});

describe('a spectra frame', () => {
  it('gives each trace of the round its spectrum', () => {
    const n = 2, dv = new DataView(new ArrayBuffer(16 + 8 + 16 + n * 8));
    dv.setUint8(0, 2);
    dv.setBigUint64(8, 4n, true);
    dv.setUint16(16, 1, true);
    dv.setUint16(24, 0, true);
    dv.setUint32(28, n, true);
    dv.setFloat64(32, NaN, true);
    [1, 2, 0.5, 0.25].forEach((v, i) => dv.setFloat32(40 + i * 4, v, true));
    const frame = decodeFrame(dv.buffer);
    expect(frame).toMatchObject({ kind: 'spectra', seq: 4 });
    const s = new Session('h');
    s.text({ type: 'trace', trace: meta(0) });
    expect(s.apply(frame)).toBe(true);
    expect([...s.byKey.get('sine.sine').spectrum.a]).toEqual([0.5, 0.25]);
    expect(s.byKey.get('sine.sine').spectrum.floor).toBeNull();
  });
});

describe('a trace', () => {
  it('lifts the pen at a break and where time runs back', () => {
    const t = new Trace(meta(0));
    t.push([1, 2], [0, 0], false);
    t.push([3, 4], [0, 0], true);
    t.push([2.5], [0], false);
    expect(t.breaks).toEqual([2, 4]);
  });

  it('drops points older than it keeps, but none a paused view holds', () => {
    const t = new Trace(meta(0));
    t.push([0, 100, 200], [0, 0, 0], false, 50);
    expect(t.xAt(t.absStart)).toBe(100);
    t.push([300], [0], false);
    expect(t.xAt(t.absStart)).toBe(200);
  });
});

describe('a session', () => {
  it('lists its signals sorted, and applies points to their traces', () => {
    const s = new Session('h');
    s.text({ type: 'trace', trace: meta(1, '/', 'sine', 'b') });
    s.text({ type: 'trace', trace: meta(0, '/', 'sine', 'a') });
    expect(s.signals.map((t) => t.key)).toEqual(['sine.a', 'sine.b']);
    expect(s.apply(decodeFrame(pointsFrame(0, [[0, false, [1, 2], [5, 6]]])))).toBe(true);
    expect(s.byKey.get('sine.a').length).toBe(2);
    expect(s.latest).toBe(2);
  });

  it('runs its clock evenly however late the batches arrive', () => {
    const s = new Session('h');
    s.text({ type: 'trace', trace: meta(0, '/', 'sine', 'a') });
    // A batch every 16.7 or 33.3 ms, each up to 10 ms late, ticked at 240 Hz.
    let seed = 1;
    const rand = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
    const FRAME = 1000 / 240, DEVICE = 100;
    let next = 0, sent = 0;
    const frames = [];
    for (let perf = 0; perf < 6000; perf += FRAME) {
      while (next <= perf) {
        const upTo = (next - 5) / 1000 + DEVICE;
        s.apply(decodeFrame(pointsFrame(0, [[0, false, [upTo], [0]]])), next + rand() * 10);
        sent++;
        next += rand() < 0.3 ? 33.3 : 16.7;
      }
      s.tick(perf);
      frames.push([perf, s.now(perf)]);
    }
    expect(sent).toBeGreaterThan(200);
    // After the first second the clock advances one device second each page second, within the slew.
    const steady = frames.filter(([perf]) => perf > 1000);
    for (let i = 1; i < steady.length; i++) {
      const rate = (steady[i][1] - steady[i - 1][1]) / ((steady[i][0] - steady[i - 1][0]) / 1000);
      expect(Math.abs(rate - 1)).toBeLessThanOrEqual(0.005 + 1e-9);
    }
    // It trails the newest point by about the least delay a batch arrived with.
    const [perf, now] = steady.at(-1);
    expect(perf / 1000 + DEVICE - now).toBeGreaterThan(0.004);
    expect(perf / 1000 + DEVICE - now).toBeLessThan(0.02);
  });

  it('sets its clock at once when the device time jumps', () => {
    const s = new Session('h');
    s.text({ type: 'trace', trace: meta(0, '/', 'sine', 'a') });
    s.apply(decodeFrame(pointsFrame(0, [[0, false, [10], [0]]])), 0);
    s.tick(0);
    expect(s.now(0)).toBe(10);
    s.apply(decodeFrame(pointsFrame(0, [[0, false, [50], [0]]])), 100);
    s.tick(100);
    expect(s.now(100)).toBe(50);
  });

  it('keeps each level of a trace apart, each for as long as the hub says', () => {
    const s = new Session('h');
    s.text({ type: 'trace', trace: meta(0, '/', 'sine', 'a') });
    s.text({ type: 'config', pps: 500, levels: [10, 40, 160] });
    s.apply(decodeFrame(pointsFrame(0, [[0, false, [0, 30, 60], [1, 2, 3]], [0, false, [0, 60], [1, 3], 2]])));
    const t = s.byKey.get('sine.a');
    expect(t.levels.length).toBe(3);
    expect([t.length, t.levels[1].length, t.levels[2].length]).toEqual([1, 0, 2]);
    expect(t.levels[2].keep).toBe(160);
    t.clear();
    expect(t.levels.every((l) => l.length === 0)).toBe(true);
  });

  it('calls a point of a trace it never heard of a gap', () => {
    const s = new Session('h');
    expect(s.apply(decodeFrame(pointsFrame(0, [[9, false, [1], [1]]])))).toBe(false);
  });

  it('keeps each device and changes only the values a device announces anew', () => {
    const s = new Session('h');
    s.text({ type: 'routes', routes: ['/', '/0'], mounts: null });
    s.text({ type: 'settings', route: '/0', values: { gain: 2, mode: 'a', name: { x: 1 } } });
    const d = s.devices.get('/0');
    const name = d.values.get('name');
    s.text({ type: 'settings', route: '/0', values: { gain: 3, name: { x: 1 } } });
    expect(s.devices.get('/0')).toBe(d);
    expect([...d.values.keys()]).toEqual(['gain', 'name']);
    expect(d.values.get('gain')).toBe(3);
    expect(d.values.get('name')).toBe(name);
    s.text({ type: 'routes', routes: ['/0'], mounts: null });
    expect([...s.devices.keys()]).toEqual(['/0']);
    s.text({ type: 'device', route: '/', name: 'gone', serial: null, firmware: null, presence: 'lost' });
    s.text({ type: 'table', route: '/', rpcs: [] });
    expect([...s.devices.keys()]).toEqual(['/0']);
  });
});

describe('the hub', () => {
  it('replaces the session whole with every snapshot, the same hub again included', () => {
    const { hub } = hubOnFakeSocket();
    hub.text({ type: 'hub', id: 'a', seq: 0 });
    hub.text({ type: 'trace', trace: meta(0) });
    hub.binary(pointsFrame(0, [[0, false, [1, 2], [1, 2]]]));
    const first = hub.session;
    // A reconnect to the same hub sends the history again, which must not
    // land on top of what the page holds.
    hub.text({ type: 'hub', id: 'a', seq: 5 });
    hub.text({ type: 'trace', trace: meta(0) });
    hub.binary(pointsFrame(0, [[0, false, [1, 2], [1, 2]]]));
    expect(hub.session).not.toBe(first);
    expect(hub.session.byKey.get('sine.sine').length).toBe(2);
  });

  it('applies changes in order and goes back for a snapshot at a gap', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text({ type: 'hub', id: 'a', seq: 3 });
    hub.text({ type: 'trace', trace: meta(0), seq: 4 });
    hub.binary(pointsFrame(5, [[0, false, [1], [1]]]));
    expect(socket.closed).toBe(false);
    hub.binary(pointsFrame(7, [[0, false, [2], [2]]]));
    expect(socket.closed).toBe(true);
    expect(hub.reconnected).toBe(1);
  });

  it('goes back for a snapshot when a change names an unknown trace', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text({ type: 'hub', id: 'a', seq: 0 });
    hub.binary(pointsFrame(1, [[4, false, [1], [1]]]));
    expect(socket.closed).toBe(true);
  });

  it('names the spectra it shows once their traces are known, and again after a snapshot', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.showSpectra(['sine.sine']);
    hub.text({ type: 'hub', id: 'a', seq: 0 });
    hub.text({ type: 'trace', trace: meta(2) });
    expect(socket.sent).toEqual([spectraMessage([2])]);
    hub.text({ type: 'trace', trace: meta(3, '/', 'aux', 'x') });
    expect(socket.sent.length).toBe(1);
    hub.text({ type: 'hub', id: 'a', seq: 0 });
    hub.text({ type: 'trace', trace: meta(2) });
    expect(socket.sent.length).toBe(2);
  });

  it('has the hub hold its history while a view is paused, in every session', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text({ type: 'hub', id: 'a', seq: 0 });
    expect(socket.sent).toEqual([]);
    hub.hold(true);
    hub.text({ type: 'hub', id: 'b', seq: 0 });
    hub.hold(false);
    expect(socket.sent).toEqual([holdMessage(true), holdMessage(true), holdMessage(false)]);
  });

});

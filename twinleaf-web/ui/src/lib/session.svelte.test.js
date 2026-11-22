import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import fs from 'node:fs';
import path from 'node:path';
import { dropping, Session, Trace } from './session.svelte.js';
import { pausedKeeps } from './words.js';
import { Hub } from './hub.svelte.js';
import { decodeFrame, holdMessage, spectraMessage } from './protocol.js';
import { message, trace } from '../../tests/fixtures/hub.js';

/** The recorded trace as trace `id`, `name`, the column `column` of `stream`. */
const traced = (id, name = 'sine.sine', stream = 'sine', column = 'sine') => trace({ id, name, stream, column });

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

describe('a spectra frame', () => {
  it('gives each trace of the round its spectrum', () => {
    const bytes = fs.readFileSync(path.resolve('tests/fixtures/hub/spectra.bin'));
    const frame = decodeFrame(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length));
    const s = new Session('h');
    s.text(message('trace'));
    expect(s.apply(frame)).toBe(true);
    const { spectrum } = s.byKey.get(message('trace').trace.name), [sent] = frame.spectra;
    expect([spectrum.a, spectrum.floor]).toEqual([sent.a, sent.floor]);
  });
});

describe('a trace', () => {
  it('lifts the pen at a break and where time runs back', () => {
    const t = new Trace(traced(0).trace);
    t.push([1, 2], [0, 0], false);
    t.push([3, 4], [0, 0], true);
    t.push([2.5], [0], false);
    expect(t.breaks).toEqual([2, 4]);
  });

  it('drops points older than it keeps, but none a paused view holds', () => {
    const t = new Trace(traced(0).trace);
    t.keep = 120;
    t.push([0, 100, 200], [0, 0, 0], false, 50);
    expect(t.xAt(t.absStart)).toBe(100);
    t.push([300], [0], false);
    expect(t.xAt(t.absStart)).toBe(200);
  });
});

describe('a session', () => {
  it('lists its signals sorted, and applies points to their traces', () => {
    const s = new Session('h');
    s.text(traced(1, 'sine.b', 'sine', 'b'));
    s.text(traced(0, 'sine.a', 'sine', 'a'));
    expect(s.signals.map((t) => t.key)).toEqual(['sine.a', 'sine.b']);
    expect(s.apply(decodeFrame(pointsFrame(0, [[0, false, [1, 2], [5, 6]]])))).toBe(true);
    expect(s.byKey.get('sine.a').length).toBe(2);
    expect(s.latest).toBe(2);
  });

  it('takes the units, rate and epoch of a trace the hub sends again', () => {
    const s = new Session('h');
    s.text(traced(0, 'sine.a', 'sine', 'a'));
    const t = s.byKey.get('sine.a');
    s.text(trace({ id: 0, name: 'sine.a', stream: 'sine', column: 'a', units: 'mV', rate: 50, epoch: 'unix' }));
    expect(s.byKey.get('sine.a')).toBe(t);
    expect([t.meta.units, t.meta.rate, t.meta.epoch]).toEqual(['mV', 50, 'unix']);
  });

  it('runs its clock evenly however late the batches arrive', () => {
    const s = new Session('h');
    s.text(traced(0, 'sine.a', 'sine', 'a'));
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
    s.text(traced(0, 'sine.a', 'sine', 'a'));
    s.apply(decodeFrame(pointsFrame(0, [[0, false, [10], [0]]])), 0);
    s.tick(0);
    expect(s.now(0)).toBe(10);
    s.apply(decodeFrame(pointsFrame(0, [[0, false, [50], [0]]])), 100);
    s.tick(100);
    expect(s.now(100)).toBe(50);
  });

  it('keeps each level of a trace apart, each for as long as the hub says', () => {
    const s = new Session('h');
    s.text(traced(0, 'sine.a', 'sine', 'a'));
    s.text(message('config', { levels: [10, 40, 160] }));
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
    s.text(message('routes', { routes: ['/', '/0'] }));
    s.text(message('settings', { route: '/0', values: { gain: 2, mode: 'a', name: { x: 1 } } }));
    const d = s.devices.get('/0');
    const name = d.values.get('name');
    s.text(message('settings', { route: '/0', values: { gain: 3, name: { x: 1 } } }));
    expect(s.devices.get('/0')).toBe(d);
    expect([...d.values.keys()]).toEqual(['gain', 'name']);
    expect(d.values.get('gain')).toBe(3);
    expect(d.values.get('name')).toBe(name);
    s.text(message('routes', { routes: ['/0'] }));
    expect([...s.devices.keys()]).toEqual(['/0']);
    s.text(message('device', { presence: 'lost' }));
    s.text(message('table'));
    expect([...s.devices.keys()]).toEqual(['/0']);
  });
});

describe('the hub', () => {
  it('replaces the session whole with every snapshot, the same hub again included', () => {
    const { hub } = hubOnFakeSocket();
    hub.text(message('hub', { seq: 0 }));
    hub.text(traced(0));
    hub.binary(pointsFrame(0, [[0, false, [1, 2], [1, 2]]]));
    const first = hub.session;
    // A reconnect to the same hub sends the history again, which must not
    // land on top of what the page holds.
    hub.text(message('hub', { seq: 5 }));
    hub.text(traced(0));
    hub.binary(pointsFrame(0, [[0, false, [1, 2], [1, 2]]]));
    expect(hub.session).not.toBe(first);
    expect(hub.session.byKey.get('sine.sine').length).toBe(2);
  });

  it('applies changes in order and goes back for a snapshot at a gap', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text(message('hub', { seq: 3 }));
    hub.text({ ...traced(0), seq: 4 });
    hub.binary(pointsFrame(5, [[0, false, [1], [1]]]));
    expect(socket.closed).toBe(false);
    hub.binary(pointsFrame(7, [[0, false, [2], [2]]]));
    expect(socket.closed).toBe(true);
    expect(hub.reconnected).toBe(1);
  });

  it('goes back for a snapshot when a change names an unknown trace', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text(message('hub', { seq: 0 }));
    hub.binary(pointsFrame(1, [[4, false, [1], [1]]]));
    expect(socket.closed).toBe(true);
  });

  it('names the spectra it shows once their traces are known, and again after a snapshot', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.showSpectra(['sine.sine']);
    hub.text(message('hub', { seq: 0 }));
    hub.text(traced(2));
    expect(socket.sent).toEqual([spectraMessage([2])]);
    hub.text(traced(3, 'aux.x', 'aux', 'x'));
    expect(socket.sent.length).toBe(1);
    hub.text(message('hub', { seq: 0 }));
    hub.text(traced(2));
    expect(socket.sent.length).toBe(2);
  });

  it('has the hub hold its history while a view is paused, in every session', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text(message('hub', { seq: 0 }));
    expect(socket.sent).toEqual([]);
    hub.hold('page', true);
    hub.text(message('hub', { id: '2', seq: 0 }));
    hub.hold('page', false);
    expect(socket.sent).toEqual([holdMessage(true), holdMessage(true), holdMessage(false)]);
  });

  it('holds its history while anyone holds a plot paused, and lets go once none does', () => {
    const { hub, socket } = hubOnFakeSocket();
    hub.text(message('hub', { seq: 0 }));
    hub.hold('page', true);
    hub.hold('figure', true);
    hub.hold('page', false);
    expect(socket.sent, 'held while the figure holds').toEqual([holdMessage(true)]);
    hub.hold('figure', false);
    expect(socket.sent).toEqual([holdMessage(true), holdMessage(false)]);
  });

});

describe('the history a hub holds', () => {
  const memory = (oldest, maxAge = 0) => message('memory', { max_age: maxAge, oldest });
  /** A session that has heard of traces 0, 1 and 2, and the traces. */
  const heard = () => {
    const s = new Session('h');
    for (const id of [0, 1, 2]) s.text(traced(id, `s.c${id}`, 's', `c${id}`));

    return { s, traces: [...s.traces.values()] };
  };

  it('starts at the oldest time the hub holds of any of the traces asked of, the longer history where they differ', () => {
    const { s, traces } = heard();
    s.text(memory({ 0: 1010, 1: 1020 }));
    expect(s.held(traces).oldest).toBe(1010);
    expect(s.held([traces[1]]).oldest).toBe(1020);
    expect(s.held([traces[1], traces[2]]).oldest).toBe(1020);
  });

  it('starts at the oldest point the page has, of any level, until the hub says, and nowhere with none', () => {
    const { s, traces } = heard();
    expect(s.held(traces).oldest).toBeNull();
    s.apply({ kind: 'points', chunks: [{ id: 0, level: 0, brk: false, xs: [50, 51], ys: [0, 0] }, { id: 1, level: 2, brk: false, xs: [7, 51], ys: [0, 0] }] }, 0);
    expect(s.held(traces).oldest).toBe(7);
    expect(s.held([traces[0]]).oldest).toBe(50);
    // The hub holds less than the page's coarse level kept.
    s.text(memory({ 0: 40, 1: 40 }));
    expect(s.held(traces).oldest).toBe(40);
  });

  it('has dropped nothing while each trace starts where it first did, however late that is', () => {
    const { s, traces } = heard();
    s.text(memory({ 0: 997, 1: 990 }));
    s.text(memory({ 0: 997, 1: 990 }));
    expect(s.held(traces).dropped).toBe(false);
  });

  it('has dropped samples of the traces asked of once one starts later than it first did', () => {
    const { s, traces } = heard();
    s.text(memory({ 0: 997, 1: 990 }));
    s.text(memory({ 0: 997, 1: 1020 }));
    expect(s.held([traces[0]]).dropped).toBe(false);
    expect(s.held([traces[1]]).dropped).toBe(true);
    expect(s.held(traces).dropped).toBe(true);
    expect(s.held([traces[2]]).dropped).toBe(false);
  });

  it('says the History setting the hub last said, and none before it has, or while it has none', () => {
    const { s, traces } = heard();
    expect(s.held(traces).history).toBeNull();
    s.text(memory({ 0: 0 }, 0));
    expect(s.held(traces).history).toBeNull();
    s.text(memory({ 0: 0 }, 86400));
    expect(s.held(traces).history).toBe(86400);
  });
});

describe('what a lower History setting drops', () => {
  /**
   * The hub's `memory` as a hub keeps it, within its History setting and
   * its budget at 1600 bytes a second, the paused copy holding no more
   * than the setting and, beyond the live history, the room the live
   * history leaves; and a new setting.
   */
  const hubAndSetting = fc
    .record({
      maxAge: fc.double({ min: 60, max: 86400, noNaN: true }),
      budget: fc.double({ min: 1e6, max: 32e9, noNaN: true }),
      fill: fc.double({ min: 0, max: 1, noNaN: true }),
      held: fc.option(fc.record({ seconds: fc.double({ min: 0, max: 1, noNaN: true }), bytes: fc.double({ min: 0, max: 1, noNaN: true }) }), { nil: null }),
      newAge: fc.double({ min: 60, max: 86400, noNaN: true }),
      newBudget: fc.double({ min: 1e6, max: 32e9, noNaN: true }),
    })
    .map(({ maxAge, budget, fill, held, newAge, newBudget }) => {
      const seconds = Math.min(maxAge, budget / 1600) * fill, bytes = seconds * 1600;
      const memory = { bytes, budget, seconds, maxAge, rate: 1600, oldest: {}, held: held && { seconds: maxAge * held.seconds, bytes: (budget - bytes) * held.bytes } };
      return { memory, maxAge: newAge, budget: newBudget };
    });
  const close = (a, b) => Math.abs(a - b) <= 1e-6 * Math.max(1, Math.abs(b));

  it('is never negative, and nothing when the hub holds no more than the new limits', () => {
    fc.assert(
      fc.property(hubAndSetting, ({ memory, maxAge, budget }) => {
        const { live, paused } = dropping(memory, maxAge, budget);
        expect(live).toBeGreaterThanOrEqual(0);
        expect(paused).toBeGreaterThanOrEqual(0);
        const beyond = memory.held?.bytes ?? 0, heldSeconds = memory.held?.seconds ?? 0;
        if (memory.seconds <= maxAge && memory.bytes <= budget) expect(live).toBe(0);
        if (Math.max(memory.seconds, heldSeconds) <= maxAge && memory.bytes + beyond <= budget) expect(close(paused, 0)).toBe(true);
      }),
    );
  });

  it('is what the hub holds beyond a lower time limit, the paused copy counted apart', () => {
    fc.assert(
      fc.property(hubAndSetting, ({ memory, maxAge }) => {
        // The budget the hub has, which holds all of it.
        const { live, paused } = dropping(memory, maxAge, memory.budget);
        expect(close(live, Math.max(0, memory.seconds - maxAge))).toBe(true);
        expect(close(paused, Math.max(0, (memory.held?.seconds ?? 0) - maxAge))).toBe(true);
        expect(dropping({ ...memory, held: null }, maxAge, memory.budget)).toEqual({ live, paused: 0 });
      }),
    );
  });

  it('is the share of the live history beyond a lower budget, its oldest', () => {
    fc.assert(
      fc.property(hubAndSetting, ({ memory, budget }) => {
        const { live } = dropping(memory, memory.maxAge, budget);
        const share = memory.bytes > budget ? (memory.bytes - budget) / memory.bytes : 0;
        expect(close(live, memory.seconds * share)).toBe(true);
      }),
    );
  });

  /**
   * The paused part alone can be less for a lower setting: a lower time
   * limit shortens the live history, which leaves the paused copy more of
   * the budget. So the live part and the sum of both parts are held.
   */
  it('is nothing for a higher setting, and no less for a lower one than for one between it and the setting', () => {
    fc.assert(
      fc.property(hubAndSetting, fc.double({ min: 0, max: 1, noNaN: true }), ({ memory, maxAge, budget }, between) => {
        const above = dropping(memory, Math.max(maxAge, memory.maxAge), Math.max(budget, memory.budget));
        expect(close(above.live, 0) && close(above.paused, 0)).toBe(true);
        if (maxAge > memory.maxAge) return;
        const lower = dropping(memory, maxAge, budget);
        const higher = dropping(memory, maxAge + (memory.maxAge - maxAge) * between, budget);
        expect(lower.live).toBeGreaterThanOrEqual(higher.live - 1e-6);
        expect(lower.live + lower.paused).toBeGreaterThanOrEqual(higher.live + higher.paused - 1e-6);
      }),
    );
  });
});

describe('what a paused plot keeps', () => {
  it('is, of an hour that needs 1.5 GB, the hour for as long as it keeps it', () => {
    const need = 1.52e9;
    expect(pausedKeeps({ need, keeps: (240 / 760) * 3600 }, 3600)).toBe('A paused plot keeps its full 60 min for ~19 min, then loses its oldest. All of it needs ~1.5 GB');
    expect(pausedKeeps({ need, keeps: 3600 }, 3600)).toBe('A paused plot keeps its full 60 min');
    expect(pausedKeeps({ need, keeps: 0 }, 3600)).toBe('A paused plot loses its oldest at once. Its full 60 min needs ~1.5 GB');
  });
});

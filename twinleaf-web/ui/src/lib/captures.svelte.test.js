// The captures in a session, and what the page asks the hub of them: one
// rule, the page asks for what its plots show, checked against a model of
// what the hub holds of the page's asks.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { Hub } from './hub.svelte.js';
import { Capture } from './session.svelte.js';
import { SHOTS_KEPT } from './shots.js';
import { captureMessage, decodeFrame } from './protocol.js';
import { encodeCapture } from '../../tests/harness/captures.js';
import { message } from '../../tests/fixtures/hub.js';
import { ask } from '../../tests/fixtures/page.js';

/** The capture RPCs the hub lists of the simulated device, fid.capture, phase.capture and test.capture, each labelled. */
const LISTED = message('captures').captures;
const LABEL = LISTED[0].label;
const KEYS = { 'fid.capture': 'capture:fid.capture', 'phase.capture': 'capture:phase.capture' };

/** A hub that reached one device, alive, which lists `rpcs`, its messages to the hub in `sent`, parsed. */
function reached(rpcs = Object.keys(KEYS)) {
  const hub = new Hub(), sent = [];
  hub.post = (message) => (sent.push(JSON.parse(message)), true);
  const say = (msg) => hub.text({ ...msg, seq: hub.seq + 1 });
  hub.text(message('hub'));
  say(message('routes'));
  say(message('device'));
  say(message('captures', { captures: rpcs.map((rpc) => ({ rpc, label: rpc === 'fid.capture' ? LABEL : null })) }));
  sent.length = 0;
  return { hub, sent, say };
}
const frame = (rpc, number, n = 3) => decodeFrame(encodeCapture({ route: '/', rpc, number, type: 0x21, calibration: 0.5, x0: 0, dx: 1, name: '', units: '', xName: '', xUnits: '', elements: Int16Array.from({ length: n }, (_, i) => i) }));

describe('a session’s captures', () => {
  it('are those its devices list, by key, each named by its metadata, or by its RPC while the device has not said', () => {
    const { hub, say } = reached();
    const [fid, phase] = hub.session.captures;
    expect(hub.session.captures.map((c) => c.key)).toEqual(Object.values(KEYS));
    expect([fid.title, fid.meta]).toEqual([LABEL.name, { units: LABEL.units, xName: LABEL.x_name, xUnits: LABEL.x_units }]);
    expect([phase.title, phase.meta]).toEqual(['phase.capture', { units: '', xName: '', xUnits: '' }]);
    expect(hub.session.trace(fid.key)).toBe(fid);
    // A capture read of it names it: the list comes again, and the Capture is the same one.
    say(message('captures', { captures: LISTED.slice(0, 2) }));
    expect(hub.session.captures[1]).toBe(phase);
    expect([phase.title, phase.meta.units]).toEqual([LISTED[1].label.name, LISTED[1].label.units]);
    // One the device lists no more goes; and all of a device the routes list no more.
    say(message('captures', { captures: [{ rpc: 'phase.capture', label: null }] }));
    expect(hub.session.captures).toEqual([phase]);
    expect(hub.session.byKey.has(fid.key)).toBe(false);
    say(message('routes', { routes: [] }));
    expect(hub.session.captures).toEqual([]);
    // They are no signals.
    expect(hub.session.signals).toEqual([]);
  });

  it('names a capture of another device after its route', () => {
    const { hub, say } = reached();
    say(message('routes', { routes: ['/', '/0'] }));
    say(message('captures', { route: '/0', captures: LISTED.slice(0, 1) }));
    const c = hub.session.byKey.get('0/capture:fid.capture');
    expect([c instanceof Capture, c.title, c.route]).toEqual([true, `0/${LABEL.name}`, '/0']);
  });

  it('hold where each stands as the hub last said, and the shots it sent, the newest SHOTS_KEPT', () => {
    const { hub, say } = reached();
    const fid = hub.session.byKey.get(KEYS['fid.capture']);
    expect(fid.status).toEqual({ state: 'idle', number: 0, repeat: null, since: 0 });
    const reading = message('capture-reading', { repeat: 0.5 });
    say(reading);
    const { state, number, repeat, read, size } = reading;
    expect(fid.status).toMatchObject({ state, number, repeat, read, size });

    for (let n = 1; n <= SHOTS_KEPT + 5; n++) expect(hub.session.apply(frame('fid.capture', n))).toBe(true);
    expect(fid.shots.map((s) => s.number)).toEqual(Array.from({ length: SHOTS_KEPT }, (_, i) => i + 6));
    expect([...fid.shots.at(-1).y]).toEqual([0, 0.5, 1]);
    // A capture of an RPC no device lists is dropped, and is no gap.
    expect(hub.session.apply(frame('other.capture', 1))).toBe(true);
    // A capture frame is no numbered change: the next change is still the next.
    const seq = hub.seq;
    hub.binary(encodeCapture({ route: '/', rpc: 'fid.capture', number: 99, type: 0x42, calibration: 1, x0: 0, dx: 1, name: '', units: '', xName: '', xUnits: '', elements: Float32Array.of(1, 2) }));
    expect([hub.seq, fid.shots.at(-1).number]).toEqual([seq, 99]);
  });
});

/** What the page's plots show of the two captures: each not shown, shown, or shown on Auto at a delay. */
const want = fc.constantFrom(undefined, null, 0.2, 0.5);
const step = fc.oneof(
  fc.record({ do: fc.constant('show'), fid: want, phase: want }),
  fc.record({ do: fc.constant('trigger'), rpc: fc.constantFrom(...Object.keys(KEYS)) }),
  // The hub says a capture's device is gone before or after it says the device is lost.
  fc.record({ do: fc.constant('lose'), goneFirst: fc.boolean() }),
  fc.record({ do: fc.constant('find') }),
  fc.record({ do: fc.constant('reconnect') }),
);

describe('what the page asks of the hub’s captures', () => {
  it('leaves the hub repeating each capture a plot shows on Auto, at its delay, and no other, whatever the plots did before, a device lost and back, a new connection', () => {
    fc.assert(
      fc.property(fc.array(step, { maxLength: 30 }), (steps) => {
        let { hub, sent, say } = reached();
        // The hub's side, by its rules: what it holds of this page's asks, by rpc.
        const held = new Map();
        // `alive` is whether the hub has the device, and `told` whether the page was last told so.
        let alive = true, told = true, wants = new Map();
        const take = () => {
          for (const m of sent.splice(0)) {
            // The page asks nothing of a device it was told is lost; what it asks of one the hub has lost, the hub drops.
            expect(told, `asked of a device it knows lost: ${JSON.stringify(m)}`).toBe(true);
            if (!alive) continue;
            if (m.type === 'repeat') held.set(m.rpc, { ...held.get(m.rpc), repeat: m.delay });
            else if (m.type === 'capture') held.set(m.rpc, { repeat: null, ...held.get(m.rpc), once: true });
            else if (m.type === 'stop') held.delete(m.rpc);
          }
        };
        for (const s of steps) {
          if (s.do === 'show') {
            wants = new Map(Object.entries({ 'fid.capture': s.fid, 'phase.capture': s.phase }).filter(([, w]) => w !== undefined).map(([rpc, w]) => [KEYS[rpc], w]));
            hub.showCaptures(wants);
          } else if (s.do === 'trigger') hub.trigger([KEYS[s.rpc]]);
          else if (s.do === 'lose' && alive) {
            alive = false;
            // The hub drops every ask, and says so of each capture it was taking.
            const lost = () => (say(message('device', { presence: 'lost' })), (told = false));
            const gone = [...held.keys()];
            held.clear();
            if (!s.goneFirst) lost();
            for (const rpc of gone) say(message('capture-error', { rpc }));
            // What the page asks again before it hears the device is lost, the hub drops.
            take();
            if (s.goneFirst) lost();
          } else if (s.do === 'find' && !alive) {
            alive = told = true;
            say(message('device'));
          } else if (s.do === 'reconnect') {
            // A new socket: the hub holds nothing of the page, which keeps what its plots show.
            const kept = hub.captureWants;
            held.clear();
            hub.text(message('hub', { id: '2' }));
            expect(hub.asked.size, 'what it asked of the hub before').toBe(0);
            told = alive;
            say(message('routes'));
            say(message('device', { presence: alive ? 'alive' : 'lost' }));
            say(message('captures', { captures: Object.keys(KEYS).map((rpc) => ({ rpc, label: null })) }));
            expect(hub.captureWants).toBe(kept);
          }
          take();
          // While the device is alive the hub repeats exactly what the plots show on Auto, each at its delay.
          const repeating = new Map([...held].filter(([, a]) => a.repeat !== null && a.repeat !== undefined).map(([rpc, a]) => [KEYS[rpc], a.repeat]));
          const auto = new Map([...wants].filter(([, w]) => w !== null));
          expect(repeating, JSON.stringify(s)).toEqual(alive ? auto : new Map());
          // And it holds no ask at all of a capture no plot shows.
          for (const rpc of held.keys()) expect(wants.has(KEYS[rpc]), `${rpc} asked while no plot shows it`).toBe(true);
        }
      }),
    );
  });

  it('asks once for a capture two plots show, the shorter delay of two on Auto, and takes it back once when neither shows it', () => {
    const { hub, sent } = reached();
    hub.showCaptures(new Map([[KEYS['fid.capture'], 0.5]]));
    hub.showCaptures(new Map([[KEYS['fid.capture'], 0.5]]));
    expect(sent).toEqual([ask('repeat', { route: '/', delay: 0.5 })]);
    hub.showCaptures(new Map([[KEYS['fid.capture'], 0.2]]));
    expect(sent.at(-1)).toEqual(ask('repeat', { route: '/', delay: 0.2 }));
    hub.showCaptures(new Map());
    hub.showCaptures(new Map());
    expect(sent.slice(2)).toEqual([ask('stop')]);
  });

  it('triggers only a capture a plot shows, and takes the ask back when the plot goes', () => {
    const { hub, sent } = reached();
    hub.trigger([KEYS['fid.capture']]);
    expect(sent).toEqual([]);
    hub.showCaptures(new Map([[KEYS['fid.capture'], null]]));
    hub.trigger([KEYS['fid.capture'], KEYS['phase.capture']]);
    expect(sent).toEqual([JSON.parse(captureMessage('capture', '/', 'fid.capture'))]);

    hub.showCaptures(new Map());
    expect(sent.at(-1)).toEqual(ask('stop'));
  });

  it('sends nothing before the socket has reached a hub', () => {
    const hub = new Hub();
    hub.showCaptures(new Map([[KEYS['fid.capture'], 0.5]]));
    expect(hub.asked.size).toBe(0);
  });
});

// How a session reads a paused plot's windows of the hub's history
// (`Session.fetchWindow`), on a hub whose replies the test lands when it
// chooses, and a clock it steps:
// - each layer of each plot has one read in flight at most, and sends one
//   a frame at most;
// - an ask the read in flight serves waits for its reply, the newest ask in
//   place of any before it;
// - a read in flight that does not serve the ask is aborted, and its reply
//   is never shown, though it lands;
// - once every reply has landed, each layer shows the window last asked
//   for, and never showed an older one after a newer;
// - the request itself ends when its signal aborts (protocol.js).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import fc from 'fast-check';
import { Session } from './session.svelte.js';
import { api } from './protocol.js';
import * as clock from './clock.js';

const WIDTH = 800;
let time, reads;

beforeEach(() => {
  time = clock.virtual();
  reads = [];
  // Every read the session sends, in order, each landed by `land()`, whether or not it was aborted first.
  vi.spyOn(api, 'window').mockImplementation(
    ({ id, t0, t1, width, signal }) =>
      new Promise((done) => {
        const read = { id, t0, t1, width, signal, at: clock.now(), landed: false };
        read.land = () => ((read.landed = true), done({ t0, t1, ratio: 2, chunks: [{ x: [t0, t1], y: [0, 1] }] }));
        reads.push(read);
      }),
  );
});
afterEach(() => {
  vi.restoreAllMocks();
  time.end();
});

/** A session that has heard of one trace, and the trace. */
function heard() {
  const s = new Session('h');
  s.text({ type: 'trace', trace: { id: 0, route: '/', stream: 's', column: 'c', units: 'V', rate: 100 } });
  return { s, trace: s.traces.get(0) };
}
/** Lets every reply landed reach the session. */
const reached = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};
/** The reads in flight: sent, not aborted, their replies not landed. */
const flying = () => reads.filter((r) => !r.signal.aborted && !r.landed);

describe('a paused plot’s window reads', () => {
  it('reads a view and its padding at once', () => {
    const { s, trace } = heard();
    s.fetchWindow('p', 'detail', [trace], 100, 110, 10, WIDTH);
    expect(reads.map((r) => [r.id, r.t0, r.t1, r.width])).toEqual([[0, 90, 120, WIDTH]]);
  });

  it('holds an ask the read in flight serves until its reply lands, the newest in place of any before it', async () => {
    const { s, trace } = heard();
    s.fetchWindow('p', 'detail', [trace], 100, 110, 10, WIDTH);
    // Two pans within the padding, and a zoom out within it.
    s.fetchWindow('p', 'detail', [trace], 102, 112, 10, WIDTH);
    s.fetchWindow('p', 'detail', [trace], 104, 114, 10, WIDTH);
    s.fetchWindow('p', 'detail', [trace], 95, 115, 20, WIDTH);
    expect(reads).toHaveLength(1);
    expect(reads[0].signal.aborted).toBe(false);
    reads[0].land();
    await reached();
    expect(s.windowOf('p', 'detail', trace).t0).toBe(90);
    // One read a frame: the ask waiting goes with the next.
    expect(reads).toHaveLength(1);
    time.nextFrame();
    expect(reads.map((r) => [r.t0, r.t1])).toEqual([[90, 120], [75, 135]]);
  });

  it('aborts a read of a stretch the view has left, and never shows its reply', async () => {
    const { s, trace } = heard();
    s.fetchWindow('p', 'detail', [trace], 100, 110, 10, WIDTH);
    s.fetchWindow('p', 'detail', [trace], 500, 510, 10, WIDTH);
    expect(reads[0].signal.aborted).toBe(true);
    time.nextFrame();
    expect(reads.map((r) => r.t0)).toEqual([90, 490]);
    // The aborted read's reply lands all the same, as one that crossed the abort would.
    reads[0].land();
    await reached();
    expect(s.windowOf('p', 'detail', trace)).toBeUndefined();
    reads[1].land();
    await reached();
    expect(s.windowOf('p', 'detail', trace).t0).toBe(490);
  });

  it('aborts a read coarser than the view wants: one of a window longer than the one asked for', () => {
    const { s, trace } = heard();
    s.fetchWindow('p', 'detail', [trace], 100, 110, 10, WIDTH);
    s.fetchWindow('p', 'detail', [trace], 104, 106, 2, WIDTH);
    expect(reads[0].signal.aborted).toBe(true);
    time.nextFrame();
    expect(reads.map((r) => [r.t0, r.t1])).toEqual([[90, 120], [102, 108]]);
  });

  it('drops a plot’s windows with the read on its way and the ask waiting', async () => {
    const { s, trace } = heard();
    s.fetchWindow('p', 'detail', [trace], 100, 110, 10, WIDTH);
    reads[0].land();
    await reached();
    time.nextFrame();
    s.fetchWindow('p', 'detail', [trace], 101, 111, 10, WIDTH);
    s.fetchWindow('p', 'detail', [trace], 102, 112, 10, WIDTH);
    s.clearWindows('p');
    expect(reads[1].signal.aborted).toBe(true);
    expect(s.windowOf('p', 'detail', trace)).toBeUndefined();
    reads[1].land();
    await reached();
    time.frames(3);
    expect(reads).toHaveLength(2);
    expect(s.windowOf('p', 'detail', trace)).toBeUndefined();
  });

  it('has one read in flight a layer and sends one a frame at most, and ends showing the window last asked for, never an older one after a newer', async () => {
    const LAYERS = ['detail', 'overview'];
    // A view as [left, span], and what happens: an ask of a layer, the oldest read in flight landing, or a frame.
    const step = fc.oneof(
      { weight: 4, arbitrary: fc.record({ t: fc.constant('ask'), layer: fc.constantFrom(...LAYERS), left: fc.integer({ min: 0, max: 60 }), span: fc.constantFrom(1, 2, 5, 10, 30) }) },
      { weight: 2, arbitrary: fc.record({ t: fc.constant('land') }) },
      { weight: 2, arbitrary: fc.record({ t: fc.constant('frame') }) },
    );
    const seen = { aborted: 0, waited: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 40 }), async (steps) => {
        reads.length = 0;
        const { s, trace } = heard();
        const last = {}, shown = { detail: [], overview: [] };
        // Each layer's reads are told apart by the width asked.
        const of = (layer) => reads.filter((r) => r.width === WIDTH + LAYERS.indexOf(layer));
        const check = () => {
          for (const layer of LAYERS) {
            expect(flying().filter((r) => of(layer).includes(r)).length, `${layer} reads in flight`).toBeLessThanOrEqual(1);
            const at = of(layer).map((r) => r.at);
            expect(new Set(at).size, `${layer} reads, one a frame`).toBe(at.length);
            const w = s.windowOf('p', layer, trace);
            if (w && shown[layer].at(-1) !== w.t0 + '|' + w.t1) shown[layer].push(w.t0 + '|' + w.t1);
          }
        };
        const asked = { detail: [], overview: [] };
        for (const a of steps) {
          if (a.t === 'ask') {
            const before = reads.filter((r) => r.signal.aborted).length, sent = reads.length;
            s.fetchWindow('p', a.layer, [trace], a.left, a.left + a.span, a.span, WIDTH + LAYERS.indexOf(a.layer));
            last[a.layer] = `${a.left - a.span}|${a.left + 2 * a.span}`;
            asked[a.layer].push(last[a.layer]);
            if (reads.filter((r) => r.signal.aborted).length > before) seen.aborted++;
            else if (reads.length === sent) seen.waited++;
          } else if (a.t === 'land') {
            flying()[0]?.land();
            await reached();
          } else time.nextFrame();
          check();
        }
        // Every reply lands, and the frames pass.
        for (let i = 0; i < 6; i++) {
          for (const r of flying()) r.land();
          await reached();
          time.nextFrame();
          check();
        }
        for (const layer of LAYERS) {
          if (last[layer] === undefined) continue;
          expect(shown[layer].at(-1), `${layer} ends at the last ask`).toBe(last[layer]);
          // What it showed, it showed in the order asked.
          let from = 0;
          for (const w of shown[layer]) {
            const k = asked[layer].indexOf(w, from);
            expect(k, `${layer} showed ${w} in the order asked`).toBeGreaterThanOrEqual(0);
            from = k;
          }
          // No aborted read's window is shown, unless a later read asked the same.
          for (const r of of(layer).filter((r) => r.signal.aborted)) {
            const same = of(layer).some((o) => !o.signal.aborted && o.t0 === r.t0 && o.t1 === r.t1);
            if (!same) expect(shown[layer]).not.toContain(`${r.t0}|${r.t1}`);
          }
        }
      }),
    );
    expect(seen.aborted).toBeGreaterThan(0);
    expect(seen.waited).toBeGreaterThan(0);
  });
});

describe('a window request', () => {
  it('goes out with its signal, and ends as the signal aborts', async () => {
    const fetched = globalThis.fetch;
    vi.restoreAllMocks();
    const calls = [];
    globalThis.fetch = (url, init) => (calls.push([String(url), init]), new Promise((_, refuse) => init.signal.addEventListener('abort', () => refuse(init.signal.reason))));
    try {
      const abort = new AbortController();
      const reply = api.window({ hub: 'h', id: 3, t0: 1, t1: 2, width: 800.4, signal: abort.signal });
      expect(calls).toHaveLength(1);
      expect(calls[0][0]).toBe('/api/window?hub=h&id=3&t0=1&t1=2&width=800');
      expect(calls[0][1].signal).toBe(abort.signal);
      abort.abort();
      await expect(reply).rejects.toHaveProperty('name', 'AbortError');
    } finally {
      globalThis.fetch = fetched;
    }
  });
});

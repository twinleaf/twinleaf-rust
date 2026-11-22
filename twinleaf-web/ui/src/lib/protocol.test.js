// The binary frames as the page reads them: the bytes the hub sends, as
// twinleaf-web/src/recorded.rs records them, and any capture the test hub
// encodes. And what the page sends, recorded in tests/fixtures/page, a file
// per message in each form it has, and in tests/fixtures/page/http a file
// per request of each /api route, its method, its URL and its body, which
// the hub's tests parse. The test fails on a file that differs from what
// the page builds now, or that it no longer builds, and names it. With
// TWINLEAF_WEB_RECORD set it writes the files instead.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import fs from 'node:fs';
import path from 'node:path';
import { captureKey, captureMessage, checkMessage, decodeFrame, holdMessage, imageSource, isCaptureKey, requests, spectraMessage, upgradeAsk, upgradeMessage } from './protocol.js';
import { exportUrl } from './exportfile.js';
import { ELEMENTS, encodeCapture } from '../../tests/harness/captures.js';

/** The bytes of the recorded frame `name`, as `points`, in a buffer of their own. */
const recorded = (name) => {
  const bytes = fs.readFileSync(path.resolve(`tests/fixtures/hub/${name}.bin`));
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length);
};
/** The frame of capture 258 of fid.capture of the device at /0: three 16-bit samples, 1, -2 and 3, 0.5 V each, from 1 s on, 0.25 s apart, and a spectrum of two bins, 0.5 at 1 and 0.25 at 2. */
const kept = () => recorded('capture');
const KEPT = new Uint8Array(kept());
const FID = { route: '/0', rpc: 'fid.capture', number: 258, type: 0x21, calibration: 0.5, x0: 1, dx: 0.25, name: 'FID', units: 'V', xName: 'Time', xUnits: 's', spectrum: { f: Float32Array.of(1, 2), a: Float32Array.of(0.5, 0.25) } };

/** Text a device may name a capture, its units or its route by. */
const text = fc.string({ maxLength: 12 });
/** A capture of any element type the frame carries, as `encodeCapture` takes it. */
const capture = fc.constantFrom(...Object.keys(ELEMENTS).map(Number)).chain((type) => {
  const Elements = ELEMENTS[type], big = Elements === BigUint64Array || Elements === BigInt64Array;
  const element = big ? fc.bigInt({ min: Elements === BigInt64Array ? -(2n ** 63n) : 0n, max: Elements === BigInt64Array ? 2n ** 63n - 1n : 2n ** 64n - 1n }) : fc.double({ min: -1e6, max: 1e6, noNaN: true });
  return fc.record({
    route: text, rpc: text, name: text, units: text, xName: text, xUnits: text,
    number: fc.nat(2 ** 40),
    type: fc.constant(type),
    calibration: fc.float({ noNaN: true, noDefaultInfinity: true }),
    x0: fc.float({ noNaN: true, noDefaultInfinity: true }),
    dx: fc.float({ noNaN: true, noDefaultInfinity: true }),
    elements: fc.array(element, { maxLength: 40 }).map((values) => Elements.from(values)),
    spectrum: fc.array(fc.tuple(fc.float({ noNaN: true }), fc.float({ noNaN: true })), { maxLength: 12 }).map((bins) => ({ f: Float32Array.from(bins, ([f]) => f), a: Float32Array.from(bins, ([, a]) => a) })),
  });
});

describe('a capture frame', () => {
  it('decodes, of the bytes the hub records, to the capture they are of, its elements read where they lie', () => {
    const buf = kept(), frame = decodeFrame(buf);
    expect(frame).toEqual({ kind: 'capture', seq: 0, ...FID, elements: Int16Array.of(1, -2, 3) });
    // The elements are the frame's own bytes, not a copy.
    expect(frame.elements.buffer).toBe(buf);
    expect(frame.elements.byteOffset % 8).toBe(0);
  });

  it('is encoded by the test hub byte for byte as the hub encodes it', () => {
    expect([...new Uint8Array(encodeCapture({ ...FID, elements: Int16Array.of(1, -2, 3) }))]).toEqual([...KEPT]);
  });

  it('decodes to the capture it was encoded of, whatever its element type, its strings and its length', () => {
    fc.assert(
      fc.property(capture, (c) => {
        const buf = encodeCapture(c), frame = decodeFrame(buf);
        expect(frame).toEqual({ kind: 'capture', seq: 0, ...c });
        expect(frame.elements.constructor).toBe(ELEMENTS[c.type]);
        expect(frame.elements.buffer).toBe(buf);
      }),
    );
  });

  it('of an element type the page does not read is no frame', () => {
    const bytes = new Uint8Array(kept());
    bytes[28] = 0x99;
    expect(decodeFrame(bytes.buffer)).toBe(null);
  });
});

describe('a points frame', () => {
  it('decodes, of the bytes the hub records, to every level of its trace, each run of points in time order, and nothing past them', () => {
    const buf = recorded('points'), frame = decodeFrame(buf);
    expect(frame.kind).toBe('points');
    expect(new Set(frame.chunks.map((c) => c.id))).toEqual(new Set([0]));
    expect(new Set(frame.chunks.map((c) => c.level)).size).toBeGreaterThan(1);
    for (const { xs, ys } of frame.chunks) {
      expect(xs.length).toBe(ys.length);
      expect(xs.every((x, i) => i === 0 || x > xs[i - 1])).toBe(true);
    }
    expect(18 + frame.chunks.reduce((n, c) => n + 8 + 12 * c.xs.length, 0)).toBe(buf.byteLength);
  });
});

describe('a spectra frame', () => {
  it('decodes, of the bytes the hub records, to its trace’s bins in rising frequency, and nothing past them', () => {
    const buf = recorded('spectra'), frame = decodeFrame(buf);
    expect(frame.kind).toBe('spectra');
    expect(frame.spectra.map((s) => s.id)).toEqual([0]);
    const [{ f, a }] = frame.spectra;
    expect(f.length).toBe(a.length);
    expect(f.every((x, i) => i === 0 || x > f[i - 1])).toBe(true);
    expect(24 + frame.spectra.reduce((n, s) => n + 16 + 8 * s.f.length, 0)).toBe(buf.byteLength);
  });
});

describe('a capture’s key', () => {

  it('names its RPC after its device’s route, as a signal’s key does, and is no signal’s', () => {
    expect(captureKey({ route: '/', rpc: 'fid.capture' })).toBe('capture:fid.capture');
    expect(captureKey({ route: '/0/1', rpc: 'fid.capture' })).toBe('0/1/capture:fid.capture');
    expect(isCaptureKey('3/1/capture.x')).toBe(false);
    fc.assert(
      fc.property(fc.constantFrom('/', '/0', '/3/1'), fc.stringMatching(/^[a-z][a-z0-9_.]{0,12}$/), (route, rpc) => {
        expect(isCaptureKey(captureKey({ route, rpc }))).toBe(true);
      }),
    );
  });
});

/** Each message the page sends, by the name of its file, as the page builds it. */
const ASKS = {
  spectra: spectraMessage([1, 4]),
  'hold-on': holdMessage(true),
  'hold-off': holdMessage(false),
  capture: captureMessage('capture', '/', 'fid.capture'),
  repeat: captureMessage('repeat', '/0', 'fid.capture', 0.5),
  stop: captureMessage('stop', '/', 'fid.capture', 0.5),
  check: checkMessage(),
  'upgrade-latest': upgradeMessage('/', imageSource.latest()),
  'upgrade-release': upgradeMessage('/0', imageSource.release('tio-test-R1-firmware-2026-06-08-000002.bin')),
  'upgrade-file': upgradeMessage('/', imageSource.file('image.bin', 20000)),
  cancel: upgradeAsk('cancel', '/'),
  retry: upgradeAsk('retry', '/1'),
};

/** Each request the page makes over HTTP, by the name of its file, as the page builds it. */
const REQUESTS = {
  call: requests.call('fid.amplitude', '0.5', ['/', '/0']),
  read: requests.read('/', ['fid.amplitude', 'dev.name']),
  discover: requests.discover(false),
  mount: requests.mount([{ url: 'tcp://sensor.local', source: '/' }, { url: 'tcp://sensor.local', source: '/0' }]),
  'mount-default': requests.mount([]),
  pps: requests.setPps(250),
  history: requests.setHistory(600, 1.5e8 + 0.25),
  window: requests.window({ hub: 'h1', id: 3, t0: 1.5, t1: 2.25, width: 800.25 }),
  rows: requests.rows({ hub: 'h1', id: 3, t0: 1.5, t1: 2.25, limit: 1000 }),
  export: { method: 'GET', url: exportUrl({ hub: 'h1', ids: [3, 4], t0: 1.5, t1: 2.25 }, 'csv', 'twinleaf.csv') },
};

/** Compares the files `files`, text by name, with those `folder` holds, or with TWINLEAF_WEB_RECORD set writes them in their place. */
function compared(folder, files) {
  const kept = fs.existsSync(folder) ? fs.readdirSync(folder).filter((file) => fs.statSync(path.join(folder, file)).isFile()) : [];
  const stale = kept.filter((file) => !(file in files));
  if (process.env.TWINLEAF_WEB_RECORD) {
    fs.mkdirSync(folder, { recursive: true });
    for (const file of stale) fs.rmSync(path.join(folder, file));
    for (const [file, text] of Object.entries(files)) fs.writeFileSync(path.join(folder, file), text);
    return;
  }
  const differ = Object.keys(files).filter((file) => !kept.includes(file) || fs.readFileSync(path.join(folder, file), 'utf8') !== files[file]);
  expect({ differ, stale }, 'TWINLEAF_WEB_RECORD set records what the page sends').toEqual({ differ: [], stale: [] });
}

describe('what the page sends', () => {
  it('is what the files the hub’s tests parse hold', () => {
    compared(path.resolve('tests/fixtures/page'), Object.fromEntries(Object.entries(ASKS).map(([name, text]) => [`${name}.json`, `${text}
`])));
  });

  it('over HTTP is what the files the hub’s tests take hold', () => {
    const file = ({ method, url, body }) => `${JSON.stringify({ method, url, ...(body !== undefined && { body: JSON.parse(body) }) })}
`;
    compared(path.resolve('tests/fixtures/page/http'), Object.fromEntries(Object.entries(REQUESTS).map(([name, request]) => [`${name}.json`, file(request)])));
  });
});

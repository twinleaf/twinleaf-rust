// A capture frame as the page reads it: the bytes the hub's own test keeps
// (twinleaf-web/src/wire.rs, which checks its encoder against the same
// file), and any capture the test hub encodes.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import fs from 'node:fs';
import path from 'node:path';
import { captureKey, captureMessage, decodeFrame, isCaptureKey, signalKey } from './protocol.js';
import { ELEMENTS, encodeCapture } from '../../tests/harness/captures.js';

/** The frame of capture 258 of fid.capture of the device at /0: three 16-bit samples, 1, -2 and 3, 0.5 V each, from 1 s on, 0.25 s apart. */
const KEPT = fs.readFileSync(path.resolve('tests/fixtures/capture-frame.bin'));
const kept = () => KEPT.buffer.slice(KEPT.byteOffset, KEPT.byteOffset + KEPT.length);
const FID = { route: '/0', rpc: 'fid.capture', number: 258, type: 0x21, calibration: 0.5, x0: 1, dx: 0.25, name: 'FID', units: 'V', xName: 'Time', xUnits: 's' };

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
  });
});

describe('a capture frame', () => {
  it('decodes, of the bytes the hub’s test keeps, to the capture they are of, its elements read where they lie', () => {
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

describe('a capture’s key', () => {
  it('names its RPC after its device’s route, as a signal’s key does, and is no signal’s', () => {
    expect(captureKey({ route: '/', rpc: 'fid.capture' })).toBe('capture:fid.capture');
    expect(captureKey({ route: '/0/1', rpc: 'fid.capture' })).toBe('0/1/capture:fid.capture');
    fc.assert(
      fc.property(fc.constantFrom('/', '/0', '/3/1'), fc.stringMatching(/^[a-z][a-z0-9_.]{0,12}$/), fc.stringMatching(/^[a-z][a-z0-9_]{0,8}$/), (route, rpc, column) => {
        expect(isCaptureKey(captureKey({ route, rpc }))).toBe(true);
        expect(isCaptureKey(signalKey({ route, stream: rpc, column }))).toBe(false);
      }),
    );
  });

  it('is asked for by its device’s route and its RPC, a delay only with repeat', () => {
    expect(JSON.parse(captureMessage('capture', '/', 'fid.capture'))).toEqual({ type: 'capture', route: '/', rpc: 'fid.capture' });
    expect(JSON.parse(captureMessage('repeat', '/0', 'fid.capture', 0.5))).toEqual({ type: 'repeat', route: '/0', rpc: 'fid.capture', delay: 0.5 });
    expect(JSON.parse(captureMessage('stop', '/', 'fid.capture', 0.5))).toEqual({ type: 'stop', route: '/', rpc: 'fid.capture' });
  });
});

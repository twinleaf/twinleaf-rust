import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { UNITS, devicesText, fileSafe, pictureSize, stampText } from './figure.js';

describe('an image’s size', () => {
  it('is its pixels in px, and its length times its resolution in inches or centimetres', () => {
    expect(pictureSize({ width: 1280, height: 600, unit: 'px', dpi: 300 })).toEqual({ ratio: 1, pixels: [1280, 600] });
    expect(pictureSize({ width: 6 * 96, height: 4 * 96, unit: 'in', dpi: 300 }).pixels).toEqual([1800, 1200]);
    expect(pictureSize({ width: 10 * UNITS.cm, height: 5 * UNITS.cm, unit: 'cm', dpi: 300 }).pixels).toEqual([1181, 591]);
    fc.assert(
      fc.property(fc.constantFrom('in', 'cm'), fc.double({ min: 0.5, max: 40, noNaN: true }), fc.integer({ min: 72, max: 1200 }), (unit, length, dpi) => {
        const { pixels } = pictureSize({ width: length * UNITS[unit], height: length * UNITS[unit], unit, dpi });
        expect(pixels[0]).toBe(Math.round((length * dpi * UNITS[unit]) / 96));
      }),
    );
  });
});

describe('a caption', () => {
  it('names one or two devices, and counts more', () => {
    expect(devicesText(['vmr TL1', 'vmr TL1'])).toBe('vmr TL1');
    expect(devicesText(['vmr TL1', 'sim S2'])).toBe('vmr TL1 · sim S2');
    expect(devicesText(['a', 'b', 'c'])).toBe('3 devices');
    expect(devicesText([])).toBe('');
  });
  it('dates a Unix stretch with the times of day and the offset, and gives any other clock’s seconds', () => {
    const t0 = new Date(2026, 10, 10, 1, 51, 30).getTime() / 1000;
    expect(stampText(t0, t0 + 10, 'unix')).toMatch(/^2026-11-10 01:51:30 – 01:51:40 UTC[+−]\d\d:\d\d$/);
    expect(stampText(t0, t0 + 86400, 'unix')).toMatch(/^2026-11-10 01:51:30 – 2026-11-11 01:51:30 UTC/);
    expect(stampText(12.5, 22.5, 'zero')).toBe('12.500 – 22.500 s');
  });
});

describe('a file name', () => {
  it('holds no character a file system refuses', () => {
    expect(fileSafe('0/sine.sine+a:b')).toBe('0_sine.sine+a_b');
  });
});

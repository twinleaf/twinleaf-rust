// What the page says of a device's firmware: its row's state for each
// state of the catalog and of an upgrade, the steps the dialog lists, and
// the Device a session keeps from the hub's `firmware` and `upgrade`
// messages.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { RUNNING, cancellable, catalogNote, defaultSource, failureText, hasUpdate, percent, rowState, running, stepsOf, versionText } from './firmware.js';
import { Session } from './session.svelte.js';

const INSTALLED = { name: 'VMR', revision: 'R4', date: '2026-01-10', build: 'aaaaaa', serial: '0412', development: false };
const RELEASES = [{ date: '2026-07-01', build: '5d1494', file: 'VMR-R4-firmware-2026-07-01-5d1494.bin' }, { date: '2026-01-10', build: 'aaaaaa', file: 'VMR-R4-firmware-2026-01-10-aaaaaa.bin' }];
const checked = (status, releases = RELEASES) => ({ state: 'checked', status, releases });
const device = (catalog, upgrade = null) => ({ route: '/', name: 'VMR', serial: '0412', fw: { installed: INSTALLED, catalog }, upgrade });
const upgrade = (state, more = {}) => ({ state, source: 'latest', target: '2026-07-01 5d1494', ...more });

describe('a device’s row', () => {
  it('says nothing of a catalog not checked or not reached, and each status of one checked', () => {
    expect(rowState(device({ state: 'unchecked' }))).toBe(null);
    expect(rowState(device({ state: 'unreachable', error: 'no network' }))).toBe(null);
    expect(rowState(device({ state: 'checking' }))).toEqual({ kind: 'note', text: 'Checking…' });
    expect(rowState(device(checked('update')))).toEqual({ kind: 'update', text: '2026-07-01 available' });
    expect(rowState(device(checked('unknown')))).toEqual({ kind: 'update', text: '2026-07-01 published' });
    expect(rowState(device(checked('current')))).toEqual({ kind: 'note', text: 'Up to date' });
    expect(rowState(device(checked('none', [])))).toEqual({ kind: 'note', text: 'None published' });
    expect(rowState(device(checked('development', [])))).toEqual({ kind: 'note', text: 'Development build' });
  });

  it('says where an upgrade is in place of the catalog, a percent only while it uploads, and a failure until it is cleared', () => {
    for (const state of RUNNING) {
      const said = rowState(device(checked('update'), upgrade(state, { sent: 50, size: 200 })));
      expect(said.kind).toBe('progress');
      expect(said.percent).toBe(state === 'uploading' ? 25 : null);
    }
    expect(rowState(device(checked('update'), upgrade('failed', { step: 'upload', error: 'x' })))).toEqual({ kind: 'failed', text: 'Upgrade failed' });
    expect(rowState(device(checked('current'), upgrade('done')))).toEqual({ kind: 'note', text: 'Up to date' });
  });

  it('counts whole percent of the upload, never past 100', () => {
    fc.assert(fc.property(fc.nat(1 << 24), fc.nat(1 << 24), (sent, size) => {
      const p = percent({ sent, size });
      expect(Number.isInteger(p) && p >= 0 && p <= 100).toBe(true);
      if (size && sent < size) expect(p).toBeLessThan(100);
    }));
  });
});

describe('an upgrade', () => {
  it('runs, and can be taken back, in the states the hub says so of', () => {
    expect(RUNNING.filter((s) => running(upgrade(s)))).toEqual(RUNNING);
    expect(['done', 'failed'].some((s) => running(upgrade(s))) || running(null)).toBe(false);
    expect(RUNNING.filter((s) => cancellable(upgrade(s)))).toEqual(['queued', 'downloading']);
    expect(hasUpdate(device(checked('update')))).toBe(true);
    expect(hasUpdate(device(checked('update'), upgrade('queued')))).toBe(false);
    expect(hasUpdate(device(checked('current')))).toBe(false);
  });

  it('lists the tool’s steps, a file’s without the download: those before the one under way done, those after waiting', () => {
    const at = (u) => stepsOf(u).map((s) => `${s.name} ${s.at}`);
    expect(at(upgrade('queued'))).toEqual(['Download wait', 'Upload wait', 'Finalize wait', 'Restart wait']);
    expect(at(upgrade('uploading', { sent: 28800, size: 412000 }))).toEqual(['Download done', 'Upload now', 'Finalize wait', 'Restart wait']);
    expect(stepsOf(upgrade('uploading', { sent: 28800, size: 412000 }))[1].value).toBe('29 kB of 412 kB');
    expect(at(upgrade('restarting', { source: 'file' }))).toEqual(['Upload done', 'Finalize done', 'Restart now']);
    expect(at(upgrade('done'))).toEqual(['Download done', 'Upload done', 'Finalize done', 'Restart done']);
  });

  it('that failed names the step and the reason, and one refused before anything was sent names no step', () => {
    const failed = upgrade('failed', { step: 'upload', error: 'firmware chunk 3/9 failed' });
    expect(stepsOf(failed).map((s) => s.at)).toEqual(['done', 'failed', 'wait', 'wait']);
    expect(failureText(failed)).toBe('Upload failed: firmware chunk 3/9 failed');
    const refused = upgrade('failed', { step: 'check', error: 'the device takes signed packages' });
    expect(stepsOf(refused).map((s) => s.at)).toEqual(['wait', 'wait', 'wait', 'wait']);
    expect(failureText(refused)).toBe('Nothing was sent to the device: the device takes signed packages');
  });
});

describe('the dialog', () => {
  it('starts on the newest release when it is newer, another when the device is up to date, and a file when the catalog has none', () => {
    expect(defaultSource(device(checked('update')))).toBe('latest');
    expect(defaultSource(device(checked('unknown')))).toBe('latest');
    expect(defaultSource(device(checked('current')))).toBe('release');
    for (const catalog of [checked('none', []), checked('development', []), { state: 'unreachable', error: 'x' }, { state: 'unchecked' }]) expect(defaultSource(device(catalog))).toBe('file');
  });

  it('says why the catalog offers nothing: not reached, a development build, or none published for the hardware', () => {
    expect(catalogNote(device({ state: 'unreachable', error: 'x' }))).toMatch(/could not be reached/);
    expect(catalogNote(device(checked('development', [])))).toMatch(/development build/);
    expect(catalogNote(device(checked('none', [])))).toBe('No firmware is published for VMR R4.');
    expect(catalogNote(device(checked('update')))).toBe(null);
    expect(catalogNote(device({ state: 'checking' }))).toBe(null);
  });

  it('writes a version as its build date and build', () => {
    expect(versionText(INSTALLED)).toBe('2026-01-10 aaaaaa');
    expect(versionText({ date: null, build: null })).toBe('unknown');
  });
});

describe('a session', () => {
  it('keeps each device’s firmware and upgrade as the hub last said, an idle upgrade as none', () => {
    const session = new Session('h');
    session.text({ type: 'routes', routes: ['/'], mounts: null });
    session.text({ type: 'firmware', route: '/', installed: INSTALLED, catalog: checked('update') });
    session.text({ type: 'upgrade', route: '/', state: 'uploading', source: 'file', target: 'image.bin', sent: 10, size: 40 });
    session.text({ type: 'firmware', route: '/9', installed: INSTALLED, catalog: checked('update') });
    const d = session.devices.get('/');
    expect(d.fw).toEqual({ installed: INSTALLED, catalog: checked('update') });
    expect(rowState(d)).toEqual({ kind: 'progress', text: 'Upload', percent: 25 });
    session.text({ type: 'upgrade', route: '/', state: 'idle' });
    expect(d.upgrade).toBe(null);
    expect(session.devices.has('/9')).toBe(false);
  });
});

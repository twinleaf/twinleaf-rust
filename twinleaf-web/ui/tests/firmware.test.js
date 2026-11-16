// Firmware on the page: each device's row in the Devices panel says the
// version it runs and, once the catalog was checked, how it stands; the
// upgrade dialog installs the newest release, another or a file; and an
// upgrade's steps show in the dialog and on the row together, whatever is
// closed, reopened, cancelled, failed or retried meanwhile.
//
// The hub is the harness's (harness/firmware.js `serveFirmware`), which
// takes the page's asks and tells an upgrade's states as the hub does.
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { page as browser } from 'vitest/browser';
import { flushSync } from 'svelte';
import { clickOn, closeAll, feed, mountPage, pickItem, plotMenu, pressKey, settle, until, wait } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { installed, release, serveFirmware } from './harness/firmware.js';
import { CATALOG_STATES, FAILED_STEPS, MAX_IMAGE, RUNNING, STATUSES } from '../src/lib/firmware.js';
import { prefs } from '../src/lib/prefs.svelte.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shoot = async (name, element) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png`, element }));

const SIGNALS = ['sine.sin', 'sine.cos'];
const VMR = [release('VMR', 'R4', '2026-07-01', '5d1494'), release('VMR', 'R4', '2026-03-17', '0c22e1'), release('VMR', 'R4', '2026-01-10', 'aaaaaa')];
const OMG = [release('OMG', 'R2', '2026-05-02', '77b0c3')];
/** Three devices: one a newer release is published for, one up to date, and one nothing is published for. */
const THREE = () => [
  { route: '/0', name: 'VMR', serial: '0412', installed: installed('VMR', 'R4', '2026-01-10', 'aaaaaa', '0412'), releases: VMR },
  { route: '/1', name: 'OMG', serial: '0077', installed: installed('OMG', 'R2', '2026-05-02', '77b0c3', '0077'), releases: OMG },
  { route: '/2', name: 'SYNC', serial: '0003', installed: installed('SYNC', 'R1', '2025-11-20', '19ae42', '0003') },
];
/** Two devices a newer release is published for, and one that is up to date. */
const TWO_OLD = () => [
  THREE()[0],
  { route: '/1', name: 'OMG', serial: '0077', installed: installed('OMG', 'R2', '2026-02-11', '31c0de', '0077'), releases: OMG },
  { route: '/2', name: 'SYNC', serial: '0003', installed: installed('SYNC', 'R1', '2025-11-20', '19ae42', '0003'), releases: [release('SYNC', 'R1', '2025-11-20', '19ae42')] },
];
/** How long the test hub takes an upgrade from a release, in ms of the page's time. */
const whole = (fw) => fw.times.download + (fw.times.chunks + 1) * fw.times.chunk + fw.times.finalize + fw.times.restart;

let page = null;
afterEach(() => {
  page?.dispose();
  page = null;
});
afterAll(() => (prefs.setTheme('system'), browser.viewport(1200, 900)));

/** The page with `devices` served, and `fw` their hub; its left dock `dock` percent of the window wide, as kept, when given. */
async function mount(devices = THREE(), dock = null) {
  page = await mountPage([['sine.sin'], ['sine.cos']], 'rows', 0, SIGNALS, [], dock === null ? {} : { saved: { 'tl-side-size': dock } });
  const fw = serveFirmware(page.hub, devices);
  await settle();
  return fw;
}

const panel = () => document.querySelector('.devices');
const row = (route) => [...document.querySelectorAll('.device')].find((r) => r.querySelector('.route').textContent === route);
const line = (route) => row(route).querySelector('.firmware');
const said = (route) => [...line(route).children].map((c) => c.textContent.replace(/\s+/g, ' ').trim());
const offer = (route) => row(route).querySelector('.offer');
const dialog = () => document.querySelector('.firmware-dialog[data-state=open]');
const button = (text, within = document) => [...within.querySelectorAll('button')].find((b) => b.textContent.trim() === text || b.getAttribute('aria-label') === text);
const steps = () => [...dialog().querySelectorAll('.fw-step')].map((s) => `${s.dataset.step} ${s.dataset.at}`);
const notes = () => [...dialog().querySelectorAll('.export-note')].map((n) => n.textContent.trim());
const notices = () => [...document.querySelectorAll('[data-sonner-toast]')].filter((t) => t.dataset.removed !== 'true').map((t) => t.textContent);
/** Where `el` is: its left, top, width and height, in px. */
const box = (el) => {
  const r = el.getBoundingClientRect();
  return [r.left, r.top, r.width, r.height];
};
/** The furthest any number of `seen`, lists of boxes noted at different times, is from the first noted, in px. */
const moved = (seen) => Math.max(0, ...seen.flatMap((boxes) => boxes.flatMap((b, i) => b.map((v, k) => Math.abs(v - seen[0][i][k])))));
/** What a box may differ by and not have moved: what laying out text of another width rounds to, far under a pixel. */
const STILL = 0.1;
/** Checks the catalog by the panel's button, and waits for its answer. */
async function check(fw) {
  await clickOn(button('Check for firmware updates'));
  await wait(fw.latency + 20);
  await settle();
}
/** Opens the dialog of the device at `route` by what its row offers, or by the row's menu where it offers nothing. */
async function open(route) {
  if (offer(route)) await clickOn(offer(route));
  else {
    await clickOn(row(route).querySelector('.name'), { right: true });
    await pickItem('Upgrade firmware…');
  }
  expect(await until(() => !!dialog()), 'the dialog opens').toBe(true);
  await settle();
}
/** Presses the dialog's button `text`. */
async function press(text) {
  await clickOn(button(text, dialog()));
  await settle();
}
/** Closes the dialog by its Close button, and waits until it has gone. */
async function close(text = 'Close') {
  await clickOn(button(text, dialog()));
  expect(await until(() => !document.querySelector('.firmware-dialog')), 'the dialog closes').toBe(true);
}
/** Picks `file` in the dialog's file field, as a person's choice in the browser's window gives it. */
async function choose(file) {
  const input = dialog().querySelector('input[type=file]'), given = new DataTransfer();
  given.items.add(file);
  input.files = given.files;
  input.dispatchEvent(new Event('change', { bubbles: true }));
  await settle();
}

describe('a device’s row', () => {
  it('says the version it runs, and of the catalog nothing until it was checked, then how each device stands', async () => {
    const fw = await mount();
    expect(['/0', '/1', '/2'].map(said)).toEqual([['2026-01-10'], ['2026-05-02'], ['2025-11-20']]);
    expect(line('/0').querySelector('.version').title).toBe('Runs firmware 2026-01-10 aaaaaa');
    expect(fw.posted, 'the catalog is not asked until a person asks').toEqual([]);
    await clickOn(button('Check for firmware updates'));
    await settle();
    expect(said('/0')).toEqual(['2026-01-10', 'Checking…']);
    await wait(fw.latency + 20);
    expect(['/0', '/1', '/2'].map(said)).toEqual([['2026-01-10', '2026-07-01 available'], ['2026-05-02', 'Up to date'], ['2025-11-20', 'None published']]);
    expect(fw.asks('check').length).toBe(1);
    expect(offer('/0').tagName, 'what is offered is pressed').toBe('BUTTON');
    expect(offer('/1'), 'what needs no press is none').toBe(null);
  });

  it('of a development build says so, and with no network says nothing, as before the check', async () => {
    const fw = await mount([{ route: '/', name: 'tio-test', installed: installed('tio-test', 'R1', '2026-06-08', '000001-DEV') }]);
    await check(fw);
    expect(said('/')).toEqual(['2026-06-08', 'Development build']);
    fw.reachable = false;
    await check(fw);
    expect(said('/')).toEqual(['2026-06-08']);
    expect(notices(), 'no error is raised of a catalog not reached').toEqual([]);
  });

  it('has the upgrade command in its menu and the panel’s, and the commands box lists it for each device', async () => {
    const fw = await mount();
    await check(fw);
    await clickOn(row('/0').querySelector('.name'), { right: true });
    await pickItem('Upgrade firmware…');
    expect(await until(() => !!dialog())).toBe(true);
    expect(dialog().querySelector('[data-installed]').textContent).toBe('2026-01-10 aaaaaa');
    await close('Cancel');
    await clickOn(document.querySelector('[data-focus-key=panel-devices]'), { right: true });
    await pickItem('Upgrade firmware…', '/1 OMG');
    expect(await until(() => !!dialog())).toBe(true);
    expect(dialog().querySelector('[data-installed]').textContent).toBe('2026-05-02 77b0c3');
    await close('Cancel');
    await pressKey('Control+k');
    expect(await until(() => !!document.querySelector('[data-slot=command-item]'))).toBe(true);
    const listed = [...document.querySelectorAll('[data-slot=command-item]')].map((i) => i.textContent.trim()).filter((t) => /firmware|Upgrade all/.test(t));
    expect(listed.sort()).toEqual(['Check for firmware updates', 'Upgrade all (1)', 'Upgrade firmware… › /0 VMR 0412', 'Upgrade firmware… › /1 OMG 0077', 'Upgrade firmware… › /2 SYNC 0003']);
    await pressKey('Escape');
  });
});

describe('the upgrade dialog', () => {
  it('shows the device, starts on the newest release, and asks the hub for it', async () => {
    const fw = await mount();
    await check(fw);
    await open('/0');
    const facts = [...dialog().querySelectorAll('.facts > span')].map((s) => s.textContent);
    expect(facts).toEqual(['Route', '/0', 'Serial', '0412', 'Hardware', 'VMR R4', 'Installed', '2026-01-10 aaaaaa']);
    expect(button('Latest', dialog()).getAttribute('aria-pressed')).toBe('true');
    expect(dialog().querySelector('[data-version]').textContent).toBe('2026-07-01 5d1494');
    expect(notes().at(-1)).toMatch(/^Streaming from the device stops/);
    await press('Upgrade to 2026-07-01 5d1494');
    expect(fw.asks('upgrade')).toEqual([{ type: 'upgrade', route: '/0', source: { kind: 'latest' } }]);
  });

  it('installs another release picked from the list, the installed one marked', async () => {
    const fw = await mount();
    await check(fw);
    await open('/0');
    await press('Another');
    const trigger = dialog().querySelector('[aria-label=Version]');
    expect(trigger.textContent.trim(), 'the newest after the latest').toBe('2026-03-17 0c22e1');
    await clickOn(trigger);
    expect(await until(() => document.querySelectorAll('[data-slot=select-item]').length === 3)).toBe(true);
    const items = [...document.querySelectorAll('[data-slot=select-item]')];
    expect(items.map((i) => i.textContent.trim())).toEqual(['2026-07-01 5d1494', '2026-03-17 0c22e1', '2026-01-10 aaaaaa (installed)']);
    await clickOn(items[2]);
    await until(() => !document.querySelector('[data-slot=select-content]'));
    await press('Upgrade to 2026-01-10 aaaaaa');
    expect(fw.asks('upgrade')).toEqual([{ type: 'upgrade', route: '/0', source: { kind: 'release', file: VMR[2].file } }]);
  });

  it('sends a file from this computer whole, in frames after its ask, and refuses one empty or over the bound itself', async () => {
    const fw = await mount();
    await check(fw);
    await open('/2');
    expect(button('File', dialog()).getAttribute('aria-pressed'), 'a device the catalog has nothing for starts on a file').toBe('true');
    expect(button('Latest', dialog()).disabled && button('Another', dialog()).disabled).toBe(true);
    expect(notes()).toContain('No firmware is published for SYNC R1.');
    expect(button('Upgrade', dialog()).disabled, 'nothing to install yet').toBe(true);
    await choose(new File([], 'empty.bin'));
    expect(notes()).toContain('empty.bin is empty.');
    expect(button('Upgrade', dialog()).disabled).toBe(true);
    await choose(new File([new Uint8Array(MAX_IMAGE + 1)], 'huge.bin'));
    expect(notes().some((n) => /^huge\.bin is 16\.8 MB, more than the 16\.8 MB/.test(n))).toBe(true);
    expect(button('Upgrade', dialog()).disabled).toBe(true);
    await choose(new File([new Uint8Array(150_000).fill(7)], 'sync-r1.bin'));
    await press('Upgrade to sync-r1.bin');
    expect(await until(() => fw.images.length === 1)).toBe(true);
    expect(fw.posted[1]).toEqual({ type: 'upgrade', route: '/2', source: { kind: 'file', name: 'sync-r1.bin', size: 150_000 } });
    const frames = fw.posted.slice(2);
    expect(frames.every((f) => f.bytes > 0 && f.bytes <= 64 * 1024) && frames.reduce((n, f) => n + f.bytes, 0), 'frames and nothing between').toBe(150_000);
    expect(fw.images).toEqual([{ route: '/2', name: 'sync-r1.bin', bytes: 150_000 }]);
    expect(steps(), 'a file has no download').toEqual(['uploading now', 'finalizing wait', 'restarting wait']);
  });

  it('says the catalog could not be reached, and still takes a file; opening it is what asks the catalog', async () => {
    const fw = await mount();
    fw.reachable = false;
    await clickOn(row('/0').querySelector('.name'), { right: true });
    await pickItem('Upgrade firmware…');
    expect(await until(() => !!dialog())).toBe(true);
    expect(fw.asks('check').length, 'opening the dialog asks').toBe(1);
    await wait(fw.latency + 20);
    expect(dialog().querySelector('[data-catalog-note]').textContent).toMatch(/^The catalog could not be reached\./);
    expect(button('File', dialog()).getAttribute('aria-pressed')).toBe('true');
    expect(button('Latest', dialog()).disabled).toBe(true);
    expect(line('/0').children.length, 'the row says nothing of it').toBe(1);
  });
});

describe('an upgrade', () => {
  it('shows its steps in the dialog and on the row together, goes on with the dialog closed, and ends with the version the device reports', async () => {
    const fw = await mount();
    await check(fw);
    await open('/0');
    // A part of the upload lasts longer than the dialog takes to close and open.
    fw.times.chunk = 3000;
    await press('Upgrade to 2026-07-01 5d1494');
    expect(steps()).toEqual(['downloading now', 'uploading wait', 'finalizing wait', 'restarting wait']);
    expect(said('/0'), 'the upgrade takes the version’s place').toEqual(['Download']);
    await wait(fw.times.download + fw.times.chunk * 4);
    expect(steps()).toEqual(['downloading done', 'uploading now', 'finalizing wait', 'restarting wait']);
    expect(dialog().querySelector('[role=progressbar]').getAttribute('aria-valuenow')).toBe('40');
    expect(dialog().querySelector('[data-step=uploading] .set-value').textContent).toBe('165 kB of 412 kB');
    expect(said('/0')).toEqual(['Upload40%']);
    expect(button('Cancel upgrade', dialog()).disabled, 'no cancel once the upload began').toBe(true);
    await close();
    expect(await until(() => said('/0')[0] === 'Upload70%', fw.times.chunk * 4), 'closing stops nothing').toBe(true);
    expect(fw.asks('cancel')).toEqual([]);
    await open('/0');
    expect(dialog().querySelector('[role=progressbar]').getAttribute('aria-valuenow'), 'reopened, it shows the same').toBe('70');
    expect(await until(() => steps()[2] === 'finalizing now', fw.times.chunk * 5)).toBe(true);
    expect(said('/0')).toEqual(['Finalize']);
    expect(await until(() => said('/0')[0] === 'Restart', fw.times.finalize + 100)).toBe(true);
    expect(await until(() => steps()[3] === 'restarting done', fw.times.restart + 100)).toBe(true);
    expect(steps()).toEqual(['downloading done', 'uploading done', 'finalizing done', 'restarting done']);
    expect(notes()).toEqual(['VMR 0412 now reports 2026-07-01 5d1494.']);
    expect(said('/0'), 'the row shows the new version').toEqual(['2026-07-01', 'Up to date']);
    expect(dialog().querySelector('[data-installed]').textContent).toBe('2026-07-01 5d1494');
    await close();
    expect(fw.asks('cancel'), 'closing a finished upgrade clears it at the hub').toEqual([{ type: 'cancel', route: '/0' }]);
    expect(page.hub.session.devices.get('/0').upgrade).toBe(null);
  }, 60_000);

  it('is taken back by Cancel until its upload begins, and the dialog offers the sources again', async () => {
    const fw = await mount();
    await check(fw);
    await open('/0');
    await press('Upgrade to 2026-07-01 5d1494');
    expect(button('Cancel upgrade', dialog()).disabled).toBe(false);
    await press('Cancel upgrade');
    expect(fw.asks('cancel')).toEqual([{ type: 'cancel', route: '/0' }]);
    expect(!!button('Upgrade to 2026-07-01 5d1494', dialog()), 'the sources again').toBe(true);
    expect(said('/0')).toEqual(['2026-01-10', '2026-07-01 available']);
    await wait(whole(fw));
    expect(said('/0'), 'nothing was installed').toEqual(['2026-01-10', '2026-07-01 available']);
  });

  it('that fails names the step and the reason in the dialog and stays on the row, and Retry runs it again', async () => {
    const fw = await mount();
    await check(fw);
    fw.failing['/0'] = { step: 'upload', error: 'firmware chunk 712/1431 failed 3 times without progress: timed out' };
    await open('/0');
    await press('Upgrade to 2026-07-01 5d1494');
    await wait(whole(fw));
    expect(steps()).toEqual(['downloading done', 'uploading failed', 'finalizing wait', 'restarting wait']);
    expect(notes()).toEqual(['Upload failed: firmware chunk 712/1431 failed 3 times without progress: timed out']);
    expect(said('/0')).toEqual(['2026-01-10', 'Upgrade failed']);
    await close();
    expect(said('/0'), 'a failure stays until it is retried or cleared').toEqual(['2026-01-10', 'Upgrade failed']);
    await open('/0');
    await press('Retry');
    expect(fw.asks('retry')).toEqual([{ type: 'retry', route: '/0' }]);
    expect(await until(() => notes()[0] === 'VMR 0412 now reports 2026-07-01 5d1494.', whole(fw) + 500)).toBe(true);
  });

  it('of an image the device cannot take says nothing was sent, and offers another source, not a retry', async () => {
    const fw = await mount();
    await check(fw);
    fw.failing['/2'] = { step: 'check', error: 'the device cannot take this image: the device takes signed packages, and this is an image' };
    await open('/2');
    await choose(new File([new Uint8Array(4000)], 'wrong.bin'));
    await press('Upgrade to wrong.bin');
    await wait(100);
    expect(notes()).toEqual(['Nothing was sent to the device: the device cannot take this image: the device takes signed packages, and this is an image']);
    expect(steps().every((s) => s.endsWith('wait'))).toBe(true);
    expect(button('Retry', dialog())).toBe(undefined);
    await press('Choose another');
    expect(button('File', dialog()).getAttribute('aria-pressed')).toBe('true');
  });

  it('the hub refuses is said as a notice', async () => {
    await mount();
    page.hub.text({ type: 'refused', ask: 'upgrade', route: '/0', error: 'an upgrade of /0 is already running' });
    expect(await until(() => notices().length === 1)).toBe(true);
    expect(notices()[0]).toMatch(/The hub refused: an upgrade of \/0 is already running/);
  });
});

describe('Upgrade all', () => {
  it('is offered once two devices have an update, lists them, and upgrades each to its newest release, one at a time', async () => {
    const fw = await mount(TWO_OLD());
    expect(button('Upgrade all (2)'), 'not before the catalog was checked').toBe(undefined);
    await check(fw);
    await clickOn(button('Upgrade all (2)'));
    expect(await until(() => !!dialog())).toBe(true);
    const rows = () => [...dialog().querySelectorAll('.fw-all .row')].map((r) => [...r.children].map((c) => c.textContent.trim()).join(' | '));
    expect(rows()).toEqual(['/0 | VMR | 2026-01-10 aaaaaa | 2026-07-01 available', '/1 | OMG | 2026-02-11 31c0de | 2026-05-02 available']);
    await press('Upgrade 2 devices');
    expect(fw.asks('upgrade')).toEqual([{ type: 'upgrade', route: '/0', source: { kind: 'latest' } }, { type: 'upgrade', route: '/1', source: { kind: 'latest' } }]);
    await wait(fw.times.download + fw.times.chunk * 3 + 20);
    expect(rows()[0]).toMatch(/^\/0 \| VMR \| 2026-01-10 aaaaaa \| Upload[34]0%$/);
    expect(rows()[1]).toBe('/1 | OMG | 2026-02-11 31c0de | Waiting');
    expect(said('/1'), 'the second waits on its row too').toEqual(['Waiting']);
    expect(button('Upgrade all (2)'), 'none is left to offer').toBe(undefined);
    await shoot('all-running');
    await wait(whole(fw));
    expect(rows()[0]).toBe('/0 | VMR | 2026-07-01 5d1494 | Done');
    expect(rows()[1]).toMatch(/^\/1 \| OMG \| 2026-02-11 31c0de \| (Download|Upload)/);
    await wait(whole(fw));
    expect(rows()).toEqual(['/0 | VMR | 2026-07-01 5d1494 | Done', '/1 | OMG | 2026-05-02 77b0c3 | Done']);
    await close();
    expect(['/0', '/1'].map(said)).toEqual([['2026-07-01', 'Up to date'], ['2026-05-02', 'Up to date']]);
  });
});

describe('the keyboard', () => {
  it('opens the dialog from the row, reaches its sources and its buttons, and Escape closes it, focus back on the row', async () => {
    const fw = await mount();
    await check(fw);
    row('/0').querySelector('[data-slot=checkbox]').focus();
    await pressKey('Tab');
    expect(document.activeElement, 'Tab reaches what the row offers').toBe(offer('/0'));
    await pressKey('Enter');
    expect(await until(() => !!dialog())).toBe(true);
    await settle();
    expect(dialog().contains(document.activeElement), 'focus is in the dialog').toBe(true);
    const reached = [];
    for (let i = 0; i < 8; i++) {
      reached.push(document.activeElement.textContent.trim() || document.activeElement.getAttribute('aria-label'));
      await pressKey('Tab');
      expect(dialog().contains(document.activeElement), 'Tab stays in the dialog').toBe(true);
    }
    for (const name of ['Latest', 'Another', 'File', 'Cancel', 'Upgrade to 2026-07-01 5d1494']) expect(reached, name).toContain(name);
    button('Upgrade to 2026-07-01 5d1494', dialog()).focus();
    await pressKey('Enter');
    await settle();
    expect(fw.asks('upgrade').length).toBe(1);
    await pressKey('Escape');
    expect(await until(() => !document.querySelector('.firmware-dialog'))).toBe(true);
    // The page hands focus back to the panel the dialog was opened from, as it does for its other dialogs.
    expect(document.activeElement.closest('.dock-panel')?.contains(row('/0')), 'focus goes back to the Devices panel').toBe(true);
    await wait(fw.times.download + fw.times.chunk * 2);
    expect(said('/0')[0], 'Escape stops nothing').toMatch(/^Upload/);
  });
});

describe('the Devices panel', () => {
  it('keeps everything where it is as an upgrade’s numbers change', async () => {
    const fw = await mount();
    await check(fw);
    await open('/0');
    await press('Upgrade to 2026-07-01 5d1494');
    await close();
    /** Where everything in the panel is, but the meter's fill and the number's own text, whose widths are the number: the room each keeps is here. */
    const places = () => [...panel().querySelectorAll('*')].filter((el) => !el.closest('.meter, .fit') || el.matches('.meter, .fit')).map(box);
    const others = () => ['/1', '/2'].map((r) => box(row(r))).concat([box(row('/0')), box(line('/0')), box(offer('/0')), box(panel())]);
    const whileUploading = [], throughout = [], percents = new Set();
    for (let t = 0; t < whole(fw) + 100; t += 50) {
      await wait(50);
      flushSync();
      if (offer('/0')?.matches('.progress')) throughout.push(others());
      const num = offer('/0')?.querySelector('.fit')?.textContent;
      if (num) (percents.add(num), whileUploading.push(places()));
    }
    expect(percents.size, 'the number changed').toBeGreaterThan(5);
    expect([...percents]).toContain('100%');
    expect(new Set(whileUploading.map((boxes) => boxes.length)).size, 'the same elements while it did').toBe(1);
    expect(moved(whileUploading), 'nothing moved while it did').toBeLessThan(STILL);
    expect(throughout.length, 'every step was seen').toBeGreaterThan(whileUploading.length + 10);
    expect(moved(throughout), 'and no row, nor the upgrade’s item, moved from its first step to its last').toBeLessThan(STILL);
  });

  it('keeps a device’s row one height, and everything under it where it is, in every state the row can be in, at a usual dock and at the narrowest', async () => {
    /** Every state of the catalog, and of an upgrade, as firmware.js lists them. */
    const catalogs = CATALOG_STATES.flatMap((state) => (state === 'checked' ? STATUSES.map((status) => ({ state, status, releases: status === 'none' || status === 'development' ? [] : VMR })) : [{ state, ...(state === 'unreachable' && { error: 'no network' }) }]));
    const upgrades = [
      ['idle'],
      ...RUNNING.flatMap((state) => (state === 'uploading' ? [0, 206_000, 412_000].map((sent) => [state, { sent, size: 412_000 }]) : [[state]])),
      ['done', { installed: installed('VMR', 'R4', '2026-07-01', '5d1494') }],
      ...FAILED_STEPS.map((step) => ['failed', { step, error: 'timed out' }]),
    ];
    try {
      // The narrowest a dock goes is 12% of the window (App.svelte DOCK.min), of the smallest window the page is made for.
      for (const [w, h, dock, name] of [[1280, 800, 22, 'usual'], [900, 600, 12, 'narrow']]) {
        await browser.viewport(w, h);
        const fw = await mount([{ ...THREE()[0], installed: null }, ...THREE().slice(1)], dock);
        const under = () => [box(row('/1')), box(row('/2')), ...[...panel().querySelectorAll('button')].filter((b) => !b.closest('.device')).map(box)];
        const height = () => row('/0').getBoundingClientRect().height;
        // Before the hub has read what the device runs.
        const first = { height: height(), under: under() };
        const heights = new Map();
        const seen = async (what) => {
          await settle(1);
          heights.set(what, height());
          expect(moved([first.under, under()]), `${name}: what is under the row, ${what}`).toBe(0);
          const pressed = offer('/0');
          if (!pressed) return;
          // What is pressed, and an upgrade's number and meter, are in the row, in view, and what a press there hits.
          const within = (el) => el.getBoundingClientRect().width > 0 && el.getBoundingClientRect().left >= row('/0').getBoundingClientRect().left - 0.5 && el.getBoundingClientRect().right <= row('/0').getBoundingClientRect().right + 0.5;
          for (const el of [pressed, ...pressed.querySelectorAll('.fit, .meter')]) expect(within(el), `${name}: ${el.className} in the row, ${what}`).toBe(true);
          const r = pressed.getBoundingClientRect();
          expect(pressed.contains(document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2)), `${name}: reachable, ${what}`).toBe(true);
          // Text cut short says itself whole in its tooltip.
          for (const el of line('/0').querySelectorAll('.version, .offer, .said, .word')) if (el.scrollWidth > el.clientWidth) expect(el.title || el.closest('[title]')?.title, `${name}: a tooltip for what is cut, ${what}`).toBeTruthy();
        };
        fw.reads('/0', THREE()[0].installed);
        for (const catalog of catalogs) {
          fw.say('/0', catalog);
          for (const [state, more] of upgrades) {
            fw.tell('/0', state, more);
            await seen(`${catalog.status ?? catalog.state}, ${state} ${more?.sent ?? more?.step ?? ''}`);
          }
        }
        expect(heights.size).toBe(catalogs.length * upgrades.length);
        expect([...new Set([first.height, ...heights.values()])], `${name}: one height`).toEqual([first.height]);
        if (SHOTS) {
          for (const theme of ['light', 'dark']) {
            prefs.setTheme(theme);
            fw.say('/0', catalogs.find((c) => c.status === 'update'));
            fw.tell('/0', 'idle');
            await settle();
            await shoot(`row-${name}-offer-${theme}-${w}x${h}`, panel());
            fw.tell('/0', 'uploading', { sent: 206_000, size: 412_000 });
            await settle();
            await shoot(`row-${name}-upload-${theme}-${w}x${h}`, panel());
            fw.tell('/0', 'failed', { step: 'upload', error: 'timed out' });
            await settle();
            await shoot(`row-${name}-failed-${theme}-${w}x${h}`, panel());
            await shoot(`page-${name}-${theme}-${w}x${h}`);
          }
          prefs.setTheme('system');
        }
        page.dispose();
        page = null;
      }
    } finally {
      await browser.viewport(1200, 900);
    }
  }, 120_000);

  it('and the dialog fit a window 900 by 600', async () => {
    await browser.viewport(900, 600);
    try {
      const fw = await mount();
      await check(fw);
      const fits = (el) => {
        const r = el.getBoundingClientRect();
        return r.left >= 0 && r.top >= 0 && r.right <= 900 && r.bottom <= 600;
      };
      for (const route of ['/0', '/1', '/2']) {
        const state = line(route).lastElementChild;
        expect(state.getBoundingClientRect().left >= row(route).getBoundingClientRect().left && state.getBoundingClientRect().right <= row(route).getBoundingClientRect().right, `${route}'s state is in its row`).toBe(true);
      }
      await open('/0');
      await press('Another');
      const inside = () => fits(dialog()) && dialog().scrollHeight <= dialog().clientHeight && dialog().scrollWidth <= dialog().clientWidth;
      expect(inside(), 'the sources').toBe(true);
      await press('File');
      await choose(new File([new Uint8Array(412_000)], 'VMR-R4-firmware-2026-07-01-5d1494-signed-development.bin'));
      expect(inside(), 'a file of a long name').toBe(true);
      expect(button('Upgrade to VMR-R4-firmware-2026-07-01-5d1494-signed-development.bin', dialog()).disabled).toBe(false);
      await press('Latest');
      await press('Upgrade to 2026-07-01 5d1494');
      await wait(fw.times.download + fw.times.chunk * 5);
      expect(inside(), 'the steps').toBe(true);
      expect(offer('/0').getBoundingClientRect().right <= row('/0').getBoundingClientRect().right, 'the row while it uploads').toBe(true);
    } finally {
      await browser.viewport(1200, 900);
    }
  });
});

describe('pictures', () => {
  it.runIf(SHOTS)('of the panel and the dialog in each state, light and dark, at two sizes, and the page’s other dialogs beside', async () => {
    for (const [w, h] of [[1280, 800], [900, 600]]) {
      for (const theme of ['light', 'dark']) {
        await browser.viewport(w, h);
        prefs.setTheme(theme);
        const tag = `${theme}-${w}x${h}`;
        let fw = await mount();
        await shoot(`panel-unchecked-${tag}`, panel());
        await check(fw);
        await shoot(`panel-states-${tag}`, panel());
        await shoot(`page-states-${tag}`);
        await open('/0');
        await settle(8);
        await shoot(`dialog-latest-${tag}`);
        await press('Another');
        await shoot(`dialog-another-${tag}`);
        await press('File');
        await choose(new File([new Uint8Array(412_000)], 'VMR-R4-firmware-2026-07-01-5d1494.bin'));
        await shoot(`dialog-file-${tag}`);
        await press('Latest');
        await press('Upgrade to 2026-07-01 5d1494');
        fw.times.chunk = 3000;
        await wait(fw.times.download + fw.times.chunk * 4 + 20);
        await settle();
        await shoot(`upload-${tag}`);
        await close();
        await shoot(`upload-row-${tag}`, panel());
        await open('/0');
        await wait(whole(fw));
        await settle(8);
        await shoot(`done-${tag}`);
        await close();
        await shoot(`panel-after-${tag}`, panel());
        fw.failing['/1'] = { step: 'upload', error: 'firmware chunk 712/1431 failed 3 times without progress: timed out' };
        await clickOn(row('/1').querySelector('.name'), { right: true });
        await pickItem('Upgrade firmware…');
        await until(() => !!dialog());
        await press('Another');
        await press('Upgrade to 2026-05-02 77b0c3');
        await wait(whole(fw));
        await settle(8);
        await shoot(`failed-${tag}`);
        await close();
        await shoot(`panel-failed-${tag}`, panel());
        page.dispose();
        fw = await mount(TWO_OLD());
        await check(fw);
        await shoot(`panel-all-${tag}`, panel());
        await clickOn(button('Upgrade all (2)'));
        await until(() => !!dialog());
        await settle(8);
        await shoot(`all-ask-${tag}`);
        fw.times.chunk = 3000;
        await press('Upgrade 2 devices');
        await wait(fw.times.download + fw.times.chunk * 6 + 20);
        await settle();
        await shoot(`all-running-${tag}`);
        await close();
        await shoot(`panel-all-running-${tag}`, panel());
        await clickOn(document.querySelector('[data-focus-key=panel-devices]'), { right: true });
        await until(() => !!document.querySelector('[data-slot=context-menu-content][data-state=open]'));
        await settle(8);
        await shoot(`panel-menu-${tag}`);
        await closeAll();
        await clickOn(document.querySelector('.status-bar .keys'));
        await until(() => !!document.querySelector('[data-slot=dialog-content][data-state=open]'));
        await settle(8);
        await shoot(`beside-keys-${tag}`);
        await pressKey('Escape');
        await until(() => !document.querySelector('[data-slot=dialog-content]'));
        await clickOn(button('Find devices…'));
        await until(() => !!document.querySelector('[data-slot=dialog-content][data-state=open]'));
        await settle(8);
        await shoot(`beside-picker-${tag}`);
        await pressKey('Escape');
        await until(() => !document.querySelector('[data-slot=dialog-content]'));
        feed(page.hub);
        await settle();
        await plotMenu(plotIds(page.ws.layout)[0]);
        await pickItem('Export…');
        await until(() => !!document.querySelector('.export-dialog'));
        await settle(12);
        await shoot(`beside-export-${tag}`);
        await pressKey('Escape');
        await until(() => !document.querySelector('[data-slot=dialog-content]'));
        page.dispose();
        page = null;
      }
    }
  }, 300_000);
});

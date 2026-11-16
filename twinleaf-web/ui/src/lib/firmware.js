// What the page says of a device's firmware, in one place: the version a
// device runs and the versions published for it, as the hub's `firmware`
// message gives them; where an upgrade stands, as its `upgrade` message
// does; and what the Devices panel's row and the upgrade dialog show of
// each (session.svelte.js Device `fw` and `upgrade`).
import { bytesText } from './units.js';

/** The most bytes of an image the hub takes (twinleaf-web/src/firmware.rs MAX_IMAGE). */
export const MAX_IMAGE = 16 * 1024 * 1024;

/** The states of the catalog of a device, as its `firmware` message says them. */
export const CATALOG_STATES = ['unchecked', 'checking', 'unreachable', 'checked'];
/** How a checked catalog's newest release stands to what a device runs. */
export const STATUSES = ['update', 'current', 'none', 'development', 'unknown'];
/** The steps a failed upgrade names. */
export const FAILED_STEPS = ['check', 'download', 'upload', 'finalize'];
/** The states of an upgrade the hub has not ended, in their order. */
export const RUNNING = ['queued', 'downloading', 'uploading', 'finalizing', 'restarting'];
/** The steps of an upgrade, each [the state it is under way in, its name], in their order; a file has no download. */
const STEPS = [['downloading', 'Download'], ['uploading', 'Upload'], ['finalizing', 'Finalize'], ['restarting', 'Restart']];
/** The step a failure names (`step` of a failed upgrade), by the state it failed in. */
const FAILED_IN = { download: 'downloading', upload: 'uploading', finalize: 'finalizing' };

/** The version `installed` is, its build date and build, or `unknown` for one that says neither. */
export const versionText = (installed) => [installed?.date, installed?.build].filter(Boolean).join(' ') || 'unknown';
/** The version release `r` is, as `versionText` writes an installed one. */
export const releaseText = (r) => `${r.date} ${r.build}`;
/** What a device's row has room for of the version `installed` is: its build date, or its build when it has no date. */
export const versionShort = (installed) => installed?.date ?? installed?.build ?? 'unknown';
/** The hardware `installed` is of, its name and revision, or null for a device that says neither. */
export const hardwareText = (installed) => [installed?.name, installed?.revision].filter(Boolean).join(' ') || null;

/** The releases the catalog listed for device `d`, newest first; none before it was checked. */
export const releasesOf = (d) => (d?.fw?.catalog.state === 'checked' ? d.fw.catalog.releases : []);
/** How the catalog's newest release stands to what device `d` runs: update, current, none, development or unknown; null before it was checked. */
export const statusOf = (d) => (d?.fw?.catalog.state === 'checked' ? d.fw.catalog.status : null);
/** Whether release `r` is the build device `d` runs. */
export const installedIs = (d, r) => !!d?.fw?.installed?.build && d.fw.installed.build.toLowerCase() === r.build.toLowerCase();

/** Whether upgrade `u` is under way: asked for and not ended. */
export const running = (u) => !!u && RUNNING.includes(u.state);
/** Whether the hub still takes upgrade `u` back: nothing of it has gone to the device. */
export const cancellable = (u) => !!u && (u.state === 'queued' || u.state === 'downloading');
/** Whether device `d` has a newer release published and no upgrade under way: what Upgrade all upgrades. */
export const hasUpdate = (d) => statusOf(d) === 'update' && !running(d.upgrade);

/** How far the upload of upgrade `u` is, in whole percent. */
export const percent = (u) => (u?.size ? Math.min(100, Math.floor((100 * (u.sent ?? 0)) / u.size)) : 0);

/**
 * What device `d`'s row says of its firmware beside the version, or null
 * for nothing: {kind, text}, `kind` 'progress' for an upgrade under way,
 * its step by name, or Waiting for one queued, with `percent` while it
 * uploads; 'failed' for one that failed; 'update'
 * for a newer release, which the row offers; or 'note' for what needs no
 * press. A catalog not checked, or not reached, says nothing.
 */
export function rowState(d) {
  const u = d.upgrade;
  if (running(u)) return { kind: 'progress', text: STEPS.find(([state]) => state === u.state)?.[1] ?? 'Waiting', percent: u.state === 'uploading' ? percent(u) : null };
  if (u?.state === 'failed') return { kind: 'failed', text: 'Upgrade failed' };
  const catalog = d.fw?.catalog;
  if (catalog?.state === 'checking') return { kind: 'note', text: 'Checking…' };
  if (catalog?.state !== 'checked') return null;
  const latest = catalog.releases[0];
  switch (catalog.status) {
    case 'update': return { kind: 'update', text: `${latest.date} available` };
    case 'unknown': return { kind: 'update', text: `${latest.date} published` };
    case 'current': return { kind: 'note', text: 'Up to date' };
    case 'development': return { kind: 'note', text: 'Development build' };
    default: return { kind: 'note', text: 'None published' };
  }
}

/**
 * The steps of upgrade `u` as the dialog lists them, each {id, name, at,
 * value}: `at` 'wait', 'now', 'done' or 'failed', and `value` what the
 * step says beside its name, the bytes sent of the image while it
 * uploads. A failure before anything was sent, `check`, is no step's.
 */
export function stepsOf(u) {
  const steps = STEPS.filter(([state]) => u.source !== 'file' || state !== 'downloading');
  const reached = u.state === 'done' ? steps.length : u.state === 'failed' ? steps.findIndex(([s]) => s === FAILED_IN[u.step]) : steps.findIndex(([s]) => s === u.state);
  return steps.map(([state, name], i) => {
    const at = reached < 0 || i > reached ? 'wait' : i < reached ? 'done' : u.state === 'failed' ? 'failed' : 'now';
    return { id: state, name, at, value: state === 'uploading' && at === 'now' ? `${bytesText(u.sent ?? 0)} of ${bytesText(u.size ?? 0)}` : '' };
  });
}

/** What a failed upgrade `u` says: the step it failed at and why. */
export function failureText(u) {
  const step = STEPS.find(([s]) => s === FAILED_IN[u.step])?.[1];
  return step ? `${step} failed: ${u.error}` : `Nothing was sent to the device: ${u.error}`;
}

/** What device `d` reads as in a sentence: its name and serial, or its route. */
export const deviceText = (d) => (d.name ? [d.name, d.serial ?? d.fw?.installed?.serial].filter(Boolean).join(' ') : d.route);

/**
 * The source the dialog starts on for device `d`: the newest release when
 * it is newer or cannot be compared, another when the device is up to
 * date, and a file when the catalog has nothing for it.
 */
export function defaultSource(d) {
  const status = statusOf(d);
  return status === 'update' || status === 'unknown' ? 'latest' : status === 'current' ? 'release' : 'file';
}

/** Why the catalog offers device `d` nothing, as the dialog says it, or null while it offers something or is being asked. */
export function catalogNote(d) {
  const catalog = d.fw?.catalog;
  if (!catalog || catalog.state === 'unchecked' || catalog.state === 'checking') return null;
  if (catalog.state === 'unreachable') return 'The catalog could not be reached. A file from this computer can still be installed.';
  if (catalog.status === 'development') return 'This device runs a development build, which no published release replaces. A file can still be installed.';
  if (catalog.status === 'none') return `No firmware is published for ${hardwareText(d.fw.installed) ?? 'this device'}.`;
  if (catalog.status === 'unknown') return 'The installed build has no date, so whether the newest release is newer is not known.';
  return null;
}

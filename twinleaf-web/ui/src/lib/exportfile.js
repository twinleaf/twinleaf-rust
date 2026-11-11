// Saving an export: the hub's raw history of signals, as the hub writes it,
// or an image of a plot, as the page draws it. A file is made whole first,
// its progress told as it goes, and then saved: where the browser has a
// save window (showSaveFilePicker), or a folder window for several files
// (showDirectoryPicker), the files are named and placed there; elsewhere
// each downloads as any file does. A browser opens those windows only
// shortly after a press, so whoever saves says whether one is still near.

import * as clock from './clock.js';

/** What each data format is, for the save window. */
export const FORMATS = {
  csv: { ext: 'csv', mime: 'text/csv', description: 'CSV table' },
  h5: { ext: 'h5', mime: 'application/x-hdf5', description: 'HDF5 file' },
};

/** The stamp of an export made at `date`, as its file names have it: 20261108-140501. */
export function exportStamp(date = new Date(clock.wall())) {
  const two = (n) => String(n).padStart(2, '0');
  return `${date.getFullYear()}${two(date.getMonth() + 1)}${two(date.getDate())}-${two(date.getHours())}${two(date.getMinutes())}${two(date.getSeconds())}`;
}

/** A file name for an export made at `date`: twinleaf-20261108-140501.csv. */
export function exportName(format, date = new Date(clock.wall())) {
  return `twinleaf-${exportStamp(date)}.${FORMATS[format].ext}`;
}

/** The URL the hub serves an export at: traces `ids` of hub `hub` within [t0, t1]. */
export function exportUrl({ hub, ids, t0, t1 }, format, name) {
  return `/api/export?${new URLSearchParams({ hub, ids: ids.join(','), t0, t1, format, name })}`;
}

/**
 * The export `request` describes, as `format`, named `name`, fetched from
 * the hub whole, as a Blob. Says how many bytes have come as they do,
 * through `progress`; `signal` aborts it.
 */
export async function fetchExport(request, format, name, { signal, progress = () => {} } = {}) {
  const response = await fetch(exportUrl(request, format, name), { signal });
  if (!response.ok) throw new Error((await response.json().catch(() => null))?.error ?? response.statusText);
  const reader = response.body.getReader(), chunks = [];
  let bytes = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    bytes += value.length;
    progress(bytes);
  }
  return new Blob(chunks, { type: FORMATS[format].mime });
}

/**
 * Saves `files`, each {name, blob, type}, `type` {description, mime, ext}:
 * one through the save window, several into a folder picked in the folder
 * window, or else each downloaded. Resolves to the files saved, each
 * {name, bytes}, or to null when the window was closed with nothing picked.
 */
export async function saveFiles(files) {
  const picks = files.length === 1 ? 'showSaveFilePicker' in window : 'showDirectoryPicker' in window;
  if (!picks) {
    for (const f of files) {
      const a = document.createElement('a');
      a.href = URL.createObjectURL(f.blob);
      a.download = f.name;
      a.click();
      clock.after(0, () => URL.revokeObjectURL(a.href));
    }
    return files.map((f) => ({ name: f.name, bytes: f.blob.size }));
  }
  try {
    if (files.length === 1) {
      const [f] = files;
      const handle = await window.showSaveFilePicker({ suggestedName: f.name, types: [{ description: f.type.description, accept: { [f.type.mime]: [`.${f.type.ext}`] } }] });
      await write(handle, f.blob);
      return [{ name: handle.name, bytes: f.blob.size }];
    }
    const folder = await window.showDirectoryPicker({ mode: 'readwrite' });
    for (const f of files) await write(await folder.getFileHandle(f.name, { create: true }), f.blob);
    return files.map((f) => ({ name: f.name, bytes: f.blob.size }));
  } catch (error) {
    if (error.name === 'AbortError') return null;
    throw error;
  }
}

/** Writes `blob` to the file `handle` names. */
async function write(handle, blob) {
  const out = await handle.createWritable();
  await out.write(blob);
  await out.close();
}

/** Whether the browser would still open a save window now: a press is near enough (navigator.userActivation), as where it does not say. */
export const mayAskWhere = () => navigator.userActivation?.isActive ?? true;

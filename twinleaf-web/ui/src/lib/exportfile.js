// Saving an export: the hub's raw history of signals, as the hub writes it,
// an image of a plot, as the page draws it, or a table the page writes. A
// browser opens its save window only shortly after a press, so where it has
// one (showSaveFilePicker for one file, showDirectoryPicker for a folder of
// several) the place is asked first, at the press (`askWhere`), the files
// are made after, and each is written there once it is whole (`putFiles`);
// a place given up before then is left as it was found (`giveUp`). Where
// the browser has no such window, the files are made and each downloads
// as any file does.

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

/** `n` files, as a sentence counts them: 1 file, 2 files. */
export const filesText = (n) => (n === 1 ? '1 file' : `${n} files`);

/**
 * Where `files`, each {name, type}, `type` {description, mime, ext}, will
 * go, asked before any is made; called within a press, as a save window
 * needs. One goes to a file picked in the save window, several into a
 * folder picked in the folder window, each {file} or {folder} of the
 * handle picked; where the browser has no such window, {download}. Resolves
 * to null when the window was closed with nothing picked.
 */
export async function askWhere(files) {
  const one = files.length === 1;
  if (typeof (one ? window.showSaveFilePicker : window.showDirectoryPicker) !== 'function') return { download: true };
  try {
    if (one) {
      const [f] = files;
      return { file: await window.showSaveFilePicker({ suggestedName: f.name, types: [{ description: f.type.description, accept: { [f.type.mime]: [`.${f.type.ext}`] } }] }) };
    }
    return { folder: await window.showDirectoryPicker({ mode: 'readwrite' }) };
  } catch (error) {
    if (error.name === 'AbortError') return null;
    throw error;
  }
}

/**
 * Writes `made`, each {name, blob}, whole, to `where` as `askWhere` gave
 * it: the one into the file picked, each by its name into the folder
 * picked, or each downloaded. Resolves to the files saved, each {name, bytes}.
 */
export async function putFiles(where, made) {
  if (where.file) {
    await write(where.file, made[0].blob);
    return [{ name: where.file.name, bytes: made[0].blob.size }];
  }
  for (const f of made) {
    if (where.folder) await write(await where.folder.getFileHandle(f.name, { create: true }), f.blob);
    else download(f);
  }
  return made.map((f) => ({ name: f.name, bytes: f.blob.size }));
}

/**
 * Leaves `where`, as `askWhere` gave it, as it was before it was picked,
 * when nothing will be written there: the save window makes the file it
 * picks, empty, which goes where the handle can remove it. A folder gains
 * a file only as `putFiles` writes it, so it needs nothing.
 */
export async function giveUp(where) {
  await where?.file?.remove?.().catch(() => {});
}

/** Writes `blob` to the file `handle` names, in one go. */
async function write(handle, blob) {
  const out = await handle.createWritable();
  await out.write(blob);
  await out.close();
}

/** Has the browser download `f`, {name, blob}, as any file does. */
function download(f) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(f.blob);
  a.download = f.name;
  a.click();
  clock.after(0, () => URL.revokeObjectURL(a.href));
}

// Saving an export of the hub's raw history to a file. The hub writes the
// file and sends it as it goes; the page never holds it whole. Where the
// browser has a save dialog (showSaveFilePicker), the file is named and
// placed there and the response flows straight into it, the page free the
// while. Elsewhere it downloads as any file does.

import * as clock from './clock.js';

/** What each export format is, for the save dialog. */
export const FORMATS = {
  csv: { ext: 'csv', mime: 'text/csv', description: 'CSV table' },
  h5: { ext: 'h5', mime: 'application/x-hdf5', description: 'HDF5 file' },
};

/** A file name for an export made at `date`: twinleaf-20261108-140501.csv. */
export function exportName(format, date = new Date(clock.wall())) {
  const two = (n) => String(n).padStart(2, '0');
  const stamp = `${date.getFullYear()}${two(date.getMonth() + 1)}${two(date.getDate())}-${two(date.getHours())}${two(date.getMinutes())}${two(date.getSeconds())}`;
  return `twinleaf-${stamp}.${FORMATS[format].ext}`;
}

/** The URL the hub serves an export at: traces `ids` of hub `hub` within [t0, t1]. */
export function exportUrl({ hub, ids, t0, t1 }, format, name) {
  return `/api/export?${new URLSearchParams({ hub, ids: ids.join(','), t0, t1, format, name })}`;
}

/**
 * Saves the export `request` describes as `format`, offering `name`. Says
 * how many bytes went to the file as they do, through `progress`. Resolves
 * to the file's name and size, its size null where the browser downloads
 * it, or to null when the save dialog is cancelled.
 */
export async function saveExport(request, format, name, progress = () => {}) {
  const url = exportUrl(request, format, name);
  const { ext, mime, description } = FORMATS[format];
  if (!('showSaveFilePicker' in window)) {
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.click();
    return { name, bytes: null };
  }
  let handle;
  try {
    handle = await window.showSaveFilePicker({ suggestedName: name, types: [{ description, accept: { [mime]: [`.${ext}`] } }] });
  } catch (error) {
    if (error.name === 'AbortError') return null;
    throw error;
  }
  const response = await fetch(url);
  if (!response.ok) throw new Error((await response.json().catch(() => null))?.error ?? response.statusText);
  let bytes = 0;
  const counted = new TransformStream({
    transform(chunk, out) {
      bytes += chunk.length;
      progress(bytes);
      out.enqueue(chunk);
    },
  });
  await response.body.pipeThrough(counted).pipeTo(await handle.createWritable());
  return { name: handle.name, bytes };
}

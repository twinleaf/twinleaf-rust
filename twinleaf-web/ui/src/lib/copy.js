// Copying plots' data to the clipboard: every raw sample of each plot's
// visible window, never the decimated points it draws, read from the hub's
// history. One block per plot, a title row then a header row then the rows,
// a blank line between blocks, as tab-separated text and as an HTML table,
// so a spreadsheet and a document each paste it as a table. Signals of one
// plot that sample at different times share the time column, each row
// holding the values sampled at that time.

/** The most rows a copy holds; past it, a copy asks to export instead, as pastes this long bog down a spreadsheet. */
export const MAX_COPY_ROWS = 100_000;

/**
 * A sample's value as text: nine significant digits, which give back a
 * 32-bit float exactly, without the digits widening it to 64 bits invents.
 */
const value = (v) => +v.toPrecision(9);

/**
 * One plot's table: a header of the time and each signal with its units,
 * and a row per distinct time, its value under each signal sampled then and
 * '' under the rest. `signals` are [{key, units, x, y}], x ascending.
 */
export function blockTable(signals, timeName = 'time (s)') {
  const times = [...new Set(signals.flatMap((s) => s.x))].sort((a, b) => a - b);
  const index = new Map(times.map((t, i) => [t, i]));
  const rows = times.map((t) => [t, ...signals.map(() => '')]);
  signals.forEach((s, j) => s.x.forEach((t, k) => (rows[index.get(t)][j + 1] = value(s.y[k]))));
  const header = [timeName, ...signals.map((s) => (s.units ? `${s.key} (${s.units})` : s.key))];
  return { header, rows };
}

/** The blocks [{title, header, rows}] as tab-separated text. */
export function toTsv(blocks) {
  return blocks.map((b) => [b.title, b.header.join('\t'), ...b.rows.map((r) => r.join('\t'))].join('\n')).join('\n\n') + '\n';
}

const escape = (v) => String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/** The blocks as HTML tables, each under its title. */
export function toHtml(blocks) {
  const table = (b) =>
    `<p><b>${escape(b.title)}</b></p><table><thead><tr>${b.header.map((h) => `<th>${escape(h)}</th>`).join('')}</tr></thead><tbody>${b.rows
      .map((r) => `<tr>${r.map((v) => `<td>${escape(v)}</td>`).join('')}</tr>`)
      .join('')}</tbody></table>`;
  return `<meta charset="utf-8">${blocks.map(table).join('<br>')}`;
}

/** Puts the blocks on the clipboard, as tab-separated text and as HTML tables. */
export async function writeTables(blocks) {
  await navigator.clipboard.write([
    new ClipboardItem({
      'text/plain': new Blob([toTsv(blocks)], { type: 'text/plain' }),
      'text/html': new Blob([toHtml(blocks)], { type: 'text/html' }),
    }),
  ]);
}

/**
 * How many seconds at the start of [t0, t1] the history no longer holds of
 * a signal whose oldest held row is at `oldest` and which samples at `rate`,
 * or 0 when it holds them all.
 */
export function lostSeconds(t0, oldest, rate) {
  if (oldest === null || oldest === undefined) return 0;
  const gap = oldest - t0;
  return gap > 1.5 / rate ? gap : 0;
}

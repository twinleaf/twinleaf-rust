// How the page writes sizes and spans of time for people to read.

/** `n` bytes, as "740 kB", "12.4 MB" or "1.50 GB". */
export const bytesText = (n) => (n >= 1e9 ? `${(n / 1e9).toFixed(2)} GB` : n >= 1e6 ? `${(n / 1e6).toFixed(1)} MB` : `${Math.round(n / 1e3)} kB`);

/** `n` bytes estimated, to two figures so it reads as one, as "~130 MB". */
export function bytesEstimate(n) {
  if (!(n > 0)) return '~0 kB';
  const unit = n >= 1e9 ? [1e9, 'GB'] : n >= 1e6 ? [1e6, 'MB'] : [1e3, 'kB'];
  return `~${+(n / unit[0]).toPrecision(2)} ${unit[1]}`;
}

/** `s` seconds, as "45 s", "12 min", "3.5 h" or "2 days". */
export function spanText(s) {
  if (!isFinite(s)) return 'forever';
  if (s < 120) return `${Math.round(s)} s`;
  if (s < 2 * 3600) return `${Math.round(s / 60)} min`;
  if (s < 2 * 86400) return `${+(s / 3600).toFixed(1)} h`;
  return `${Math.round(s / 86400)} days`;
}

// How the page writes sizes, spans of time and measured values for people to read.

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

// A value is written in one of four notations, each to a number of
// significant figures (`numberText`). Each place on the page that writes a
// value has the form it writes it in on Auto, {digits, notation}; an axis's
// settings (`axisFormat`) put their digits and notation over it (`formOf`).

const PREFIXES = { '-15': 'f', '-12': 'p', '-9': 'n', '-6': 'µ', '-3': 'm', 0: '', 3: 'k', 6: 'M', 9: 'G' };

/** The significant figures an axis's numbers may be set to; Auto is null. */
export const DIGITS = [2, 3, 4, 5];
/** How an axis's numbers may be written: as the page writes each on Auto, plain, or with an exponent. */
export const NOTATIONS = ['auto', 'plain', 'exp'];
/** What an axis's label may name: nothing, its units, or its signals with their units. */
export const LABELS = ['off', 'units', 'name'];

/**
 * A plot's settings `s` for one axis, as {label, digits, notation}, with
 * every setting absent or not one it takes at Auto: label 'off', digits
 * null, notation 'auto'.
 */
export function axisFormat(s) {
  return {
    label: LABELS.includes(s?.label) ? s.label : 'off',
    digits: DIGITS.includes(s?.digits) ? s.digits : null,
    notation: NOTATIONS.includes(s?.notation) ? s.notation : 'auto',
  };
}

/** Form `auto`, {digits, notation}, with axis format `f` over it: its digits where set, and its notation unless Auto. */
export const formOf = (auto, f) => ({ digits: f?.digits ?? auto.digits, notation: f && f.notation !== 'auto' ? f.notation : auto.notation });

/** The power of ten of the first figure of `m` > 0 once it is rounded to `d` figures, as 9.996 to 3 is 10.0. */
const placeOf = (m, d) => Math.floor(Math.log10(Number(m.toPrecision(d))));

/**
 * `v` in `form`, {digits, notation}, to `digits` significant figures:
 * - 'si' with an SI prefix on `unit`, as 197.8 mV, and 0 as 0 V; with no
 *   unit, or in arb, as 'fig';
 * - 'fig' as toPrecision writes it: plain, or with an exponent where plain
 *   would need more figures before the point than it has, or more than six
 *   zeros after it;
 * - 'plain' with no exponent, as 0.0001978: to the figures of `of` in
 *   place of its own where given, so the numbers of one axis share their
 *   decimals;
 * - 'exp' with an exponent, as 1.978e-4.
 * `unit` follows after a space, but where 'si' joins it to its prefix, and
 * not at all in arb. A value that is not finite is a dash.
 */
export function numberText(v, { digits, notation }, unit = '', of = v) {
  if (!isFinite(v)) return '—';
  const shown = unit && unit !== 'arb';
  if (notation === 'si' && shown) {
    if (v === 0) return `0 ${unit}`;
    const e = Math.max(-15, Math.min(9, Math.floor(Math.log10(Math.abs(v)) / 3) * 3));
    return `${prefixed(v / 10 ** e, digits)} ${PREFIXES[e]}${unit}`;
  }
  const text = notation === 'exp' ? v.toExponential(digits - 1) : notation === 'plain' ? plain(v, digits, of) : v.toPrecision(digits);
  return shown ? `${text} ${unit}` : text;
}

/**
 * The figures of `m`, a value over its SI prefix, to `digits`: as
 * toPrecision writes them, but from 1 to 1000, a prefix's range, with no
 * exponent, so fewer figures than its whole part has round it, as 570.
 */
function prefixed(m, digits) {
  const text = m.toPrecision(digits);
  return text.includes('e') && Math.abs(m) >= 1 && Math.abs(m) < 1000 ? plain(m, digits, m) : text;
}

/** `v` to `digits` significant figures of `of`, with no exponent: to the decimals of its last figure, or rounded to that figure where it is left of the point. */
function plain(v, digits, of) {
  const m = Math.abs(of) || Math.abs(v);
  const last = m ? placeOf(m, digits) - digits + 1 : 1 - digits;
  return last <= 0 ? v.toFixed(Math.min(100, -last)) : (Math.round(v / 10 ** last) * 10 ** last).toFixed(0);
}

/** `v` with four significant digits and an SI prefix on `unit`, as 197.8 mV; without a physical unit, or in arbitrary units, with none. */
export const si = (v, unit) => numberText(v, { digits: 4, notation: 'si' }, unit);

/**
 * The range lo … hi in `form`, as `numberText` writes each end, `unit`
 * once after both: in 'si' both ends with the prefix of the larger, and in
 * 'plain' with its decimals.
 */
export function rangeText(lo, hi, form, unit) {
  if (!isFinite(lo) || !isFinite(hi)) return '—';
  const big = Math.max(Math.abs(lo), Math.abs(hi)), shown = unit && unit !== 'arb';
  if (form.notation === 'si' && shown && big !== 0) {
    const e = Math.max(-15, Math.min(9, Math.floor(Math.log10(big) / 3) * 3));
    return `${prefixed(lo / 10 ** e, form.digits)} … ${prefixed(hi / 10 ** e, form.digits)} ${PREFIXES[e]}${unit}`;
  }
  const bare = form.notation === 'si' ? { ...form, notation: 'fig' } : form;
  return `${numberText(lo, bare, '', big)} … ${numberText(hi, bare, '', big)}${shown ? ` ${unit}` : ''}`;
}

/** The range lo … hi in `unit`, both ends with four significant digits and the prefix of the larger. */
export const siRange = (lo, hi, unit) => rangeText(lo, hi, { digits: 4, notation: 'si' }, unit);

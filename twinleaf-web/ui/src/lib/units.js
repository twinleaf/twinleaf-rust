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
// settings (`axisFormat`) put their digits, notation and unit over it
// (`formOf`).
//
// A unit is known when it is one of BASE_UNITS with one SI prefix or none,
// as nT, or such a unit over another, as nT/√Hz, the prefix on the first
// (`unitOf`). A value in a known unit takes the prefix its size gives it,
// or the one an axis fixes; a value in any other unit is written in it as
// reported, with no prefix and never rescaled.

/** Each SI prefix the page writes, by its power of ten. */
const PREFIXES = { '-15': 'f', '-12': 'p', '-9': 'n', '-6': 'µ', '-3': 'm', 0: '', 3: 'k', 6: 'M', 9: 'G' };
/** The power of ten of each SI prefix a reported unit may carry: those the page writes but G, which a unit such as Gs, the gauss, starts with, and u and the Greek mu for micro. */
const READ_PREFIXES = { ...Object.fromEntries(Object.entries(PREFIXES).filter(([, p]) => p && p !== 'G').map(([e, p]) => [p, Number(e)])), u: -6, μ: -6 };
/**
 * The units the page knows without a prefix, as devices report them:
 * tesla, volt, ampere, hertz, second, kelvin, ohm and watt.
 */
export const BASE_UNITS = ['T', 'V', 'A', 'Hz', 's', 'K', 'Ω', 'W'];
/** The base units a fixed unit may take the prefixes k and M on, as well as those from f to none. */
const LARGE = ['Hz', 'Ω'];

/**
 * Reported unit `unit` read as {e, base, per}: the power of ten of its
 * prefix, its base unit, and what follows the base, from a slash on, as
 * "/√Hz"; or null for a unit the page does not know.
 */
export function unitOf(unit) {
  if (!unit) return null;
  const slash = unit.indexOf('/'), lead = slash < 0 ? unit : unit.slice(0, slash), per = slash < 0 ? '' : unit.slice(slash);
  if (BASE_UNITS.includes(lead)) return { e: 0, base: lead, per };
  const e = READ_PREFIXES[lead[0]];
  return e !== undefined && BASE_UNITS.includes(lead.slice(1)) ? { e, base: lead.slice(1), per } : null;
}

/** `v` times ten to the `k`, with the power of ten exact: 0.005 times 10 to the 3 is 5. */
const ten = (v, k) => (k >= 0 ? v * 10 ** k : v / 10 ** -k);

/**
 * The units an axis in reported unit `unit` may fix its numbers in, as
 * {e, text}, the power of ten of the prefix and the unit written: its base
 * unit with each prefix from f to none, and k and M on Hz and Ω, as fT/√Hz
 * to T/√Hz. A unit the page does not know has none.
 */
export function unitChoices(unit) {
  const u = unitOf(unit);
  if (!u) return [];
  return [-15, -12, -9, -6, -3, 0, ...(LARGE.includes(u.base) ? [3, 6] : [])].map((e) => ({ e, text: `${PREFIXES[e]}${u.base}${u.per}` }));
}

/**
 * The unit axis format `f` fixes the numbers of an axis in reported unit
 * `unit` in, as {shift, text}: the power of ten a value in `unit` is
 * multiplied by to be in it, and the unit written; or undefined with none,
 * as on Auto and in a unit the page does not know.
 */
export function fixedUnit(f, unit) {
  if (f?.unit === null || f?.unit === undefined) return undefined;
  const choice = unitChoices(unit).find((c) => c.e === f.unit);
  return choice && { shift: unitOf(unit).e - choice.e, text: choice.text };
}

/** Value `v` of a reported unit in the unit `fixed` (`fixedUnit`) writes it in; with none, `v`. */
export const inUnit = (v, fixed) => (fixed ? ten(v, fixed.shift) : v);
/** Value `v` in the unit `fixed` writes, in the reported unit; with none, `v`. */
export const fromUnit = (v, fixed) => (fixed ? ten(v, -fixed.shift) : v);

/** The significant figures an axis's numbers may be set to; Auto is null. */
export const DIGITS = [2, 3, 4, 5];
/** How an axis's numbers may be written: as the page writes each on Auto, plain, or with an exponent. */
export const NOTATIONS = ['auto', 'plain', 'exp'];
/** What an axis's label may name: nothing, its units, or its signals with their units. */
export const LABELS = ['off', 'units', 'name'];
/** The prefixes an axis's unit may be fixed at, by their power of ten (`unitChoices`); Auto is null. */
export const UNIT_PREFIXES = [-15, -12, -9, -6, -3, 0, 3, 6];

/**
 * A plot's settings `s` for one axis, as {label, digits, notation, unit},
 * with every setting absent or not one it takes at Auto: label 'off',
 * digits null, notation 'auto', unit null. A unit is the power of ten of
 * the prefix it fixes, which holds on an axis whose units offer it
 * (`fixedUnit`).
 */
export function axisFormat(s) {
  return {
    label: LABELS.includes(s?.label) ? s.label : 'off',
    digits: DIGITS.includes(s?.digits) ? s.digits : null,
    notation: NOTATIONS.includes(s?.notation) ? s.notation : 'auto',
    unit: UNIT_PREFIXES.includes(s?.unit) ? s.unit : null,
  };
}

/**
 * Form `auto`, {digits, notation}, with axis format `f` of an axis in
 * reported unit `unit` over it: its digits where set, its notation unless
 * Auto, and as `fixed` the unit it fixes (`fixedUnit`), where it fixes one.
 */
export function formOf(auto, f, unit = '') {
  const form = { digits: f?.digits ?? auto.digits, notation: f && f.notation !== 'auto' ? f.notation : auto.notation };
  const fixed = fixedUnit(f, unit);
  return fixed ? { ...form, fixed } : form;
}

/** The power of ten of the first figure of `m` > 0 once it is rounded to `d` figures, as 9.996 to 3 is 10.0. */
const placeOf = (m, d) => Math.floor(Math.log10(Number(m.toPrecision(d))));
/** The power of ten of the SI prefix a value of size `m` > 0 in a base unit takes. */
const prefixOf = (m) => Math.max(-15, Math.min(9, Math.floor(Math.log10(m) / 3) * 3));

/**
 * `v` of reported unit `unit` in `form`, {digits, notation, fixed}, to
 * `digits` significant figures:
 * - 'si' in a known unit (`unitOf`) with the SI prefix its size gives it
 *   on the base unit, as 0.005 nT is 5.000 pT, and 0 in `unit`; in any
 *   other unit, or with none, as 'fig';
 * - 'fig' as toPrecision writes it: plain, or with an exponent where plain
 *   would need more figures before the point than it has, or more than six
 *   zeros after it;
 * - 'plain' with no exponent, as 0.0001978: to the figures of `of` in
 *   place of its own where given, so the numbers of one axis share their
 *   decimals;
 * - 'exp' with an exponent, as 1.978e-4.
 * With `fixed` (`fixedUnit`), `v` and `of` are first taken to the unit it
 * fixes, which 'si' writes with no prefix of its own. `unit`, or the unit
 * fixed, follows after a space, but where 'si' joins it to its prefix, and
 * not at all in arb or where `unit` is empty. A value that is not finite
 * is a dash.
 */
export function numberText(v, { digits, notation, fixed }, unit = '', of = v) {
  if (!isFinite(v)) return '—';
  const shown = unit && unit !== 'arb';
  if (fixed) [v, of, unit] = [inUnit(v, fixed), inUnit(of, fixed), shown ? fixed.text : ''];
  const u = !fixed && unitOf(unit);
  if (notation === 'si' && shown && (fixed || u)) {
    if (v === 0) return `0 ${unit}`;
    if (fixed) return `${prefixed(v, digits)} ${unit}`;
    const base = ten(v, u.e), e = prefixOf(Math.abs(base));
    return `${prefixed(base / 10 ** e, digits)} ${PREFIXES[e]}${u.base}${u.per}`;
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

/** `v` with four significant digits and an SI prefix on `unit` where the page knows it, as 197.8 mV; in arbitrary units, or none, with none. */
export const si = (v, unit) => numberText(v, { digits: 4, notation: 'si' }, unit);

/**
 * `v` of reported unit `unit` as a tick of an axis reads on Auto: as few
 * digits as it needs, four at most, with the SI prefix its size gives it
 * where the page knows the unit, as 250 µs and 1.5 ms; in any other unit
 * as reported, and in none bare.
 */
export function tickText(v, unit) {
  const u = unitOf(unit), base = u ? ten(v, u.e) : v;
  if (!u || base === 0) return `${+v.toPrecision(4)}${unit && unit !== 'arb' ? ` ${unit}` : ''}`;
  const e = prefixOf(Math.abs(base));
  return `${+(base / 10 ** e).toPrecision(4)} ${PREFIXES[e]}${u.base}${u.per}`;
}

/**
 * The range lo … hi of reported unit `unit` in `form`, as `numberText`
 * writes each end, the unit once after both: in 'si' in a known unit both
 * ends with the prefix of the larger, and in 'plain' with its decimals.
 */
export function rangeText(lo, hi, form, unit) {
  if (!isFinite(lo) || !isFinite(hi)) return '—';
  const shown = unit && unit !== 'arb', fixed = form.fixed;
  if (fixed) [lo, hi, unit] = [inUnit(lo, fixed), inUnit(hi, fixed), shown ? fixed.text : ''];
  const big = Math.max(Math.abs(lo), Math.abs(hi)), u = !fixed && unitOf(unit);
  if (form.notation === 'si' && shown && big !== 0 && (fixed || u)) {
    if (fixed) return `${prefixed(lo, form.digits)} … ${prefixed(hi, form.digits)} ${unit}`;
    const e = prefixOf(ten(big, u.e));
    return `${prefixed(ten(lo, u.e) / 10 ** e, form.digits)} … ${prefixed(ten(hi, u.e) / 10 ** e, form.digits)} ${PREFIXES[e]}${u.base}${u.per}`;
  }
  const bare = { digits: form.digits, notation: form.notation === 'si' ? 'fig' : form.notation };
  return `${numberText(lo, bare, '', big)} … ${numberText(hi, bare, '', big)}${shown ? ` ${unit}` : ''}`;
}

/** The range lo … hi in `unit`, both ends with four significant digits and the prefix of the larger where the page knows `unit`. */
export const siRange = (lo, hi, unit) => rangeText(lo, hi, { digits: 4, notation: 'si' }, unit);

// Each signal's colour: one of the theme's trace colours (--trace-1 to
// --trace-8 in app.css), by index. A signal keeps the colour it was first
// given. A new one takes the colour the fewest of the hub's signals have,
// the first such in order, so the signals on the page all differ while there
// are colours enough. A colour picked by hand, one of them or any other as
// '#rrggbb', stands in for the given one. Both are kept in localStorage by
// signal key, so a signal keeps its colour from visit to visit.

/** How many colours there are, and their names, in the theme's order. */
export const NAMES = ['Cyan', 'Magenta', 'Lime', 'Amber', 'Violet', 'Rose', 'Teal', 'Orange'];
const STORE = 'twinleaf-colors';

export class Colors {
  /** Signal key to the colour index it was given. */
  given = $state({});
  /** Signal key to the colour picked for it by hand: an index, or any colour as '#rrggbb'. */
  picked = $state({});
  /** Signal key to the colour last picked for it, which unticking Automatic goes back to. */
  last = $state({});
  /** The look the colours are drawn in: true dark, false light, null the page's (plot.js `theme`). */
  dark = $state(null);

  /** `storage` is where the colours are kept, as localStorage; none keeps them for this page alone. */
  constructor(storage) {
    this.storage = storage;
    try {
      const kept = JSON.parse(storage?.getItem(STORE) ?? '{}');
      this.given = kept.given ?? {};
      this.picked = kept.picked ?? {};
      this.last = kept.last ?? {};
    } catch {
      // Unreadable colours are given anew.
    }
  }

  /** A Colors with the colours as they are here now, in the same look, kept nowhere: a colour picked on it, or a look given it, changes it alone. */
  copy() {
    const c = new Colors(null);
    Object.assign(c, { given: { ...this.given }, picked: { ...this.picked }, last: { ...this.last }, dark: this.dark });
    return c;
  }

  /** The colour of signal `key`: an index, or a picked '#rrggbb'. One never given reads as the first. */
  of(key) {
    return this.picked[key] ?? this.given[key] ?? 0;
  }

  /** Gives each of the hub's signals `keys` without a colour the one the fewest of them have. */
  give(keys) {
    const fresh = keys.filter((k) => this.given[k] === undefined);
    if (!fresh.length) return;
    const uses = NAMES.map(() => 0);
    for (const k of keys) {
      const c = this.given[k] !== undefined || this.picked[k] !== undefined ? this.of(k) : null;
      if (typeof c === 'number') uses[c]++;
    }
    const given = { ...this.given };
    for (const k of fresh) {
      const i = uses.indexOf(Math.min(...uses));
      given[k] = i;
      uses[i]++;
    }
    this.given = given;
    this.save();
  }

  /** Picks colour `color` for signal `key`, an index or '#rrggbb', or with null, goes back to the one it was given. */
  pick(key, color) {
    const picked = { ...this.picked };
    if (color === null) delete picked[key];
    else {
      picked[key] = color;
      this.last = { ...this.last, [key]: color };
    }
    this.picked = picked;
    this.save();
  }

  save() {
    try {
      this.storage?.setItem(STORE, JSON.stringify({ given: this.given, picked: this.picked, last: this.last }));
    } catch {
      // Storage full or blocked: the colours last for this page.
    }
  }
}

export const colors = new Colors(typeof localStorage === 'undefined' ? null : localStorage);

/** Colour `hex` ('#rrggbb') as {h, s, v}: hue in degrees, saturation and value 0 to 1. */
export function hexToHsv(hex) {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
  const v = Math.max(r, g, b), d = v - Math.min(r, g, b);
  const s = v === 0 ? 0 : d / v;
  let h = 0;
  if (d) h = v === r ? ((g - b) / d) % 6 : v === g ? (b - r) / d + 2 : (r - g) / d + 4;
  return { h: (h * 60 + 360) % 360, s, v };
}

/** Colour {h, s, v} as '#rrggbb'. */
export function hsvToHex({ h, s, v }) {
  const f = (n) => {
    const k = (n + h / 60) % 6;
    return Math.round(255 * (v - v * s * Math.max(0, Math.min(k, 4 - k, 1))));
  };
  return '#' + [f(5), f(3), f(1)].map((c) => c.toString(16).padStart(2, '0')).join('');
}

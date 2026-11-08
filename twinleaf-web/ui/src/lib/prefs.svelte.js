// The page's own preferences, kept in localStorage under KEY so they hold
// from visit to visit: the theme, how wide the plots draw their traces, and
// whether key caps and shortcut text show. System follows the system's
// light or dark, as it changes.
//
// The theme is the `dark` class on the page's root, which app.css's tokens
// follow. It is set before `dark` changes, so whatever reads the tokens when
// `dark` changes, as plot.js's theme() does, reads the new ones.

const KEY = 'tl-prefs';

/** The themes, as [value, label]. */
export const THEMES = [
  ['system', 'System'],
  ['light', 'Light'],
  ['dark', 'Dark'],
];

/** The widths a trace is drawn at, in CSS px. */
export const TRACE_WIDTHS = [1, 1.3, 2, 3];

export class Prefs {
  /** Whether key caps and shortcut text show in menus, tooltips and the page. */
  hints = $state(true);
  /** 'system', 'light' or 'dark'. */
  theme = $state('system');
  /** The width the plots draw traces at, one of TRACE_WIDTHS. */
  traceWidth = $state(1.3);
  /** Whether the page shows dark: the theme's, or with System, the system's. */
  dark = $state(true);

  /** `storage` is where they are kept, as localStorage; `media` the system's dark MediaQueryList. None of either keeps them for this page, dark. */
  constructor(storage, media) {
    this.storage = storage;
    this.media = media;
    try {
      const kept = JSON.parse(storage?.getItem(KEY) ?? '{}');
      if (typeof kept.hints === 'boolean') this.hints = kept.hints;
      if (THEMES.some(([v]) => v === kept.theme)) this.theme = kept.theme;
      if (TRACE_WIDTHS.includes(kept.traceWidth)) this.traceWidth = kept.traceWidth;
    } catch {
      // Unreadable preferences start from the defaults.
    }
    media?.addEventListener('change', () => this.apply());
    this.apply();
  }

  setHints(on) {
    this.hints = on;
    this.save();
  }

  setTraceWidth(px) {
    this.traceWidth = px;
    this.save();
  }

  setTheme(theme) {
    this.theme = theme;
    this.save();
    this.apply();
  }

  /** Shows the theme: the root's class first, then `dark`. */
  apply() {
    const dark = this.theme === 'dark' || (this.theme === 'system' && (this.media?.matches ?? true));
    if (typeof document !== 'undefined') document.documentElement.classList.toggle('dark', dark);
    this.dark = dark;
  }

  save() {
    try {
      this.storage?.setItem(KEY, JSON.stringify({ hints: this.hints, theme: this.theme, traceWidth: this.traceWidth }));
    } catch {
      // Storage full or blocked: the preferences last for this page.
    }
  }
}

export const prefs = new Prefs(
  typeof localStorage === 'undefined' ? null : localStorage,
  typeof matchMedia === 'undefined' ? null : matchMedia('(prefers-color-scheme: dark)'),
);

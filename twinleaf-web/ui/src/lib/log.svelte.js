// The hub's log on the page: its lines as the log drawer lists them, what
// of them is unseen, and the drawer's own state, open or closed, its
// height and its filters.
//
// The hub sends each line as a `log` message (protocol.js), the newest of
// them again in every snapshot. The lines are a hub's: a snapshot of a hub
// new to the page drops what the page held, and a line the page holds
// already, by its number, is not taken twice. The Hub (hub.svelte.js) hands
// every snapshot and every `log` here, and the lines outlive its sessions.
//
// A row is one line, or several in a row that say the same: lines of one
// level from one source with the same words, or losses of one cause of one
// stream of one device, their samples summed. A link's drop is always a
// row of its own, and the line of its return gives that row how long the
// link was down, which the drawer draws as the outage's divider.
//
// A warning or an error that arrives while the drawer is closed is unseen
// until it opens. The lines a hub held before the page first heard it, its
// first snapshot's, were never unseen: they are what led up to now.
//
// Whether the drawer is open and how tall it is are kept under KEY, as the
// docks' arrangement is under its own.

/** The most rows the page keeps: the oldest go first. */
export const KEPT = 1000;
/** The levels, gravest first, as the drawer's toggles list them. */
export const LEVELS = ['error', 'warn', 'info', 'debug'];
/** The localStorage key of the drawer's state. */
export const KEY = 'tl-log-v1';
/** The drawer's height, a share of the page between the top bar and the status bar: where it starts, and how far a drag takes it. */
export const SIZE = { default: 30, min: 15, max: 70 };

/** What tells two lines that say the same apart from any other, or null for a line that is always a row of its own. */
function sameAs(line) {
  if (line.loss?.cause === 'link') return null;
  if (line.loss) return `loss ${line.route} ${line.loss.cause} ${line.loss.stream}`;
  return `${line.level} ${line.source} ${line.route} ${line.text}`;
}

/**
 * Whether `row` shows under `filter`, {levels, lossOnly, source, query}:
 * its level is on, it is of a loss when only those show, it is of the
 * source chosen ('' for every source, 'proxy' for the proxy's own lines,
 * or a device's route for the lines of that device, whoever said them),
 * and `text(row)`, what the drawer writes of it, holds every word typed.
 */
export function matches(row, { levels, lossOnly, source, query }, text) {
  if (!levels[row.level] || (lossOnly && !row.loss)) return false;
  if (source && (source === 'proxy' ? row.source !== 'proxy' : row.route !== source)) return false;
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (!words.length) return true;
  const said = text(row).toLowerCase();
  return words.every((w) => said.includes(w));
}

export class Log {
  /**
   * The rows kept, oldest first, each {id, n, count, wall, time, level,
   * source, route, text, loss, back}: `id` names the row, `n` is its
   * newest line's number and `count` how many lines it stands for; `wall`,
   * `time`, `level`, `source`, `route` and `text` are its newest line's,
   * `loss` what it says was lost, its samples summed, or undefined; and
   * `back`, on the row of a link's drop, the seconds the link was down
   * once it is back.
   */
  rows = $state.raw([]);
  /** How many warnings and errors arrived unseen, by level. */
  unseen = $state.raw({ error: 0, warn: 0 });
  /** Whether the drawer is open. */
  open = $state(false);
  /** The levels the drawer shows. */
  levels = $state({ error: true, warn: true, info: true, debug: false });
  /** Whether the drawer shows only the lines of a loss. */
  lossOnly = $state(false);
  /** The source whose lines the drawer shows: '' for all, 'proxy', or a device's route. */
  source = $state('');
  /** What the drawer's filter holds. */
  query = $state('');
  /** The drawer's height, as a share of SIZE's. */
  size = SIZE.default;

  /** The id of the hub the lines are of. */
  #hub = null;
  /** Whether the hub is new to the page since its last snapshot, so that snapshot's lines were never unseen. */
  #fresh = false;
  /** The number of the newest line taken of the hub. */
  #last = 0;

  /** `storage` is localStorage, or anything with its getItem and setItem. */
  constructor(storage = null) {
    this.storage = storage;
    try {
      const saved = JSON.parse(storage?.getItem(KEY) ?? 'null');
      this.open = saved?.open === true;
      if (saved?.size >= SIZE.min && saved?.size <= SIZE.max) this.size = saved.size;
    } catch {}
  }

  #save() {
    try {
      this.storage?.setItem(KEY, JSON.stringify({ open: this.open, size: this.size }));
    } catch {}
  }

  /** How many lines the rows kept stand for, by level, and `loss` those of a loss. */
  counts = $derived.by(() => {
    const out = { error: 0, warn: 0, info: 0, debug: 0, loss: 0 };
    for (const row of this.rows) {
      out[row.level] += row.count;
      if (row.loss) out.loss += row.count;
    }
    return out;
  });

  /** Notes a snapshot of hub `id`: the lines of another hub go, the unseen among them too. */
  snapshot(id) {
    this.#fresh = id !== this.#hub;
    if (!this.#fresh) return;
    this.#hub = id;
    this.#last = 0;
    this.rows = [];
    this.unseen = { error: 0, warn: 0 };
  }

  /** Takes `log` message `msg`, which a snapshot holds when `inSnapshot`. */
  take(msg, inSnapshot) {
    if (!(msg.n > this.#last)) return;
    this.#last = msg.n;
    const { n, wall, time = null, level, source, route = null, text, loss } = msg;
    if (level in this.unseen && !this.open && !(inSnapshot && this.#fresh)) this.unseen = { ...this.unseen, [level]: this.unseen[level] + 1 };
    const rows = this.rows.slice(), last = rows.at(-1), key = sameAs(msg);
    if (loss?.cause === 'link' && loss.seconds != null) {
      const down = rows.findLastIndex((r) => r.route === route && r.loss?.cause === 'link' && r.loss.seconds == null && r.back === undefined);
      if (down >= 0) rows[down] = { ...rows[down], back: loss.seconds };
    }
    if (last && key !== null && last.key === key) {
      const summed = loss ? { ...loss, samples: last.loss.samples + loss.samples } : undefined;
      rows[rows.length - 1] = { ...last, n, count: last.count + 1, wall, time, text, loss: summed };
    } else rows.push({ id: n, key, n, count: 1, wall, time, level, source, route, text, loss });
    this.rows = rows.length > KEPT ? rows.slice(rows.length - KEPT) : rows;
  }

  /** Opens the drawer, on the lines of a loss alone with `lossOnly`, on every line without: what was unseen is seen. */
  show(lossOnly = false) {
    this.lossOnly = lossOnly;
    this.open = true;
    this.unseen = { error: 0, warn: 0 };
    this.#save();
  }

  /** Closes the drawer. */
  hide() {
    this.open = false;
    this.#save();
  }

  /** Shows the lines of `level`, or hides them while they show. */
  toggleLevel(level) {
    this.levels[level] = !this.levels[level];
  }

  /** Shows only the lines of a loss, with `on`, or every line. */
  onlyLoss(on) {
    this.lossOnly = on;
  }

  /** Shows the lines of `source`: '' for every source, 'proxy', or a device's route. */
  setSource(source) {
    this.source = source;
  }

  /** Shows the lines that hold every word of `query`. */
  setQuery(query) {
    this.query = query;
  }

  /** Keeps `size` as the drawer's height, a share of SIZE's. */
  resize(size) {
    if (!(size >= SIZE.min && size <= SIZE.max) || size === this.size) return;
    this.size = size;
    this.#save();
  }
}

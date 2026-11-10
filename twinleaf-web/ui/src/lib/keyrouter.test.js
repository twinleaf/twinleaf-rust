// The key router's order, on a bare document: whichever claimants would
// take a key, the first of them in ORDER that is offered it gets it, and
// the element with focus hears it only when none before the element did.
import { afterEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { ORDER, PAGE, claim, lastKey } from './keyrouter.js';
import { KEYMAP, actionFor } from './keymap.js';

const AT = ORDER.findIndex((o) => o.id === 'element');
const BEFORE = ORDER.slice(0, AT).map((o) => o.id), AFTER = ORDER.slice(AT + 1).map((o) => o.id);
const KEYS = [
  { key: 'Escape' }, { key: ' ' }, { key: 'Enter' }, { key: 'Enter', shiftKey: true }, { key: 'a' }, { key: 'ArrowLeft' }, { key: 'End' }, { key: 'Delete' },
  { key: '1' }, { key: ']' }, { key: 'F2' }, { key: 'Tab' }, { key: 'k', ctrlKey: true }, { key: 'z', ctrlKey: true }, { key: 'Z', ctrlKey: true, shiftKey: true },
  { key: 'a', metaKey: true }, { key: 'c', ctrlKey: true }, { key: 'a', altKey: true }, { key: 'q' },
];

let undo = [];
afterEach(() => {
  undo.forEach((f) => f());
  undo = [];
  document.body.replaceChildren();
});

/** An element of `tag` on the page that notes each key it hears in `heard`, and takes it when `takes`. */
function element(tag, heard, takes) {
  const el = document.body.appendChild(document.createElement(tag));
  el.tabIndex = 0;
  el.addEventListener('keydown', (e) => {
    heard.push(e.key);
    if (takes) e.preventDefault();
  });
  return el;
}
/** Presses `key` on `el`, and returns the event. */
function press(el, key) {
  const e = new KeyboardEvent('keydown', { ...key, bubbles: true, cancelable: true });
  el.dispatchEvent(e);
  return e;
}

describe('the order', () => {
  it('names each claimant once, each with a place the keymap has', () => {
    expect(new Set(ORDER.map((o) => o.id)).size).toBe(ORDER.length);
    for (const o of ORDER) if (o.place) expect(KEYMAP.some((p) => p.place === o.place), o.id).toBe(true);
  });
  it('has a claimant for every place of the keymap that has keys', () => {
    const places = KEYMAP.filter((p) => p.entries.some((e) => e.keys)).map((p) => p.place);
    expect(places.filter((p) => !ORDER.some((o) => o.place === p))).toEqual([]);
  });
  it('takes no claimant it does not name', () => {
    expect(() => claim('nobody', { on: () => {} })).toThrow();
    expect(() => claim('element', { on: () => {} })).toThrow();
  });
});

describe('a key', () => {
  it('goes to the first claimant in the order that is offered it and takes it', () => {
    fc.assert(
      fc.property(fc.constantFrom(...KEYS), fc.subarray(ORDER.map((o) => o.id).filter((id) => id !== 'element')), fc.boolean(), fc.boolean(), (key, takers, kept, taken) => {
        const heard = [], asked = [];
        // Every claimant is there, and says whether it takes what it is offered.
        for (const o of ORDER) if (o.id !== 'element') undo.push(claim(o.id, { on: () => (asked.push(o.id), takers.includes(o.id)) }));
        // A field keeps what is typed in it; another element keeps nothing.
        const el = element(kept ? 'input' : 'div', heard, taken);
        const e = press(el, key);
        const offered = (o) => !o.place || actionFor(o.place, key) !== null;
        const before = ORDER.slice(0, AT).find((o) => takers.includes(o.id) && offered(o));
        const after = ORDER.slice(AT + 1).find((o) => takers.includes(o.id) && offered(o) && (!kept || o.kept) && (!taken || o.taken));
        const by = before ? before.id : (after?.id ?? null);
        expect(lastKey()).toEqual({ key: key.key, by, page: !before });
        // The element hears the key unless a claimant before it took it.
        expect(heard).toEqual(before ? [] : [key.key]);
        expect(e.defaultPrevented).toBe(by !== null || (!before && taken));
        // No claimant after the one that took it was asked, and each asked once at most.
        expect(new Set(asked).size).toBe(asked.length);
        if (by) expect(asked.at(-1)).toBe(by);
        const order = ORDER.map((o) => o.id);
        expect([...asked].sort((a, b) => order.indexOf(a) - order.indexOf(b))).toEqual(asked);
        undo.forEach((f) => f());
        undo = [];
        el.remove();
      }),
    );
  });

  it('a claimant hands to the page skips the element, and is the page’s as pressed on no element', () => {
    fc.assert(
      fc.property(fc.constantFrom(...BEFORE.filter((id) => ORDER.find((o) => o.id === id).place)), fc.constantFrom(...AFTER), (from, to) => {
        const place = (id) => ORDER.find((o) => o.id === id).place;
        const keys = KEYS.filter((k) => actionFor(place(from), k) && (!place(to) || actionFor(place(to), k)));
        fc.pre(keys.length > 0);
        const heard = [], ats = [];
        undo.push(claim(from, { on: () => PAGE }));
        undo.push(claim(to, { on: (action, e, at) => void ats.push(at) }));
        // A field would keep the key from the page; handed on, it is pressed on no element.
        const el = element('input', heard, false);
        const e = press(el, keys[0]);
        expect(heard).toEqual([]);
        expect(ats).toEqual([null]);
        expect(lastKey()).toEqual({ key: keys[0].key, by: to, page: true });
        expect(e.defaultPrevented).toBe(true);
        undo.forEach((f) => f());
        undo = [];
        el.remove();
      }),
    );
  });

  it('is offered to a claimant only while it claims, and only when its `when` holds', () => {
    const el = element('div', [], false);
    let on = false;
    const unclaim = claim('drag', { when: () => on, on: { cancel: () => {} } });
    press(el, { key: 'Escape' });
    expect(lastKey().by).toBe(null);
    on = true;
    press(el, { key: 'Escape' });
    expect(lastKey().by).toBe('drag');
    unclaim();
    press(el, { key: 'Escape' });
    expect(lastKey().by).toBe(null);
  });
});

describe('Space let go', () => {
  it('presses nothing on an element that does not keep it, and is a field’s own', () => {
    const up = (el) => {
      const e = new KeyboardEvent('keyup', { key: ' ', bubbles: true, cancelable: true });
      el.dispatchEvent(e);
      return e.defaultPrevented;
    };
    expect(up(element('button', [], false))).toBe(true);
    expect(up(element('input', [], false))).toBe(false);
  });
});

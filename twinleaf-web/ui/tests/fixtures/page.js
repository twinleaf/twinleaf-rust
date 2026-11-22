// The messages the page sends, as src/lib/protocol.test.js records them in
// page/, each by the name of its file, as `upgrade-latest`. A test reads one
// and changes the fields it needs.

const files = import.meta.glob('./page/*.json', { eager: true, import: 'default' });
const recorded = Object.fromEntries(Object.entries(files).map(([path, message]) => [path.slice('./page/'.length, -'.json'.length), message]));

/** The recorded message `name`, a copy of it with `fields` in place of its own. */
export const ask = (name, fields = {}) => {
  if (!(name in recorded)) throw new Error(`the page records no ${name}`);
  return { ...structuredClone(recorded[name]), ...fields };
};

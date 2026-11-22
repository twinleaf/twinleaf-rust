// The messages the hub sends, as twinleaf-web/src/recorded.rs records
// them in hub/, each by the name of its file, as `upgrade-done`. A test
// reads one and changes the fields it needs.

const files = import.meta.glob('./hub/*.json', { eager: true, import: 'default' });
const recorded = Object.fromEntries(Object.entries(files).map(([path, message]) => [path.slice('./hub/'.length, -'.json'.length), message]));

/** The recorded message `name`, a copy of it with `fields` in place of its own. */
export const message = (name, fields = {}) => {
  if (!(name in recorded)) throw new Error(`the hub records no ${name}`);
  return { ...structuredClone(recorded[name]), ...fields };
};

/** The recorded message `name` as change `seq`, as the hub numbers a change, with `fields` in place of its own. */
export const change = (name, seq, fields = {}) => ({ seq, ...message(name, fields) });

/** The recorded table's first entry of type `type`, as `u8` or null, a copy with `fields` in place of its own. */
export const entry = (type, fields = {}) => {
  const found = recorded.table.rpcs.find((r) => r.type === type);
  if (!found) throw new Error(`the hub records no entry of type ${type}`);
  return { ...structuredClone(found), ...fields };
};

/** The recorded `trace`, with `fields` in place of its trace's own. */
export const trace = (fields = {}) => message('trace', { trace: { ...recorded.trace.trace, ...fields } });

// What the hub's routes answer, as it records them in hub/http, each by the
// name of its file, as `call-ok`. A success is its body, and a refusal its
// {status, body}.
const answered = import.meta.glob('./hub/http/*.json', { eager: true, import: 'default' });
const replies = Object.fromEntries(Object.entries(answered).map(([path, reply]) => [path.slice('./hub/http/'.length, -'.json'.length), reply]));
/** Each outcome of a call the hub records, of each state once. */
const outcomes = Object.entries(replies).flatMap(([name, reply]) => (name.startsWith('call-') ? reply.results : []));

/** The recorded reply `name`, a copy of it with `fields` in place of its own. */
export const reply = (name, fields = {}) => {
  if (!(name in replies)) throw new Error(`the hub records no reply ${name}`);
  return { ...structuredClone(replies[name]), ...fields };
};

/** The recorded outcome of a call in `state`, as `ok`, with `fields` in place of its own. */
export const outcome = (state, fields = {}) => {
  const recorded = outcomes.find((o) => o.state === state);
  if (!recorded) throw new Error(`the hub records no ${state} outcome`);
  return { ...structuredClone(recorded), ...fields };
};

/** The recorded export, as its text. */
export { default as exported } from './hub/http/export.csv?raw';

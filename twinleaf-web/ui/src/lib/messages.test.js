// The page's tests read the messages the hub sends and those the page sends
// from the files each side records (tests/fixtures/hub and
// tests/fixtures/page), and write none of their own. This reads the tests
// as text and fails on an object written with the `type` of one of them,
// the variants of `Message` and of `PageMessage` in
// twinleaf-web/src/protocol.rs, naming the file and the line. It fails too
// on an object written with every key of a reply the hub records of its
// routes, or of an outcome of a call, unless it is the fields given a
// recorded one, as `reply('rows', {...})`. A reply whose keys a request the
// page makes has too, as the history's, cannot be told apart and is passed
// over. A file under tests/fixtures may.
import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

/** The type of each variant of `Message` and of `PageMessage`, as serde names it. */
function messageTypes() {
  const source = fs.readFileSync(path.resolve('../src/protocol.rs'), 'utf8');
  const bodies = ['Message', 'PageMessage'].map((name) => source.split(`pub(crate) enum ${name} {`)[1].split('\n}')[0]);
  return new Set(bodies.flatMap((body) => [...body.matchAll(/^ {4}([A-Z]\w*)/gm)].map(([, name]) => name.toLowerCase())));
}

/** The keys of each reply the hub records and of each outcome among them, those of two keys or more that no request the page makes has. */
function replyShapes() {
  const read = (dir) => fs.readdirSync(dir).filter((file) => file.endsWith('.json')).map((file) => JSON.parse(fs.readFileSync(path.join(dir, file), 'utf8')));
  const asked = new Set(read('tests/fixtures/page/http').map((request) => Object.keys(request.body ?? {}).sort().join()));
  const shapes = read('tests/fixtures/hub/http').flatMap((reply) => [reply, ...(reply.results ?? [])]).map((reply) => Object.keys(reply).sort());
  return [...new Set(shapes.filter((keys) => keys.length > 1).map((keys) => keys.join()))].filter((keys) => !asked.has(keys)).map((keys) => keys.split(','));
}

/** Each object `text` writes, but the fields given a recorded one: the keys it writes at its own level, and the line it starts on. */
function objects(text) {
  const found = [], open = [];
  let line = 1;
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '\n') line++;
    else if (text[i] === '{') open.push({ at: i, line });
    else if (text[i] === '}' && open.length) {
      const { at, line: from } = open.pop();
      if (/\(\s*['"][\w-]+['"]\s*,\s*$/.test(text.slice(Math.max(0, at - 80), at))) continue;
      let own = text.slice(at + 1, i);
      for (let was = ''; was !== own; ) [was, own] = [own, own.replace(/\([^()]*\)|\[[^[\]]*\]|\{[^{}]*\}/g, '')];
      found.push({ line: from, keys: new Set([...own.matchAll(/(?:^|,)\s*(\w+)\s*(?=[:,]|$)/g)].map(([, key]) => key)) });
    }
  }
  return found;
}

/** The page's test sources under `dir`, by path from the page's folder. */
function sources(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const at = path.posix.join(dir, entry.name);
    if (entry.isDirectory()) return at === 'tests/fixtures' ? [] : sources(at);
    const test = dir.startsWith('tests') ? /\.(js|svelte)$/ : /\.test\.js$/;
    return test.test(entry.name) ? [at] : [];
  });
}

describe('the page’s tests', () => {
  it('write none of the messages between the hub and the page', () => {
    const types = messageTypes();
    expect(types.has('hub') && types.has('retry')).toBe(true);
    const found = [];
    for (const file of [...sources('src'), ...sources('tests')]) {
      const lines = fs.readFileSync(file, 'utf8').split('\n');
      lines.forEach((line, i) => {
        for (const [, type] of line.matchAll(/\btype:\s*['"](\w+)['"]/g)) {
          if (types.has(type)) found.push(`${file}:${i + 1} writes a ${type} message`);
        }
      });
    }
    expect(found).toEqual([]);
  });

  it('write none of the replies of the hub’s routes', () => {
    const shapes = replyShapes();
    expect(shapes.some((keys) => keys.includes('chunks'))).toBe(true);
    const found = [];
    for (const file of [...sources('src'), ...sources('tests')]) {
      for (const { line, keys } of objects(fs.readFileSync(file, 'utf8'))) {
        const shape = shapes.find((reply) => reply.every((key) => keys.has(key)));
        if (shape) found.push(`${file}:${line} writes a reply of ${shape.join(', ')}`);
      }
    }
    expect(found).toEqual([]);
  });
});

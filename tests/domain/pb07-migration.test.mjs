// PB-07 contract: schema version table, forward-only migration v0(empty) -> v1,
// unknown newer versions refused (no silent downgrade), corrupt/non-DB files
// refused with a clear error and nothing overwritten, no coordinate-named fields.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync, writeFileSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createOwnedBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-migration-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

function rawVersion(file) {
  const db = new DatabaseSync(file);
  try {
    const row = db.prepare("SELECT value FROM storage_meta WHERE key = 'schema_version'").get();
    return row ? row.value : null;
  } finally {
    db.close();
  }
}

test('a fresh store initializes at schema version 1 with the bubbles table', (t) => {
  const file = join(tempDir(t), 'fresh.db');
  const {repo} = createOwnedBubbleRepository({file, now: () => 1000});
  const a = repo.create();
  assert.equal(rawVersion(file), '1');
  const db = new DatabaseSync(file);
  try {
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all().map(r => r.name);
    assert.ok(tables.includes('bubbles'));
    assert.ok(tables.includes('storage_meta'));
    assert.deepEqual(repo.get(a.id), a);
  } finally {
    db.close();
  }
});

test('a v0 empty file is upgraded to v1 on open and remains writable', (t) => {
  const file = join(tempDir(t), 'v0.db');
  writeFileSync(file, ''); // v0 = empty file, no schema
  const {repo} = createOwnedBubbleRepository({file, now: () => 1000});
  assert.equal(rawVersion(file), '1', 'v0 file upgraded to v1');
  const a = repo.create();
  repo.close(a.id);
  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 2000});
  assert.deepEqual(reopened.get(a.id), {id: a.id, state: 'closed', version: 2, createdAt: 1000, closedAt: 1000});
});

test('an unknown newer schema version is refused, not silently downgraded, and the file is untouched', (t) => {
  const file = join(tempDir(t), 'future.db');
  const db = new DatabaseSync(file);
  db.exec('CREATE TABLE storage_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)');
  db.prepare("INSERT INTO storage_meta (key, value) VALUES ('schema_version', '2')").run();
  db.close();
  const before = readFileSync(file);
  assert.throws(() => createOwnedBubbleRepository({file, now: () => 1000}),
    {code: 'UNSUPPORTED_STORAGE_VERSION'});
  assert.deepEqual(readFileSync(file), before, 'refused open writes nothing');
  assert.equal(rawVersion(file), '2', 'file still reports its original version after refusal');
});

test('a corrupt non-DB file is refused with a clear error and is not overwritten', (t) => {
  const file = join(tempDir(t), 'corrupt.db');
  const garbage = Buffer.from('PB07-NOT-A-DATABASE-'.repeat(8));
  writeFileSync(file, garbage);
  assert.throws(() => createOwnedBubbleRepository({file, now: () => 1000}),
    (err) => err.code === 'INVALID_STORAGE_FILE' && /not a database/.test(err.message));
  assert.deepEqual(readFileSync(file), garbage, 'corrupt file bytes unchanged');
});

test('storage schema text contains no coordinate-named fields (lat/lng/latitude/longitude)', (t) => {
  const file = join(tempDir(t), 'schema.db');
  createOwnedBubbleRepository({file, now: () => 1000});
  const db = new DatabaseSync(file);
  try {
    const sql = db.prepare("SELECT sql FROM sqlite_master WHERE type IN ('table','index') AND sql IS NOT NULL")
      .all().map(r => r.sql).join('\n');
    assert.doesNotMatch(sql, /latitude|longitude|\blat\b|\blng\b/i,
      'schema must not name exact-coordinate fields');
  } finally {
    db.close();
  }
});

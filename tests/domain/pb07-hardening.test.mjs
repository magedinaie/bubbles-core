// PB-07 correction cycle: foreign-DB refusal, secure purge, 0600 creation, process-kill crash.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync, writeFileSync, readFileSync, statSync, existsSync, chmodSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawn} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import {createOwnedBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

const ADAPTER = join(dirname(fileURLToPath(import.meta.url)), '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs');

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-hardening-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

test('a non-empty foreign SQLite file without storage_meta is refused and left byte-identical', (t) => {
  const file = join(tempDir(t), 'foreign.db');
  const db = new DatabaseSync(file);
  db.exec('CREATE TABLE other_app (x TEXT); INSERT INTO other_app VALUES (\'keep me\')');
  db.close();
  const before = readFileSync(file);
  assert.throws(() => createOwnedBubbleRepository({file, now: () => 1000}),
    {code: 'INVALID_STORAGE_FILE'});
  assert.deepEqual(readFileSync(file), before, 'foreign DB not migrated or modified');
  const check = new DatabaseSync(file);
  try {
    const names = check.prepare("SELECT name FROM sqlite_master ORDER BY name").all().map(r => r.name);
    assert.deepEqual(names, ['other_app']);
  } finally { check.close(); }
});

test('an empty SQLite database (zero schema objects) and an empty file still migrate v0 -> v1', (t) => {
  const dir = tempDir(t);
  const file = join(dir, 'emptydb.db');
  new DatabaseSync(file).close();
  const {repo} = createOwnedBubbleRepository({file, now: () => 1000});
  assert.equal(repo.create().version, 1);
});

test('purge physically removes the purged bubble id from the DB file bytes', (t) => {
  const file = join(tempDir(t), 'purge.db');
  let time = 1000;
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => time, retentionMs: 100});
  const doomed = repo.create();
  const keeper = repo.create();
  repo.close(doomed.id);
  assert.ok(readFileSync(file).includes(Buffer.from(doomed.id)), 'precondition: id is on disk before purge');
  time = 5000;
  assert.equal(internal.purgeClosed(), 1);
  internal.close();
  const bytes = readFileSync(file);
  assert.ok(!bytes.includes(Buffer.from(doomed.id)), 'purged id must not remain in the file');
  assert.ok(bytes.includes(Buffer.from(keeper.id)), 'unpurged id still present');
  for (const ext of ['-journal', '-wal']) {
    if (existsSync(file + ext)) assert.ok(!readFileSync(file + ext).includes(Buffer.from(doomed.id)));
  }
});

test('a newly created DB file has owner-only permissions (0600)', (t) => {
  const file = join(tempDir(t), 'perm.db');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  repo.create();
  internal.close();
  assert.equal(statSync(file).mode & 0o777, 0o600);
});

test('process-kill mid-write: every acknowledged bubble survives, file passes integrity_check', async (t) => {
  const file = join(tempDir(t), 'kill.db');
  const script = `
    import {createOwnedBubbleRepository} from ${JSON.stringify(ADAPTER)};
    const {repo} = createOwnedBubbleRepository({file: ${JSON.stringify(file)}});
    for (;;) { process.stdout.write(repo.create().id + '\\n'); }
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script], {stdio: ['ignore', 'pipe', 'inherit']});
  const acked = [];
  let buf = '';
  await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.stdout.on('data', (chunk) => {
      buf += chunk;
      const lines = buf.split('\n'); buf = lines.pop();
      for (const l of lines) if (/^bub_[a-f0-9]{32}$/.test(l)) acked.push(l);
      if (acked.length >= 200 && !child.killed) child.kill('SIGKILL');
    });
    child.on('close', resolve);
  });
  assert.ok(acked.length >= 200);
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  for (const id of acked) assert.equal(repo.get(id)?.state, 'active', 'acked bubble lost after kill');
  internal.close();
  const db = new DatabaseSync(file);
  try {
    assert.equal(db.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
  } finally { db.close(); }
});

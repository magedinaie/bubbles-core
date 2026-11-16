// PB-07 contract: crash safety. A write interrupted inside a transaction leaves
// the previous consistent state (transaction rollback test): verified in memory,
// on disk after reopen, and by absence of a dangling transaction.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createOwnedBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-crash-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

test('a close interrupted mid-transaction rolls back to the previous consistent state', (t) => {
  const file = join(tempDir(t), 'crash.db');
  let time = 1000;
  let failing = false;
  const clockError = new Error('synthetic crash mid-write');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => {
    if (failing) throw clockError;
    return time;
  }});

  const a = repo.create();
  const b = repo.create();
  time = 1500;
  const closedA = repo.close(a.id);
  const snapshotAfterWrites = readFileSync(file);

  failing = true;
  assert.throws(() => repo.close(b.id), (err) => err === clockError);
  assert.deepEqual(repo.get(b.id), b, 'in-memory record unchanged by interrupted close');
  assert.deepEqual(repo.get(a.id), closedA, 'earlier committed writes untouched');
  assert.deepEqual(readFileSync(file), snapshotAfterWrites, 'no bytes written by interrupted close');

  failing = false;
  const c = repo.create(); // store is still usable: no dangling transaction
  assert.ok(c.id);

  internal.close();
  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 2000});
  assert.deepEqual(reopened.get(b.id), b, 'interrupted close left on-disk state at its previous consistent value');
  assert.deepEqual(reopened.get(a.id), closedA);
});

test('a create interrupted mid-transaction rolls back and leaves no partial row', (t) => {
  const file = join(tempDir(t), 'crash-create.db');
  let failing = false;
  const clockError = new Error('synthetic crash during create');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => {
    if (failing) throw clockError;
    return 1000;
  }});
  const a = repo.create();
  const snapshot = readFileSync(file);
  failing = true;
  assert.throws(() => repo.create(), (err) => err === clockError);
  failing = false;
  const b = repo.create();
  assert.ok(b.id);
  internal.close();
  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 1000});
  assert.equal(reopened.get(a.id).id, a.id);
  assert.equal(reopened.get(b.id).id, b.id);
  // no phantom id from the interrupted create survived anywhere
  assert.equal(reopened.get('bub_' + 'f'.repeat(32)), null);
});

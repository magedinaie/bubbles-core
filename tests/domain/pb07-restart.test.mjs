// PB-07 contract: restart behavior. Close and reopen the store; the same active
// and closed bubbles come back with the same versions.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createOwnedBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-restart-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

test('close and reopen restores active and closed bubbles with identical versions', (t) => {
  const file = join(tempDir(t), 'store.db');
  let time = 1000;
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => time});
  const a = repo.create();
  const b = repo.create();
  time = 1500;
  const closed = repo.close(a.id);
  assert.ok(readFileSync(file).length > 0, 'storage file exists on disk');

  internal.close(); // close the store

  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 2000});
  assert.deepEqual(reopened.get(a.id), closed, 'closed bubble identical after reopen');
  assert.deepEqual(reopened.get(b.id), b, 'active bubble identical after reopen');
  assert.equal(reopened.get(a.id).version, 2);
  assert.equal(reopened.get(a.id).closedAt, 1500);
  assert.equal(reopened.get(b.id).version, 1);
  assert.equal(reopened.get(b.id).closedAt, null);
  assert.equal(reopened.get('bub_' + '0'.repeat(32)), null);
});

test('a third reopen keeps records stable across multiple restarts', (t) => {
  const file = join(tempDir(t), 'store.db');
  const {repo} = createOwnedBubbleRepository({file, now: () => 1000});
  const a = repo.create();
  repo.close(a.id);
  const snapshot = repo.get(a.id);
  const {internal} = createOwnedBubbleRepository({file, now: () => 1000});
  internal.close();
  internal.close(); // idempotent close must not throw
  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 5000});
  assert.deepEqual(reopened.get(a.id), snapshot);
});

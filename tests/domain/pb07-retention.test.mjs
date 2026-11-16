// PB-07 contract: minimal retention. Closed records past the bounded parameter
// are purged by an explicit call; active records are never purged; invitation
// tokens never appear in the storage file.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createOwnedBubbleRepository, DEFAULT_RETENTION_MS} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-retention-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

test('purge removes only closed records older than the retention bound; active records never purged', (t) => {
  const file = join(tempDir(t), 'retention.db');
  let time = 1000;
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => time, retentionMs: 500});
  const old = repo.create();                 // t=1000
  time = 2000;
  repo.close(old.id);                        // closedAt 2000
  time = 2200;
  const mid = repo.create();
  time = 2700;
  repo.close(mid.id);                        // closedAt 2700
  time = 2800;
  const active = repo.create();              // active, createdAt 2800
  const closedMid = {...repo.get(mid.id)};   // snapshot of closed mid

  time = 3100;                               // bound = 2600
  assert.equal(internal.purgeClosed(), 1, 'exactly the old closed record purged');
  assert.equal(repo.get(old.id), null, 'old closed record purged');
  assert.deepEqual(repo.get(mid.id), closedMid, 'younger closed record kept');

  time = 3200;                               // bound = 2700: exact boundary purged
  assert.equal(internal.purgeClosed(), 1, 'mid closed record now past the bound');
  assert.equal(repo.get(mid.id), null);

  time = 3200;
  assert.deepEqual(repo.get(active.id), active, 'active record never purged');
  time = 1_000_000;
  assert.equal(internal.purgeClosed(), 0);
  assert.deepEqual(repo.get(active.id), {id: active.id, state: 'active', version: 1, createdAt: 2800, closedAt: null},
    'ancient active record still alive');
});

test('purged rows are gone from disk after reopen', (t) => {
  const file = join(tempDir(t), 'retention-reopen.db');
  let time = 1000;
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => time, retentionMs: 1000});
  const a = repo.create();
  time = 2000;
  repo.close(a.id);                          // closedAt 2000, purged once now >= 3000
  time = 3500;
  assert.equal(internal.purgeClosed(), 1);
  internal.close();
  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 3500, retentionMs: 1000});
  assert.equal(reopened.get(a.id), null, 'purge is durable');
});

test('the default retention bound is 24 hours and is a parameter', () => {
  assert.equal(DEFAULT_RETENTION_MS, 24 * 60 * 60 * 1000);
  const dir = mkdtempSync(join(tmpdir(), 'pb07-retention-'));
  try {
    let time = 0;
    const file = join(dir, 'default.db');
    const {repo, internal} = createOwnedBubbleRepository({file, now: () => time});
    const a = repo.create();
    time = 1000;
    repo.close(a.id);                        // closedAt 1000
    time = 1000 + DEFAULT_RETENTION_MS - 1;
    assert.equal(internal.purgeClosed(), 0, 'kept one millisecond inside the default bound');
    time = 1000 + DEFAULT_RETENTION_MS;
    assert.equal(internal.purgeClosed(), 1, 'purged at 24 h exactly (closedAt <= now - retention)');
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});

test('invitation tokens are never persisted in the storage file', (t) => {
  const file = join(tempDir(t), 'tokens.db');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  const a = repo.create();
  const invA = repo.issueInvitation(a.id, {expiresAt: 9_000_000});
  const b = repo.create();
  const invB = repo.issueInvitation(b.id, {expiresAt: 9_000_000});
  repo.close(a.id);
  internal.close();
  const bytes = readFileSync(file).toString('latin1');
  assert.ok(!bytes.includes('inv_'), 'no invitation prefix anywhere in the file');
  assert.ok(!bytes.includes(invA.token) && !bytes.includes(invB.token), 'neither token string appears verbatim');
  assert.ok(!bytes.includes(Buffer.from(invA.token).toString('hex')), 'token does not appear hex-encoded');
  // tokens are still resolvable in RAM before close and never leak to disk
  const {repo: reopened} = createOwnedBubbleRepository({file, now: () => 1000});
  assert.equal(reopened.get(b.id).id, b.id);
});

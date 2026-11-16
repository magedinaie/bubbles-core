// PB-07 contract: lifecycle index. Active-bubble listing is served by the state
// index (EXPLAIN QUERY PLAN shows an index, so closed rows are not scanned) and
// returns only active records.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createOwnedBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-index-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

test('active listing plan uses the state index, not a scan of closed rows', (t) => {
  const file = join(tempDir(t), 'index.db');
  let time = 1000;
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => time});
  const bubbles = Array.from({length: 30}, () => repo.create());
  time = 1500;
  for (const b of bubbles.slice(0, 25)) repo.close(b.id); // 25 closed, 5 active

  const plan = internal.explainActiveListing();
  assert.ok(Array.isArray(plan) && plan.length > 0, `plan rows present: ${JSON.stringify(plan)}`);
  assert.ok(plan.some(detail => /USING (COVERING )?INDEX idx_bubbles_state/i.test(detail)),
    `plan must use idx_bubbles_state, got: ${plan.join(' | ')}`);

  const active = internal.listActive();
  assert.equal(active.length, 5);
  assert.ok(active.every(b => b.state === 'active'));
  const closedIds = new Set(bubbles.slice(0, 25).map(b => b.id));
  assert.ok(active.every(b => !closedIds.has(b.id)), 'no closed record leaks into active listing');
});

test('active listing shape matches the repository port DTO', (t) => {
  const file = join(tempDir(t), 'index-dto.db');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  const a = repo.create();
  const active = internal.listActive();
  assert.deepEqual(active, [a]);
  assert.deepEqual(active[0], {id: a.id, state: 'active', version: 1, createdAt: 1000, closedAt: null});
});

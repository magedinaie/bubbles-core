// PB-07 contract: bounded geographic query. Bubbles carry only a coarse opaque
// cell id (no geometry, no coordinates); the query takes a list of cell ids
// (max 32), returns at most 100 active results, and rejects over-limit input.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createOwnedBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'pb07-geo-'));
  t.after(() => rmSync(dir, {recursive: true, force: true}));
  return dir;
}

test('assignCell stores only a coarse opaque cell id; get() stays on the port DTO', (t) => {
  const file = join(tempDir(t), 'geo.db');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  const a = repo.create();
  internal.assignCell(a.id, 'cell:test-cell-16');
  const dto = repo.get(a.id);
  assert.deepEqual(dto, {id: a.id, state: 'active', version: 1, createdAt: 1000, closedAt: null},
    'public DTO shape unchanged');
  assert.ok(!('cellId' in dto), 'coarse cell id is not exposed on the public DTO');
  const db = new DatabaseSync(file);
  try {
    const row = db.prepare('SELECT cellId FROM bubbles WHERE id = ?').get(a.id);
    assert.equal(row.cellId, 'cell:test-cell-16', 'coarse cell id persisted on the record');
  } finally {
    db.close();
  }
});

test('assignCell rejects malformed cells, unknown bubbles and malformed ids', (t) => {
  const file = join(tempDir(t), 'geo-validation.db');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  const a = repo.create();
  for (const bad of [undefined, null, 7, {}, [], '', 'x'.repeat(129)]) {
    assert.throws(() => internal.assignCell(a.id, bad), {code: 'INVALID_CELL_ID'});
  }
  assert.throws(() => internal.assignCell('bub_' + '0'.repeat(32), 'cell-x'), {code: 'BUBBLE_NOT_FOUND'});
  assert.throws(() => internal.assignCell('not-a-bubble', 'cell-x'), {code: 'INVALID_BUBBLE_ID'});
});

test('queryCells rejects over-limit input: more than 32 cells fails safely', (t) => {
  const file = join(tempDir(t), 'geo-limit.db');
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => 1000});
  repo.create();
  const cells33 = Array.from({length: 33}, (_, i) => `c${i}`);
  assert.throws(() => internal.queryCells(cells33), {code: 'GEO_CELL_LIMIT_EXCEEDED'});
  const cells32 = cells33.slice(0, 32);
  const result = internal.queryCells(cells32);
  assert.ok(Array.isArray(result.bubbles), '32 cells is accepted');
  for (const bad of [null, 'c1', {}, 7, ['ok', 7], ['ok', '']]) {
    assert.throws(() => internal.queryCells(bad), {code: 'INVALID_GEO_CELLS'});
  }
  assert.deepEqual(internal.queryCells([]), {bubbles: [], truncated: false}, 'empty list is a safe no-op');
});

test('queryCells returns at most 100 active results, truncation flagged, closed rows excluded', (t) => {
  const file = join(tempDir(t), 'geo-100.db');
  let time = 1000;
  const {repo, internal} = createOwnedBubbleRepository({file, now: () => time});
  const created = Array.from({length: 120}, () => repo.create());
  for (const b of created) internal.assignCell(b.id, 'cellA');
  time = 1500;
  for (const b of created.slice(0, 10)) repo.close(b.id); // 10 closed + 110 active in cellA
  const other = repo.create();
  internal.assignCell(other.id, 'cellB');

  const {bubbles, truncated} = internal.queryCells(['cellA']);
  assert.equal(bubbles.length, 100, 'bounded at 100 results');
  assert.equal(truncated, true, 'truncation flagged');
  assert.ok(bubbles.every(b => b.state === 'active'), 'closed rows excluded');
  assert.ok(bubbles.every(b => 'id' in b && 'state' in b && 'version' in b && 'createdAt' in b && 'closedAt' in b));
  assert.ok(bubbles.every(b => !('cellId' in b)), 'results keep the public DTO shape');
  const ids = new Set(bubbles.map(b => b.id));
  assert.equal(ids.size, 100, 'no duplicates');

  const {bubbles: otherB} = internal.queryCells(['cellB', 'empty-cell']);
  assert.equal(otherB.length, 1);
  assert.equal(otherB[0].id, other.id);
});

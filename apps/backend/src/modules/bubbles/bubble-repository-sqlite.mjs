// PB-07 stage: SQLite-backed bubble-record store behind the same repository port.
// Bounded contract: id/state/version/createdAt/closedAt persisted; invitation tokens
// stay in RAM only. Port DTO shape matches bubble-repository.mjs exactly (no cellId).
// Stage-only module; the active in-memory repository is untouched.
import {randomBytes} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {closeSync, existsSync, openSync} from 'node:fs';

const BUBBLE_ID_RE = /^bub_[a-f0-9]{32}$/;
const INVITATION_TOKEN_RE = /^inv_[a-f0-9]{64}$/;
const MAX_GEO_CELLS = 32;
const MAX_GEO_RESULTS = 100;
const MAX_CELL_LENGTH = 128;

function validateCellId(cell) {
  if (typeof cell !== 'string' || cell.length < 1 || cell.length > MAX_CELL_LENGTH) {
    throw Object.assign(new TypeError('INVALID_CELL_ID'), {code: 'INVALID_CELL_ID'});
  }
}

function validateCellList(cells) {
  if (!Array.isArray(cells) ||
      cells.some(c => typeof c !== 'string' || c.length < 1 || c.length > MAX_CELL_LENGTH)) {
    throw Object.assign(new TypeError('INVALID_GEO_CELLS'), {code: 'INVALID_GEO_CELLS'});
  }
}

// v1 schema: bubble records + storage meta version table (forward-only migration
// from v0 empty file). No coordinate-named fields, per contract.
const SCHEMA_V1 = `
CREATE TABLE IF NOT EXISTS bubbles (
  id TEXT PRIMARY KEY NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active','closed')),
  version INTEGER NOT NULL,
  createdAt INTEGER NOT NULL,
  closedAt INTEGER,
  cellId TEXT
);
CREATE INDEX IF NOT EXISTS idx_bubbles_state ON bubbles(state);
CREATE INDEX IF NOT EXISTS idx_bubbles_cell ON bubbles(cellId) WHERE state = 'active';
CREATE TABLE IF NOT EXISTS storage_meta (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
);
`;
const STORAGE_SCHEMA_VERSION = 1;

// Provisional minimal retention bound (contract: parameter, default 24 h).
export const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000;

function openStore(db) {
  const hasMeta = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'storage_meta'").get();
  if (!hasMeta) {
    // Only a truly empty database (v0) may be migrated; never adopt a foreign DB.
    const objects = db.prepare('SELECT COUNT(*) AS n FROM sqlite_master').get();
    if (Number(objects.n) > 0) {
      throw Object.assign(new Error('INVALID_STORAGE_FILE: non-empty database without storage_meta (foreign file, not migrated)'),
        {code: 'INVALID_STORAGE_FILE'});
    }
    migrateToV1(db);
    return;
  }
  const row = db.prepare("SELECT value FROM storage_meta WHERE key = 'schema_version'").get();
  const version = row != null ? Number(row.value) : 0;
  if (version < STORAGE_SCHEMA_VERSION) { migrateToV1(db); return; }
  if (version > STORAGE_SCHEMA_VERSION) {
    throw Object.assign(new Error(
      `UNSUPPORTED_STORAGE_VERSION: file schema v${version} is newer than supported v${STORAGE_SCHEMA_VERSION}`),
      {code: 'UNSUPPORTED_STORAGE_VERSION', version});
  }
  const hasBubbles = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'bubbles'").get();
  if (!hasBubbles) {
    throw Object.assign(new Error('INVALID_STORAGE_FILE: v1 file is missing the bubbles table'),
      {code: 'INVALID_STORAGE_FILE'});
  }
}

// Forward-only migration: v0 (empty file, no schema) -> v1.
function migrateToV1(db) {
  db.exec(SCHEMA_V1);
  db.prepare("INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('schema_version', ?)")
    .run(String(STORAGE_SCHEMA_VERSION));
}

function openDatabase(file) {
  let db;
  try {
    // New on-disk files are created owner-only (0600) before SQLite touches them.
    if (file !== ':memory:' && file !== '' && !existsSync(file)) {
      closeSync(openSync(file, 'wx', 0o600));
    }
    db = new DatabaseSync(file);
    // Deleted content is overwritten with zeros so purge really removes it from disk.
    db.exec('PRAGMA secure_delete = ON');
    openStore(db);
    return db;
  } catch (err) {
    try { if (db) db.close(); } catch { /* already closed */ }
    if (err && err.code === 'UNSUPPORTED_STORAGE_VERSION') throw err;
    throw Object.assign(new Error('INVALID_STORAGE_FILE: ' + (err && err.message ? err.message : String(err))),
      {code: 'INVALID_STORAGE_FILE', cause: err});
  }
}

// Public port factory (repo only), parity with createBubbleRepository in the app.
export function createBubbleRepository(options = {}) {
  return buildRepository(options).repo;
}
// Owned factory exposing internal storage operations for contract tests.
export function createOwnedBubbleRepository(options = {}) {
  return buildRepository(options);
}

function buildRepository({now = Date.now, file = ':memory:', retentionMs = DEFAULT_RETENTION_MS} = {}) {
  const db = openDatabase(file);

  const invitations = new Map();

  function timestamp() {
    const value = now();
    if (!Number.isSafeInteger(value) || value < 0) {
      throw Object.assign(new TypeError('INVALID_CLOCK'), {code: 'INVALID_CLOCK'});
    }
    return value;
  }

  // Every multi-step SQL mutation runs inside an explicit transaction; a failure
  // inside the transaction rolls back to the previous consistent state.
  function inTransaction(fn) {
    db.exec('BEGIN');
    try {
      const result = fn();
      db.exec('COMMIT');
      return result;
    } catch (err) {
      try { db.exec('ROLLBACK'); } catch { /* keep original error */ }
      throw err;
    }
  }

  function toDTO(row) {
    return {id: row.id, state: row.state, version: row.version, createdAt: row.createdAt, closedAt: row.closedAt};
  }

  function get(id) {
    if (typeof id !== 'string' || !BUBBLE_ID_RE.test(id)) {
      throw Object.assign(new TypeError('INVALID_BUBBLE_ID'), {code: 'INVALID_BUBBLE_ID'});
    }
    const row = db.prepare('SELECT id, state, version, createdAt, closedAt FROM bubbles WHERE id = ?').get(id);
    return row ? toDTO(row) : null;
  }

  const repo = {
    close(id) {
      const bubble = get(id);
      if (!bubble || bubble.state === 'closed') return bubble;
      return inTransaction(() => {
        const closedAt = timestamp();
        db.prepare('UPDATE bubbles SET state = ?, version = ?, closedAt = ? WHERE id = ?')
          .run('closed', bubble.version + 1, closedAt, id);
        for (const [token, entry] of invitations) {
          if (entry.bubbleId === id) invitations.delete(token);
        }
        return toDTO({...bubble, state: 'closed', version: bubble.version + 1, closedAt});
      });
    },
    issueInvitation(bubbleId, options) {
      const bubble = get(bubbleId);
      const issuedAt = timestamp();
      const descriptor = options && Object.getOwnPropertyDescriptor(options, 'expiresAt');
      const expiresAt = descriptor?.value;
      if (!options || Object.getPrototypeOf(options) !== Object.prototype ||
          Reflect.ownKeys(options).length !== 1 || !descriptor || !Object.hasOwn(descriptor, 'value') ||
          !Number.isSafeInteger(expiresAt) || expiresAt <= issuedAt) {
        throw Object.assign(new TypeError('INVALID_INVITATION_OPTIONS'), {code: 'INVALID_INVITATION_OPTIONS'});
      }
      if (!bubble) throw Object.assign(new Error('BUBBLE_NOT_FOUND'), {code: 'BUBBLE_NOT_FOUND'});
      if (bubble.state === 'closed') {
        throw Object.assign(new Error('BUBBLE_CLOSED'), {code: 'BUBBLE_CLOSED'});
      }
      let token;
      do { token = `inv_${randomBytes(32).toString('hex')}`; } while (invitations.has(token));
      const invitation = {token, bubbleId, expiresAt};
      for (const [previous, entry] of invitations) {
        if (entry.bubbleId === bubbleId) invitations.delete(previous);
      }
      invitations.set(token, invitation);
      return {...invitation};
    },
    resolveInvitation(token) {
      if (typeof token !== 'string' || !INVITATION_TOKEN_RE.test(token)) {
        throw Object.assign(new TypeError('INVALID_INVITATION_TOKEN'), {code: 'INVALID_INVITATION_TOKEN'});
      }
      const invitation = invitations.get(token);
      if (!invitation) return null;
      if (timestamp() >= invitation.expiresAt) {
        invitations.delete(token);
        return null;
      }
      return get(invitation.bubbleId);
    },
    create(...args) {
      if (args.length) throw Object.assign(new TypeError('INVALID_CREATE_ARGUMENTS'), {code: 'INVALID_CREATE_ARGUMENTS'});
      return inTransaction(() => {
        const createdAt = timestamp();
        let id;
        const insert = db.prepare(
          'INSERT INTO bubbles (id, state, version, createdAt, closedAt, cellId) VALUES (?, ?, ?, ?, NULL, NULL)');
        for (let attempt = 0; attempt < 16; attempt++) {
          const candidate = `bub_${randomBytes(16).toString('hex')}`;
          const exists = db.prepare('SELECT id FROM bubbles WHERE id = ?').get(candidate);
          if (!exists) { id = candidate; break; }
        }
        if (!id) throw Object.assign(new Error('BUBBLE_ID_UNAVAILABLE'), {code: 'BUBBLE_ID_UNAVAILABLE'});
        insert.run(id, 'active', 1, createdAt);
        return toDTO({id, state: 'active', version: 1, createdAt, closedAt: null});
      });
    },
    get
  };

  const internal = Object.freeze({
    close() {
      if (db.isOpen) db.close();
    },
    // Explicit retention call: purge closed records whose closedAt is at or past
    // the retention bound (now - retentionMs). Active records are never touched.
    purgeClosed() {
      const bound = timestamp() - retentionMs;
      const result = db.prepare(
        "DELETE FROM bubbles WHERE state = 'closed' AND closedAt IS NOT NULL AND closedAt <= ?").run(bound);
      return Number(result.changes);
    },
    // Active bubbles only; the state index serves this query so closed rows are
    // not scanned (asserted via EXPLAIN QUERY PLAN in the contract suite).
    listActive() {
      return db.prepare(
        "SELECT id, state, version, createdAt, closedAt FROM bubbles WHERE state = 'active' ORDER BY id")
        .all().map(toDTO);
    },
    // Query plan for the exact active-listing query, for the EXPLAIN assertion.
    explainActiveListing() {
      return db.prepare(
        "EXPLAIN QUERY PLAN SELECT id, state, version, createdAt, closedAt FROM bubbles WHERE state = 'active' ORDER BY id")
        .all().map(row => row.detail);
    },
    // Coarse cell assignment: caller-supplied opaque id, no geometry computed here.
    assignCell(id, cell) {
      const bubble = get(id);
      if (!bubble) throw Object.assign(new Error('BUBBLE_NOT_FOUND'), {code: 'BUBBLE_NOT_FOUND'});
      validateCellId(cell);
      db.prepare('UPDATE bubbles SET cellId = ? WHERE id = ?').run(cell, id);
      return get(id);
    },
    // Bounded geographic query: list of cell ids (max 32), at most 100 active
    // results, over-limit input rejected. Results keep the public DTO shape.
    queryCells(cells) {
      validateCellList(cells);
      if (cells.length > MAX_GEO_CELLS) {
        throw Object.assign(new Error(
          `GEO_CELL_LIMIT_EXCEEDED: ${cells.length} cells exceeds the maximum of ${MAX_GEO_CELLS}`),
          {code: 'GEO_CELL_LIMIT_EXCEEDED', count: cells.length});
      }
      if (cells.length === 0) return {bubbles: [], truncated: false};
      const placeholders = cells.map(() => '?').join(', ');
      const rows = db.prepare(
        `SELECT id, state, version, createdAt, closedAt FROM bubbles
         WHERE state = 'active' AND cellId IN (${placeholders}) ORDER BY id LIMIT ?`)
        .all(...cells, MAX_GEO_RESULTS + 1);
      const truncated = rows.length > MAX_GEO_RESULTS;
      return {bubbles: rows.slice(0, MAX_GEO_RESULTS).map(toDTO), truncated};
    }
  });
  return {repo, internal};
}

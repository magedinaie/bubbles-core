import test from 'node:test';
import assert from 'node:assert/strict';
import {createBubbleRepository} from '../../apps/backend/src/modules/bubbles/bubble-repository-sqlite.mjs';

test('independently addresses simultaneous active bubbles without profile identity', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const b = repo.create();
  assert.match(a.id, /^bub_[a-f0-9]{32}$/);
  assert.notEqual(a.id, b.id);
  assert.deepEqual(a, {id:a.id, state:'active', version:1, createdAt:1000, closedAt:null});
  assert.deepEqual(repo.get(a.id), a);
  assert.deepEqual(repo.get(b.id), b);
  assert.equal(repo.get('bub_' + '0'.repeat(32)), null);
});

test('caller mutation cannot corrupt records or another repository instance', () => {
  const repo = createBubbleRepository();
  const a = repo.create();
  const id = a.id;
  a.state = 'closed';
  a.id = 'tampered';
  repo.get(id).version = 99;
  assert.equal(repo.get(id).state, 'active');
  assert.equal(repo.get(id).version, 1);
  assert.equal(createBubbleRepository().get(id), null);
});

test('rejects malformed IDs without normalization or coercion', () => {
  const repo = createBubbleRepository();
  const {id} = repo.create();
  for (const bad of [undefined, null, 7, {}, [], '__proto__', '../x', id+' ', id.toUpperCase(), 'inv_'+'a'.repeat(64), 'bub_'+'a'.repeat(33)]) {
    assert.throws(() => repo.get(bad), {code:'INVALID_BUBBLE_ID'});
  }
});

test('issues distinct invitation references that resolve only their target', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const b = repo.create();
  const invitation = repo.issueInvitation(a.id, {expiresAt:2000});
  assert.match(invitation.token, /^inv_[a-f0-9]{64}$/);
  assert.notEqual(invitation.token, a.id);
  assert.deepEqual(invitation, {token:invitation.token, bubbleId:a.id, expiresAt:2000});
  assert.deepEqual(repo.resolveInvitation(invitation.token), a);
  assert.notDeepEqual(repo.resolveInvitation(invitation.token), b);
  assert.equal(repo.resolveInvitation('inv_'+'0'.repeat(64)), null);
  assert.deepEqual(repo.get(a.id), a);
  assert.throws(() => repo.get(invitation.token), {code:'INVALID_BUBBLE_ID'});
});

test('rotating an invitation invalidates the stale token without changing identity', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const old = repo.issueInvitation(a.id, {expiresAt:2000});
  const current = repo.issueInvitation(a.id, {expiresAt:3000});
  assert.notEqual(old.token, current.token);
  assert.equal(repo.resolveInvitation(old.token), null);
  current.bubbleId = 'tampered';
  current.expiresAt = 0;
  const resolved = repo.resolveInvitation(current.token);
  resolved.state = 'closed';
  assert.deepEqual(repo.resolveInvitation(current.token), a);
  assert.deepEqual(repo.get(a.id), a);
});

test('expired invitations fail closed at the exact deadline and cannot revive', () => {
  let time = 1000;
  const repo = createBubbleRepository({now: () => time});
  const a = repo.create();
  const {token} = repo.issueInvitation(a.id, {expiresAt:2000});
  time = 1999;
  assert.deepEqual(repo.resolveInvitation(token), a);
  time = 2000;
  assert.equal(repo.resolveInvitation(token), null);
  time = 1000;
  assert.equal(repo.resolveInvitation(token), null);
  assert.deepEqual(repo.get(a.id), a);
});

test('closing one bubble is terminal and invalidates only its invitation', () => {
  let time = 1000;
  const repo = createBubbleRepository({now: () => time});
  const a = repo.create();
  const b = repo.create();
  const ia = repo.issueInvitation(a.id, {expiresAt:3000});
  const ib = repo.issueInvitation(b.id, {expiresAt:3000});
  time = 1500;
  const closed = repo.close(a.id);
  assert.deepEqual(closed, {...a, state:'closed', version:2, closedAt:1500});
  closed.state = 'active';
  time = 1600;
  assert.deepEqual(repo.close(a.id), {...a, state:'closed', version:2, closedAt:1500});
  assert.equal(repo.resolveInvitation(ia.token), null);
  assert.throws(() => repo.issueInvitation(a.id, {expiresAt:3000}), {code:'BUBBLE_CLOSED'});
  assert.deepEqual(repo.get(b.id), b);
  assert.deepEqual(repo.resolveInvitation(ib.token), b);
  assert.equal(repo.close('bub_'+'0'.repeat(32)), null);
});

test('validates invitation commands atomically and rejects wrong identifier domains', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const current = repo.issueInvitation(a.id, {expiresAt:3000});
  for (const options of [undefined, null, {}, [], {expiresAt:1000}, {expiresAt:999}, {expiresAt:NaN}, {expiresAt:Infinity}, {expiresAt:1.5}, {expiresAt:'2000'}, {expiresAt:2000, profileId:'person'}, {expiresAt:2000, token:current.token}]) {
    assert.throws(() => repo.issueInvitation(a.id, options), {code:'INVALID_INVITATION_OPTIONS'});
    assert.deepEqual(repo.resolveInvitation(current.token), a);
  }
  assert.throws(() => repo.issueInvitation('bub_'+'0'.repeat(32), {expiresAt:2000}), {code:'BUBBLE_NOT_FOUND'});
  for (const bad of [undefined, null, {}, [], '__proto__', a.id, current.token+' ', current.token.toUpperCase()]) {
    assert.throws(() => repo.resolveInvitation(bad), {code:'INVALID_INVITATION_TOKEN'});
  }
  assert.throws(() => repo.close(current.token), {code:'INVALID_BUBBLE_ID'});
  assert.throws(() => repo.issueInvitation(current.token, {expiresAt:2000}), {code:'INVALID_BUBBLE_ID'});
});

test('bubble creation rejects caller-selected IDs, profiles and lifecycle fields', () => {
  const repo = createBubbleRepository();
  for (const input of [null, 'person', {}, {id:'bub_'+'a'.repeat(32)}, {profileId:'person'}, {profile:{name:'Synthetic'}}, {state:'closed'}]) {
    assert.throws(() => repo.create(input), {code:'INVALID_CREATE_ARGUMENTS'});
  }
  assert.equal(repo.create().state, 'active');
});

test('invalid clock samples fail before writes or invitation revocation', () => {
  let time = 1000;
  const repo = createBubbleRepository({now: () => time});
  const a = repo.create();
  const {token} = repo.issueInvitation(a.id, {expiresAt:3000});
  for (const invalid of [NaN, Infinity, -1, 1.1, '2000', Number.MAX_SAFE_INTEGER+1]) {
    time = invalid;
    assert.throws(() => repo.create(), {code:'INVALID_CLOCK'});
    assert.throws(() => repo.close(a.id), {code:'INVALID_CLOCK'});
    assert.throws(() => repo.resolveInvitation(token), {code:'INVALID_CLOCK'});
    assert.throws(() => repo.issueInvitation(a.id, {expiresAt:4000}), {code:'INVALID_CLOCK'});
    assert.deepEqual(repo.get(a.id), a);
  }
  time = 1500;
  assert.deepEqual(repo.resolveInvitation(token), a);
});

test('invitation options reject accessors and non-data shapes without executing them', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const {token} = repo.issueInvitation(a.id, {expiresAt:3000});
  let reads = 0;
  const accessor = {get expiresAt() { reads++; return 2000; }};
  for (const options of [accessor, Object.create({expiresAt:2000}), Object.assign(Object.create(null), {expiresAt:2000}), {expiresAt:2000, [Symbol('extra')]:true}]) {
    assert.throws(() => repo.issueInvitation(a.id, options), {code:'INVALID_INVITATION_OPTIONS'});
  }
  assert.equal(reads, 0);
  assert.deepEqual(repo.resolveInvitation(token), a);
});

test('storage operations do not dispatch through caller-replaced repository methods', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const b = repo.create();
  const {token} = repo.issueInvitation(a.id, {expiresAt:3000});
  const originalGet = repo.get;
  repo.get = () => ({...b});
  assert.deepEqual(repo.resolveInvitation(token), a);
  const close = repo.close;
  assert.equal(close(a.id).id, a.id);
  assert.deepEqual(originalGet(b.id), b);
  const issue = repo.issueInvitation;
  assert.throws(() => issue(a.id, {expiresAt:3000}), {code:'BUBBLE_CLOSED'});
});

test('throwing clock leaves records and current invitations unchanged', () => {
  let failed = false;
  const clockError = new Error('synthetic clock unavailable');
  const repo = createBubbleRepository({now: () => {
    if (failed) throw clockError;
    return 1000;
  }});
  const bubble = repo.create();
  const {token} = repo.issueInvitation(bubble.id, {expiresAt:3000});
  failed = true;
  for (const action of [
    () => repo.create(),
    () => repo.close(bubble.id),
    () => repo.issueInvitation(bubble.id, {expiresAt:4000}),
    () => repo.resolveInvitation(token)
  ]) {
    assert.throws(action, error => error === clockError);
    assert.deepEqual(repo.get(bubble.id), bubble);
  }
  failed = false;
  assert.deepEqual(repo.resolveInvitation(token), bubble);
});

test('identifier validation rejects trailing line terminators exactly', () => {
  const repo = createBubbleRepository({now: () => 1000});
  const a = repo.create();
  const {token} = repo.issueInvitation(a.id, {expiresAt:2000});
  for (const suffix of ['\n', '\r', '\r\n', '\u2028', '\u2029']) {
    assert.throws(() => repo.get(a.id+suffix), {code:'INVALID_BUBBLE_ID'});
    assert.throws(() => repo.resolveInvitation(token+suffix), {code:'INVALID_INVITATION_TOKEN'});
  }
});

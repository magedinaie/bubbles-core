import test from 'node:test';
import assert from 'node:assert/strict';
import {existsSync} from 'node:fs';
const moduleURL = new URL('../../apps/backend/src/modules/membership/membership-service.mjs', import.meta.url);
const load = async () => { assert.ok(existsSync(moduleURL), 'membership service must exist'); return import(moduleURL); };

test('explicit authenticated Join requires server-issued eligibility; reads require membership generation', async () => {
  const {createMembershipService} = await load();
  const s = createMembershipService({now:()=>1000});
  try {
    const a = s.bootstrap.session(); const b = s.bootstrap.bubble();
    assert.throws(()=>s.read(a.credential,b.id,'unknown'), {code:'MEMBERSHIP_REQUIRED'});
    assert.throws(()=>s.join(a.credential,b.id,{}), {code:'ADMISSION_DENIED'});
    const grant = s.bootstrap.syntheticEligibility(a.credential,b.id);
    const m = s.join(a.credential,b.id,{eligibilityToken:grant.token});
    assert.equal(m.bubbleId,b.id); assert.equal(m.basis,'synthetic-local');
    assert.deepEqual(s.read(a.credential,b.id,m.generation),m);
    assert.equal(m.actorId,undefined); assert.equal(m.credential,undefined);
  } finally {s.shutdown();}
});

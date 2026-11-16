import test from 'node:test';
import assert from 'node:assert/strict';
import {createMembershipService} from '../../apps/backend/src/modules/membership/membership-service.mjs';
const denied=(fn,code)=>assert.throws(fn,{code});
function fixture(options={}) {
  let time=1000; const s=createMembershipService({now:()=>time,...options});
  const a=s.bootstrap.session(), b=s.bootstrap.bubble(), other=s.bootstrap.bubble();
  const grant=s.bootstrap.syntheticEligibility(a.credential,b.id);
  const join=()=>s.join(a.credential,b.id,{eligibilityToken:grant.token});
  return {s,a,b,other,grant,join,set:v=>time=v};
}
test('consumed admission retry is idempotent after handle expiry while unused expired handles need fresh eligibility',()=>{
  const f=fixture();try{
    const m=f.join();f.set(21000);assert.deepEqual(f.join(),m);
    const fresh=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);
    assert.deepEqual(f.s.join(f.a.credential,f.b.id,{eligibilityToken:fresh.token}),m);
    f.s.leave(f.a.credential,f.b.id,m.generation);denied(f.join,'ADMISSION_DENIED');
    const g=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);f.set(31000);
    denied(()=>f.s.join(f.a.credential,f.b.id,{eligibilityToken:g.token}),'ADMISSION_DENIED');
    const renewed=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);
    assert.notEqual(f.s.join(f.a.credential,f.b.id,{eligibilityToken:renewed.token}).generation,m.generation);
  }finally{f.s.shutdown();}
});
test('revocation invalidates pre-issued unused admissions and late old-generation callbacks',async()=>{
  const f=fixture();try{
    const spare=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id),m=f.join();
    let old=0,newEvents=0;
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>old++);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'queued-old'});
    f.s.leave(f.a.credential,f.b.id,m.generation);
    denied(()=>f.s.join(f.a.credential,f.b.id,{eligibilityToken:spare.token}),'ADMISSION_DENIED');
    const fresh=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);
    const next=f.s.join(f.a.credential,f.b.id,{eligibilityToken:fresh.token});
    f.s.subscribe(f.a.credential,f.b.id,next.generation,()=>newEvents++);
    await new Promise(resolve=>setImmediate(resolve));assert.equal(old,0);assert.equal(newEvents,0);
    denied(()=>f.s.write(f.a.credential,f.b.id,next.generation,{operationKey:'queued-old'}),'OPERATION_CONFLICT');
  }finally{f.s.shutdown();}
});
test('private closure and admission revocation stop streams including pending delivery, with no resurrection',async()=>{
  const f=fixture(); try {
    f.s.bootstrap.revokeAdmission(f.grant.token); denied(f.join,'ADMISSION_DENIED');
    const g=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);
    const m=f.s.join(f.a.credential,f.b.id,{eligibilityToken:g.token});
    let count=0,closed=0;
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>count++,()=>closed++);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'before-close'});
    f.s.bootstrap.closeBubble(f.b.id); assert.equal(closed,1);
    for(const method of ['read','heartbeat','leave','write','subscribe']) denied(()=>f.s[method](f.a.credential,f.b.id,m.generation,method==='subscribe'?()=>{}:{operationKey:'after-close'}),'BUBBLE_UNAVAILABLE');
    denied(()=>f.s.join(f.a.credential,f.b.id,{eligibilityToken:g.token}),'BUBBLE_UNAVAILABLE');
    await new Promise(resolve=>setImmediate(resolve)); assert.equal(count,0); assert.equal(closed,1);
  } finally {f.s.shutdown();}
});
test('owner maintenance expires idle streams at lease equality and heartbeat only renews live transport',()=>{
  const f=fixture(); try {
    const m=f.join(); let closed=0;
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{},()=>closed++);
    f.set(60000); const renewed=f.s.heartbeat(f.a.credential,f.b.id,m.generation);
    assert.equal(renewed.generation,m.generation); assert.equal(renewed.expiresAtMs,120000);
    f.set(120000); f.s.bootstrap.tick(); assert.equal(closed,1);
    denied(()=>f.s.heartbeat(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
    const g=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);
    const next=f.s.join(f.a.credential,f.b.id,{eligibilityToken:g.token}); assert.notEqual(next.generation,m.generation);
    denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
  } finally {f.s.shutdown();}
});
test('bounded writes deduplicate operation keys without duplicate events; capacities fail before mutation',async()=>{
  const f=fixture({policy:{maxOperationKeys:1,maxStreams:1}}); try {
    const m=f.join(), events=[];
    const stop=f.s.subscribe(f.a.credential,f.b.id,m.generation,e=>events.push(e));
    denied(()=>f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{}),'LIMIT_REACHED');
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'retry'});
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'retry'});
    denied(()=>f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'new'}),'LIMIT_REACHED');
    await new Promise(resolve=>setImmediate(resolve)); assert.equal(events.length,1); stop();
  } finally {f.s.shutdown();}
  for(const [capacity,action] of [
    ['maxSessions',s=>s.bootstrap.session()],
    ['maxBubbles',s=>s.bootstrap.bubble()],
    ['maxGrants',s=>{const a=s.bootstrap.session(),b=s.bootstrap.bubble(); s.bootstrap.syntheticEligibility(a.credential,b.id); s.bootstrap.syntheticEligibility(a.credential,b.id);}]
  ]) {
    const s=createMembershipService({now:()=>1000,policy:{[capacity]:1}});
    try {if(capacity!=='maxGrants') action(s); denied(()=>action(s),'LIMIT_REACHED');} finally{s.shutdown();}
  }
  denied(()=>createMembershipService({policy:{leaseMs:NaN}}),'INVALID_POLICY');
  denied(()=>createMembershipService({policy:{leaseSeconds:60}}),'INVALID_POLICY');
});
test('authorized scoped probe writes deliver only to current subscriptions and revoke queued deliveries',async()=>{
  const f=fixture(); try {
    const m=f.join(), events=[];
    const stranger=f.s.bootstrap.session();
    const grant=f.s.bootstrap.syntheticEligibility(stranger.credential,f.other.id);
    const other=f.s.join(stranger.credential,f.other.id,{eligibilityToken:grant.token});
    for (const method of ['read','write','subscribe']) {
      const extra=method==='subscribe'?()=>{}:method==='write'?{operationKey:'probe-1'}:undefined;
      denied(()=>f.s[method](f.a.credential,f.other.id,other.generation,extra),'MEMBERSHIP_REQUIRED');
      denied(()=>f.s[method](stranger.credential,f.b.id,m.generation,extra),'MEMBERSHIP_REQUIRED');
      denied(()=>f.s[method](f.a.credential,f.b.id,other.generation,extra),'MEMBERSHIP_REQUIRED');
    }
    const stop=f.s.subscribe(f.a.credential,f.b.id,m.generation,e=>events.push(e));
    assert.deepEqual(f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'probe-1'}),{accepted:true});
    await new Promise(resolve=>setImmediate(resolve));
    assert.deepEqual(events,[{type:'authorized-probe',bubbleId:f.b.id,generation:m.generation}]);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'probe-2'});
    f.s.bootstrap.revokeMembership(f.a.credential,f.b.id,m.generation);
    await new Promise(resolve=>setImmediate(resolve)); assert.equal(events.length,1);
    for(const method of ['read','write','subscribe']) denied(()=>f.s[method](f.a.credential,f.b.id,m.generation,method==='subscribe'?()=>{}:{operationKey:'probe-3'}),'MEMBERSHIP_REQUIRED');
    stop(); stop();
  } finally {f.s.shutdown();}
});
test('private session/admission lifetimes and strict client schemas fail closed',()=>{
  const f=fixture(); try {
    const c=f.s.bootstrap.session();
    denied(()=>f.s.join(c.credential,f.b.id,{eligibilityToken:f.grant.token}),'ADMISSION_DENIED');
    denied(()=>f.s.join(f.a.credential,f.other.id,{eligibilityToken:f.grant.token}),'ADMISSION_DENIED');
    denied(()=>f.s.join(f.a.credential,f.b.id,{eligibilityToken:f.grant.token,local:true}),'INVALID_INPUT');
    denied(()=>f.s.join(f.a.credential,f.b.id,{eligibilityToken:f.grant.token,issuedAtMs:0}),'INVALID_INPUT');
    f.set(11000); denied(f.join,'ADMISSION_DENIED');
    const fresh=f.s.bootstrap.syntheticEligibility(f.a.credential,f.b.id);
    const m=f.s.join(f.a.credential,f.b.id,{eligibilityToken:fresh.token});
    denied(()=>f.s.read(c.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
    f.s.bootstrap.revokeSession(f.a.credential);
    denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'AUTH_REQUIRED');
  } finally {f.s.shutdown();}
  const g=fixture(); try {g.set(3601000); denied(g.join,'AUTH_REQUIRED');} finally {g.s.shutdown();}
});
test('lease equality revokes before reads and heartbeat; polling never renews',()=>{
  const f=fixture(); try {
    const m=f.join(); f.set(60999); f.s.read(f.a.credential,f.b.id,m.generation);
    f.set(61000); denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
    denied(()=>f.s.heartbeat(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
    f.set(2000); denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'INVALID_CLOCK');
  } finally {f.s.shutdown();}
});
test('one-use admission retries preserve generation but cannot restore a left membership',()=>{
  const f=fixture(); try {
    const m=f.join(); assert.deepEqual(f.join(),m);
    f.s.leave(f.a.credential,f.b.id,m.generation);
    denied(f.join,'ADMISSION_DENIED');
    denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
  } finally {f.s.shutdown();}
});

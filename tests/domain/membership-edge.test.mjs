import test from 'node:test';
import assert from 'node:assert/strict';
import {createMembershipService,MEMBERSHIP_POLICY} from '../../apps/backend/src/modules/membership/membership-service.mjs';
const denied=(f,code)=>assert.throws(f,{code});
const turn=()=>new Promise(resolve=>setImmediate(resolve));
function setup(policy={}) {
  let t=1000;const s=createMembershipService({now:()=>t,policy});
  const a=s.bootstrap.session(),b=s.bootstrap.bubble();
  const issue=()=>s.bootstrap.syntheticEligibility(a.credential,b.id).token;
  const join=token=>s.join(a.credential,b.id,{eligibilityToken:token??issue()});
  return {s,a,b,issue,join,set:v=>t=v};
}
test('a queued callback checks exact lease deadline even without another request or tick',async()=>{
  const f=setup();try{
    const m=f.join();let delivered=0,closed=0;
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>delivered++,()=>closed++);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'queued'});
    f.set(m.expiresAtMs);await turn();assert.equal(delivered,0);assert.equal(closed,1);
    denied(()=>f.s.heartbeat(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
  }finally{f.s.shutdown();}
});
test('queued delivery rechecks active after sweep callbacks unsubscribe its stream',async()=>{
  let t=1000;const s=createMembershipService({now:()=>t,policy:{leaseMs:100}});
  try{
    const a=s.bootstrap.session(),b=s.bootstrap.session();
    const bubbleA=s.bootstrap.bubble(),bubbleB=s.bootstrap.bubble();
    const join=(session,bubble)=>s.join(session.credential,bubble.id,{
      eligibilityToken:s.bootstrap.syntheticEligibility(session.credential,bubble.id).token
    });
    const mA=join(a,bubbleA);let stopB,closedA=0,closedB=0,deliveredB=0;
    s.subscribe(a.credential,bubbleA.id,mA.generation,()=>{},()=>{closedA++;stopB();});
    t=1050;const mB=join(b,bubbleB);
    stopB=s.subscribe(b.credential,bubbleB.id,mB.generation,()=>deliveredB++,()=>closedB++);
    s.write(b.credential,bubbleB.id,mB.generation,{operationKey:'queued'});
    t=1100;await turn();
    assert.equal(closedA,1);assert.equal(closedB,1);assert.equal(deliveredB,0);
    assert.equal(s.read(b.credential,bubbleB.id,mB.generation).generation,mB.generation);
  }finally{s.shutdown();}
});
test('shutdown denies reentrant authorization before closing consumer streams',()=>{
  const f=setup(),m=f.join();let closed=0;
  f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{},()=>{
    closed++;denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'SERVICE_CLOSED');
  });
  // onClose exceptions are intentionally contained, so preserve the observed code separately.
  let observed;
  f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{},()=>{try{f.s.read(f.a.credential,f.b.id,m.generation);}catch(e){observed=e.code;}});
  f.s.shutdown();assert.equal(closed,1);assert.equal(observed,'SERVICE_CLOSED');
  f.s.shutdown();denied(()=>f.s.bootstrap.session(),'SERVICE_CLOSED');
});
test('lease cleanup shutdown aborts an already running session allocation',()=>{
  const f=setup({leaseMs:100});let closed=0,allocated,error;
  try{
    const m=f.join();
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{},()=>{
      closed++;f.s.shutdown();throw Error('contained close callback');
    });
    f.set(1100);
    try{allocated=f.s.bootstrap.session();}catch(e){error=e;}
    assert.equal(closed,1);
    assert.equal(error?.code,'SERVICE_CLOSED');
    assert.equal(allocated,undefined);
    denied(()=>f.s.bootstrap.session(),'SERVICE_CLOSED');
  }finally{f.s.shutdown();}
});
test('membership bound is atomic and a refused admission is usable after capacity is freed',()=>{
  const f=setup({maxMemberships:1});try{
    const m=f.join(),c=f.s.bootstrap.session(),g=f.s.bootstrap.syntheticEligibility(c.credential,f.b.id);
    denied(()=>f.s.join(c.credential,f.b.id,{eligibilityToken:g.token}),'LIMIT_REACHED');
    f.s.leave(f.a.credential,f.b.id,m.generation);
    const next=f.s.join(c.credential,f.b.id,{eligibilityToken:g.token});
    assert.equal(f.s.read(c.credential,f.b.id,next.generation).generation,next.generation);
  }finally{f.s.shutdown();}
});
test('queued delivery bound rejects atomically and exact retries never re-publish',async()=>{
  const f=setup({maxQueuedDeliveries:1});try{
    const m=f.join();let n=0;
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>n++);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'one'});
    denied(()=>f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'two'}),'LIMIT_REACHED');
    await turn();assert.equal(n,1);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'two'});await turn();assert.equal(n,2);
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'two'});await turn();assert.equal(n,2);
  }finally{f.s.shutdown();}
});
test('stream cap per membership and callback failures release only that stream',async()=>{
  const f=setup({maxStreamsPerMembership:1});try{
    const m=f.join();let closed=0;
    f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{throw Error('fixture consumer');},()=>closed++);
    denied(()=>f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{}),'LIMIT_REACHED');
    f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'one'});await turn();assert.equal(closed,1);
    const stop=f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>{});stop();stop();
  }finally{f.s.shutdown();}
});
test('second valid membership in another bubble never grants authority to the first',()=>{
  const f=setup();try{
    const m=f.join(),b=f.s.bootstrap.bubble(),g=f.s.bootstrap.syntheticEligibility(f.a.credential,b.id);
    const n=f.s.join(f.a.credential,b.id,{eligibilityToken:g.token});
    for(const method of ['read','write','subscribe','heartbeat','leave']) {
      denied(()=>f.s[method](f.a.credential,b.id,m.generation,method==='subscribe'?()=>{}:{operationKey:'wrong'}),'MEMBERSHIP_REQUIRED');
      denied(()=>f.s[method](f.a.credential,f.b.id,n.generation,method==='subscribe'?()=>{}:{operationKey:'wrong'}),'MEMBERSHIP_REQUIRED');
    }
    assert.equal(f.s.read(f.a.credential,f.b.id,m.generation).generation,m.generation);
    assert.equal(f.s.read(f.a.credential,b.id,n.generation).generation,n.generation);
  }finally{f.s.shutdown();}
});
test('stale private revocation cannot revoke a newer generation',()=>{
  const f=setup();try{
    const m=f.join();f.s.leave(f.a.credential,f.b.id,m.generation);const n=f.join();
    f.s.bootstrap.revokeMembership(f.a.credential,f.b.id,m.generation);
    assert.equal(f.s.read(f.a.credential,f.b.id,n.generation).generation,n.generation);
  }finally{f.s.shutdown();}
});
test('invalid clock samples reject all caller operations without renewing or consuming grants',()=>{
  const f=setup();try{
    const grant=f.issue(),m=f.join(grant);
    for(const v of [NaN,Infinity,-1,1.5,'2000',Number.MAX_SAFE_INTEGER,999]) {
      f.set(v);
      for(const method of ['read','write','subscribe','heartbeat','leave']) denied(()=>f.s[method](f.a.credential,f.b.id,m.generation,method==='subscribe'?()=>{}:{operationKey:'one'}),'INVALID_CLOCK');
      denied(()=>f.join(grant),'INVALID_CLOCK');
    }
    f.set(2000);assert.equal(f.s.read(f.a.credential,f.b.id,m.generation).expiresAtMs,61000);
  }finally{f.s.shutdown();}
});
test('hidden own policy data is copied and isolated from caller mutation',()=>{
  const policy=Object.defineProperties({},{leaseMs:{value:1,writable:true},maxSessions:{value:1,writable:true}});
  const f=setup(policy);try{
    policy.leaseMs=100;policy.maxSessions=100;
    const m=f.join();assert.equal(m.expiresAtMs,1001);
    denied(()=>f.s.bootstrap.session(),'LIMIT_REACHED');
    f.set(1001);denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
  }finally{f.s.shutdown();}
});
test('hidden own policy data is validated with the same integer and upper bounds',()=>{
  for(const key of Object.keys(MEMBERSHIP_POLICY)) {
    for(const value of [NaN,-1,'1',0,Infinity,1.5,key.endsWith('Ms')?3600001:4097]) {
      const policy=Object.defineProperty({},key,{value});
      denied(()=>createMembershipService({policy}),'INVALID_POLICY');
    }
  }
});
test('policy accessors are rejected without execution even when hidden',()=>{
  let reads=0;
  for(const enumerable of [false,true]) {
    const policy=Object.defineProperty({},'leaseMs',{enumerable,get(){reads++;return 1;}});
    denied(()=>createMembershipService({policy}),'INVALID_POLICY');
  }
  assert.equal(reads,0);
});
test('strict data schemas do not execute accessors or accept forged authority fields',()=>{
  const f=setup();try{
    const token=f.issue();let reads=0;
    for(const [bad,code] of [
      [Object.create({eligibilityToken:token}),'ADMISSION_DENIED'],
      [Object.assign(Object.create(null),{eligibilityToken:token}),'INVALID_INPUT'],
      [{get eligibilityToken(){reads++;return token;}},'INVALID_INPUT'],
      [{eligibilityToken:token,[Symbol('extra')]:true},'INVALID_INPUT'],
      [{eligibilityToken:token,expiresAtMs:999999},'INVALID_INPUT'],
      [{eligibilityToken:token,basis:'local'},'INVALID_INPUT']
    ]) denied(()=>f.s.join(f.a.credential,f.b.id,bad),code);
    assert.equal(reads,0);const m=f.join(token);
    for(const bad of ['', 'x'.repeat(65),undefined,1,{},'has space']) denied(()=>f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:bad}),'INVALID_INPUT');
    denied(()=>f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'x',actorId:'forged'}),'INVALID_INPUT');
    m.generation='edited';denied(()=>f.s.read(f.a.credential,f.b.id,m.generation),'MEMBERSHIP_REQUIRED');
    assert.equal(f.s.api.bootstrap,undefined);assert.equal(f.s.api.shutdown,undefined);assert.equal(f.s.api.tick,undefined);
  }finally{f.s.shutdown();}
});
test('session revocation and expiry terminate subscriptions and all session-bound grants',async()=>{
  for(const reason of ['revocation','expiry']) {
    const f=setup();try{
      const grant=f.issue(),m=f.join(grant);let delivered=0,closed=0;
      f.s.subscribe(f.a.credential,f.b.id,m.generation,()=>delivered++,()=>closed++);
      f.s.write(f.a.credential,f.b.id,m.generation,{operationKey:'queued'});
      if(reason==='revocation')f.s.bootstrap.revokeSession(f.a.credential);else{f.set(3601000);f.s.bootstrap.tick();}
      await turn();assert.equal(delivered,0);assert.equal(closed,1);
      denied(()=>f.join(grant),'AUTH_REQUIRED');
      for(const method of ['read','write','subscribe']) denied(()=>f.s[method](f.a.credential,f.b.id,m.generation,method==='subscribe'?()=>{}:{operationKey:'x'}),'AUTH_REQUIRED');
    }finally{f.s.shutdown();}
  }
});

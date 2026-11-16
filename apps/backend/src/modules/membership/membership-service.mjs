import {randomBytes} from 'node:crypto';
import {createOwnedMessaging} from './messaging-service.mjs';
import {createOwnedBubbleRepository} from '../bubbles/bubble-repository.mjs';

const fail = code => {throw Object.assign(new Error(code), {code});};
const token = purpose => `${purpose}_${randomBytes(32).toString('hex')}`;
function shape(o,required,optional=[]) {
  if (!o || Object.getPrototypeOf(o)!==Object.prototype ||
      required.some(k=>!Object.hasOwn(o,k)) || Reflect.ownKeys(o).some(k=>
        ![...required,...optional].includes(k) || !Object.hasOwn(Object.getOwnPropertyDescriptor(o,k),'value'))) fail('INVALID_INPUT');
}
export const MEMBERSHIP_POLICY=Object.freeze({
  sessionTtlMs:3600000, admissionTtlMs:10000, leaseMs:60000,
  maxSessions:128, maxBubbles:64, maxGrants:256, maxMemberships:128,
  maxStreams:128, maxStreamsPerMembership:2, maxOperationKeys:512, maxQueuedDeliveries:256
});

// Independent process-local boundary. bootstrap is a PRIVATE composition/test-owner port.
export function createMembershipService(options={}) {return buildMembershipService(options).service;}
// PRIVATE owner: composition must not expose these unguarded objects.
export function createOwnedMembershipService(options={},dispatch=fn=>fn()) {return buildMembershipService(options,dispatch);}
function buildMembershipService({now=Date.now,policy={}}={},dispatch=fn=>fn()) {
  try {shape(policy,[],Object.keys(MEMBERSHIP_POLICY));} catch {fail('INVALID_POLICY');}
  const p={...MEMBERSHIP_POLICY};
  for(const k of Object.keys(MEMBERSHIP_POLICY)) if(Object.hasOwn(policy,k)) p[k]=Object.getOwnPropertyDescriptor(policy,k).value;
  Object.freeze(p);
  for(const [k,v] of Object.entries(p)) if(!Number.isSafeInteger(v) || v<1 || v>(k.endsWith('Ms')?3600000:4096)) fail('INVALID_POLICY');
  if(typeof now!=='function') fail('INVALID_CLOCK');
  let highWater=-1, stopped=false, bubbleCount=0, queued=0;
  function timestamp() {
    if(stopped) fail('SERVICE_CLOSED');
    const t=now();
    if(!Number.isSafeInteger(t) || t<0 || t<highWater || t>Number.MAX_SAFE_INTEGER-3600000) fail('INVALID_CLOCK');
    highWater=t; return t;
  }
  const {repo,internal:repository}=createOwnedBubbleRepository({now:timestamp});
  const transitions=[];
  const sessions=new Map(), grants=new Map(), memberships=new Map(), operations=new Map(), streams=new Set();
  const key=(actor,id)=>`${actor}:${id}`;
  const limit=(size,max)=>{if(size>=max) fail('LIMIT_REACHED');};
  function stopStream(stream) {
    if(!stream.active) return;
    stream.active=false; streams.delete(stream);
    try {stream.onClose();} catch { /* Consumer failure cannot undo revocation. */ }
  }
  function invalidate(actor,id,generation) {
    const k=key(actor,id), m=memberships.get(k);
    if(m?.generation===generation) {
      messaging.prepareTransitions();transitions.push({id,type:'member-left'});
      memberships.delete(k);
      messaging.invalidate(actor,id,generation);
      for(const [value,g] of grants) if(g.actor===actor && g.bubbleId===id) grants.delete(value);
    }
    for(const stream of streams) if(stream.actor===actor && stream.id===id && stream.generation===generation) stopStream(stream);
  }
  function removeSession(credential) {
    const session=sessions.get(credential); if(!session) return;
    sessions.delete(credential);
    for(const m of memberships.values()) if(m.actor===session.actor) invalidate(m.actor,m.bubbleId,m.generation);
    for(const [t,g] of grants) if(g.actor===session.actor) grants.delete(t);
    for(const [k,o] of operations) if(o.actor===session.actor) operations.delete(k);
  }
  function sweep(t) {
    for(const [c,s] of sessions) if(t>=s.expiresAtMs) removeSession(c);
    for(const [token,g] of grants) if(t>=g.expiresAtMs) grants.delete(token);
    for(const m of memberships.values()) if(t>=m.expiresAtMs) invalidate(m.actor,m.bubbleId,m.generation);
  }
  function enter() {const t=timestamp(); sweep(t); if(stopped) fail('SERVICE_CLOSED');messaging.flushTransitions();if(stopped)fail('SERVICE_CLOSED'); return t;}
  function authenticate(credential) {
    const session=typeof credential==='string'?sessions.get(credential):null;
    if(!session) fail('AUTH_REQUIRED'); return session;
  }
  function bubble(id) {
    let b; try {b=repo.get(id);} catch {fail('BUBBLE_UNAVAILABLE');}
    if(!b || b.state!=='active') fail('BUBBLE_UNAVAILABLE'); return b;
  }
  function authorize(credential,id,generation) {
    const actor=authenticate(credential).actor; bubble(id);
    const m=memberships.get(key(actor,id));
    if(!m || m.generation!==generation) fail('MEMBERSHIP_REQUIRED'); return m;
  }
  const dto=m=>({bubbleId:m.bubbleId,generation:m.generation,basis:m.basis,expiresAtMs:m.expiresAtMs});
  function operation(actor,operationKey,signature) {
    if(typeof operationKey!=='string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(operationKey)) fail('INVALID_INPUT');
    const k=key(actor,operationKey), previous=operations.get(k);
    if(previous && previous.signature!==signature) fail('OPERATION_CONFLICT');
    if(!previous) limit(operations.size,p.maxOperationKeys);
    return {k,previous};
  }
  const api=Object.freeze({
    join(credential,id,options) {
      const t=enter(), s=authenticate(credential); bubble(id);
      if(!options || !Object.hasOwn(options,'eligibilityToken')) fail('ADMISSION_DENIED');
      shape(options,['eligibilityToken']);
      const g=grants.get(options.eligibilityToken);
      if(!g || g.actor!==s.actor || g.bubbleId!==id) fail('ADMISSION_DENIED');
      const current=memberships.get(key(s.actor,id));
      if(g.used) {
        if(!current || current.generation!==g.used) fail('ADMISSION_DENIED');
        return dto(current);
      }
      if(!current) limit(memberships.size,p.maxMemberships);
      const m=current || {actor:s.actor,bubbleId:id,generation:token('gen'),historyFloor:messaging.boundary(id),basis:'synthetic-local',expiresAtMs:t+p.leaseMs};
      // Consumed handles are bounded retry receipts, not renewable admission.
      if(!current){messaging.prepareTransitions();transitions.push({id,type:'member-joined'});}
      g.used=m.generation; g.expiresAtMs=s.expiresAtMs;
      memberships.set(key(s.actor,id),m); return dto(m);
    },
    read(credential,id,generation) {enter(); return dto(authorize(credential,id,generation));},
    leave(credential,id,generation) {
      enter(); const m=authorize(credential,id,generation); invalidate(m.actor,id,generation); return {left:true};
    },
    heartbeat(credential,id,generation) {
      const t=enter(),m=authorize(credential,id,generation); m.expiresAtMs=t+p.leaseMs; return dto(m);
    },
    subscribe(credential,id,generation,deliver,onClose=()=>{}) {
      enter(); const m=authorize(credential,id,generation);
      if(typeof deliver!=='function' || typeof onClose!=='function') fail('INVALID_INPUT');
      limit(streams.size,p.maxStreams);
      limit([...streams].filter(s=>s.actor===m.actor && s.id===id && s.generation===generation).length,p.maxStreamsPerMembership);
      const stream={actor:m.actor,credential,id,generation,deliver,onClose,active:true};
      streams.add(stream); return ()=>dispatch(()=>stopStream(stream));
    },
    write(credential,id,generation,options) {
      enter(); const m=authorize(credential,id,generation); shape(options,['operationKey']);
      const op=operation(m.actor,options.operationKey,`probe:${id}:${generation}`);
      if(op.previous) return {accepted:true};
      const targets=[...streams].filter(s=>s.id===id);
      if(queued+targets.length>p.maxQueuedDeliveries) fail('LIMIT_REACHED');
      operations.set(op.k,{actor:m.actor,signature:`probe:${id}:${generation}`});
      for(const stream of targets) {
        queued++;
        queueMicrotask(()=>{
          try {dispatch(()=>{
            queued--;
            if(!stream.active) return;
            try {enter(); authorize(stream.credential,stream.id,stream.generation);} catch {stopStream(stream); return;}
            if(!stream.active) return;
            try {stream.deliver({type:'authorized-probe',bubbleId:id,generation:stream.generation});} catch {stopStream(stream);}
          });} catch { /* Shared-owner clock/shutdown/reentry denies delivery. */ }
        });
      }
      return {accepted:true};
    }
  });
  const bootstrap=Object.freeze({
    tick:enter,
    revokeAdmission(value) {enter(); grants.delete(value);},
    closeBubble(id) {
      enter(); bubble(id); repo.close(id);
      for(const m of memberships.values()) if(m.bubbleId===id) invalidate(m.actor,id,m.generation);
      messaging.closeBubble(id);
      for(const [value,g] of grants) if(g.bubbleId===id) grants.delete(value);
    },
    session() {
      const t=enter(); limit(sessions.size,p.maxSessions);
      const credential=token('ses'); sessions.set(credential,Object.freeze({actor:token('actor'),expiresAtMs:t+p.sessionTtlMs})); return {credential};
    },
    revokeSession(credential) {enter(); removeSession(credential);},
    revokeMembership(credential,id,generation) {enter(); const s=authenticate(credential); invalidate(s.actor,id,generation);},
    bubble() {enter(); limit(bubbleCount,p.maxBubbles); const b=repo.create(); bubbleCount++; return b;},
    syntheticEligibility(credential,id) {
      const t=enter(),s=authenticate(credential); bubble(id); limit(grants.size,p.maxGrants);
      const value=token('adm'); grants.set(value,{actor:s.actor,bubbleId:id,expiresAtMs:t+p.admissionTtlMs}); return {token:value};
    }
  });
  const messaging=createOwnedMessaging({authorize:(c,id,g)=>{enter();return authorize(c,id,g);},dispatch,transitions});
  const service=Object.freeze({api,bootstrap,messaging:messaging.api,...api,shutdown() {
    stopped=true;
    messaging.shutdown();
    for(const s of streams) stopStream(s);
    sessions.clear(); grants.clear(); memberships.clear(); operations.clear();
  }});
  let prepared=null,closing=null;
  const internal=Object.freeze({
    identity(credential) {enter();return authenticate(credential);},
    active(binding) {enter();return [...memberships.values()].some(m=>m.actor===binding.actor);},
    current(binding) {enter();const m=[...memberships.values()].find(m=>m.actor===binding.actor);return m?dto(m):null;},
    // PRIVATE read-only predicate: owner entry already swept actual memberships.
    // No clock, callbacks, preparation, allocation or lease renewal on this path.
    admissionCapacityAvailable() {return memberships.size<p.maxMemberships;},
    bubbleSize(id) {return [...memberships.values()].filter(m=>m.bubbleId===id).length;},
    emptyBubble(id) {enter();const b=repo.get(id);return Boolean(b)&&![...memberships.values()].some(m=>m.bubbleId===id);},
    prepareEmptyClosure(id) {
      enter();if(!internal.emptyBubble(id))return null;
      const closure=repository.prepareClose(id);if(!closure)return null;
      const handle=Object.freeze({});closing={handle,id,closure};return handle;
    },
    validateEmptyClosure(handle) {
      if(stopped)fail('SERVICE_CLOSED');
      if(!closing || closing.handle!==handle || [...memberships.values()].some(m=>m.bubbleId===closing.id))fail('INVALID_PREPARATION');
      repository.validateClose(closing.closure);
    },
    publishEmptyClosure(handle) {
      internal.validateEmptyClosure(handle);repository.publishClose(closing.closure);
      messaging.closeBubble(closing.id);
      for(const [value,g] of grants)if(g.bubbleId===closing.id)grants.delete(value);
      closing=null;
    },
    prepareAdmission(requester,peer,id) {
      const t=enter();bubble(id);
      [requester,peer]=[requester,peer].map(b=>{
        shape(b,['credential','binding']);const ds=Object.getOwnPropertyDescriptors(b);
        return {credential:ds.credential.value,binding:ds.binding.value};
      });
      for(const b of [requester,peer])if(authenticate(b.credential)!==b.binding)fail('AUTH_REQUIRED');
      if(requester.binding===peer.binding)fail('ADMISSION_DENIED');
      const pm=memberships.get(key(peer.binding.actor,id));
      if(!pm || !['mutual-local','joined-local'].includes(pm.basis))fail('ADMISSION_DENIED');
      if([...memberships.values()].some(m=>m.actor===requester.binding.actor))fail('ALREADY_PARTICIPATING');
      limit(memberships.size,p.maxMemberships);
      const row={actor:requester.binding.actor,bubbleId:id,generation:token('gen'),historyFloor:messaging.boundary(id),basis:'joined-local',expiresAtMs:t+p.leaseMs};
      messaging.prepareTransitions();const joined={id,type:'member-joined'};
      const handle=Object.freeze({});prepared={handle,admission:true,row,requester,peer,pm,joined};
      return Object.freeze({handle,membership:dto(row)});
    },
    prepareFormation(input) {
      const t=enter();
      if(!Array.isArray(input) || Object.getPrototypeOf(input)!==Array.prototype || input.length!==2 || Reflect.ownKeys(input).length!==3)fail('INVALID_PARTICIPANTS');
      const bindings=[0,1].map(i=>{
        const d=Object.getOwnPropertyDescriptor(input,String(i));
        if(!d || !Object.hasOwn(d,'value'))fail('INVALID_PARTICIPANTS');
        shape(d.value,['credential','binding']);
        const ds=Object.getOwnPropertyDescriptors(d.value);
        return {credential:ds.credential.value,binding:ds.binding.value};
      });
      if(bindings[0].binding===bindings[1].binding) fail('INVALID_PARTICIPANTS');
      for(const b of bindings) if(authenticate(b.credential)!==b.binding) fail('AUTH_REQUIRED');
      if(bindings.some(b=>[...memberships.values()].some(m=>m.actor===b.binding.actor))) fail('ALREADY_PARTICIPATING');
      limit(bubbleCount,p.maxBubbles);
      if(memberships.size+2>p.maxMemberships) fail('LIMIT_REACHED');
      const generations=bindings.map(()=>token('gen'));
      if(generations[0]===generations[1])fail('GENERATION_UNAVAILABLE');
      const allocation=repository.prepareCreate();
      const rows=bindings.map((b,i)=>({actor:b.binding.actor,bubbleId:allocation.bubble.id,generation:generations[i],basis:'mutual-local',expiresAtMs:t+p.leaseMs}));
      messaging.prepareTransitions();const joined=rows.map(m=>({id:m.bubbleId,type:'member-joined'}));
      const handle=Object.freeze({});
      prepared={handle,allocation,rows,keys:rows.map(m=>key(m.actor,m.bubbleId)),bindings,joined};
      return Object.freeze({handle,bubble:allocation.bubble,memberships:rows.map(dto)});
    },
    validate(handle) {
      if(!prepared || prepared.handle!==handle)fail('INVALID_PREPARATION');
      if(prepared.admission) {
        const {row,requester,peer,pm}=prepared;
        if(stopped)fail('SERVICE_CLOSED');
        if(highWater>=row.expiresAtMs)fail('INVALID_PREPARATION');
        for(const b of [requester,peer])if(authenticate(b.credential)!==b.binding || highWater>=b.binding.expiresAtMs)fail('AUTH_REQUIRED');
        bubble(row.bubbleId);
        if(memberships.get(key(peer.binding.actor,row.bubbleId))!==pm || highWater>=pm.expiresAtMs || !['mutual-local','joined-local'].includes(pm.basis))fail('ADMISSION_DENIED');
        if([...memberships.values()].some(m=>m.actor===requester.binding.actor))fail('ALREADY_PARTICIPATING');
        limit(memberships.size,p.maxMemberships);return;
      }
      if(prepared.rows.some(m=>highWater>=m.expiresAtMs))fail('INVALID_PREPARATION');
      if(stopped)fail('SERVICE_CLOSED');
      for(const b of prepared.bindings) if(authenticate(b.credential)!==b.binding || highWater>=b.binding.expiresAtMs) fail('AUTH_REQUIRED');
      if(prepared.bindings.some(b=>[...memberships.values()].some(m=>m.actor===b.binding.actor)))fail('ALREADY_PARTICIPATING');
      limit(bubbleCount,p.maxBubbles);
      if(memberships.size+2>p.maxMemberships)fail('LIMIT_REACHED');
      repository.validate(prepared.allocation.handle);
    },
    // The validated owner handle is never accepted from a caller. Callback-free tail.
    publish(handle) {
      internal.validate(handle);
      if(prepared.admission){memberships.set(key(prepared.row.actor,prepared.row.bubbleId),prepared.row);transitions.push(prepared.joined);prepared=null;return;}
      repository.publish(prepared.allocation.handle);
      memberships.set(prepared.keys[0],prepared.rows[0]);
      memberships.set(prepared.keys[1],prepared.rows[1]);transitions.push(prepared.joined[0],prepared.joined[1]);bubbleCount++;prepared=null;
    },
    inspect() {return {...repository.inspect(),memberships:memberships.size};}
  });
  return {service,internal};
}

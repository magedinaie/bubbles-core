import {randomBytes} from 'node:crypto';
import {createOwnedMessaging} from './lifecycle-messaging-service.mjs';
import {createOwnedBubbleRepository} from '../bubbles/bubble-repository.mjs';

// Opt-in isolated essentials composition. Upstream membership-service.mjs stays unchanged.
// Initial provisional timings; no physical presence, geometry, media or persistence claim.
export const LIFECYCLE_POLICY=Object.freeze({version:'essentials-lifecycle-1',evidenceMaxAgeMs:10000,departureGraceMs:30000,lonelyWarningMs:15000,maintenanceTickMs:100});
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
function buildMembershipService({now=Date.now,policy={},lifecyclePolicy={}}={},dispatch=fn=>fn()) {
  try {shape(lifecyclePolicy,[],Object.keys(LIFECYCLE_POLICY));}catch{fail('INVALID_POLICY');}
  const lp={...LIFECYCLE_POLICY};
  for(const k of Object.keys(LIFECYCLE_POLICY))if(Object.hasOwn(lifecyclePolicy,k))lp[k]=Object.getOwnPropertyDescriptor(lifecyclePolicy,k).value;
  if(lp.version!==LIFECYCLE_POLICY.version || Object.entries(lp).some(([k,v])=>k!=='version' && (!Number.isSafeInteger(v) || v<1 || v>(k==='maintenanceTickMs'?1000:86400000))))fail('INVALID_POLICY');
  Object.freeze(lp);
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
    if(!Number.isSafeInteger(t) || t<0 || t<highWater || t>Number.MAX_SAFE_INTEGER-Math.max(3600000,lp.evidenceMaxAgeMs+lp.departureGraceMs+lp.lonelyWarningMs)) fail('INVALID_CLOCK');
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
  const lifecycles=new Map();
  const cleanup={bubbles:0,lastPoppedAtMs:null,lastCleanedAtMs:null};
  const lifecycleRow=()=>({state:'uncertain',lonelyAt:null,poppedAt:null});
  function presenceRow(t) {return {joinedAtMs:t,assessment:'unknown',sequence:0,observedAtMs:null,freshUntil:null,departureAt:t+lp.departureGraceMs};}
  function pop(id,at) {
    const l=lifecycles.get(id);if(l?.state==='popped')return;
    if(l){l.state='popped';l.lonelyAt=null;l.poppedAt=at;}
    repo.close(id);
    for(const m of memberships.values())if(m.bubbleId===id)invalidate(m.actor,id,m.generation);
    messaging.closeBubble(id);
    for(const [value,g] of grants)if(g.bubbleId===id)grants.delete(value);
    cleanup.bubbles++;cleanup.lastPoppedAtMs=at;cleanup.lastCleanedAtMs=highWater;
  }
  function reconcile(t) {
    for(const [id,l] of lifecycles) {
      if(l.state==='popped')continue;
      const rows=[...memberships.values()].filter(m=>m.bubbleId===id);
      const viable=rows.filter(m=>m.presence.assessment==='inside' && t<m.presence.freshUntil).length;
      if(!rows.length){pop(id,t);continue;}
      if(l.lonelyAt!==null && t>=l.lonelyAt){pop(id,l.lonelyAt);continue;}
      if(viable>=2){l.state='active';l.lonelyAt=null;continue;}
      if(rows.some(m=>m.presence.departureAt!==null)){l.state='uncertain';continue;}
      l.state='lonely';if(l.lonelyAt===null)l.lonelyAt=t+lp.lonelyWarningMs;
    }
  }
  function advancePresence(t) {
    // Process original chronological boundaries; polling never restarts grace/loneliness.
    for(;;) {
      const times=[...sessions.values()].map(s=>s.expiresAtMs);
      for(const m of memberships.values()) {
        times.push(m.expiresAtMs);
        const r=m.presence;times.push(r.assessment==='inside'?r.freshUntil:r.departureAt);
      }
      for(const l of lifecycles.values())if(l.state!=='popped')times.push(l.lonelyAt);
      const at=Math.min(...times.filter(v=>v!==null));
      if(at>t)break;
      sweep(at);
      for(const m of memberships.values()) {
        const r=m.presence;
        if(r.assessment==='inside' && at>=r.freshUntil){r.assessment='unknown';r.departureAt=r.freshUntil+lp.departureGraceMs;}
        if(r.departureAt!==null && at>=r.departureAt)invalidate(m.actor,m.bubbleId,m.generation);
      }
      reconcile(at);
    }
    sweep(t);reconcile(t);
  }
  function enter() {const t=timestamp();advancePresence(t); if(stopped) fail('SERVICE_CLOSED');messaging.flushTransitions();if(stopped)fail('SERVICE_CLOSED'); return t;}
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
    lifecycle(credential,id,generation) {
      enter();const m=authorize(credential,id,generation),r=m.presence,l=lifecycles.get(id);
      return {bubbleId:id,state:l?.state??'uncertain',assessment:r.assessment,departureDeadlineMs:r.departureAt,evidenceExpiresAtMs:r.freshUntil,connectionExpiresAtMs:m.expiresAtMs,lonelyDeadlineMs:l?.lonelyAt??null,physicalPresence:'not-established'};
    },
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
      const m=current || {actor:s.actor,bubbleId:id,generation:token('gen'),historyFloor:messaging.boundary(id),presence:presenceRow(t),basis:'synthetic-local',expiresAtMs:t+p.leaseMs};
      // Consumed handles are bounded retry receipts, not renewable admission.
      if(!current){messaging.prepareTransitions();transitions.push({id,type:'member-joined'});}
      g.used=m.generation; g.expiresAtMs=s.expiresAtMs;
      memberships.set(key(s.actor,id),m);if(!lifecycles.has(id))lifecycles.set(id,lifecycleRow());return dto(m);
    },
    read(credential,id,generation) {enter(); return dto(authorize(credential,id,generation));},
    leave(credential,id,generation) {
      const t=enter(); const m=authorize(credential,id,generation); invalidate(m.actor,id,generation);reconcile(t); return {left:true};
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
    // PRIVATE trusted-owner synthetic classification, bound to the exact ACTUAL seat.
    // Never expose this capability as a participant-controlled presence endpoint.
    presence(input) {
      const t=enter();
      try {shape(input,['credential','bubbleId','generation','assessment','sequence','observedAtMs']);}catch{return {accepted:false,reason:'invalid-input'};}
      const v=Object.fromEntries(Reflect.ownKeys(input).map(k=>[k,Object.getOwnPropertyDescriptor(input,k).value]));
      if(!['inside','outside','unknown'].includes(v.assessment) || !Number.isSafeInteger(v.sequence) || v.sequence<1 || !Number.isSafeInteger(v.observedAtMs) || v.observedAtMs<0)return {accepted:false,reason:'invalid-input'};
      let m;try{m=authorize(v.credential,v.bubbleId,v.generation);}catch{return {accepted:false,reason:'seat-inactive'};}
      const r=m.presence;
      if(v.observedAtMs>t)return {accepted:false,reason:'future-sample'};
      if(v.observedAtMs<r.joinedAtMs)return {accepted:false,reason:'pre-seat-sample'};
      if(t-v.observedAtMs>=lp.evidenceMaxAgeMs)return {accepted:false,reason:'stale-sample'};
      if(v.sequence<=r.sequence || (r.observedAtMs!==null && v.observedAtMs<r.observedAtMs))return {accepted:false,reason:'replayed-sample'};
      r.sequence=v.sequence;r.observedAtMs=v.observedAtMs;r.assessment=v.assessment;
      r.freshUntil=v.observedAtMs+lp.evidenceMaxAgeMs;
      r.departureAt=v.assessment==='inside'?null:Math.min(r.departureAt??Infinity,v.observedAtMs+lp.departureGraceMs);
      advancePresence(t);return {accepted:true,reason:'synthetic-observation-recorded'};
    },
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
    revokeSession(credential) {const t=enter(); removeSession(credential);reconcile(t);},
    revokeMembership(credential,id,generation) {const t=enter(); const s=authenticate(credential); invalidate(s.actor,id,generation);reconcile(t);},
    bubble() {enter(); limit(bubbleCount,p.maxBubbles); const b=repo.create(); bubbleCount++; return b;},
    syntheticEligibility(credential,id) {
      const t=enter(),s=authenticate(credential); bubble(id); limit(grants.size,p.maxGrants);
      const value=token('adm'); grants.set(value,{actor:s.actor,bubbleId:id,expiresAtMs:t+p.admissionTtlMs}); return {token:value};
    }
  });
  const messaging=createOwnedMessaging({authorize:(c,id,g)=>{enter();return authorize(c,id,g);},dispatch,transitions});
  const maintenance=setInterval(()=>{
    if(stopped)return;
    try{dispatch(()=>{if(!stopped)enter();});}catch{
      // Clock/owner failure cannot authorize delivery. Requests retain their own guard.
      // Do not renew, infer departure, or recreate seats on a scheduling fault.
    }
  },lp.maintenanceTickMs);maintenance.unref();
  const service=Object.freeze({api,bootstrap,messaging:messaging.api,...api,shutdown() {
    clearInterval(maintenance);
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
      const row={actor:requester.binding.actor,bubbleId:id,generation:token('gen'),historyFloor:messaging.boundary(id),presence:presenceRow(t),basis:'joined-local',expiresAtMs:t+p.leaseMs};
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
      const rows=bindings.map((b,i)=>({actor:b.binding.actor,bubbleId:allocation.bubble.id,generation:generations[i],presence:presenceRow(t),basis:'mutual-local',expiresAtMs:t+p.leaseMs}));
      messaging.prepareTransitions();const joined=rows.map(m=>({id:m.bubbleId,type:'member-joined'}));
      const handle=Object.freeze({});
      prepared={handle,allocation,rows,keys:rows.map(m=>key(m.actor,m.bubbleId)),bindings,joined,lifecycle:lifecycleRow()};
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
      lifecycles.set(prepared.allocation.bubble.id,prepared.lifecycle);
      memberships.set(prepared.keys[0],prepared.rows[0]);
      memberships.set(prepared.keys[1],prepared.rows[1]);transitions.push(prepared.joined[0],prepared.joined[1]);bubbleCount++;prepared=null;
    },
    inspect() {return {...repository.inspect(),memberships:memberships.size,messageMemory:messaging.inspect(),lifecycleCleanup:{...cleanup}};}
  });
  return {service,internal};
}

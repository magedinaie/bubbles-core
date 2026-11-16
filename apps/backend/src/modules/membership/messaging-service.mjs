const fail=code=>{throw Object.assign(new Error(code),{code});};
export const MESSAGING_POLICY=Object.freeze({
  maxTextBytes:4096,maxEventsPerBubble:128,maxReceipts:1024,
  maxStreams:128,maxStreamsPerSeat:2,maxQueuedDeliveries:256
});
function data(input,keys) {
  if(!input || Object.getPrototypeOf(input)!==Object.prototype)fail('INVALID_INPUT');
  const descriptors=Object.getOwnPropertyDescriptors(input),own=Reflect.ownKeys(input);
  if(own.length!==keys.length || !keys.every(k=>Object.hasOwn(descriptors,k) && Object.hasOwn(descriptors[k],'value')))fail('INVALID_INPUT');
  return Object.fromEntries(keys.map(k=>[k,descriptors[k].value]));
}

// PRIVATE composition only. Credentials, actors and generations never enter events.
// The membership owner appends preallocated transition records in its publication
// tail. It schedules the drain BEFORE publication; no consumer callback is added
// to formation/admission's atomic tail. The private journal has at most two rows:
// prepareTransitions drains first; formation publishes two, all other paths one.
export function createOwnedMessaging({authorize,dispatch,transitions}) {
  const p=MESSAGING_POLICY,bubbles=new Map(),streams=new Set(),receipts=new Map();
  let stopped=false,queued=0,scheduled=false;
  function state(id) {
    if(!bubbles.has(id))bubbles.set(id,{sequence:0,events:[]});
    return bubbles.get(id);
  }
  function auth(credential,id,generation) {
    flushTransitions();
    if(stopped)fail('SERVICE_CLOSED');
    const membership=authorize(credential,id,generation);
    // Authorization may sweep expired memberships and invoke close callbacks.
    if(stopped)fail('SERVICE_CLOSED');
    return membership;
  }
  function stop(stream) {
    if(!stream.active)return;
    stream.active=false;streams.delete(stream);
    try{stream.onClose();}catch{/* Consumer failure cannot undo cleanup. */}
  }
  function read(credential,id,generation,options) {
    const membership=auth(credential,id,generation),{after}=data(options,['after']),b=state(id);
    if(!Number.isSafeInteger(after) || after<0 || after>b.sequence)fail('INVALID_CURSOR');
    const floor=membership.historyFloor??0,effective=Math.max(after,floor);
    if(effective<(b.events[0]?.cursor??1)-1)fail('CURSOR_EXPIRED');
    return {events:b.events.filter(e=>e.cursor>effective).map(e=>({...e})),cursor:b.sequence};
  }
  function enqueue(stream,event) {
    queued++;
    queueMicrotask(()=>{
      queued--;
      try{dispatch(()=>{
        if(!stream.active || stopped)return;
        try{auth(stream.credential,stream.id,stream.generation);}catch{stop(stream);return;}
        // Another bubble's expiry callback may have stopped this stream.
        if(stream.active && !stopped)try{stream.deliver({...event});}catch{stop(stream);}
      });}catch{stop(stream);}
    });
  }
  function append(id,payload) {
    const b=state(id),event=Object.freeze({...payload,bubbleId:id,cursor:++b.sequence});
    b.events.push(event);
    if(b.events.length>p.maxEventsPerBubble)b.events.shift();
    return event;
  }
  function flushTransitions() {
    if(stopped){transitions.length=0;return;}
    while(transitions.length) {
      const {id,type}=transitions.shift(),event=append(id,{type});
      for(const stream of streams)if(stream.id===id && stream.active) {
        // An unavoidable member removal never fails because a consumer is slow.
        // End an overloaded stream; its cursor makes any gap explicit on resume.
        if(queued>=p.maxQueuedDeliveries)stop(stream);else enqueue(stream,event);
      }
    }
  }
  function prepareTransitions() {
    flushTransitions();
    if(!scheduled) {
      scheduled=true;
      queueMicrotask(()=>{
        scheduled=false;
        try{dispatch(flushTransitions);}catch{/* Owner fault/shutdown denies delivery. */}
      });
    }
  }
  const api=Object.freeze({
    read,
    send(credential,id,generation,input) {
      const membership=auth(credential,id,generation),v=data(input,['operationKey','text']);
      if(typeof v.operationKey!=='string' || !/^[A-Za-z0-9_-]{1,64}$/.test(v.operationKey) ||
         typeof v.text!=='string' || !v.text.trim() || Buffer.byteLength(v.text,'utf8')>p.maxTextBytes)fail('INVALID_INPUT');
      const key=`${membership.actor}:${v.operationKey}`,signature=JSON.stringify([id,generation,v.text]),previous=receipts.get(key);
      if(previous) {
        if(previous.signature!==signature)fail('OPERATION_CONFLICT');
        return {...previous.event};
      }
      if(receipts.size>=p.maxReceipts)fail('LIMIT_REACHED');
      const targets=[...streams].filter(s=>s.id===id && s.active);
      if(queued+targets.length>p.maxQueuedDeliveries)fail('LIMIT_REACHED');
      const event=append(id,{type:'message',text:v.text});
      receipts.set(key,{signature,event});
      for(const stream of targets)enqueue(stream,event);
      return {...event};
    },
    subscribe(credential,id,generation,options,deliver,onClose=()=>{}) {
      auth(credential,id,generation);
      if(typeof deliver!=='function' || typeof onClose!=='function')fail('INVALID_INPUT');
      const replay=read(credential,id,generation,options).events;
      if(streams.size>=p.maxStreams || [...streams].filter(s=>s.id===id && s.generation===generation).length>=p.maxStreamsPerSeat ||
         queued+replay.length>p.maxQueuedDeliveries)fail('LIMIT_REACHED');
      const stream={credential,id,generation,deliver,onClose,active:true};
      streams.add(stream);
      for(const event of replay)enqueue(stream,event);
      return ()=>stop(stream);
    }
  });
  return {
    api,prepareTransitions,flushTransitions,boundary:id=>bubbles.get(id)?.sequence??0,
    invalidate(actor,id,generation) {
      for(const stream of streams)if(stream.id===id && stream.generation===generation)stop(stream);
      for(const [key,receipt] of receipts)if(key.startsWith(actor+':') && JSON.parse(receipt.signature)[1]===generation)receipts.delete(key);
    },
    closeBubble(id) {
      for(let i=transitions.length-1;i>=0;i--)if(transitions[i].id===id)transitions.splice(i,1);
      bubbles.delete(id);
      for(const [key,receipt] of receipts)if(receipt.event.bubbleId===id)receipts.delete(key);
    },
    shutdown() {
      stopped=true;transitions.length=0;
      for(const stream of streams)stop(stream);
      bubbles.clear();receipts.clear();
    }
  };
}

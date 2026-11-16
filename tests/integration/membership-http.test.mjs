import test from 'node:test';
import assert from 'node:assert/strict';
import {existsSync} from 'node:fs';
import {request} from 'node:http';
import {createMembershipService} from '../../apps/backend/src/modules/membership/membership-service.mjs';
const adapterURL=new URL('../../apps/backend/src/transport/membership-http.mjs',import.meta.url);
async function harness(options={}) {
  assert.ok(existsSync(adapterURL),'real membership HTTP adapter must exist');
  const {createMembershipHttpAdapter}=await import(adapterURL);
  let time=1000;
  const s=createMembershipService({now:()=>time,...options});
  const app=createMembershipHttpAdapter({membership:s.api,maintain:s.bootstrap.tick});
  const {origin}=await app.listen();
  const alice=s.bootstrap.session(),bob=s.bootstrap.session(),a=s.bootstrap.bubble(),b=s.bootstrap.bubble();
  const grant=(who,bubble)=>s.bootstrap.syntheticEligibility(who.credential,bubble.id).token;
  async function req(who,bubble,action,{method=action==='membership'||action==='events'?'GET':'POST',generation,body,headers={},raw}={}) {
    return new Promise((resolve,reject)=>{
      const outgoing=request(`${origin}/v1/bubbles/${bubble.id}/${action}`,{
      method,headers:{Origin:origin,Cookie:`bubble_session=${who.credential}`,
        ...(method==='POST'?{'content-type':'application/json','x-bubbles-request':'1'}:{}),
        ...(generation?{'x-membership-generation':generation}:{}),...headers}
      },response=>{
        let text='';response.on('data',chunk=>text+=chunk);
        response.on('end',()=>{try {resolve({status:response.statusCode,body:JSON.parse(text),headers:new Headers(response.headers)});}catch(e){reject(e);}});
      });
      outgoing.on('error',reject); outgoing.end(method==='POST'?(raw??JSON.stringify(body??{})):undefined);
    });
  }
  const join=(who,bubble)=>req(who,bubble,'join',{body:{eligibilityToken:grant(who,bubble)}});
  return {s,app,origin,alice,bob,a,b,grant,req,join,set:v=>time=v,close:async()=>{await app.close();s.shutdown();}};
}
test('real separate-cookie peers share only their scoped live event audience',{timeout:5000},async()=>{
  const h=await harness();const streams=[];try{
    const ma=(await h.join(h.alice,h.a)).body,mb=(await h.join(h.bob,h.a)).body;
    const outsider=h.s.bootstrap.session(),mc=(await h.join(outsider,h.b)).body;
    const alice=await events(h,h.alice,h.a,ma.generation),bob=await events(h,h.bob,h.a,mb.generation),other=await events(h,outsider,h.b,mc.generation);
    streams.push(alice,bob,other);for(const stream of streams)await stream.next();
    await h.req(h.alice,h.a,'probe',{generation:ma.generation,body:{operationKey:'peer-one'}});
    assert.deepEqual(await bob.next(),{type:'authorized-probe',bubbleId:h.a.id,generation:mb.generation});
    assert.deepEqual(await alice.next(),{type:'authorized-probe',bubbleId:h.a.id,generation:ma.generation});
    await h.req(outsider,h.b,'probe',{generation:mc.generation,body:{operationKey:'other-one'}});
    assert.deepEqual(await other.next(),{type:'authorized-probe',bubbleId:h.b.id,generation:mc.generation});
    h.s.bootstrap.revokeSession(h.bob.credential);assert.equal(await bob.next(),null);
    await h.req(h.alice,h.a,'probe',{generation:ma.generation,body:{operationKey:'peer-two'}});
    assert.equal((await alice.next()).generation,ma.generation);
    assert.equal((await h.req(h.bob,h.a,'probe',{generation:mb.generation,body:{operationKey:'revoked'}})).body.error,'AUTH_REQUIRED');
  }finally{for(const stream of streams)stream.close();await h.close();}
});
test('HTTP duplicate authority headers and slow incomplete bodies fail closed',{timeout:5000},async()=>{
  const h=await harness();try{
    const token=h.grant(h.alice,h.a),path=`/v1/bubbles/${h.a.id}/join`;
    const headers={Origin:h.origin,Cookie:`bubble_session=${h.alice.credential}`,'content-type':'application/json','x-bubbles-request':'1'};
    const duplicate=await wire(h,path,{headers:[...Object.entries(headers).flat(),'Host',new URL(h.origin).host,'Host','evil.invalid'],chunks:[JSON.stringify({eligibilityToken:token})]});
    assert.equal(duplicate.status,400);assert.equal(duplicate.body.error,'INVALID_INPUT');
    const slow=await new Promise((resolve,reject)=>{
      const outgoing=request(`${h.origin}${path}`,{method:'POST',headers:{...headers,'content-length':'100'}},response=>{
        let text='';response.on('data',c=>text+=c);response.on('end',()=>{outgoing.destroy();try{resolve({status:response.statusCode,body:JSON.parse(text)});}catch(e){reject(e);}});
      });outgoing.on('error',reject);outgoing.write('{');
    });
    assert.equal(slow.status,408);assert.equal(slow.body.error,'REQUEST_TIMEOUT');
    assert.equal((await h.req(h.alice,h.a,'membership')).body.error,'MEMBERSHIP_REQUIRED');
    assert.equal((await h.req(h.alice,h.a,'join',{body:{eligibilityToken:token}})).status,200);
  }finally{await h.close();}
});
test('HTTP admission replay, credential confusion, stale generations and closed targets fail closed',{timeout:5000},async()=>{
  const h=await harness();let stream;try{
    const token=h.grant(h.alice,h.a);
    for(const [who,bubble,eligibilityToken] of [[h.bob,h.a,token],[h.alice,h.b,token],[h.alice,h.a,h.alice.credential]]) {
      const r=await h.req(who,bubble,'join',{body:{eligibilityToken}});assert.equal(r.status,403);assert.equal(r.body.error,'ADMISSION_DENIED');
    }
    const concurrent=await Promise.all([h.req(h.alice,h.a,'join',{body:{eligibilityToken:token}}),h.req(h.alice,h.a,'join',{body:{eligibilityToken:token}})]);
    assert.equal(concurrent[0].status,200);assert.equal(concurrent[1].status,200);assert.deepEqual(concurrent[0].body,concurrent[1].body);
    const ma=concurrent[0].body;
    h.set(21000);const retry=await h.req(h.alice,h.a,'join',{body:{eligibilityToken:token}});assert.deepEqual(retry.body,ma);
    assert.equal((await h.req(h.alice,h.a,'membership',{generation:ma.generation,headers:{Cookie:`bubble_session=${token}`}})).body.error,'AUTH_REQUIRED');
    await h.req(h.alice,h.a,'leave',{generation:ma.generation});
    assert.equal((await h.req(h.alice,h.a,'join',{body:{eligibilityToken:token}})).body.error,'ADMISSION_DENIED');
    const unused=h.grant(h.alice,h.a);h.set(61000);
    assert.equal((await h.req(h.alice,h.a,'join',{body:{eligibilityToken:unused}})).body.error,'ADMISSION_DENIED');
    const current=(await h.join(h.alice,h.a)).body;assert.notEqual(current.generation,ma.generation);
    for(const action of ['membership','probe','events'])assert.equal((await h.req(h.alice,h.a,action,{generation:ma.generation,body:{operationKey:'stale'}})).body.error,'MEMBERSHIP_REQUIRED');
    stream=await events(h,h.alice,h.a,current.generation);await stream.next();h.s.bootstrap.closeBubble(h.a.id);
    assert.equal(await stream.next(),null);
    for(const action of ['membership','probe','events','heartbeat','leave'])assert.equal((await h.req(h.alice,h.a,action,{generation:current.generation,body:action==='probe'?{operationKey:'closed'}:{}})).body.error,'BUBBLE_UNAVAILABLE');
  }finally{stream?.close();await h.close();}
});
function wire(h,path,{method='POST',headers={},chunks=[]}={}) {
  return new Promise((resolve,reject)=>{
    const outgoing=request(`${h.origin}${path}`,{method,headers},response=>{
      let text='';response.on('data',c=>text+=c);response.on('end',()=>{try{resolve({status:response.statusCode,body:JSON.parse(text)});}catch(e){reject(e);}});
    });outgoing.on('error',reject);for(const chunk of chunks)outgoing.write(chunk);outgoing.end();
  });
}
test('HTTP refuses GET bodies and returns a bounded rejection for chunked oversized POSTs',{timeout:5000},async()=>{
  const h=await harness();try{
    const m=(await h.join(h.alice,h.a)).body;
    const headers={Origin:h.origin,Cookie:`bubble_session=${h.alice.credential}`,'x-membership-generation':m.generation};
    const get=await wire(h,`/v1/bubbles/${h.a.id}/membership`,{method:'GET',headers:{...headers,'content-length':'2'},chunks:['{}']});
    assert.equal(get.status,400);assert.equal(get.body.error,'INVALID_INPUT');
    const post=await wire(h,`/v1/bubbles/${h.a.id}/probe`,{headers:{...headers,'content-type':'application/json','x-bubbles-request':'1','transfer-encoding':'chunked'},chunks:[' '.repeat(600),' '.repeat(600)]});
    assert.equal(post.status,413);assert.equal(post.body.error,'BODY_TOO_LARGE');
    assert.equal((await h.req(h.alice,h.a,'membership',{generation:m.generation})).status,200);
  }finally{await h.close();}
});
async function events(h,who,bubble,generation) {
  const controller=new AbortController();
  const response=await fetch(`${h.origin}/v1/bubbles/${bubble.id}/events`,{headers:{Origin:h.origin,Cookie:`bubble_session=${who.credential}`,'x-membership-generation':generation},signal:controller.signal});
  assert.equal(response.status,200); assert.equal(response.headers.get('content-type'),'text/event-stream');
  const reader=response.body.getReader(),decoder=new TextDecoder();let buffer='';
  return {close:()=>controller.abort(),async next(){
    for(;;){const end=buffer.indexOf('\n\n');if(end>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);return JSON.parse(frame.slice('data: '.length));}
      const {done,value}=await reader.read();if(done)return null;buffer+=decoder.decode(value,{stream:true});}
  }};
}
test('actual HTTP SSE delivers an authorized event then revocation ends the stream and denies all access',{timeout:5000},async()=>{
  const h=await harness();let stream;try {
    const ma=(await h.join(h.alice,h.a)).body,mb=(await h.join(h.bob,h.b)).body;
    for(const [who,bubble,generation] of [[h.alice,h.b,mb.generation],[h.bob,h.a,ma.generation],[h.alice,h.a,mb.generation]]) {
      const r=await h.req(who,bubble,'events',{generation});assert.equal(r.status,403);assert.equal(r.body.error,'MEMBERSHIP_REQUIRED');
    }
    stream=await events(h,h.alice,h.a,ma.generation);
    assert.deepEqual(await stream.next(),{type:'subscribed',bubbleId:h.a.id,generation:ma.generation});
    assert.equal((await h.req(h.alice,h.a,'probe',{generation:ma.generation,body:{operationKey:'event-1'}})).status,200);
    assert.deepEqual(await stream.next(),{type:'authorized-probe',bubbleId:h.a.id,generation:ma.generation});
    h.s.bootstrap.revokeMembership(h.alice.credential,h.a.id,ma.generation);
    assert.equal(await stream.next(),null);
    for(const action of ['membership','probe','events','heartbeat','leave']) {
      const r=await h.req(h.alice,h.a,action,{generation:ma.generation,body:action==='probe'?{operationKey:'event-2'}:{}});
      assert.equal(r.status,403);assert.equal(r.body.error,'MEMBERSHIP_REQUIRED');
    }
    assert.equal((await h.req(h.bob,h.b,'membership',{generation:mb.generation})).status,200);
  }finally{stream?.close();await h.close();}
});
test('HTTP heartbeat renews only a live lease; idle SSE expires without a further request',{timeout:5000},async()=>{
  const h=await harness();let stream;try {
    const m=(await h.join(h.alice,h.a)).body;
    stream=await events(h,h.alice,h.a,m.generation);await stream.next();
    h.set(60000);const renewed=await h.req(h.alice,h.a,'heartbeat',{generation:m.generation});
    assert.equal(renewed.status,200);assert.equal(renewed.body.expiresAtMs,120000);
    h.set(120000);assert.equal(await stream.next(),null);
    const r=await h.req(h.alice,h.a,'heartbeat',{generation:m.generation});assert.equal(r.body.error,'MEMBERSHIP_REQUIRED');
    const next=(await h.join(h.alice,h.a)).body;assert.notEqual(next.generation,m.generation);
    assert.equal((await h.req(h.alice,h.a,'leave',{generation:next.generation})).body.left,true);
    assert.equal((await h.req(h.alice,h.a,'membership',{generation:next.generation})).body.error,'MEMBERSHIP_REQUIRED');
  }finally{stream?.close();await h.close();}
});
test('HTTP origin, CSRF, method, body, host and cookie guards reject without joining',{timeout:5000},async()=>{
  const h=await harness(); try {
    const token=h.grant(h.alice,h.a);
    const cases=[
      [{headers:{Origin:'https://evil.invalid'}},403,'ORIGIN_DENIED'],
      [{headers:{'x-bubbles-request':'0'}},403,'CSRF_DENIED'],
      [{headers:{'content-type':'text/plain'}},415,'CONTENT_TYPE_REQUIRED'],
      [{headers:{Cookie:`bubble_session=${h.alice.credential}; bubble_session=${h.bob.credential}`}},401,'AUTH_REQUIRED'],
      [{headers:{Cookie:`host_session=${h.alice.credential}`}},401,'AUTH_REQUIRED'],
      [{headers:{Host:'evil.invalid'}},403,'ORIGIN_DENIED'],
      [{headers:{'sec-fetch-site':'cross-site'}},403,'ORIGIN_DENIED'],
      [{raw:'{"eligibilityToken":'},400,'INVALID_INPUT'],
      [{raw:' '.repeat(1025)},413,'BODY_TOO_LARGE'],
      [{body:{eligibilityToken:token,userId:'pretend',local:true}},400,'INVALID_INPUT'],
      [{method:'PUT'},404,'NOT_FOUND']
    ];
    for(const [options,status,error] of cases) {
      const r=await h.req(h.alice,h.a,'join',{body:{eligibilityToken:token},...options});
      assert.equal(r.status,status,JSON.stringify(options)); assert.equal(r.body.error,error);
    }
    const missing=await fetch(`${h.origin}/v1/bubbles/${h.a.id}/join`,{method:'POST',headers:{'content-type':'application/json','x-bubbles-request':'1',Cookie:`bubble_session=${h.alice.credential}`},body:JSON.stringify({eligibilityToken:token})});
    assert.equal(missing.status,403);assert.equal((await missing.json()).error,'ORIGIN_DENIED');
    const r=await h.req(h.alice,h.a,'membership');assert.equal(r.body.error,'MEMBERSHIP_REQUIRED');
    const valid=await h.req(h.alice,h.a,'join',{body:{eligibilityToken:token}});assert.equal(valid.status,200);
    assert.equal(valid.headers.get('x-content-type-options'),'nosniff');
    assert.equal(valid.headers.get('access-control-allow-origin'),null);
  } finally {await h.close();}
});
test('real loopback HTTP requests enforce separate cookie membership on read and probe writes', {timeout:5000},async()=>{
  const h=await harness(); try {
    let r=await h.req(h.alice,h.a,'membership'); assert.equal(r.status,403); assert.equal(r.body.error,'MEMBERSHIP_REQUIRED');
    const ma=(await h.join(h.alice,h.a)).body,mb=(await h.join(h.bob,h.b)).body;
    assert.equal((await h.req(h.alice,h.a,'membership',{generation:ma.generation})).status,200);
    assert.equal((await h.req(h.alice,h.a,'probe',{generation:ma.generation,body:{operationKey:'actual-write'}})).status,200);
    for(const action of ['membership','probe']) {
      const r=await h.req(h.alice,h.b,action,{generation:mb.generation,body:{operationKey:'cross'}});
      assert.equal(r.status,403); assert.equal(r.body.error,'MEMBERSHIP_REQUIRED');
    }
    assert.equal((await h.req(h.bob,h.a,'membership',{generation:ma.generation})).status,403);
    assert.equal((await fetch(`${h.origin}/bootstrap/session`,{method:'POST'})).status,404);
    assert.equal((await h.req(h.alice,h.a,'membership',{generation:ma.generation})).headers.get('cache-control'),'no-store');
  } finally {await h.close();}
});

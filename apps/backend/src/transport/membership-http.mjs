import {createServer} from 'node:http';

export const MEMBERSHIP_HTTP_LIMITS=Object.freeze({
  maxHeaderBytes:4096,maxBodyBytes:1024,requestTimeoutMs:2000,
  maxStreamMs:60000,maintenanceTickMs:25,maxConnections:128,maxRequestsPerSocket:100
});
const statusFor=code=>({AUTH_REQUIRED:401,MEMBERSHIP_REQUIRED:403,ADMISSION_DENIED:403,
  BUBBLE_UNAVAILABLE:404,INVALID_CURSOR:400,CURSOR_EXPIRED:409,INVALID_INPUT:400,LIMIT_REACHED:429,OPERATION_CONFLICT:409,
  INVALID_CLOCK:503,CLOCK_FAULT:503,SERVICE_CLOSED:503,ORIGIN_DENIED:403,CSRF_DENIED:403,
  CONTENT_TYPE_REQUIRED:415,BODY_TOO_LARGE:413,REQUEST_TIMEOUT:408})[code]??500;
const fail=code=>{throw Object.assign(new Error(code),{code});};

// No automatic listener, issuer, host login or control-plane route.
// maintain is a PRIVATE composition capability, never a client operation.
export function createMembershipHttpAdapter({membership,messaging,maintain}) {
  if(!membership || typeof maintain!=='function') throw new TypeError('PRIVATE_COMPOSITION_REQUIRED');
  let origin=null,timer=null;
  const closers=new Set();
  function json(res,status,body) {res.writeHead(status,{'content-type':'application/json'});res.end(JSON.stringify(body));}
  const limits=MEMBERSHIP_HTTP_LIMITS;
  const server=createServer({maxHeaderSize:limits.maxHeaderBytes,requestTimeout:limits.requestTimeoutMs,headersTimeout:limits.requestTimeoutMs},async(req,res)=>{
    res.setHeader('cache-control','no-store');
    res.setHeader('x-content-type-options','nosniff');
    res.setHeader('referrer-policy','no-referrer');
    res.setHeader('content-security-policy',"default-src 'none'; frame-ancestors 'none'");
    try {
      const route=/^\/v1\/bubbles\/(bub_[a-f0-9]{32})\/(join|membership|probe|events|heartbeat|leave|messages|message-events)$/.exec(req.url);
      if(!route || (['messages','message-events'].includes(route[2])&&!messaging) || !(route[2]==='messages'?['GET','POST']:['membership','events','message-events'].includes(route[2])?['GET']:['POST']).includes(req.method)) return json(res,404,{error:'NOT_FOUND'});
      const unique=['host','origin','cookie','content-type','content-length','transfer-encoding','x-membership-generation','x-bubbles-request','x-message-cursor'];
      const seen=new Set();
      for(let i=0;i<req.rawHeaders.length;i+=2) {
        const name=req.rawHeaders[i].toLowerCase();
        if(unique.includes(name)) {if(seen.has(name))fail('INVALID_INPUT');seen.add(name);}
      }
      if(req.headers.host!==new URL(origin).host || req.headers.origin!==origin || req.headers['sec-fetch-site']==='cross-site') fail('ORIGIN_DENIED');
      if(req.method==='POST') {
        if(req.headers['x-bubbles-request']!=='1') fail('CSRF_DENIED');
        if(req.headers['content-type']!=='application/json') fail('CONTENT_TYPE_REQUIRED');
      }
      if(req.method==='GET' && (req.headers['transfer-encoding'] || Number(req.headers['content-length']??0)!==0)) fail('INVALID_INPUT');
      const cookies=(req.headers.cookie??'').split(';').map(s=>s.trim()).filter(s=>s.startsWith('bubble_session='));
      if(cookies.length!==1) fail('AUTH_REQUIRED');
      const credential=cookies[0].slice('bubble_session='.length);
      if(!/^ses_[a-f0-9]{64}$/.test(credential)) fail('AUTH_REQUIRED');
      const id=route[1],generation=req.headers['x-membership-generation'];
      if(route[2]==='membership') return json(res,200,membership.read(credential,id,generation));
      const messageRoute=['messages','message-events'].includes(route[2]);
      let after=0;if(messageRoute){const cursor=req.headers['x-message-cursor']??'0';if(!/^(0|[1-9][0-9]{0,15})$/.test(cursor)||!Number.isSafeInteger(Number(cursor)))fail('INVALID_CURSOR');after=Number(cursor);}
      if(route[2]==='messages'&&req.method==='GET')return json(res,200,messaging.read(credential,id,generation,{after}));
      if(route[2]==='events'||route[2]==='message-events') {
        let stop=()=>{},closed=false;
        const close=(unsubscribe=true)=>{
          if(closed)return;closed=true;
          // Owner invalidation already stopped the subscription. Never reenter it.
          if(unsubscribe){try{stop();}catch{/* A sealed owner must not prevent transport cleanup. */}}
          closers.delete(close);clearTimeout(deadline);res.end();
        };
        const deadline=setTimeout(close,limits.maxStreamMs);deadline.unref();
        try {
          const subscribe=route[2]==='message-events'?((deliver,close)=>messaging.subscribe(credential,id,generation,{after},deliver,close)):((deliver,close)=>membership.subscribe(credential,id,generation,deliver,close));
          stop=subscribe(event=>{
            if(closed)return;
            if(!res.write(`data: ${JSON.stringify(event)}\n\n`)) {
              close(false);
              // Delivery owns the service gate; cancellation gets a fresh entry.
              queueMicrotask(()=>{try{stop();}catch{/* Already shut down/fail-closed. */}});
            }
          },()=>close(false));
        } catch(error) {clearTimeout(deadline);throw error;}
        closers.add(close);res.once('close',close);
        res.writeHead(200,{'content-type':'text/event-stream'});
        if(!res.write(`data: ${JSON.stringify(route[2]==='message-events'?{type:'subscribed',bubbleId:id}:{type:'subscribed',bubbleId:id,generation})}\n\n`))close();
        return;
      }
      const bodyLimit=route[2]==='messages'?8192:limits.maxBodyBytes;
      if(Number(req.headers['content-length']??0)>bodyLimit) fail('BODY_TOO_LARGE');
      const raw=await new Promise((resolve,reject)=>{
        const chunks=[];let length=0;
        const deadline=setTimeout(()=>{req.pause();reject(Object.assign(new Error('REQUEST_TIMEOUT'),{code:'REQUEST_TIMEOUT'}));},limits.requestTimeoutMs);
        deadline.unref();
        req.on('data',chunk=>{
          length+=chunk.length;
          if(length>bodyLimit) {clearTimeout(deadline);req.pause();reject(Object.assign(new Error('BODY_TOO_LARGE'),{code:'BODY_TOO_LARGE'}));}
          else chunks.push(chunk);
        });
        req.once('end',()=>{clearTimeout(deadline);resolve(Buffer.concat(chunks).toString('utf8'));});
        req.once('error',error=>{clearTimeout(deadline);reject(error);});
      });
      let body;try{body=JSON.parse(raw);}catch{fail('INVALID_INPUT');}
      let result;
      if(route[2]==='messages')result=messaging.send(credential,id,generation,body);
      else if(route[2]==='join') result=membership.join(credential,id,body);
      else if(route[2]==='probe') result=membership.write(credential,id,generation,body);
      else {
        if(!body || Object.getPrototypeOf(body)!==Object.prototype || Object.keys(body).length) fail('INVALID_INPUT');
        result=membership[route[2]](credential,id,generation);
      }
      json(res,200,result);
    } catch(error) {
      res.setHeader('connection','close');
      const status=statusFor(error.code);json(res,status,{error:status===500?'INTERNAL_ERROR':error.code});
    }
  });
  server.maxConnections=limits.maxConnections;server.maxRequestsPerSocket=limits.maxRequestsPerSocket;
  return Object.freeze({
    async listen() {
      if(origin) throw new Error('ALREADY_LISTENING');
      await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
      origin=`http://127.0.0.1:${server.address().port}`;
      timer=setInterval(()=>{try{maintain();}catch{for(const close of closers)close();}},limits.maintenanceTickMs);timer.unref();
      return {origin};
    },
    async close() {clearInterval(timer);for(const close of closers)close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
  });
}

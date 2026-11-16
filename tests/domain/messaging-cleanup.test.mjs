import test from 'node:test';
import assert from 'node:assert/strict';
import {createOwnedMessaging} from '../../apps/backend/src/modules/membership/messaging-service.mjs';
import {createMembershipService} from '../../apps/backend/src/modules/membership/membership-service.mjs';
test('standalone private owner seals session allocation before messaging close callbacks',()=>{
 const s=createMembershipService({now:()=>0}),{credential}=s.bootstrap.session(),b=s.bootstrap.bubble(),grant=s.bootstrap.syntheticEligibility(credential,b.id),seat=s.join(credential,b.id,{eligibilityToken:grant.token});let error;
 s.messaging.subscribe(credential,b.id,seat.generation,{after:0},()=>{},()=>{try{s.bootstrap.session();}catch(e){error=e.code;}});s.shutdown();assert.equal(error,'SERVICE_CLOSED');
});
test('private shutdown clears unpublished transition journal even without a later read',()=>{
 const transitions=[{id:'bubble',type:'member-left'}],m=createOwnedMessaging({authorize:()=>{},dispatch:fn=>fn(),transitions});m.shutdown();assert.equal(transitions.length,0);
});
test('queue overflow onClose shutdown cannot recreate state from the remainder of a running transition flush',()=>{
 const transitions=[],m=createOwnedMessaging({authorize:()=>({actor:'private-unit',historyFloor:0}),dispatch:fn=>fn(),transitions});m.api.subscribe('c','bubble','g',{after:0},()=>{},()=>m.shutdown());m.api.subscribe('c','bubble','g',{after:0},()=>{});for(let i=0;i<128;i++)m.api.send('c','bubble','g',{operationKey:`p${i}`,text:'bounded'});transitions.push({id:'bubble',type:'member-left'},{id:'other',type:'member-left'});m.flushTransitions();assert.equal(m.boundary('other'),0);assert.equal(transitions.length,0);m.shutdown();
});
test('private pop cleanup discards pending actual transition records and cannot recreate retained state',()=>{
 const transitions=[],m=createOwnedMessaging({authorize:()=>{throw Error('not used');},dispatch:fn=>fn(),transitions});
 transitions.push({id:'bubble',type:'member-joined'});m.flushTransitions();assert.equal(m.boundary('bubble'),1);transitions.push({id:'bubble',type:'member-left'});m.closeBubble('bubble');m.flushTransitions();assert.equal(m.boundary('bubble'),0);m.shutdown();
});

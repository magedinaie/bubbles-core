import {randomBytes} from 'node:crypto';

export const BUBBLE_PREPARATION_LIMITS=Object.freeze({version:'pc03-bubble-preparation-1',maxIdAttempts:16});
// INTERNAL storage port: not an API, membership check or authorization boundary.
export function createBubbleRepository(options = {}) {return buildRepository(options).repo;}
// PRIVATE composition port; never attached to public repository or transport.
export function createOwnedBubbleRepository(options = {}) {return buildRepository(options);}
function buildRepository({now = Date.now} = {}) {
  const bubbles = new Map();
  const invitations = new Map();
  function timestamp() {
    const value = now();
    if (!Number.isSafeInteger(value) || value < 0) {
      throw Object.assign(new TypeError('INVALID_CLOCK'), {code:'INVALID_CLOCK'});
    }
    return value;
  }
  function get(id) {
    if (typeof id !== 'string' || !/^bub_[a-f0-9]{32}$/.test(id)) {
      throw Object.assign(new TypeError('INVALID_BUBBLE_ID'), {code:'INVALID_BUBBLE_ID'});
    }
    const bubble = bubbles.get(id);
    return bubble ? {...bubble} : null;
  }
  const repo = {
    close(id) {
      const bubble = get(id);
      if (!bubble || bubble.state === 'closed') return bubble;
      bubble.state = 'closed';
      bubble.closedAt = timestamp();
      bubble.version += 1;
      bubbles.set(id, bubble);
      for (const [token, entry] of invitations) {
        if (entry.bubbleId === id) invitations.delete(token);
      }
      return {...bubble};
    },
    issueInvitation(bubbleId, options) {
      const bubble = get(bubbleId);
      const issuedAt = timestamp();
      const descriptor = options && Object.getOwnPropertyDescriptor(options, 'expiresAt');
      const expiresAt = descriptor?.value;
      if (!options || Object.getPrototypeOf(options) !== Object.prototype ||
          Reflect.ownKeys(options).length !== 1 || !descriptor || !Object.hasOwn(descriptor, 'value') ||
          !Number.isSafeInteger(expiresAt) || expiresAt <= issuedAt) {
        throw Object.assign(new TypeError('INVALID_INVITATION_OPTIONS'), {code:'INVALID_INVITATION_OPTIONS'});
      }
      if (!bubble) throw Object.assign(new Error('BUBBLE_NOT_FOUND'), {code:'BUBBLE_NOT_FOUND'});
      if (bubble.state === 'closed') {
        throw Object.assign(new Error('BUBBLE_CLOSED'), {code:'BUBBLE_CLOSED'});
      }
      let token;
      do { token = `inv_${randomBytes(32).toString('hex')}`; } while (invitations.has(token));
      const invitation = {token, bubbleId, expiresAt};
      for (const [previous, entry] of invitations) {
        if (entry.bubbleId === bubbleId) invitations.delete(previous);
      }
      invitations.set(token, invitation);
      return {...invitation};
    },
    resolveInvitation(token) {
      if (typeof token !== 'string' || !/^inv_[a-f0-9]{64}$/.test(token)) {
        throw Object.assign(new TypeError('INVALID_INVITATION_TOKEN'), {code:'INVALID_INVITATION_TOKEN'});
      }
      const invitation = invitations.get(token);
      if (!invitation) return null;
      if (timestamp() >= invitation.expiresAt) {
        invitations.delete(token);
        return null;
      }
      return get(invitation.bubbleId);
    },
    create(...args) {
      if (args.length) throw Object.assign(new TypeError('INVALID_CREATE_ARGUMENTS'), {code:'INVALID_CREATE_ARGUMENTS'});
      let id;
      do { id = `bub_${randomBytes(16).toString('hex')}`; } while (bubbles.has(id));
      const bubble = {id, state:'active', version:1, createdAt:timestamp(), closedAt:null};
      bubbles.set(id, bubble);
      return {...bubble};
    },
    get
  };
  let pending=null,closing=null;
  const internal=Object.freeze({
    prepareClose(id) {
      const original=bubbles.get(id);if(!original)return null;
      const record=original.state==='closed'?original:{...original,state:'closed',closedAt:timestamp(),version:original.version+1};
      const handle=Object.freeze({});closing={handle,original,record};return handle;
    },
    validateClose(handle) {if(!closing || closing.handle!==handle || bubbles.get(closing.original.id)!==closing.original)throw Object.assign(new Error('INVALID_PREPARATION'),{code:'INVALID_PREPARATION'});},
    publishClose(handle) {
      internal.validateClose(handle);bubbles.set(closing.record.id,closing.record);
      for(const [token,entry] of invitations)if(entry.bubbleId===closing.record.id)invitations.delete(token);
      closing=null;
    },
    prepareCreate() {
      let id;
      for(let attempt=0;attempt<BUBBLE_PREPARATION_LIMITS.maxIdAttempts;attempt++) {
        const candidate=`bub_${randomBytes(16).toString('hex')}`;
        if(!bubbles.has(candidate)){id=candidate;break;}
      }
      if(!id)throw Object.assign(new Error('BUBBLE_ID_UNAVAILABLE'),{code:'BUBBLE_ID_UNAVAILABLE'});
      const record={id,state:'active',version:1,createdAt:timestamp(),closedAt:null};
      const handle=Object.freeze({});
      pending={handle,record,dto:{...record}};
      return Object.freeze({handle,bubble:pending.dto});
    },
    validate(handle) {if(!pending || pending.handle!==handle) throw Object.assign(new Error('INVALID_PREPARATION'),{code:'INVALID_PREPARATION'});},
    // Only after validate, under the shared owner gate. No callbacks or allocation ports.
    publish(handle) {internal.validate(handle);bubbles.set(pending.record.id,pending.record);pending=null;},
    inspect() {return {bubbles:bubbles.size,activeBubbles:[...bubbles.values()].filter(b=>b.state==='active').length};}
  });
  return {repo,internal};
}

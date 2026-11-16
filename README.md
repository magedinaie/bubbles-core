# Bubbles Core

Backend core of **Bubbles**, a proximity-first social app: people who are physically near each other can form a short-lived "bubble" and chat inside it. The goal is to get people talking face to face, then get out of the way.

This repository is a **public excerpt**. It contains the membership, messaging and transport layers, plus their tests. The proximity detection, bubble formation and merge logic are not published here.

## What's in here

| Path | What it does |
|---|---|
| `apps/backend/src/modules/membership/` | Per-bubble membership: server-issued sessions, seats with a generation counter, leases, heartbeat, Leave |
| `apps/backend/src/modules/membership/*messaging*` | Chat scoped to one bubble: cursors, history floor per seat, idempotent sends, bounded queues |
| `apps/backend/src/modules/bubbles/` | Bubble repository (in-memory and SQLite variants) |
| `apps/backend/src/transport/membership-http.mjs` | HTTP and Server-Sent Events adapter with origin, CSRF and size checks |
| `tests/` | 14 test files |

## Design points

- **Every read, write and subscription is checked against the caller's membership in that specific bubble.** A session for one bubble gives no access to another.
- **Seats carry a generation.** After Leave or expiry, an old generation cannot send, read or resubscribe.
- **Chat stays inside its bubble.** There is no cross-bubble messaging.
- **Delivery is bounded.** Queues, streams, receipts and message sizes have hard limits, and the service fails closed when they are hit.
- **Dependency-free.** Node standard library only.

## Run the tests

Requires Node.js with the built-in test runner and `node:sqlite` (developed on Node 26).

```
npm test
```

At the last run: 89 tests, 89 passing. That run was on the author's machine only, with no CI.

## Honest limits

- **Not end-to-end encrypted.** Messages are readable by the server. HTTPS and membership checks are not E2EE. E2EE is planned, not built.
- **State is in process memory** (the SQLite repository is a separate storage variant). A restart loses bubbles and messages.
- **Proximity is not included here.** Nothing in this excerpt senses or verifies physical location.
- **No production authentication, scaling or deployment work.** This is a development codebase.
- Not a complete app. The excerpt does not run as a product on its own.

## Licence

[PolyForm Noncommercial 1.0.0](LICENSE). You may read, study and use this code for noncommercial purposes. Commercial use needs a separate licence from the author.

Required Notice: Copyright magedinaie (https://github.com/magedinaie)

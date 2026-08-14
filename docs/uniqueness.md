# Uniqueness and idempotency

## Uniqueness

`unique` decides which of several jobs sharing a key survives. `key` reads the **validated** payload and returns that key, so one definition can be unique per user, per entity or per account. Redis decides it atomically at the moment of the enqueue, so two processes racing on the same key still leave one winner.

| Mode        | Which job wins                                                   |
| ----------- | ---------------------------------------------------------------- |
| `keepFirst` | the first enqueued; every later one is dropped                   |
| `keepLast`  | the last enqueued inside the window; it replaces the one waiting |
| `noOverlap` | the running one, and the latest of those enqueued while it runs  |

Use `keepFirst` for work that must happen once. A welcome email per user: the signup flow fires twice, the second enqueue is dropped, and the user is welcomed once.

```ts
export const sendWelcomeEmail = defineJob({
	name: "email.welcome",
	queue: "mail",
	payload: type({ userId: "string" }),
	delivery: {
		unique: { key: (data) => data.userId, mode: "keepFirst", ttlMs: 24 * 60 * 60 * 1_000 },
	},
})
```

`ttlMs` is how long the key stays taken, counted from the enqueue and outliving the job itself. Leave it out and the key is released the moment the job finishes, so the next enqueue after that is a new job.

Use `keepLast` for work that should collapse into its latest version. Rebuilding a search index for an entity: ten edits in five seconds enqueue ten jobs, and the index is rebuilt once, from the tenth payload.

```ts
export const rebuildIndex = defineJob({
	name: "search.rebuild",
	queue: "search",
	payload: type({ entityId: "string" }),
	delivery: { unique: { key: (data) => data.entityId, mode: "keepLast", ttlMs: 5_000 } },
})
```

`keepLast` needs `ttlMs` and refuses the enqueue without it, because a window of zero has nothing to collapse. The window also **delays** the job: it waits `ttlMs` before becoming available, and that wait is exactly what gives a later enqueue the room to replace it. When the definition also names `delayMs`, the larger of the two wins — so a 5 second window and a 30 second delay run 30 seconds later, still collapsed.

Use `noOverlap` for work that must not run twice at once. One sync per account: while a sync runs, further enqueues start no second one; the latest of them is kept and runs when the first finishes.

```ts
export const syncAccount = defineJob({
	name: "account.sync",
	queue: "sync",
	payload: type({ accountId: "string" }),
	delivery: { unique: { key: (data) => data.accountId, mode: "noOverlap" } },
})
```

`noOverlap` ignores `ttlMs`, so jubs drops it: the key lives exactly as long as the job it guards. At most two jobs per key exist at any moment — one running, one waiting.

The memory driver throws on `unique`. Uniqueness is decided inside Redis, atomically and on a clock; an inline imitation would agree with your test and disagree with production. Test it against `redisDriver`.

A definition that declares `unique` cannot be enqueued inside `jobs.atomic(fn, { tx })` either, and that enqueue throws: uniqueness does not survive the [outbox](./outbox.md#outbox-and-relay).

## Idempotency

`idempotencyKey` stops a second **execution**; `unique` stops a second **enqueue**. That is the whole difference. Uniqueness works before the job exists and decides which one survives; the idempotency key works when a delivery is already in a worker's hands, and decides whether the handler runs at all.

The key sits on the definition, beside the payload schema — it is not a delivery option. The function reads the **validated** payload and returns the key, exactly as `unique`'s `key` does.

```ts
export const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ orderId: "string", cents: "number" }),
	idempotencyKey: (data) => data.orderId,
})
```

Two deliveries of that job for the same `orderId` capture the card once. The second one never reaches the payment gateway.

A key is in one of three states, and the state decides what the delivery does.

| State                                                  | What the delivery does                           |
| ------------------------------------------------------ | ------------------------------------------------ |
| absent                                                 | takes the key and runs the handler               |
| held by a running delivery, under a lease that expires | is rescheduled, and arrives again later          |
| complete, with a kept record                           | skips the handler and gives back the kept result |

**A rescheduled delivery consumes no attempt.** It is moved back to the queue with a delay and delivered again; the attempt counter does not move, `onAttemptFailed` and `onDead` do not fire, and nothing is buried. There is no ceiling on it either — a delivery that keeps meeting a held key keeps being rescheduled, for as long as the key stays held. The lease is what ends that wait: when it expires, the next delivery takes the key and runs.

The wait follows a doubling cadence: **100 ms** on the first wait, then 200, 400, 800, and the **1 second** ceiling from the fifth on. What counts the steps is not the attempt counter, which stays still — it is the number of times Redis has handed this job to a worker, which climbs once per reschedule and never resets while the delivery lives. The cadence starts short on purpose — a duplicate delivered behind a job that finishes in milliseconds settles in milliseconds too, instead of waiting out the lease.

Four values are fixed today and configurable by nothing: the lease is **30 seconds**, it is renewed every **10 seconds** while the handler runs, a complete key keeps its result for **24 hours**, and a rescheduled delivery waits the doubling cadence above, from a **100 ms** floor to a **1 second** ceiling. Renewal is why a handler slower than 30 seconds is safe — the lease follows it for as long as it runs.

A kept result is stored as JSON, so a repeated delivery gets the JSON projection of it: a `Date` comes back a string, and anything JSON drops is dropped.

It also has a size limit of **64 KB**. Above it, jubs keeps the completion marker alone: the key still counts as complete, the handler is still skipped, but the repeated delivery gets `undefined` instead of the result. Return a receipt id, not the receipt.

A result JSON cannot serialise at all — a circular object, a `BigInt` — leaves a state jubs cannot repair. jubs keeps the completion marker alone, as above, and reports the job a success: `onSuccess` fires. BullMQ then throws while writing the return value, outside jubs' reach and after the dispatch has returned, so the attempt is failed from under it. The delivery arrives again, meets the complete key, skips the handler and replays the empty marker — which serialises — so the job settles `completed` while still carrying the `failedReason` of the attempt that threw. `onAttemptFailed` and `onDead` never fire, and nothing is buried. On a definition with no key the handler runs again on every attempt and the job ends `failed`. Nothing warns you either way, so return a value JSON can hold.

The lease is what makes this correct, and the reason is worth spelling out. Marking the key before running and skipping it on the repeat would be at-most-once, not idempotent: a worker killed between the mark and the end would leave a key that says done over work that never happened, and every later delivery would report success for a charge nobody made. The lease says _in progress_, not _done_, and it expires — so a killed worker gives the job back instead of losing it.

**A handler that throws releases the lease.** It is not held for the rest of its 30 seconds: the retry BullMQ schedules finds the key absent and runs. A failed job retries at its own pace, as it would with no key at all.

Hooks see a rescheduled delivery as a start with no end. `onStart` fires before the key is read, so a delivery that meets a held lease fires `onStart` and nothing else. A hooks consumer that pairs a start with an end has to tolerate a start alone — the same way it has to tolerate an end alone, which a job whose name no handler owns already produces.

`dead.replay` and `dead.discard` are at-least-once: two operators acting on the same dead id can both succeed, and the job is enqueued twice. `idempotencyKey` is the remedy — the second delivery skips the handler.

A scheduled job reaches exactly-once only through `idempotencyKey`. The occurrence a scheduler produces does not carry `unique`, as described above, so the key is the only exclusion left to it.

### Forgetting a key

A complete key holds its result for 24 hours, and every delivery of that payload inside the window skips the handler. `jobs.idempotency.forget(definition, data)` deletes the key, so the next delivery runs the work for real.

Reach for it when the kept result is the wrong answer: a bug you fixed after the run, a handler that reported a success it did not have, a payload you want to run again on purpose. Clearing the queue does not do it — keys live under `jubs:idem:`, outside the `bull:<queue>:` namespace, so an `obliterate`, a purge from a Bull Board, a redeploy and a restart all leave them exactly where they were.

```ts
const forgotten = await jobs.idempotency.forget(chargeCard, { orderId: "ord_1", cents: 500 })

if (forgotten.outcome === "running") {
	console.log("a delivery holds that key right now — ask again once it ends")
}
```

The payload is validated first, and the key is computed from what the schema gave back — the very key a delivery of that payload computes. An invalid payload fails the call and deletes nothing.

```ts
type ForgetResult =
	| { outcome: "forgotten" }
	| { outcome: "not_found" }
	| { outcome: "running" }
	| { outcome: "not_guarded" }
```

`not_guarded` is a definition that declares no `idempotencyKey`: there is no key to forget, which is an answer, not a failure.

**Forgetting is refused while a delivery holds the key.** The outcome is `running` and nothing is deleted. What is stored then is a lease, not a result: a worker took that key and its handler is running under it right now. Deleting it would take the lease out from under that handler, and the next delivery would find the key absent, take it and run a second body beside the first — the double execution the lease exists to prevent. Every other operation on a key carries the token that names the one possession it means; `forget` is asked by an operator, not by the delivery that took the key, so it carries none, and refusing is the only safe move it has. Wait for the delivery to end and ask again: the key is complete by then, and forgetting it deletes a result, not a lease.

**`dead.replay` forgets the key itself, before it enqueues.** That is the case which made this operation necessary. A job with both `timeoutMs` and `idempotencyKey` has its attempt and its body judged apart: the attempt fails on its deadline and the job is buried as `attempts_exhausted`, while the detached body keeps running and, when it returns, completes that job's key with its result. A replay that met that complete key would hand the kept result straight back and never call the handler — green, with nothing run.

**A replay meeting a held key is refused.** Right after a `timeoutMs` burial that is the ordinary state, not a rare one: the body that outlived the attempt still holds the key, and jubs keeps renewing its lease for as long as that body lives, so the lease does not expire under it. A replay enqueued over it would be rescheduled onto the held key, and the delivery after that would hand back the result the body kept — with the dead record already dropped, so nothing would be left to replay. `dead.replay` throws instead, before it enqueues anything, which is what leaves the dead record where it is. Ask again once the body has ended: the key is complete by then, forgetting it deletes a result, and the job runs. So a replay either runs the handler or refuses — it never comes back green over work nobody did.

The memory driver keeps the absent and the complete states for real: a repeated delivery of a completed key skips the handler and gives back the kept result — as the JSON projection of it, the same one Redis gives back — and a failing handler releases the key. Forgetting a key is real here too, because it needs no clock either: a key a delivery holds is refused and stays where it is, and a complete key is deleted. It keeps no clock, so `leaseMs` and the retention are ignored and a lease never expires on its own. A delivery that meets a held lease has to be rescheduled, which needs a clock, so the memory driver throws instead. Test a held lease, an expired lease and a killed worker against `redisDriver`.

# @juicerq/jubs

A typed job library over BullMQ. BullMQ is a good queue and a poor contract: a job is a string name plus untyped `data`, so every team rebuilds payload validation, a name-to-handler lookup, and a delivery policy that lives at the call site instead of with the job. jubs gives a job a name, a payload schema, a delivery policy and a place to run, so producers and consumers never share code. You declare a definition once and get a typed enqueue, a typed handler, validation on both sides, and correct behaviour for the expensive failure modes — an enqueue inside a database transaction that rolls back, a schedule deleted from the code but still firing in Redis, a job that runs twice after a pod restart, an envelope a rolling deploy cannot parse.

## Install

```sh
bun add @juicerq/jubs bullmq
```

You also need a Redis client. jubs does not depend on one — you create the connection, and you close it.

```sh
bun add ioredis
```

## Defining a job

A definition is the producer-side description of a job: its name, its queue and its payload schema. `defineJob` returns a plain data object. It performs no I/O, so it is safe to import from any process.

`payload` is any [Standard Schema](https://standardschema.dev) validator — arktype, Zod and Valibot all qualify. The enqueue argument and the handler's `data` are both inferred from it.

```ts
// jobs/definitions.ts
import { defineJob } from "@juicerq/jubs"
import { type } from "arktype"

export const sendWelcomeEmail = defineJob({
	name: "email.welcome",
	queue: "mail",
	payload: type({ userId: "string", locale: "'en' | 'pt'" }),
})
```

## Handling a job

A handler is the consumer-side function that runs a job. One handler per definition. `defineHandler` binds the two, and `data` arrives already validated.

The second argument is the handler context: `id` is the job id, `attempt` is 1-based, `maxAttempts` is how many attempts this job gets in total, and `origin` says what caused the job to exist.

```ts
// jobs/handlers.ts
import { defineHandler } from "@juicerq/jubs"
import { sendWelcomeEmail } from "./definitions"

export const welcomeEmailHandler = defineHandler(sendWelcomeEmail, async (data, context) => {
	console.log(`attempt ${context.attempt} of ${context.maxAttempts} for job ${context.id}`)

	await mailer.send(data.userId, data.locale)
})
```

## Typing what a handler returns

`result` is the schema of the value the handler resolves. Like `payload`, it is any Standard Schema validator, and it sits on the definition — so the producer's side of the code knows the shape of the answer without importing the handler.

```ts
export const renderInvoice = defineJob({
	name: "invoice.render",
	queue: "reports",
	payload: type({ invoiceId: "string" }),
	result: type({ url: "string.url", bytes: "number" }),
})
```

The handler's return type is inferred from it. A definition that declares no `result` returns `unknown`, and jubs validates nothing at all.

The value is validated the moment the handler resolves, before anything is stored. jubs keeps what the schema gives back, never what the handler returned. The handler therefore returns the schema's **input**: a schema that transforms cannot validate its own output, which is the same rule `payload` follows.

The validated value is what the handler's return value becomes. No client method reads it back — `jobs.get` answers with the job's state, not with its result. Underneath it is BullMQ's own `returnvalue`, which jubs does not surface. The one place it comes back to you is a repeated delivery under an [idempotency key](#idempotency): the key replays the validated value, as the JSON projection of it, so a `Date` the schema produced comes back a string. The size limit described there applies to it.

A return value the schema rejects fails the attempt unrecoverably: it burns one attempt and is not retried, exactly as an invalid stored payload does. The failure is yours to fix in code, and five more attempts would only produce it again.

## The two-process split

A definition carries no handler code, so the process that enqueues never imports the process that runs. This is the point of the library: your web process stays small and cannot accidentally pull in a mail client, a PDF renderer or a payment SDK.

Both processes build a client with `createJobs({ driver })`. `redisDriver` takes a Redis client you created yourself.

**The web process enqueues.** It imports definitions only.

```ts
// web.ts
import { createJobs, redisDriver } from "@juicerq/jubs"
import Redis from "ioredis"
import { sendWelcomeEmail } from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL)
const jobs = createJobs({ driver: redisDriver(connection) })

await jobs.enqueue(sendWelcomeEmail, { userId: "u_1", locale: "pt" })
```

`enqueue` validates the payload before it touches Redis. An invalid payload rejects at the call site with a `PayloadError`, and nothing is written.

**Wrapping `enqueue` keeps its types.** The generics that tie a definition to its payload and to what fills its slots are not spelled out by hand — the types they need are internal. Annotate the wrapper with `JobsClient["enqueue"]` instead, and pass anything extra around the call:

```ts
import { createJobs, type JobsClient, redisDriver } from "@juicerq/jubs"

const jobs = createJobs({ driver: redisDriver(connection) })

function tracedEnqueue(traceId: string): JobsClient["enqueue"] {
	return async (definition, data, ...awaits) => {
		console.log(traceId, definition.name)

		return jobs.enqueue(definition, data, ...awaits)
	}
}

const traced = tracedEnqueue("t-1")

await traced(sendWelcomeEmail, { userId: "u_1", locale: "pt" })
```

The wrapper checks every call the way `jobs.enqueue` does: a wrong payload field, a missing slot, and a wrong leaf deep inside a flow input all still fail at the call site, pointing at the leaf that is wrong.

**The worker process executes.** It calls `jobs.start(handlers)`, which opens one worker per queue in use and dispatches each job to the handler that owns its name. It returns a runtime with `close()`.

A worker connection must be created with `maxRetriesPerRequest: null`, because BullMQ workers open a blocking connection. `start` checks this and throws with the fix before opening any worker.

```ts
// worker.ts
import { createJobs, redisDriver } from "@juicerq/jubs"
import Redis from "ioredis"
import { welcomeEmailHandler } from "./jobs/handlers"

const connection = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: null })
const jobs = createJobs({ driver: redisDriver(connection) })

const runtime = await jobs.start([welcomeEmailHandler])

process.on("SIGTERM", async () => {
	await runtime.close()
	await connection.quit()
})
```

jubs installs no signal handler of its own. Shutdown is yours, and it is two steps: close the runtime, then close the connection you created. The runtime owns the workers it opened — including the separate blocking connection each worker duplicates for itself — and nothing else. jubs never closes a client you passed in.

A process that only enqueues has nothing to close. Its queue handles hold no socket of their own, so quitting your connection is enough.

## Testing

`@juicerq/jubs/testing` exports `memoryDriver()`. It satisfies the same driver interface as `redisDriver`, so a test builds its client the same way the worker does — with the real payload validation and the real name-to-handler dispatch, in milliseconds, with no Redis.

`enqueue` only records. Nothing runs until you ask: `drain()` runs every pending job and returns how many ran, `runNext()` runs the oldest one, and `enqueued(definition)` returns the payloads enqueued for that definition, exactly as they were passed in.

```ts
import { createJobs, defineHandler } from "@juicerq/jubs"
import { memoryDriver } from "@juicerq/jubs/testing"
import { expect, test } from "bun:test"
import { sendWelcomeEmail } from "./jobs/definitions"

test("welcoming a user sends one email", async () => {
	const driver = memoryDriver()
	const jobs = createJobs({ driver })

	await jobs.start([defineHandler(sendWelcomeEmail, async (data) => mailer.send(data.userId))])
	await jobs.enqueue(sendWelcomeEmail, { userId: "u_1", locale: "pt" })

	expect(driver.enqueued(sendWelcomeEmail)).toEqual([{ userId: "u_1", locale: "pt" }])
	expect(await driver.drain()).toBe(1)
})
```

The memory driver does not simulate the clock, delays, backoff, retries, priority ordering, uniqueness windows, schedules, flows or stalled recovery. Jobs run first in, first out, always on attempt 1, and a failing handler throws out of `drain()` instead of being retried. Anything time-dependent is only testable against `redisDriver`.

It accepts `attempts`, `backoff`, `priority`, `keepCompletedForMs` and `keepFailedCount`, but only `attempts` reaches your handler, as `maxAttempts`. `backoff` and `priority` are accepted and ignored. Per-queue `concurrency` is accepted and ignored too — jobs run inline, one at a time.

Everything else throws, and that is the point. `delayMs`, `unique` and a queue `limiter` are time-dependent, so the memory driver refuses them instead of pretending. Enqueueing a definition that declares `awaits` throws for a related reason: jobs run inline, so a parent would run before its children — see [Flows](#flows). The error names the option and sends you to `redisDriver`, so a behaviour this driver never learns to simulate fails loudly instead of passing a test it would fail in production.

## Payload validation on both sides

The payload is validated twice — once on enqueue, and again when the job runs. The second check matters because the job may have been written by an older deploy.

A job whose stored payload no longer validates fails unrecoverably: it burns one attempt and is not retried. So does a job whose name no handler owns. Retrying either would only fail the same way five more times.

## Payload versioning

A payload shape changes, and Redis still holds jobs written in the old shape. `version` and `migrations` let you change it without losing them.

`version` is the shape number of the payload. It defaults to `1`, and every enqueue writes it into the envelope as `v`. `migrations` raises an older envelope to the running version, one step at a time, **before** the payload is validated. The key is the version the step migrates **from**.

```ts
export const syncContact = defineJob({
	name: "contact.sync",
	queue: "crm",
	payload: type({ email: "string" }),
	version: 2,
	migrations: {
		1: (data) => ({ email: (data as { mail: string }).mail }),
	},
})
```

A job stored at `v: 1` runs step `1`, and the result is what the schema validates. A job stored at `v: 2` runs no step at all. Version 3 would add a step `2`, and a `v: 1` job would run step `1` then step `2`, in that order.

A migration receives the **raw** stored data, not a validated payload — the old shape is exactly what the old schema no longer describes. It may return a promise. Its output is only validated once the last step has run.

Each step is an ordinary function, so it is testable on its own, with no queue and no Redis:

```ts
expect(syncContact.migrations?.[1]?.({ mail: "ada@example.com" })).toEqual({
	email: "ada@example.com",
})
```

### The two directions of a deploy

A deploy is never atomic. Some jobs in Redis were written by the deploy before, and — during a rollout, or after a rollback — some were written by the deploy after.

**Backwards: an old envelope on a new worker.** This is what `migrations` is for. Raise `version` by one and add the step that goes from the old version to the new one. Deploy it. Every job still in Redis runs through the step and works.

**Forwards: a new envelope on an old worker.** The old worker cannot read a shape it has never heard of, and guessing would corrupt data. jubs refuses instead: an envelope whose `v` is greater than the running `version` is **never** interpreted. It fails unrecoverably, burns one attempt, is not retried, and — if the queue is in `deadQueues` — is kept in the dead queue with the reason `version_ahead`. It never fires `onStart` — a job held by a future version never started. Once the new workers are up, replay it.

The safe rollout is two deploys: workers first, producers second. Deploy the new version to the workers while producers still write the old one — the new workers migrate what the old producers write. Then deploy the producers. Nothing is ever `version_ahead` in that order.

**A flow in flight is buried, not migrated.** `migrations` carries a payload across a deploy; it carries no children. A parent whose envelope was written before this build has no record of how many children each slot was given, so the first delivery after the deploy buries it as `children_short` instead of running its handler over slots it cannot read. Drain the flows before the deploy goes up, or accept enqueueing them again from the top with `jobs.enqueue(definition, data, awaits)`.

**A scheduled job has no second deploy.** Its producer is `start` itself: at boot it rewrites the schedule template with the version it runs, on the key every other pod shares. During a rolling deploy an occurrence written at the new version can reach a pod still running the old one, and that occurrence fails `version_ahead`. Stop the queue's workers before you raise the `version` of a scheduled job. The occurrence waits in Redis and runs once the new workers are up — nothing is lost, only delayed, and BullMQ writes the next occurrence only once this one is consumed, so nothing piles up either.

If you roll instead, keep the queue in `deadQueues`: without it the occurrence is only a failed job, and `dead.replay` has nothing to replay. Replay recovers the occurrence exactly — `origin: "schedule"` survives and the delivery matches, only the id is new — which needs both the queue in `deadQueues` and the definition registered in `createJobs({ definitions })`.

The rule when a rollback is likely: keep the step, do not delete it. A migration you delete makes every envelope still stored at that version unrunnable.

### What `defineJob` refuses

`version` must be a whole number of `1` or more, and every migration key must be a whole number between `1` and `version - 1`. A step outside that range never runs, which is always a mistake — a typo, or a `version` you forgot to raise.

The full chain is **not** required. A definition can jump to `version: 3` with only a step `2`, because a job raising its version with nothing old left in Redis should not have to write identity functions. The price is that the missing step is found at run time: a job stored at a version with no step from it fails unrecoverably and names the version it needed.

## What `start` checks at boot

A worker whose wiring is wrong should die at boot, not at three in the morning on the one job nobody tested. `start` runs three checks before it opens a single worker, and each error says what to change.

Two handlers sharing a job name is the first. Only one of them could ever run, and which one would depend on array order.

A definition registered on a started queue with no handler is the second. Register your definitions to get it:

```ts
import { createJobs, redisDriver } from "@juicerq/jubs"
import * as definitions from "./jobs/definitions"

const jobs = createJobs({
	driver: redisDriver(connection),
	definitions: Object.values(definitions),
})
```

Now a worker that starts the `mail` queue must own a handler for every definition on `mail`. A definition on a queue this process does not start is ignored, so splitting queues across processes stays legal. Without `definitions`, this check has nothing to check and is skipped.

A connection created without `maxRetriesPerRequest: null` is the third, described above.

Tuning aimed at a queue this process does not start is the fourth: a typo in `start(handlers, { queues })` would otherwise swallow your concurrency and your limiter without a word, so the error names the key and lists the queues that did start.

## Delivery policy

`delivery` on a definition says how the job is delivered: how many attempts it gets, how it backs off, how it is prioritised, how long its record is kept, and whether it waits before becoming available. It lives with the definition, so every enqueue of that job gets the same policy — no delivery options at the call site.

| Option | Default | What it does |
| --- | --- | --- |
| `attempts` | `5` | How many attempts in total, the first one included |
| `backoff` | `{ type: "exponential", delayMs: 2000 }` | Wait before a retry, doubling each attempt |
| `priority` | `20` | Smaller runs first; leave room on both sides |
| `keepCompletedForMs` | `3600000` | How long a completed job stays in Redis |
| `keepFailedCount` | `200` | How many failed jobs stay in Redis |
| `delayMs` | none | Wait this long before the job becomes available |
| `unique` | none | Which job survives when several share a key — see below |

You override only what you name. Anything you leave out keeps its default.

```ts
export const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ userId: "string", cents: "number" }),
	delivery: { attempts: 8, delayMs: 5_000 },
})
```

The function form decides per payload. It receives the **validated** payload as `data` and the resolved defaults as `options`, so you can spread what you do not want to think about:

```ts
export const sendReport = defineJob({
	name: "report.send",
	queue: "reports",
	payload: type({ userId: "string", size: "number" }),
	delivery: ({ data, options }) => ({
		priority: data.size > 10_000 ? options.priority + 10 : options.priority,
		attempts: 3,
	}),
})
```

`delayMs` is the one option the memory driver refuses, because a fake clock that resolves instantly would turn a delay into a green test and a production surprise. Test a delay against `redisDriver`.

## Uniqueness

`unique` decides which of several jobs sharing a key survives. `key` reads the **validated** payload and returns that key, so one definition can be unique per user, per entity or per account. Redis decides it atomically at the moment of the enqueue, so two processes racing on the same key still leave one winner.

| Mode | Which job wins |
| --- | --- |
| `keepFirst` | the first enqueued; every later one is dropped |
| `keepLast` | the last enqueued inside the window; it replaces the one waiting |
| `noOverlap` | the running one, and the latest of those enqueued while it runs |

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

A definition that declares `unique` cannot be enqueued inside `jobs.atomic(fn, { tx })` either, and that enqueue throws: uniqueness does not survive the [outbox](#outbox-and-relay).

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

| State | What the delivery does |
| --- | --- |
| absent | takes the key and runs the handler |
| held by a running delivery, under a lease that expires | is rescheduled, and arrives again later |
| complete, with a kept record | skips the handler and gives back the kept result |

**A rescheduled delivery consumes no attempt.** It is moved back to the queue with a delay and delivered again; the attempt counter does not move, `onAttemptFailed` and `onDead` do not fire, and nothing is buried. There is no ceiling on it either — a delivery that keeps meeting a held key keeps being rescheduled, for as long as the key stays held. The lease is what ends that wait: when it expires, the next delivery takes the key and runs.

Three values are fixed today and configurable by nothing: the lease is **30 seconds**, it is renewed every **10 seconds** while the handler runs, and a complete key keeps its result for **24 hours**. Renewal is why a handler slower than 30 seconds is safe — the lease follows it for as long as it runs.

A kept result is stored as JSON, so a repeated delivery gets the JSON projection of it: a `Date` comes back a string, and anything JSON drops is dropped.

It also has a size limit of **64 KB**. Above it, jubs keeps the completion marker alone: the key still counts as complete, the handler is still skipped, but the repeated delivery gets `undefined` instead of the result. Return a receipt id, not the receipt.

A result JSON cannot serialise at all — a circular object, a `BigInt` — leaves a state jubs cannot repair. jubs keeps the completion marker alone, as above, and reports the job a success: `onSuccess` fires. BullMQ then throws while writing the return value, outside jubs' reach and after the dispatch has returned, so the attempt is failed from under it. The delivery arrives again, meets the complete key, skips the handler and replays the empty marker — which serialises — so the job settles `completed` while still carrying the `failedReason` of the attempt that threw. `onAttemptFailed` and `onDead` never fire, and nothing is buried. On a definition with no key the handler runs again on every attempt and the job ends `failed`. Nothing warns you either way, so return a value JSON can hold.

The lease is what makes this correct, and the reason is worth spelling out. Marking the key before running and skipping it on the repeat would be at-most-once, not idempotent: a worker killed between the mark and the end would leave a key that says done over work that never happened, and every later delivery would report success for a charge nobody made. The lease says *in progress*, not *done*, and it expires — so a killed worker gives the job back instead of losing it.

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

## Atomic block

An enqueue lands in Redis the moment you make it. A transaction around it does not hold it back, because Redis knows nothing of your database:

```ts
await db.transaction(async (tx) => {
	const order = await createOrder(tx, cart)

	await jobs.enqueue(chargeCard, { orderId: order.id }) // in Redis already

	await reserveStock(tx, cart) // throws
})
```

The transaction rolls back and the order is gone. The job is not: a worker takes it, reads an order that no row answers to, and fails — or worse, charges a card for an order nobody placed.

`jobs.atomic(fn)` holds every job enqueued inside `fn` and delivers them, in the order they were enqueued, only once `fn` resolves. An `fn` that throws delivers nothing.

```ts
await jobs.atomic(async () => {
	await db.transaction(async (tx) => {
		const order = await createOrder(tx, cart)

		await jobs.enqueue(chargeCard, { orderId: order.id }) // held

		await reserveStock(tx, cart)
	})
})
```

The block wraps the transaction, not the other way around: it has to outlive the commit, because delivering a job the commit then refuses is the very thing it prevents.

It holds every kind of enqueue — a job on its own and a whole flow — and it holds nothing else.

**Only enqueues are held.** `jobs.dead.discard(id)`, `cancel`, `retry`, `pause` and `resume` act the moment you call them, inside a block or out of it. A `discard` inside a block that then throws has already destroyed the dead record. `jobs.dead.replay(id)` is refused outright inside a block, and says so: a replay forgets the payload's idempotency key, enqueues the job and drops the dead record, and no block makes those three one act.

**A worker started inside a block does not join it.** `jobs.start(handlers)` leaves the block before it consumes, so the jobs its handlers enqueue are delivered at once, however long the worker runs.

**A held enqueue gives back `null`.** `jobs.enqueue` answers with `EnqueuedJob | null`, and it is `null` exactly when a block held the job: there is no job yet, so there is nothing to name. Nothing tells a call site which of the two it is — the block is invisible to the code inside it, which is what lets you wrap code that knows nothing of jubs — so every enqueue reads the same type.

**Every enqueue pays for that, block or no block.** `jobs.get(enqueued.id)` no longer compiles on its own, because `enqueued` may be `null`. Narrow once and pass the narrowed id on.

```ts
const enqueued = await jobs.enqueue(chargeCard, { orderId })

if (!enqueued) {
	throw new Error("this enqueue was held by an atomic block")
}

await jobs.get(enqueued.id)
```

Inside a block, do not ask for the id at all: ask after the block, or move the enqueue that needs one out of it.

**A block inside a block joins the outer one.** Only the outermost block delivers, and it delivers once. Two blocks running at once hold their own jobs and never see each other's.

**A delay counts from the flush, not from the call.** A job whose delivery declares `delayMs` starts counting when the block delivers it, so a block that runs for `D` puts that job `D + delayMs` away from the moment you enqueued it.

**Two enqueues raced inside one block may swap places.** The order is the order in which each call reaches the buffer, which is after its payload has been validated. Validation is synchronous for an Arktype schema, so calls made in sequence keep their sequence; only enqueues started together, with `Promise.all` over a schema that validates asynchronously, can land in the other order.

### What a block does not cover

**What decides is where the callback was scheduled, not whether it is detached.** A block is an async context, and a timer, a listener or a promise created inside `fn` inherits it. So a `setImmediate` scheduled from inside the block is still inside the block when it runs — and it runs after `fn` resolved, which is after the block already delivered what it held.

An enqueue that arrives then is **refused**, because it can no longer be held and delivering it would break the promise the block makes:

```ts
await jobs.atomic(async () => {
	setImmediate(async () => {
		await jobs.enqueue(chargeCard, { orderId }) // throws: the block has ended
	})
})
```

The same holds for a promise `fn` starts and never awaits. Await the work inside `fn`, and the block holds its jobs; or enqueue outside the block, and it is delivered at once. A callback registered **outside** the block, even one an event inside the block triggers, never sees the block at all: its enqueue lands in Redis immediately.

**The delivery of a block is not atomic.** The held jobs are delivered one by one. A driver that fails on the third leaves the first two in Redis, and everything after the third is abandoned without being tried. `atomic` rejects with an error naming both counts, carrying the driver's own failure as its `cause`. Rolling the first two back is not on offer: a job already taken by a worker cannot be unenqueued — so running the block again re-delivers the ones that did land.

**The jobs are held in memory.** A process that dies inside a block loses every job it held, and nothing records that they existed. This is the gap an outbox closes, by writing the envelopes inside your own transaction — that is `jobs.atomic(fn, { tx })`, and the next section is all of it.

Use a block for what a rollback would otherwise contradict. For a job that must survive a crash between the commit and the delivery, use the outbox.

## Outbox and relay

An atomic block holds its jobs in memory. A process killed between the commit and the flush takes them with it, and the row it wrote is left with no job to act on it. The **outbox** closes that window: the jobs are written to a table of your own, in the same transaction as the state change, so the two commit together or neither does. The **relay** reads that table afterwards and puts the jobs in the queue.

You own the table, the columns and the queries. This library opens no connection, knows no SQL and never sees your transaction — it hands you envelopes and takes back what you claim.

```ts
import { createJobs, redisDriver } from "@juicerq/jubs"

const jobs = createJobs({
	driver: redisDriver(connection),
	definitions: [chargeCard],
	outbox: kyselyOutbox(db),
})

await db.transaction().execute(async (tx) => {
	await jobs.atomic(async () => {
		const order = await createOrder(tx, cart)

		await jobs.enqueue(chargeCard, { orderId: order.id })

		await reserveStock(tx, cart)
	}, { tx })
})
```

The block enqueues nothing. It collects the envelopes, and when it resolves it hands them to `outbox.save(envelopes, tx)` — one call, inside your transaction. A block that throws saves nothing, and the transaction that would have rolled back rolls back the rows too.

**The two scopes invert.** Without a `tx`, the block wraps the transaction, because it has to outlive the commit. With a `tx`, the block sits *inside* the transaction, because `save` writes on a transaction that is still open. Wrapping a transaction around a block that already holds a `tx` writes the rows and delivers nothing, forever.

**`tx` is opaque.** It travels from your `jobs.atomic` call to your `outbox.save` untouched. Whatever your database library calls a transaction is what arrives, and casting it back is the one cast the adapter makes.

### The relay

```ts
const relay = jobs.startRelay()

process.on("SIGTERM", async () => {
	await relay.close()
})
```

One cycle claims a batch of rows, enqueues a job for each, and marks the rows it delivered. `close()` stops the loop and waits for the cycle under way.

- `limit` — how many rows one cycle claims. 100 by default.
- `intervalMs` — how long the relay waits between two cycles. 1000 by default.
- `onLeftBehind` — runs for every row the relay leaves behind, with the reason it was left behind on. See [what the outbox refuses](#what-the-outbox-refuses).

**One cycle runs at a time.** A cycle that outruns the interval holds the next tick back instead of running beside itself.

**The relay client needs the definitions.** A row keeps the envelope and nothing else: the queue the job goes to and the delivery policy it is enqueued with are read from the definition, the way a replay reads them. Register in `createJobs({ definitions })` every job your outbox may carry. The client that writes and the client that relays need not be the same one, and usually are not — every producer writes, and one process relays.

**The producer's enqueue answers `null`,** as it does in any block. There is no job yet — there is a row.

### A row delivered twice is one job

The relay dies between the enqueue and the mark, and the rows it delivered are claimed again. That is the normal end of a deploy, not an accident, and it produces no second job: the job is stored under an id derived from the row, `outbox-<row id>`, and BullMQ's `add` refuses to store a second job under an id it already keeps. It hands back the one that is there, without overwriting its data. The job runs once.

The same rule sets what your row ids may be. **A row id carrying a colon is refused**, and so is an empty one: BullMQ reads a custom id with colons as a queue-and-id pair, and answers an empty id by making one up — which would mark the row delivered for a job nothing can name. A `bigserial` or a `uuid` is what to name rows with.

**The ceiling of the guarantee is Redis's retention.** The id only collides while the job is still in Redis, and every promise above holds up to that line and no further. A completed job leaves on time — `keepCompletedForMs`, one hour by default. A failed job leaves on volume: `keepFailedCount` keeps the last 200 failures of the queue, so a busy queue drops a failure in minutes and a quiet one keeps it for weeks. Past either line the id answers to nothing, and the same row delivered again enqueues a job that runs a second time. Keep the completed window wider than the time a dead relay may take to come back, and read [delivery policy](#delivery-policy) for both settings.

**A row whose job died for good is a silent no-op.** The failed job is still retained, so the `add` hands it back and nothing runs — and the relay is told nothing, because from Redis's side that is an ordinary answer. Replay it from the dead queue with `jobs.dead.replay(id)`.

### What the outbox refuses

**A flow is refused inside `jobs.atomic(fn, { tx })`,** and the enqueue throws. A row holds one envelope; a flow is a tree, and what each slot was given would be lost on the way through the table. Enqueue the flow outside the block, or hold it with `jobs.atomic(fn)` alone.

**A `tx` inside a block that is already open is refused,** and throws before the block runs. The outer block delivers what it holds while the inner one would write it, and the inner block does not get to decide for both.

**`jobs.dead.replay(id)` is refused in any block,** with or without a `tx`.

**A job that declares `unique` is refused,** and the enqueue throws. Uniqueness does not survive the outbox: BullMQ reads the derived id first and the deduplication key second, so a key already pointing at another job answers with *that* job's id — the row would be marked delivered by a job that is not its own, and the job the row stands for would never exist. Passing it through in silence is the one thing not on offer. Drop `unique` from the definition, or enqueue that job outside the block, where uniqueness works and the transaction does not cover it. A job that must not run **twice** wants [`idempotencyKey`](#idempotency) instead, which travels through the outbox untouched.

**A row the relay cannot deliver is left behind, and the rows around it go on.** A job name no definition on that client answers to, or a payload its schema refuses, takes that row out of the cycle and nothing else: the rows before and after it are enqueued and marked, and only the bad one stays unmarked. The failure names the row, through `console.error`, once per cycle.

**That row does not fix itself.** Every cycle claims it again and fails on it again, so it holds a place in each batch and writes a line in each log. Two acts end it, and both are outside the relay: register the definition the failure names on the relaying client, or take the row out of the claim — mark it delivered by hand, or delete it. The adapter below automates the second one: it counts how often a row has been claimed and stops claiming it after ten, so a poisoned row leaves the batch on its own. Read what it gave up on:

```sql
select id, envelope, claims from jubs_outbox where delivered_at is null and claims >= 10;
```

**A hook says why the row was left behind.** The relay classifies the failure and hands it to `onLeftBehind`, so your table can keep a status of its own:

```ts
const relay = jobs.startRelay({
	async onLeftBehind({ rowId, name, reason, error }) {
		await db
			.updateTable("jubs_outbox")
			.set({ status: reason, failure: `${name}: ${error.message}` })
			.where("id", "=", rowId)
			.execute()
	},
})
```

`reason` is one of three. `driver_failed` is a delivery that failed inside the driver: the row is good and its job is not lost. `version_ahead` is a payload a newer deploy wrote: the row is good, and a process running that version delivers it. `unrecoverable` is a row this process cannot deliver until something outside the relay changes — an unregistered definition, a definition that waits on children, a row id no job id can be derived from, a payload its schema refuses, or a migration of yours that threw. The two good reasons are the rows to leave in the claim; only `unrecoverable` is the row an operator takes out of it.

The console report happens either way, and the hook is what you add on top of it. It may be async, and the cycle awaits it before it marks the rows it delivered. A hook that throws is reported to the console and changes nothing else: the row stays left behind, and the rows around it are delivered and marked as usual.

### Running more than one relay

Nothing in this library keeps two relays off one row. **The claim does**, and the claim is yours: lock the rows you read and skip the rows another relay holds. The adapter below does it with `for update skip locked`, and leases the rows it claims, so a relay killed mid-cycle releases its rows after a minute instead of holding them until someone notices.

**The lease is part of the contract, not a nicety of that adapter.** A cycle marks the rows it delivered and leaves the rest claimed — the row nothing could deliver, and every row of the batch a killed relay never reached. Those rows come back to a later cycle only because the claim expires. A `claim` of your own that holds a row for good hands it out once and never again: the job that row carries never runs, and nothing tells you — no failure, no line in the log, a row sitting undelivered in a table nobody reads. Every promise here about a row being claimed again rests on your claim releasing it.

### A Kysely adapter

The table:

```sql
create table jubs_outbox (
	id bigserial primary key,
	envelope jsonb not null,
	claims integer not null default 0,
	claimed_at timestamptz,
	delivered_at timestamptz,
	status text,
	failure text,
	created_at timestamptz not null default now()
);

create index jubs_outbox_claimable on jubs_outbox (id) where delivered_at is null;
```

`id` is a `bigserial`, so it carries no colon and is never empty. The partial index is what keeps the claim off the rows already delivered, however long you keep them. `claims` counts how many times a row has been handed to a relay, and is what lets the claim give up on a row it cannot get past. `status` and `failure` are what the `onLeftBehind` hook above writes: the reason the relay classified, and the failure it was left behind on. The relay never reads them, so they stay null until a row is left behind, and dropping both columns costs you only that hook.

```ts
import type { Envelope, Outbox } from "@juicerq/jubs"
import type { ColumnType, Generated, Kysely, Transaction } from "kysely"

export interface Database {
	jubs_outbox: {
		id: Generated<string>
		envelope: ColumnType<Envelope, string, string>
		claims: Generated<number>
		claimed_at: Date | null
		delivered_at: Date | null
		status: string | null
		failure: string | null
		created_at: Generated<Date>
	}
}

const CLAIM_LEASE_MS = 60_000

const MAX_CLAIMS = 10

export function kyselyOutbox(db: Kysely<Database>): Outbox {
	return {
		async save(envelopes, tx) {
			await (tx as Transaction<Database>)
				.insertInto("jubs_outbox")
				.values(envelopes.map((envelope) => ({ envelope: JSON.stringify(envelope) })))
				.execute()
		},

		async claim(limit) {
			const rows = await db
				.updateTable("jubs_outbox")
				.set((eb) => ({ claimed_at: new Date(), claims: eb("claims", "+", 1) }))
				.where((eb) =>
					eb(
						"id",
						"in",
						eb
							.selectFrom("jubs_outbox as o")
							.select("o.id")
							.where("o.delivered_at", "is", null)
							.where("o.claims", "<", MAX_CLAIMS)
							.where((claimable) =>
								claimable.or([
									claimable("o.claimed_at", "is", null),
									claimable("o.claimed_at", "<", new Date(Date.now() - CLAIM_LEASE_MS)),
								]),
							)
							.orderBy("o.id")
							.limit(limit)
							.forUpdate()
							.skipLocked(),
					),
				)
				.returning(["id", "envelope"])
				.execute()

			return rows
		},

		async markDelivered(ids) {
			await db
				.updateTable("jubs_outbox")
				.set({ delivered_at: new Date() })
				.where("id", "in", ids)
				.execute()
		},
	}
}
```

The claim is one statement: the inner select locks the rows it reads and skips the ones another relay holds, and the update stamps `claimed_at` on exactly those. A row is claimable again once its lease has run out, which is what makes a killed relay's rows move on their own.

`MAX_CLAIMS` is where the poisoned row is dealt with, and it belongs here rather than in the library — how many attempts a row deserves is a policy about your table. A row handed out ten times and never marked stops being claimed, so it stops taking a place in every batch. Nothing is lost by giving up: the row is still there, with its envelope and its count, and the query above is what reads it. **Read it.** A row counts a claim whether it failed on its own or a relay died holding it, so a healthy row can reach ten as well — and a row that stopped being claimed is a job that will never run until you act.

**A `version_ahead` row is the healthy row that reaches ten.** During a rolling deploy an old pod claims a row a new pod wrote, cannot read it, and leaves it behind; at the default interval and a one-minute lease it counts ten claims in about ten minutes, and the claim gives up on it. The row is good, and a new pod would deliver it. Do not mark it delivered and do not delete it — that loses a job committed with the state change beside it. Let the deploy finish, and if it ran past ten claims, put the row back in the claim by zeroing its count: `update jubs_outbox set claims = 0 where id = $1`.

`markDelivered` stamps a date rather than deleting, so the table doubles as the record of what was enqueued and when. Delete instead if you would rather not sweep it; the relay reads neither column.

The column types are `pg`'s: a `jsonb` comes back parsed and goes in as a string, and a `bigserial` comes back as a string. Another driver reads them its own way, and `ColumnType` is where you say so — the envelope has to come back out of `claim` as the object `save` was given.

### Testing the outbox

`memoryDriver` honours a derived id the way Redis does: a second enqueue under an id it already keeps gives the kept job back, and stores nothing. A test can kill a relay between the delivery and the mark, run it again, and see one job.

It is **more** faithful than Redis in one way, and that way is the ceiling above: it keeps every job it ever recorded, so it never loses an id to a retention sweep. Test the sweep against `redisDriver`.

## Scheduling

A schedule is the recurrence rule that makes a job run on its own, without a producer. It sits on the definition, beside the payload schema and the delivery policy, so the recurrence is declared where the job is declared — not in a crontab, not in a wiring file some other team owns.

```ts
// jobs/definitions.ts
import { dailyAt, defineJob } from "@juicerq/jubs"
import { type } from "arktype"

export const sendDigest = defineJob({
	name: "report.digest",
	queue: "reports",
	payload: type({ range: "string" }),
	schedule: dailyAt("02:00", { data: { range: "day" } }),
})
```

The handler is the ordinary one. Nothing about it knows it is scheduled.

```ts
export const digestHandler = defineHandler(sendDigest, async (data) => {
	await reports.send(data.range)
})
```

Five constructors build a schedule.

| Constructor | When the job runs |
| --- | --- |
| `every("5 minutes")` | at that interval — second, minute, hour or day |
| `dailyAt("07:00")` | every day at that time, in 24 hours |
| `weeklyOn("monday", "09:00")` | that weekday, at that time |
| `monthlyOn(1, "00:00")` | that day of the month, at that time |
| `cron("0 */6 * * *")` | whenever the pattern says |

The payload goes in the constructor's options, as `data`. A scheduled job has no producer to hand it one, so it carries its own. `start` validates that `data` against the definition's payload schema and throws when it does not pass — the check an enqueue would have run at the call site, run at boot instead.

The default time zone is UTC, and it is explicit: without it BullMQ reads the pattern in the server's local time, so one deploy fires at a different hour in a different region. `createJobs({ timezone })` moves the default for every schedule, and a schedule overrides it with the `timezone` option. Both refuse a name that is not IANA the moment you write it, not at the first run that never came.

```ts
const jobs = createJobs({
	driver: redisDriver(connection),
	definitions: Object.values(definitions),
	timezone: "America/Sao_Paulo",
})

export const closeBooks = defineJob({
	name: "billing.close",
	queue: "billing",
	payload: type({ month: "string" }),
	schedule: monthlyOn(1, "00:00", { timezone: "UTC", data: { month: "previous" } }),
})
```

`every` takes no time zone, in the type and at runtime. The zone only enters the calculation when the recurrence is a cron pattern. On an interval recurrence BullMQ never calls the cron parser, so a wrong zone there would throw nothing at all and be stored in silence. jubs refuses it instead of keeping a value that does nothing.

`start` upserts every declared schedule and **removes** the ones this library created that the code no longer declares. That removal is the point. BullMQ's `upsertJobScheduler` never removes anything on its own, so a schedule you delete from the code keeps firing in Redis until a human goes looking for it. Reconciliation touches **only** scheduler names that start with `jubs.`. The prefix is the whole test — jubs keeps no record of what it created, so any undeclared scheduler carrying that prefix is removed, whoever wrote it. A scheduler another tool created survives as long as its name does not start with `jubs.`.

Two limits are worth saying out loud, because either one destroys state when you do not know it.

**The declared schedules come from the handlers you pass to `start`, not from `createJobs({ definitions })`.** A `start` that does not receive the handler of a scheduled job removes that job's scheduler. This is the mechanism, not an accident — it is exactly how a schedule deleted from the code stops firing. It is also the trap: every process that starts a queue must pass the handlers of every scheduled job on it.

**The scope is the queue, not the process.** Two processes that start the same queue with different handler sets fight over it: the second `start` removes the schedulers the first one wrote. Splitting the handlers of one queue across processes needs the queue split too.

A definition with a `schedule` gets **1 attempt** by default instead of 5. A five minute recurrence with exponential backoff would still have attempts in flight when the next occurrence fires, and the two runs overlap. An explicit `attempts` still wins.

A scheduled job is an ordinary job. You can enqueue it by hand as well, and both paths run the same handler. `origin` in the handler context is what tells them apart.

```ts
export const digestHandler = defineHandler(sendDigest, async (data, context) => {
	if (context.origin === "schedule") {
		metrics.increment("digest.scheduled")
	}

	await reports.send(data.range)
})
```

The occurrence the scheduler produces does **not** carry `unique`. BullMQ writes the deduplication option onto the scheduler's template and then ignores it — no key is ever taken — so jubs drops it rather than promise what the layer below does not keep. The recurrence still gives every occurrence its own identity: BullMQ produces one job per occurrence, with a deterministic id. That is identity, not exclusion. `unique` keeps working normally when the same definition is enqueued by hand. An occurrence that must run exactly once needs `idempotencyKey` instead — see [Idempotency](#idempotency).

**An occurrence that takes longer than the interval overlaps the next one.** The scheduler produces the next occurrence by the clock, without looking at whether the previous one finished: a 3 second handler on `every("1 second")` reaches four runs at the same time. jubs does not prevent it, and `noOverlap` cannot help — it is the very option the scheduler's template drops. The defence is yours: an interval longer than the worst duration, or a lock inside the handler.

A scheduled definition with `delayMs` in its delivery makes `start` throw. A delay postpones one enqueue, and a recurrence has no single enqueue to postpone.

A definition cannot declare `schedule` and `awaits` together, and `defineJob` refuses the pair. A recurrence enqueues one job on its own, with nothing to fill its slots — see [Flows](#flows) for what to write instead.

The memory driver refuses a schedule. It does not simulate the clock, so starting a queue whose handlers declare one throws. A started queue that declares no schedule reconciles to nothing and passes. Test a schedule against `redisDriver`.

## Flows

A flow is a job that waits on child jobs and reads their results. The parent runs once, after every child has settled, with what they returned at hand. It nests to any depth and crosses queues.

**A definition declares what it waits on.** `awaits` maps a **slot** name to the definition that fills it. The slot is what names that child everywhere after — in the id Redis stores it under, in the results the handler reads, and in the failure that buries the parent — so two slots holding the same definition stay apart.

```ts
// jobs/definitions.ts
export const fetchRows = defineJob({
	name: "report.fetch",
	queue: "analytics",
	payload: type({ source: "string" }),
	result: type({ rows: "number" }),
})

export const readBudget = defineJob({
	name: "report.budget",
	queue: "analytics",
	payload: type({ month: "string" }),
	result: type({ target: "number" }),
})

export const buildReport = defineJob({
	name: "report.build",
	queue: "reports",
	payload: type({ month: "string" }),
	result: type({ url: "string.url" }),
	awaits: { rows: [fetchRows], budget: readBudget },
})

export const mailReport = defineJob({
	name: "report.mail",
	queue: "reports",
	payload: type({ to: "string" }),
	awaits: { report: buildReport },
})
```

A definition that declares `awaits` makes every enqueue of it a flow. `jobs.enqueue` then takes a third argument: what fills every slot.

```ts
const enqueued = await jobs.enqueue(
	mailReport,
	{ to: "finance@example.com" },
	{
		report: {
			data: { month: "2026-01" },
			awaits: {
				rows: [{ source: "ledger" }, { source: "invoices" }],
				budget: { month: "2026-01" },
			},
		},
	},
)
```

The two `fetchRows` jobs and `readBudget` run first, on `analytics`. `buildReport` runs when all three are done, `mailReport` when `buildReport` is done. `enqueue` gives back the id of the **root** — the job the producer holds — in the same form an ordinary enqueue gives back. The whole tree is added in one step and validated before anything reaches Redis, so a node a rule refuses stops the flow with nothing enqueued.

**The shape of a slot declares how many children it holds.** `rows: [fetchRows]` waits on many `fetchRows`; `budget: readBudget` waits on exactly one. The array holds one definition and means *many of these* — it is not a list of different jobs. Two definitions in one slot do not typecheck, and the error TypeScript gives for `[a, b]` is `not assignable to type 'undefined'`, which says nothing useful; the rule is one definition per slot, a slot per job you wait on.

The arity carries through: a slot declared as an array is filled with an array and read back as an array, and a slot declared bare is filled with one value and read back as one value.

**A slot is filled with a payload — unless the job in it waits on children of its own.** Then it is filled with `{ data, awaits }`: `data` is that job's payload, `awaits` is what fills *its* slots. That is the whole of nesting, and it repeats to any depth. In the example above, `report` takes the wrapper because `buildReport` declares `awaits`, while `rows` and `budget` take plain payloads because `fetchRows` and `readBudget` wait on nothing.

`context.children` is how a parent reads what its children returned.

```ts
export const buildReportHandler = defineHandler(buildReport, async (data, context) => {
	const rows = context.children.rows.reduce((sum, part) => sum + part.rows, 0)

	return { url: await reports.render(data.month, rows, context.children.budget.target) }
})
```

It is a plain object, not a call: one entry per slot, typed from `awaits`. Here `children.rows` is `readonly { rows: number }[]` and `children.budget` is `{ target: number }`. Each value comes back through the `result` schema of the definition its slot declares, applied to the JSON Redis kept.

**Every slot is read and validated before the handler runs**, not inside it. So a value a schema refuses fails the job for good, with no handler code having run — the only honest outcome, since the child is done and another attempt would read the very same value. A definition that declares no `awaits` reads an empty object and touches Redis for nothing.

The read is decided by the definition, never by what enqueued the job. A definition that declares `awaits` has its children read on every delivery — one enqueued by hand, one left in Redis by an older deploy, one a schedule produced. There is no path where a job that waits on children reaches its handler over slots nobody filled.

**A flow is for fan-in.** Use it when one job needs the results of several. A job that merely *follows* another is not a flow: enqueue it at the end of the first job's handler. That way a failure retries the second job alone, where a flow would keep the first job's whole subtree in Redis to say the same thing.

```ts
export const chargeCardHandler = defineHandler(chargeCard, async (data) => {
	const receipt = await gateway.capture(data.orderId)

	await jobs.enqueue(sendReceipt, { orderId: data.orderId, receipt: receipt.id })
})
```

**Uniqueness does not apply inside a flow, at any position.** BullMQ forbids deduplication beside a parent, so `defineJob` refuses `unique` on a definition that declares `awaits`, and the enqueue refuses it again at every node of the tree. The second refusal is not a duplicate: a definition that declares no `awaits` is a perfectly ordinary job on its own, and still cannot be deduplicated once it fills somebody's slot. Drop `unique` from its delivery, or enqueue that job on its own.

**`idempotencyKey` is refused on a definition that declares `awaits`.** A parent is fed by its children as much as by its payload, so a key derived from the payload alone names two different runs: a second flow over different children would meet the complete key and replay the first flow's result. A definition that waits on nothing keeps its key, because it really is its payload alone — including when it fills a slot.

**`schedule` is refused on a definition that declares `awaits`.** A recurrence enqueues one job on its own, with no producer to fill its slots, so every tick would run over children it never had. `defineJob` refuses the pair. Give the `schedule` to a job that waits on nothing, and let that job's handler enqueue the flow with `jobs.enqueue`.

```ts
export const nightlyReport = defineJob({
	name: "report.nightly",
	queue: "reports",
	payload: type({ month: "string" }),
	schedule: dailyAt("02:00", { data: { month: "2026-01" } }),
})

export const nightlyReportHandler = defineHandler(nightlyReport, async (data) => {
	await jobs.enqueue(
		mailReport,
		{ to: "finance@example.com" },
		{
			report: {
				data: { month: data.month },
				awaits: { rows: [{ source: "ledger" }], budget: { month: data.month } },
			},
		},
	)
})
```

**A slot name cannot hold `:` or `~`.** A child is stored under an id built from the slot it fills, `<slot>~<uuid>`, and a job id is cut at its first colon to find the queue. Either character would make that id come apart, so `defineJob` refuses the name up front instead of letting the failure surface as an unreadable id later.

**Every slot is filled at enqueue, and `jobs.enqueue` checks it at run time too.** The types say this already, and they stop saying it wherever a `JobDefinition` is widened to its default type parameters — that erases `Awaits`, and the third argument becomes optional at the type level. So the run-time check is not redundant. It reads the definition alone and refuses, before Redis is touched, an enqueue that passes nothing, one that leaves a slot empty, and one whose arity disagrees with the declaration. Each refusal names the slot.

**A child that fails every attempt buries its parent.** The child does not fail the parent inside Redis — it drops out of the parent's dependencies, the parent runs, finds the failure and is buried with the reason `child_dead` before its handler is ever called. The dead entry keeps `children`: what the children that **did** finish returned, as the raw JSON Redis holds. So a replay is an informed decision rather than a guess. It propagates: a burial for `child_dead` buries the grandparent the same way.

```ts
const [dead] = await jobs.dead.list("reports")

console.log(dead.reason) // "child_dead"
console.log(dead.error.message) // names each failed child: its slot, its definition, its id
console.log(dead.children) // what the others returned
```

The message names the slot **and** the definition that slot declares, because the slot alone does not say what ran and the definition alone does not say which of two slots it was.

**A flow job cannot be replayed.** `dead.replay` refuses any entry whose `origin` is `flow`, and says so: the replayed parent would be enqueued with no children at all, run over an empty result set and complete green. Put the flow back together instead.

```ts
await jobs.retry(childId) // the id from the parent entry's error message
await jobs.retry(parentId) // dead.jobId — the parent itself, still in Redis

await jobs.dead.discard(childEntry.id) // the records left behind
await jobs.dead.discard(parentEntry.id)
```

`jobs.retry(childId)` returns the child to its parent's dependencies, and the parent then runs over a full set of results.

**A child still running sends the parent back to waiting, spending no attempt.** A parent can reach its dispatch while one of its children is still in flight — a child that was retried after the parent was already queued behind it. The runtime counts the children that have not settled and ends that delivery before the handler, so no handler runs over a slot that is still filling. The parent then returns to `waiting_children`, and the child that settles last releases it, the same way it does the first time around. The attempt count is untouched, so a child slower than the parent's backoff costs the parent nothing.

**A slot that lost a child buries the parent.** Redis records nothing when a child leaves its parent: a cancelled or removed child drops out of the parent's dependencies, and what stays behind is a slot short by exactly the children it lost — byte for byte a slot that was enqueued smaller. So the envelope carries how many children each slot was given at enqueue, one count per slot. The runtime reads those counts against what arrived, and buries the parent with the reason `children_short` when a slot holds fewer. The handler is never called: it would read that slot short and complete green.

Nothing here is retryable, because there is nothing to retry — the missing child was cancelled, removed, or never enqueued at all. Enqueue the flow again from the top with `jobs.enqueue(definition, data, awaits)`, which builds the whole tree. `dead.replay` is not the repair: it enqueues one envelope, and this envelope cannot be replayed into children it never had.

The same burial catches an envelope that carries no counts at all. That envelope was written by a build that did not record them — a flow left in Redis across the deploy that added the counts — or it was enqueued with no children at all. Nothing left in Redis tells the two apart, so the runtime cannot know whether those slots are full or empty, and running the handler would risk reading an empty slot and completing green. It is buried as `children_short` too, and the message tells the two burials apart: a slot that went short names its counts, a missing record names the absent counts.

```ts
const [dead] = await jobs.dead.list("reports")

console.log(dead.reason) // "children_short"
console.log(dead.error.message) // names each short slot: what it was given, what arrived
```

**A cancellation that reaches a parent whose children are still running cancels nothing.** The result is `children_running`, and nothing changed in Redis: the job cannot be removed while a descendant holds a lock, and it is not running itself, so there is no delivery to abort either. Cancel the running child first, or ask again once the children have settled. Cancelling a parent that is merely waiting does remove it, together with its children — see [Operations](#operations) for what cancelling a single child does instead.

**Keep a child's `result` schema idempotent.** It runs twice: once when the child finishes, on what the handler returned, and once when the parent reads it, on the JSON Redis kept. A transform that is not idempotent under JSON passes the first and fails the second — `string.numeric.parse` stores `500` and then refuses `500`, because `500` is no longer a string. The parent dies for good on that, with no further attempt: the child is done, so running the parent again would read the very same value.

The memory driver does not simulate flows. Enqueueing a definition that declares `awaits` throws there, and so does reading what the children of such a job settled into. Jobs run inline, so nothing ever reaches `waiting_children`, and a parent that ran before its children would agree with your test and disagree with production. An empty answer would be worse than a refusal: the runtime treats empty slots as a burial, so this driver would manufacture a `children_short` no Redis ever produced. Test a flow against `redisDriver`.

### Two sharp edges

**Build the third argument inline, or annotate it.** An input written straight inside the `enqueue` call is checked slot by slot, so a mistake points at the leaf that is wrong. The same input built in a `const` first is checked as a whole afterwards, and the error becomes a wall as deep as the tree, with the real cause at the bottom. `AwaitsInput<typeof definition>` as an explicit annotation puts the leaf error back.

```ts
import type { AwaitsInput } from "@juicerq/jubs"

const input: AwaitsInput<typeof mailReport> = {
	report: {
		data: { month: "2026-01" },
		awaits: {
			rows: [{ source: "ledger" }],
			budget: { month: "2026-01" },
		},
	},
}

await jobs.enqueue(mailReport, { to: "finance@example.com" }, input)
```

**`{ data, awaits }` is the least guessable rule in this API.** Whether a slot takes a payload or that wrapper depends on the job in it, not on the slot, so the same slot changes shape the day its definition starts waiting on something. Get it wrong and the compiler complains that a field of your payload does not exist on `{ data, awaits }` — true, and no help at all in finding the rule. At run time it is worse: a value in neither shape reads as nothing at all, so what you see is that child's own payload refusal, or the refusal of the slots it was never given. If a slot's error talks about a payload you did think you passed, the wrapper is what is missing.

## Hooks

Hooks are the observability path. They are declared once on the client, fire on every execution the runtime performs, and are driver-agnostic — the memory driver fires them too, so a test can assert what your metrics would record.

```ts
const jobs = createJobs({
	driver: redisDriver(connection),
	hooks: {
		onStart: (event) => metrics.increment("job.start", { job: event.name }),
		onSuccess: (event) => metrics.increment("job.success", { job: event.name }),
		onAttemptFailed: (event) => logger.warn(event.error.message, event),
		onDead: (event) => pager.alert(`${event.name} gave up`, event.error),
	},
})
```

Every event carries `name`, `queue`, `id`, `attempt` and `origin`. `onAttemptFailed` and `onDead` add `error`, serialised as `{ name, message, stack, cause? }`, with `cause` one level deep — an error's cause is kept, its cause's cause is not.

The two failure hooks answer different questions. `onAttemptFailed` fires on **every** failed attempt, so five attempts fire it five times; it is your noise-tolerant log line. `onDead` fires **once**, only when the job gives up — the last attempt failed, the handler threw BullMQ's `UnrecoverableError`, or the job was cancelled while it ran. That is the one worth paging on. `onDead` fires whether or not a dead queue is configured.

A job that never becomes an execution fires nothing: a stored value that is not a jubs envelope has no job name to report. A job whose name no handler owns is the other way round — the envelope names it, so it fires `onAttemptFailed` and `onDead` without ever firing `onStart`.

A hook that throws never changes the job's outcome. jubs reports it on `console.error` and carries on: a broken metrics client must not fail a job that worked. Hooks are awaited, so an async hook finishes before the execution ends.

## Dead queue

A dead queue holds the jobs of one queue that failed every attempt, so a human can read them and run them again. It is consumed by nobody: `<queue>.dead` exists to be inspected, not worked.

It is opt-in, because it stores a second copy of the payload in Redis. `keepFailedCount` already keeps the last 200 failed jobs, and for most queues that window is enough. Turn the dead queue on for the queue whose jobs must not be lost when that window rolls over — a payment capture, an invoice, anything you would have to reconstruct by hand.

```ts
const jobs = createJobs({
	driver: redisDriver(connection),
	definitions: Object.values(definitions),
	deadQueues: ["billing"],
})
```

Name the queue, not the job. Every definition on `billing` is kept the moment it gives up — its last attempt failed, its handler threw `UnrecoverableError`, or someone cancelled it while it ran.

Two checks guard the wiring. When you register `definitions`, `createJobs` refuses a dead queue no definition uses, because a typo would otherwise keep nothing and say nothing. And `start` refuses to open a worker on `billing.dead` itself, because a worker there would eat the copies you meant to keep.

What is kept is the envelope, the serialised error and one of six reasons.

| Reason | What died |
| --- | --- |
| `attempts_exhausted` | the last attempt failed |
| `unrecoverable` | the handler threw `UnrecoverableError` |
| `version_ahead` | the payload was written by a newer deploy than this worker runs |
| `cancelled` | `jobs.cancel(id)` reached the job while it ran — see [Operations](#operations) |
| `child_dead` | a child of this job failed every attempt — see [Flows](#flows) |
| `children_short` | a slot of this job holds fewer children than it was enqueued with — see [Flows](#flows) |

```ts
const dead = await jobs.dead.list("billing")

for (const job of dead) {
	console.log(job.id, job.envelope.name, job.reason, job.error.message)
}

await jobs.dead.replay(dead[0].id)
await jobs.dead.discard(dead[1].id)
```

`id` is opaque — it names the queue and the stored job together. Read it from `list` and pass it back; do not build one.

`replay` enqueues the job again from the stored envelope, so it runs with the payload it was first given, then drops the dead entry. It needs the definition, so the client that replays must register it in `createJobs({ definitions })` — the dead entry stores a job name, and only the definition knows the queue and the delivery policy that name gets today. `discard` drops the entry and enqueues nothing.

`replay` refuses a job that was part of a flow, whatever killed it, and names the repair path instead — see [Flows](#flows).

A replay ignores `unique`. The key of a dead job is often still taken — by the dead job itself — and honouring it would drop the replay in silence, which is the one outcome a dead queue exists to prevent. For the same reason a replayed `keepLast` job does not sit out its window: you asked for this job now.

Replay and discard are at-least-once, like every delivery here. Two operators acting on the same dead id at the same moment can both succeed — two replays enqueue the job twice, and a replay racing a discard can do both. Nothing is lost, but a handler can run twice, so keep your handlers safe to repeat. `idempotencyKey` is the remedy — see [Idempotency](#idempotency).

One narrow window belongs to the replay itself: it forgets the key and then enqueues, so an enqueue Redis refuses leaves the key deleted with nothing enqueued. The dead record survives that failure and replaying again recovers the job — what does not come back is the guard, so another producer of the same payload inside the retention window runs the handler unguarded.

**A replay forgets the job's idempotency key before it enqueues, and refuses while a delivery holds that key** — so it either runs the handler or throws, and never comes back green over work nobody did. The refusal is the ordinary answer right after a `timeoutMs` burial; the dead record survives it, so you replay again once the body has ended — see [Forgetting a key](#forgetting-a-key).

Writing to the dead queue never changes a job's outcome. If Redis refuses the write, jubs reports it on `console.error` and the job still fails the way it would have.

`onDead` is separate, and fires whether or not a dead queue is configured. The hook is the page; the dead queue is the copy you replay from.

## Per-queue tuning

Definitions sharing a queue share its concurrency budget. One slow job type starves the others — head-of-line blocking. `start` takes the two levers per queue:

```ts
const runtime = await jobs.start(handlers, {
	queues: {
		mail: { concurrency: 50 },
		reports: { concurrency: 2, limiter: { max: 10, durationMs: 1_000 } },
	},
})
```

`concurrency` is how many jobs that queue's worker runs at once. It defaults to 10. `limiter` is BullMQ's native rate limiter: at most `max` jobs started per `durationMs`, across every worker on that queue. Use it for a queue that talks to an API with a published rate limit.

Tuning is not the remedy for head-of-line blocking, though — it only moves the budget around. The remedy is to give the hot job type its own queue, by changing `queue` on its definition. A queue is a string, it costs nothing to create, and a slow PDF render then has no way to hold up a password reset.

## Timeouts and shutdown

A handler that hangs and a deploy that arrives mid-job are the same problem: work that has to be cut off. Both are cut off through one lever, `context.signal`.

### `timeoutMs`

`timeoutMs` on a definition is how long one attempt may take.

```ts
export const renderReport = defineJob({
	name: "reports.render",
	queue: "reports",
	payload: type({ id: "string" }),
	timeoutMs: 30_000,
})
```

On expiry jubs aborts `context.signal` and fails the attempt at once. It does not wait for the handler. That is the honest limit: nothing can kill a running function in Node, so a handler that ignores its signal keeps running, **detached**, until it returns — and its return goes nowhere, because the attempt has already failed. `timeoutMs` is a deadline on the attempt, not a kill.

A detached body is outside every budget. The moment the attempt fails, BullMQ frees the concurrency slot and starts the next job, while the body runs on **outside** `concurrency`. So with a dependency that hangs, the bodies in flight grow without a ceiling above the number you configured, and the retry of the same job can enter the handler while the previous body is still executing. Without an `idempotencyKey`, a handler that ignores its signal has to tolerate running twice at once.

The failure is an ordinary one: the attempt is retried under the definition's `attempts` and `backoff`, and the job dies to the dead queue when its attempts run out. One case escapes that path. A job whose worker died twice with it active is marked stalled beyond BullMQ's `maxStalledCount`, which defaults to 1 — the next worker kills it while fetching, without ever calling the handler. No hook fires, nothing is written to the dead queue, and the remaining attempts are ignored. The job lands in BullMQ's `failed` set with the reason "job stalled more than allowable limit", visible only in Redis or in a Bull Board.

So pass the signal to whatever waits:

```ts
export const renderReportHandler = defineHandler(renderReport, async (data, context) => {
	const response = await fetch(`${API}/reports/${data.id}`, { signal: context.signal })

	return response.json()
})
```

`fetch`, `setTimeout`, most database clients and every `AbortSignal`-aware library take one.

**A handler that sees its signal abort must throw.** A handler that returns normally after an abort is recorded as a success, and jubs has no way to know the work stopped half done. A return is still a success for the handler that finished just before the abort — that one is right to return. `signal.reason` tells the two aborts apart: a shutdown aborts with a `ShutdownAbortError`, which jubs exports, and a deadline aborts with an error whose message names the `timeoutMs` it ran past.

### `timeoutMs` with `idempotencyKey`

When a definition has both, the idempotency key follows the **body**, not the attempt. The key stays held, and jubs keeps renewing its lease, for as long as the detached body runs.

That is what stops the second body. Every delivery of the same key while the body lives meets a held lease, so it is rescheduled without spending an attempt — the same path a concurrent delivery already takes. When the body finally returns, the key becomes complete with its result, and the next delivery replays that result instead of doing the work again. When the body throws, the key is released and the next delivery runs it for real.

If the process dies, the renewal stops with it and the lease expires on its own after 30 seconds. That is the same guarantee every lease here carries.

Pairing the two is the remedy for the overlap described above. Reach for it on any job whose `timeoutMs` you expect to fire.

One thing the pair does not reconcile is a job that dies while its body lives. The dead queue judges the **attempt**; the key follows the **body**. A job on its last attempt fails on its deadline and is buried as `attempts_exhausted`, and the body that outlived it then completes the key with its result. Both stand: the dead entry is there to replay, and the key is complete for 24 hours. `dead.replay` refuses while that body still holds the key, and forgets the key once the body has ended — so a replay runs the handler instead of handing back what the body kept — see [Dead queue](#dead-queue).

### `close`

`runtime.close()` with no argument is the graceful close: it stops fetching new jobs and waits for every job BullMQ still counts as in flight, however long that takes. It does **not** wait for a body already detached by a `timeoutMs` — that body left the worker's books when its attempt failed, so `close` can resolve, and your process exit, while it is mid-write. If you have jobs that must not be cut mid-write, give them an `idempotencyKey` so a redelivery is safe, or no `timeoutMs` at all.

`runtime.close({ timeoutMs })` puts a ceiling on that wait. It resolves as soon as the in-flight jobs drain, and if the timeout expires first it aborts the signal of every job still running and resolves anyway.

A shutdown abort costs the job nothing. When the handler throws, jubs does not treat it as a failed attempt: no `onAttemptFailed`, no dead queue, no `onDead`. The delivery is put back with `moveToDelayed`, which spends no attempt, and another worker takes it. A handler punished for obeying its signal would be the wrong lesson to teach.

A handler that ignores its signal is the worse case, not the better one. `close` still resolves inside the window, but the job it holds stays active in Redis, and it is recovered only as stalled — once, since `maxStalledCount` defaults to 1. If the process dies before `moveToDelayed` reaches Redis, a cooperative job takes that same stalled path. That is acceptable, and better than burying a job that never failed.

**jubs installs no `SIGTERM` or `SIGINT` handler.** A library that grabbed those would fight your HTTP server, your database pool and your tracer for the same signal. Your process owns its shutdown; jubs gives it a `close` to call. The recommended shape for a worker process:

```ts
const runtime = await jobs.start(handlers)

async function shutdown(signal: NodeJS.Signals) {
	console.log(`${signal} received, draining jobs`)

	await runtime.close({ timeoutMs: 25_000 })
	await connection.quit()

	process.exit(0)
}

process.once("SIGTERM", shutdown)
process.once("SIGINT", shutdown)
```

Set `timeoutMs` below your orchestrator's grace period, not above it. Kubernetes sends `SIGKILL` 30 seconds after `SIGTERM` by default, so 25 seconds leaves the process a few seconds to close its connections and exit on its own terms. A `close` that outlives the grace period is the same as no `close` at all.

`memoryDriver` runs jobs inline on the caller's stack, so nothing is in flight while `close` runs and its abort path is never reached. `timeoutMs` on a definition still works there, and so does the held key a timed-out body keeps — test a deadline against the memory driver, and test a shutdown that cuts a running handler short against `redisDriver`.

## Operations

Five operations act on work already in Redis. `get`, `retry` and `cancel` take a job id. `pause` and `resume` take a queue name.

| Operation | What it does |
| --- | --- |
| `jobs.get(id)` | describes the job, or gives back `undefined` |
| `jobs.retry(id)` | runs a failed job again, from attempt 1 |
| `jobs.cancel(id)` | removes a job that has not started, or aborts the signal of one that runs |
| `jobs.pause(queue)` | stops every worker on that queue from taking new jobs |
| `jobs.resume(queue)` | lets them take jobs again |

### The id

A job id is the queue and the stored job together, as `queue:storedId`. It is the only thing you need to understand before using any of these operations, because the queue inside the id is where the operation looks.

Three places hand you one: `jobs.enqueue` returns it, `jobs.dead.list(queue)` returns it, and `context.id` carries it into the handler and into every hook event.

```ts
const enqueued = await jobs.enqueue(sendWelcomeEmail, { userId: "u_1", locale: "pt" })

if (!enqueued) {
	return
}

const snapshot = await jobs.get(enqueued.id)
```

The id is opaque. Read it and pass it back; do not build one.

`jobs.enqueue` gives back `EnqueuedJob | null`, and it is `null` only when an [atomic block](#atomic-block) held the job instead of delivering it. That is not only the code written inside the block: a callback scheduled from inside it inherits the block, so what it enqueues is held too — and refused outright once the block has ended. Narrow, and the id you pass on is the real one.

**A dead job id names the dead queue.** `jobs.dead.list("billing")` gives ids like `billing.dead:12`, because the copy is kept on `billing.dead`. That id serves `dead.replay` and `dead.discard`, and nothing else. `get`, `retry` and `cancel` speak of the live queues only, so a dead id is an absence to them: `get` gives back `undefined` and the other two `unknown_job`. A dead job is not a job — it is the record of one, it runs nothing, and letting `cancel` reach it would destroy the very copy you keep to replay.

An id that is not `queue:storedId` throws, and names what to pass instead. That is a mistake at the call site, not an absent job — see below.

### Absence is typed

An id no job answers to is an ordinary result, never an exception. A job is dropped by `keepCompletedForMs`, by `keepFailedCount`, or by an operator, so asking about one that is gone is normal.

`get` gives back `undefined`. The snapshot it gives back otherwise carries the composed `id`, the `queue`, the job `name`, the `state`, the `envelope`, the `attempts` it has already ended, its `maxAttempts` and the `failure` message of the last attempt that threw.

```ts
const snapshot = await jobs.get(id)

if (!snapshot) {
	return
}

console.log(snapshot.state, snapshot.attempts, snapshot.maxAttempts, snapshot.failure)
```

`state` is one of `waiting`, `active`, `delayed`, `completed`, `failed`, `waiting_children` or `unknown`. `envelope` is `undefined` when what is stored is not a jubs envelope — a job another producer put on the same queue.

`attempts` is a **count of the attempts that have ended**, not the one under way. A job that never ran reads `0`, and a job running its second attempt reads `1`, because the attempt in flight has not ended. It is not `context.attempt`, which is 1-based and names the attempt the handler is running — the same job reads `1` here and `2` there at the same moment.

`retry` and `cancel` answer with a union you switch on.

```ts
type RetryResult =
	| { outcome: "retried" }
	| { outcome: "unknown_job" }
	| { outcome: "not_failed"; state: JobState }

type CancelResult =
	| { outcome: "removed" }
	| { outcome: "aborting" }
	| { outcome: "children_running" }
	| { outcome: "unknown_job" }
	| { outcome: "finished"; state: JobState }
```

### `cancel`

What a cancellation does depends on whether the job has started.

**A job that has not started is removed.** It is gone from the queue, `get` stops answering for it, and nothing is written to the dead queue. The result is `removed`.

`removed` says the job is gone, not that it never ran. The state is read and the job is deleted in two steps, so a job that was waiting when it was read and started and finished before the deletion landed is deleted all the same, and the work it did stands. The window is narrow and it is left open on purpose: closing it would cost a lock on every cancellation to buy nothing, because whoever cancels a job racing its own start cannot know which of the two won anyway. The dead queue is declared at-least-once for the same reason.

A job that waits on children is removed with those children. They exist to feed the job you cancelled, and would otherwise finish into a parent that is gone — see [Flows](#flows).

**A cancellation that reaches a job whose children are still running cancels nothing.** The result is `children_running`. The job cannot be removed while a descendant holds its lock, and it is not running itself, so there is no delivery to abort either. Nothing changed in Redis. Cancel the running child first, or ask again once the children have settled.

**Cancelling a child of a flow kills the parent, whenever it lands.** A child that has not started is `removed`: its branch leaves the parent's dependencies, the parent is dispatched, and the slot that lost it reads short — so the parent is buried with `children_short` before its handler runs. The **same** child, once it is `active`, is aborted instead, which fails it for good and buries the parent with `child_dead`. Same call, same id, two reasons decided by a race, and neither of them lets the parent run over what is left. Cancel the **root** to cancel a whole flow; cancel a child only when you mean to end the parent too — see [Flows](#flows).

**A job that is already running cannot be removed.** BullMQ refuses to remove a job a worker holds the lock on, and jubs does not pretend otherwise. What happens instead is an abort: jubs marks the job, the runtime that holds the delivery aborts `context.signal`, and the handler ends the work itself. The result is `aborting` — the abort was asked for, not that the job has stopped.

```ts
const cancelled = await jobs.cancel(id)

if (cancelled.outcome === "aborting") {
	console.log("the handler was asked to stop; read the job again to see how it ended")
}
```

The abort crosses processes. The mark goes to Redis, so the client that cancels does not have to be the process that runs the job. It is not instant either: each runtime sweeps its running jobs every 250 milliseconds, so an abort lands within about that.

**A mark names one delivery, not the job.** A job id comes back on every attempt, so a mark that named only the id would abort a delivery nobody cancelled — the retry, or the run a `jobs.retry` asked for. The mark carries the count of deliveries the job has been handed out for, and the sweep hands it to that delivery alone. One that arrives after its delivery ended matches nothing and kills nothing. It is then only garbage, and the 30 second expiry is what sweeps it up.

The two limits of the signal are the same two a shutdown has, and they are the whole honesty of this operation. **A handler that ignores its signal runs to the end**, because nothing in Node can kill a running function. **A handler that notices the abort and returns counts as a success**, because the contract is to throw: rethrow `context.signal.reason`.

```ts
export const renderReportHandler = defineHandler(renderReport, async (data, context) => {
	const response = await fetch(`${API}/reports/${data.id}`, { signal: context.signal })

	if (context.signal.aborted) {
		throw context.signal.reason
	}

	return response.json()
})
```

A cancelled job dies with its own reason. The attempt fails, `onAttemptFailed` and `onDead` both fire, **no further attempt is spent** whatever `attempts` says, and — if the queue is in `deadQueues` — the entry is kept with the reason `cancelled` — see [Dead queue](#dead-queue). The error is a `CancelledError`, which jubs exports. So a job you cancelled by mistake is still there to replay.

A cancellation that arrives after the job settled comes back as `finished`, with the state it settled in. It changes nothing.

### `retry`

`retry` runs a **failed** job again, in place, under the same id, **from attempt 1**. Resetting the attempts is the point: a job that died with its attempts exhausted would otherwise be refused another run by BullMQ, so the operation would look like it worked and nothing would run. `get` reads the `attempts` count back at `0` afterwards, and the job gets its whole `maxAttempts` again.

A job in any other state comes back as `not_failed` with the state it is in, and nothing happens to it. A job Redis no longer keeps comes back as `unknown_job`.

```ts
const retried = await jobs.retry(id)

if (retried.outcome === "not_failed") {
	console.log(`nothing to retry — the job is ${retried.state}`)
}
```

`retry` and `dead.replay` are different operations on different objects, and the difference decides which one you can use.

| | `jobs.retry(id)` | `jobs.dead.replay(id)` |
| --- | --- | --- |
| Acts on | the failed job itself, still in Redis | the copy kept in the dead queue |
| Id | the live id, from `enqueue` or `context.id` | the dead id, from `dead.list(queue)` |
| Result | the same job runs again, same id, attempt 1 | a **new** job is enqueued, new id, and the dead entry is dropped |
| Needs | the job to be inside the `keepFailedCount` window | the definition registered in `createJobs({ definitions })` |

Retry the job while it is still there. Replay it once the failed window has rolled over it — which is the only reason the dead queue exists.

### `pause` and `resume`

**A pause is on the queue, not on the process that asks.** The state lives in Redis, so every worker on that queue stops taking new jobs, in every process — not only in the one that called. Resuming is the same in reverse.

```ts
await jobs.pause("billing")

await jobs.resume("billing")
```

A pause stops **fetching**, not the jobs already fetched: what is active when you pause runs to its end. Producers are not affected either — `enqueue` keeps working and the jobs pile up, waiting, until you resume. That is the tool for a broken downstream API: pause, fix, resume, and no job is lost.

### What these operations do not reach

**Clearing a queue does not clear the idempotency keys.** They live outside the `bull:<queue>:` namespace, so a purge leaves them where they were, and a job you enqueue again right after it can meet its own complete key and skip the handler in silence. `jobs.idempotency.forget(definition, data)` is what deletes one — see [Forgetting a key](#forgetting-a-key).

### The memory driver

`memoryDriver` keeps every job it records, so `get` and `retry` answer for real. Its jobs are only ever `waiting`, `active`, `completed` or `failed` — the four states an inline run passes through — and never `delayed`, `waiting_children` or `unknown`, which need a clock or a queue it does not have. `attempts` counts the runs that ended here, so a job reads `0` before its first run and `1` after it, and climbs only when you run it again — nothing is retried on its own. `retry` puts a failed job back in the pending line, clears its failure and puts the count back to `0`, the way a retry over Redis does.

`cancel` answers with what it knows: a job it has not run is `removed` and stops answering to `get`, a job that finished is `finished`, an unknown id is `unknown_job`, and a job it is running is marked and comes back as `aborting` — a real abort, because the sweep belongs to the runtime, not to a driver. A mark names its delivery here too, so one left behind kills no later delivery. It keeps no clock, so the 30 second expiry is ignored and a mark nobody collects never expires. Nor is there a race between reading a job's state and removing it, since jobs run inline on the caller's stack — so `removed` here really does mean the job never ran. Test the expiry, and a job that turns active under a cancellation, against `redisDriver`.

`pause` and `resume` are simulated where this driver consumes. A paused queue holds its pending jobs back: `runNext` skips them and throws when every pending job sits on a paused queue, and `drain` runs the other queues and counts only what it ran.

## Development

This section is for contributors to jubs itself, not for consumers of the library.

The unit tests need nothing. The integration tests run against a real Redis, so start one locally before you run them.

```sh
cp .env.test.example .env.test
```

`.env.test` is ignored by git. It sets `REDIS_URL` to `redis://127.0.0.1:6381` — edit it if your local Redis listens on another port. Give the suite a Redis of its own: it obliterates queues and deletes keys by pattern, so pointing it at an instance another project shares makes both suites unreliable. `bun test` loads the file on its own, so no environment prefix is needed.

```sh
bun run test
bun run test:integration
```

`test` runs the unit suite, `test:integration` runs the integration one. The integration tests namespace their queues and job names per run, so two runs never collide in Redis.

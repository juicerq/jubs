# @juicerq/juibs

A typed job library over BullMQ. BullMQ is a good queue and a poor contract: a job is a string name plus untyped `data`, so every team rebuilds payload validation, a name-to-handler lookup, and a delivery policy that lives at the call site instead of with the job. juibs gives a job a name, a payload schema, a delivery policy and a place to run, so producers and consumers never share code. You declare a definition once and get a typed enqueue, a typed handler, validation on both sides, and correct behaviour for the expensive failure modes — an enqueue inside a database transaction that rolls back, a schedule deleted from the code but still firing in Redis, a job that runs twice after a pod restart, an envelope a rolling deploy cannot parse.

## Install

```sh
bun add @juicerq/juibs bullmq
```

You also need a Redis client. juibs does not depend on one — you create the connection, and you close it.

```sh
bun add ioredis
```

## Defining a job

A definition is the producer-side description of a job: its name, its queue and its payload schema. `defineJob` returns a plain data object. It performs no I/O, so it is safe to import from any process.

`payload` is any [Standard Schema](https://standardschema.dev) validator — arktype, Zod and Valibot all qualify. The enqueue argument and the handler's `data` are both inferred from it.

```ts
// jobs/definitions.ts
import { defineJob } from "@juicerq/juibs"
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
import { defineHandler } from "@juicerq/juibs"
import { sendWelcomeEmail } from "./definitions"

export const welcomeEmailHandler = defineHandler(sendWelcomeEmail, async (data, context) => {
	console.log(`attempt ${context.attempt} of ${context.maxAttempts} for job ${context.id}`)

	await mailer.send(data.userId, data.locale)
})
```

## The two-process split

A definition carries no handler code, so the process that enqueues never imports the process that runs. This is the point of the library: your web process stays small and cannot accidentally pull in a mail client, a PDF renderer or a payment SDK.

Both processes build a client with `createJobs({ driver })`. `redisDriver` takes a Redis client you created yourself.

**The web process enqueues.** It imports definitions only.

```ts
// web.ts
import { createJobs, redisDriver } from "@juicerq/juibs"
import Redis from "ioredis"
import { sendWelcomeEmail } from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL)
const jobs = createJobs({ driver: redisDriver(connection) })

await jobs.enqueue(sendWelcomeEmail, { userId: "u_1", locale: "pt" })
```

`enqueue` validates the payload before it touches Redis. An invalid payload rejects at the call site with a `PayloadError`, and nothing is written.

**The worker process executes.** It calls `jobs.start(handlers)`, which opens one worker per queue in use and dispatches each job to the handler that owns its name. It returns a runtime with `close()`.

A worker connection must be created with `maxRetriesPerRequest: null`, because BullMQ workers open a blocking connection. `start` checks this and throws with the fix before opening any worker.

```ts
// worker.ts
import { createJobs, redisDriver } from "@juicerq/juibs"
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

juibs installs no signal handler of its own. Shutdown is yours, and it is two steps: close the runtime, then close the connection you created. The runtime owns the workers it opened — including the separate blocking connection each worker duplicates for itself — and nothing else. juibs never closes a client you passed in.

A process that only enqueues has nothing to close. Its queue handles hold no socket of their own, so quitting your connection is enough.

## Testing

`@juicerq/juibs/testing` exports `memoryDriver()`. It satisfies the same driver interface as `redisDriver`, so a test builds its client the same way the worker does — with the real payload validation and the real name-to-handler dispatch, in milliseconds, with no Redis.

`enqueue` only records. Nothing runs until you ask: `drain()` runs every pending job and returns how many ran, `runNext()` runs the oldest one, and `enqueued(definition)` returns the payloads enqueued for that definition, exactly as they were passed in.

```ts
import { createJobs, defineHandler } from "@juicerq/juibs"
import { memoryDriver } from "@juicerq/juibs/testing"
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

The memory driver does not simulate the clock, delays, backoff, retries, priority ordering, uniqueness windows, schedules or stalled recovery. Jobs run first in, first out, always on attempt 1, and a failing handler throws out of `drain()` instead of being retried. Anything time-dependent is only testable against `redisDriver`.

It accepts `attempts`, `backoff`, `priority`, `keepCompletedForMs` and `keepFailedCount`, but only `attempts` reaches your handler, as `maxAttempts`. `backoff` and `priority` are accepted and ignored. Per-queue `concurrency` is accepted and ignored too — jobs run inline, one at a time.

Everything else throws, and that is the point. `delayMs`, `unique` and a queue `limiter` are time-dependent, so the memory driver refuses them instead of pretending. The error names the option and sends you to `redisDriver`, so a behaviour this driver never learns to simulate fails loudly instead of passing a test it would fail in production.

## Payload validation on both sides

The payload is validated twice — once on enqueue, and again when the job runs. The second check matters because the job may have been written by an older deploy.

A job whose stored payload no longer validates fails unrecoverably: it burns one attempt and is not retried. So does a job whose name no handler owns. Retrying either would only fail the same way five more times.

## What `start` checks at boot

A worker whose wiring is wrong should die at boot, not at three in the morning on the one job nobody tested. `start` runs three checks before it opens a single worker, and each error says what to change.

Two handlers sharing a job name is the first. Only one of them could ever run, and which one would depend on array order.

A definition registered on a started queue with no handler is the second. Register your definitions to get it:

```ts
import { createJobs, redisDriver } from "@juicerq/juibs"
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

`noOverlap` ignores `ttlMs`, so juibs drops it: the key lives exactly as long as the job it guards. At most two jobs per key exist at any moment — one running, one waiting.

The memory driver throws on `unique`. Uniqueness is decided inside Redis, atomically and on a clock; an inline imitation would agree with your test and disagree with production. Test it against `redisDriver`.

## Scheduling

A schedule is the recurrence rule that makes a job run on its own, without a producer. It sits on the definition, beside the payload schema and the delivery policy, so the recurrence is declared where the job is declared — not in a crontab, not in a wiring file some other team owns.

```ts
// jobs/definitions.ts
import { dailyAt, defineJob } from "@juicerq/juibs"
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

`every` takes no time zone, in the type and at runtime. The zone only enters the calculation when the recurrence is a cron pattern. On an interval recurrence BullMQ never calls the cron parser, so a wrong zone there would throw nothing at all and be stored in silence. juibs refuses it instead of keeping a value that does nothing.

`start` upserts every declared schedule and **removes** the ones this library created that the code no longer declares. That removal is the point. BullMQ's `upsertJobScheduler` never removes anything on its own, so a schedule you delete from the code keeps firing in Redis until a human goes looking for it. Reconciliation touches **only** scheduler names that start with `juibs.`. The prefix is the whole test — juibs keeps no record of what it created, so any undeclared scheduler carrying that prefix is removed, whoever wrote it. A scheduler another tool created survives as long as its name does not start with `juibs.`.

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

The occurrence the scheduler produces does **not** carry `unique`. BullMQ writes the deduplication option onto the scheduler's template and then ignores it — no key is ever taken — so juibs drops it rather than promise what the layer below does not keep. The recurrence still gives every occurrence its own identity: BullMQ produces one job per occurrence, with a deterministic id. That is identity, not exclusion. `unique` keeps working normally when the same definition is enqueued by hand.

**An occurrence that takes longer than the interval overlaps the next one.** The scheduler produces the next occurrence by the clock, without looking at whether the previous one finished: a 3 second handler on `every("1 second")` reaches four runs at the same time. juibs does not prevent it, and `noOverlap` cannot help — it is the very option the scheduler's template drops. The defence is yours: an interval longer than the worst duration, or a lock inside the handler.

A scheduled definition with `delayMs` in its delivery makes `start` throw. A delay postpones one enqueue, and a recurrence has no single enqueue to postpone.

The memory driver refuses a schedule. It does not simulate the clock, so starting a queue whose handlers declare one throws. A started queue that declares no schedule reconciles to nothing and passes. Test a schedule against `redisDriver`.

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

The two failure hooks answer different questions. `onAttemptFailed` fires on **every** failed attempt, so five attempts fire it five times; it is your noise-tolerant log line. `onDead` fires **once**, only when the job gives up — the last attempt failed, or the handler threw BullMQ's `UnrecoverableError`. That is the one worth paging on. `onDead` fires whether or not a dead queue is configured.

A job that never becomes an execution fires nothing: a stored value that is not a juibs envelope has no job name to report. A job whose name no handler owns is the other way round — the envelope names it, so it fires `onAttemptFailed` and `onDead` without ever firing `onStart`.

A hook that throws never changes the job's outcome. juibs reports it on `console.error` and carries on: a broken metrics client must not fail a job that worked. Hooks are awaited, so an async hook finishes before the execution ends.

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

Name the queue, not the job. Every definition on `billing` is kept the moment it gives up — its last attempt failed, or its handler threw `UnrecoverableError`.

Two checks guard the wiring. When you register `definitions`, `createJobs` refuses a dead queue no definition uses, because a typo would otherwise keep nothing and say nothing. And `start` refuses to open a worker on `billing.dead` itself, because a worker there would eat the copies you meant to keep.

What is kept is the envelope, the serialised error and a reason: `attempts_exhausted` or `unrecoverable`.

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

A replay ignores `unique`. The key of a dead job is often still taken — by the dead job itself — and honouring it would drop the replay in silence, which is the one outcome a dead queue exists to prevent. For the same reason a replayed `keepLast` job does not sit out its window: you asked for this job now.

Replay and discard are at-least-once, like every delivery here. Two operators acting on the same dead id at the same moment can both succeed — two replays enqueue the job twice, and a replay racing a discard can do both. Nothing is lost, but a handler can run twice, so keep your handlers safe to repeat. An idempotency key is the coming remedy.

Writing to the dead queue never changes a job's outcome. If Redis refuses the write, juibs reports it on `console.error` and the job still fails the way it would have.

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

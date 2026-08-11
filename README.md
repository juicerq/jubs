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

Everything else throws, and that is the point. `delayMs` and a queue `limiter` are time-dependent, so the memory driver refuses them instead of pretending. The error names the option and sends you to `redisDriver`, so a behaviour this driver never learns to simulate fails loudly instead of passing a test it would fail in production.

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

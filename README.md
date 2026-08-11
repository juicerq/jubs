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

Of the delivery options, only `attempts` reaches your handler, as `maxAttempts`. `backoff` and `priority` are accepted and ignored — the memory driver takes the whole base `Delivery` without complaint.

An option outside that set is what throws. The error names the option and sends you to `redisDriver`, so a delivery behaviour this driver never learns to simulate fails loudly on enqueue instead of passing a test it would fail in production.

## Payload validation on both sides

The payload is validated twice — once on enqueue, and again when the job runs. The second check matters because the job may have been written by an older deploy.

A job whose stored payload no longer validates fails unrecoverably: it burns one attempt and is not retried. So does a job whose name no handler owns. Retrying either would only fail the same way five more times.

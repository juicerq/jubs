# @juicerq/jubs

A typed job library over BullMQ.

BullMQ is a good queue and a poor contract. A job is a string name plus untyped `data`, so every team rebuilds the same three things: payload validation, a name-to-handler lookup, and a delivery policy that ends up at the call site instead of on the job.

jubs gives a job a name, a payload schema, a delivery policy and a place to run. You declare it once, and the producer and the consumer never share code.

## Without jubs

```ts
await queue.add("email.welcome", { userId: "u_1", locale: "pt" })

new Worker("mail", async (job) => {
	switch (job.name) {
		case "email.welcome":
			return mailer.send(job.data.userId, job.data.locale)
	}
})
```

`job.data` is `any`. Rename `locale` on the producer and nothing complains — not the compiler, not the worker, not the job that has been sitting in Redis since the deploy before. The two sides agree on a string, and the string is checked by nobody.

## With jubs

```ts
await jobs.enqueue(sendWelcomeEmail, { userId: "u_1", locale: "pt" })
```

The payload is checked at the call site, and again when the job runs — because the job may have been written by an older deploy. The handler's `data` is typed from the same schema. There is no `switch`.

And the expensive failure modes are already handled: an enqueue inside a database transaction that rolls back, a schedule deleted from the code but still firing in Redis, a job that runs twice after a pod restart, an envelope a rolling deploy cannot parse.

## Install

```sh
bun add @juicerq/jubs bullmq ioredis
```

jubs depends on no Redis client. You create the connection, and you close it.

## Quickstart

**Define the job.** A definition is a plain data object: a name, a queue and a payload schema. It carries no handler code and does no I/O, so any process can import it.

`payload` is any [Standard Schema](https://standardschema.dev) validator — arktype, Zod and Valibot all qualify.

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

**Handle it.** One handler per definition. `data` arrives already validated.

```ts
// jobs/handlers.ts
import { defineHandler } from "@juicerq/jubs"
import { sendWelcomeEmail } from "./definitions"

export const welcomeEmailHandler = defineHandler(sendWelcomeEmail, async (data) => {
	await mailer.send(data.userId, data.locale)
})
```

**Enqueue it from the web process.** It imports definitions only, so it never pulls in the mail client.

```ts
// web.ts
import { createJobs, redisDriver } from "@juicerq/jubs"
import Redis from "ioredis"
import { sendWelcomeEmail } from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379")
const jobs = createJobs({ driver: redisDriver(connection) })

await jobs.enqueue(sendWelcomeEmail, { userId: "u_1", locale: "pt" })
```

**Run it from the worker process.** `start` opens one worker per queue in use and dispatches each job to the handler that owns its name.

A worker connection needs `maxRetriesPerRequest: null`, because BullMQ workers open a blocking connection. `start` checks this and throws with the fix before opening any worker.

```ts
// worker.ts
import { createJobs, redisDriver } from "@juicerq/jubs"
import Redis from "ioredis"
import { welcomeEmailHandler } from "./jobs/handlers"

const connection = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379", {
	maxRetriesPerRequest: null,
})

const jobs = createJobs({ driver: redisDriver(connection) })
const runtime = await jobs.start([welcomeEmailHandler])

process.on("SIGTERM", async () => {
	await runtime.close()
	await connection.quit()
})
```

That is the whole loop. Everything below is optional.

## Docs

| Guide | What it covers |
| --- | --- |
| [Definitions and handlers](https://github.com/juicerq/jubs/blob/main/docs/definitions.md) | the payload schema, the handler context, and typing what a handler returns |
| [The two-process split](https://github.com/juicerq/jubs/blob/main/docs/processes.md) | why the two processes stay apart, wrapping `enqueue`, and the four checks `start` runs at boot |
| [Delivery, tuning and shutdown](https://github.com/juicerq/jubs/blob/main/docs/delivery.md) | attempts, backoff, delay, priority, per-queue concurrency and limiter, `timeoutMs`, `close` |
| [Uniqueness and idempotency](https://github.com/juicerq/jubs/blob/main/docs/uniqueness.md) | one job in flight per key, and a job that runs once across a pod restart |
| [The atomic block and the outbox](https://github.com/juicerq/jubs/blob/main/docs/outbox.md) | enqueue inside a database transaction, delivered only once it commits |
| [Scheduling](https://github.com/juicerq/jubs/blob/main/docs/scheduling.md) | recurrence declared on the definition — delete it from the code and it stops firing |
| [Flows](https://github.com/juicerq/jubs/blob/main/docs/flows.md) | a parent job that waits on children and reads what they returned |
| [Validation and versioning](https://github.com/juicerq/jubs/blob/main/docs/versioning.md) | change a payload shape without losing the jobs already in Redis |
| [Operations](https://github.com/juicerq/jubs/blob/main/docs/operations.md) | `get`, `retry`, `cancel`, `pause`, `resume`, the lifecycle hooks, and the dead queue |
| [Testing](https://github.com/juicerq/jubs/blob/main/docs/testing.md) | `memoryDriver()` — the real validation and the real dispatch, with no Redis |
| [Dashboard](https://github.com/juicerq/jubs/blob/main/docs/dashboard.md) | a read-only Bull Board over the queues your definitions use |

The [decision records](https://github.com/juicerq/jubs/tree/main/docs/adr) say why the sharp parts are shaped the way they are.

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

# The two-process split

A definition carries no handler code, so the process that enqueues never imports the process that runs. This is the point of the library: your web process stays small and cannot accidentally pull in a mail client, a PDF renderer or a payment SDK.

Both processes build a client with `createJobs({ driver })`. `redisDriver` takes a Redis client you created yourself.

**The web process enqueues.** It imports definitions only.

```ts
// web.ts
import { createJobs, redisDriver } from "@juicerq/jubs"
import Redis from "ioredis"
import { sendWelcomeEmail } from "./jobs/definitions"

const connection = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379")
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

jubs installs no signal handler of its own. Shutdown is yours, and it is two steps: close the runtime, then close the connection you created. The runtime owns the workers it opened — including the separate blocking connection each worker duplicates for itself — and nothing else. jubs never closes a client you passed in.

A process that only enqueues has nothing to close. Its queue handles hold no socket of their own, so quitting your connection is enough.


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


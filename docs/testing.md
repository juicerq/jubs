# Testing

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

Everything else throws, and that is the point. `delayMs`, `unique` and a queue `limiter` are time-dependent, so the memory driver refuses them instead of pretending. Enqueueing a definition that declares `awaits` throws for a related reason: jobs run inline, so a parent would run before its children — see [Flows](./flows.md). The error names the option and sends you to `redisDriver`, so a behaviour this driver never learns to simulate fails loudly instead of passing a test it would fail in production.


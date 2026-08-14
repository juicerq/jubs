# Delivery, tuning and shutdown

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

One thing the pair does not reconcile is a job that dies while its body lives. The dead queue judges the **attempt**; the key follows the **body**. A job on its last attempt fails on its deadline and is buried as `attempts_exhausted`, and the body that outlived it then completes the key with its result. Both stand: the dead entry is there to replay, and the key is complete for 24 hours. `dead.replay` refuses while that body still holds the key, and forgets the key once the body has ended — so a replay runs the handler instead of handing back what the body kept — see [Dead queue](./operations.md#dead-queue).

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


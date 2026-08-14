# Drivers

`createJobs({ driver })` takes a `JobDriver`, and everything jubs knows about where a job is kept, when it is handed out and when it is handed out again lives behind that one interface. `redisDriver(connection)` is one implementation of it; `memoryDriver()` from `@juicerq/jubs/testing` is the other. No other part of the library speaks to a store.

This page is for writing a third one. `JobDriver` is exported from `@juicerq/jubs`, so a driver is an ordinary object of that shape: write it against the interface and the parameter types come with it.

## What the seam divides

The runtime above the seam owns everything that is not storage. It reads the envelope of each delivery, refuses a payload written by a newer deploy, migrates and validates that payload, finds the handler the job name belongs to, runs it under an `AbortController` and its `timeoutMs`, validates what the handler returned, fires the hooks, decides whether a failure buries the job, and writes the burial through the driver's dead store.

The driver owns the rest. It keeps the job, hands it out, hands it out again after a failure, and answers the five operations a person calls. **Nothing above the seam retries a failed delivery.** A driver that hands a failed job out only once turns `attempts: 5` into `attempts: 1`, and no test above it says so.

Every method here speaks a queue name and a stored id, never the composed `queue:storedId` id a person holds. The client cuts that id apart before it calls you and composes the id you answer with on the way back out.

## The surface

| Member                          | What it does                                                                            |
| ------------------------------- | --------------------------------------------------------------------------------------- |
| `enqueue(request)`              | keeps one job on `request.queue` and answers with the id it is kept under               |
| `consume(request)`              | opens a consumer that calls `request.run` for each delivery of `request.queue`          |
| `get(queue, id)`                | describes the job, or gives back `undefined`                                            |
| `retry(queue, id)`              | runs a failed job again, in place, from attempt 1                                       |
| `cancel(queue, id)`             | removes a job that has not started, or marks one that runs                              |
| `takeCancelled(queue, running)` | gives back the deliveries among `running` that were cancelled, and consumes those marks |
| `pause(queue)`, `resume(queue)` | stop and restart the fetching of every consumer on that queue                           |
| `reconcileSchedules(request)`   | makes the queue's schedules match the set the request declares                          |
| `dead`                          | keeps, lists, reads and removes the copies of the jobs that gave up                     |
| `idempotency`                   | keeps the three states of a key: absent, held under a lease, complete with a result     |
| `flow`                          | adds a whole tree of jobs, and reads what one parent's children settled into            |

## Enqueue

An `EnqueueRequest` carries the queue, the `Envelope` to store as the job's data, and a resolved `Delivery`. The `Delivery` is the whole tuning of that job — `attempts`, `backoff`, `priority`, `keepCompletedForMs`, `keepFailedCount`, and the optional `delayMs` and `unique`. It is resolved above the seam and applied below it: no other part of the library reads it.

`jobId` is the fourth field and the only subtle one. It is present when the caller derived the id from something of its own, which the relay does from the outbox row it delivers. A driver given one **keeps the job under that id, and gives back the job already kept under it when there is one** — storing no second job and overwriting no first one. That is what makes one row delivered twice one job.

```ts
import { randomUUID } from "node:crypto"
import type { EnqueuedJob, EnqueueRequest } from "@juicerq/jubs"

const stored = new Map<string, EnqueueRequest>()

async function enqueue(request: EnqueueRequest): Promise<EnqueuedJob> {
	const id = request.jobId ?? randomUUID()
	const key = `${request.queue}:${id}`

	if (!stored.has(key)) {
		stored.set(key, request)
	}

	return { id }
}
```

A driver that made the id up instead would run the job twice — see [A row delivered twice is one job](./outbox.md#a-row-delivered-twice-is-one-job). The guarantee only reaches as far as the job is kept: a store that drops finished jobs answers to nothing once it has dropped this one.

## Consume

`consume` opens a consumer on one queue and calls `request.run` for each delivery, no more than `concurrency` of them at a time. `request.limiter`, when it is there, caps the deliveries at `max` per `durationMs`.

The `JobDelivery` you pass to `run` names the delivery:

- `id` is the stored id, not the composed one.
- `envelope` is whatever you stored as the job's data, `unknown` and unread. The runtime reads it and refuses what is not a jubs envelope.
- `attempt` is 1-based and names the attempt about to run. `maxAttempts` is the `attempts` of the `Delivery` this job was enqueued with.
- `attemptsStarted` counts the deliveries this job has been handed out for. It grows on every one of them and a `retry` does not reset it, which is what lets a cancellation name one delivery rather than the job.

What `run` resolves with is what the handler returned. Keep it if you support flows: it is the value the parent reads back out of the slot this child filled.

### Three rejections that are not failures

`run` rejects with an error the driver is free to treat as a failed attempt — with one set of exceptions. Three rejections end a delivery **without ending an attempt**, and a driver that fails the attempt on them spends attempts on jobs nothing is wrong with. The runtime fires no failure hook and buries nothing for these three, so no one above the seam corrects for it.

| Rejection              | What the driver does instead                                       |
| ---------------------- | ------------------------------------------------------------------ |
| `ShutdownAbortError`   | hands the delivery out again after the `delayMs` the error carries |
| `LeaseHeldError`       | hands the delivery out again after a delay of its own choosing     |
| `ChildrenPendingError` | puts the job back to waiting on its children                       |

```ts
import { LeaseHeldError, ShutdownAbortError } from "@juicerq/jubs"

function postponedBy(error: unknown): number | undefined {
	if (error instanceof ShutdownAbortError) {
		return error.delayMs
	}

	if (error instanceof LeaseHeldError) {
		return 1_000
	}

	return undefined
}
```

`ShutdownAbortError` names its own delay, because a deploy that cut a handler short should cost the job nothing. `LeaseHeldError` names none: `redisDriver` picks one that doubles from 100ms up to a second, so a delivery that keeps meeting a held key backs off instead of spinning. `ChildrenPendingError` is the third, and only a driver that supports flows ever meets it — it is a parent delivered while a child it lost is still running, and the answer is to wait on the children again rather than to run a handler over a slot that is not filled. That class is not exported from `@juicerq/jubs` today, so a driver outside this package cannot narrow to it.

### Closing

`Consumer.close()` stops taking new deliveries and **resolves only once every delivery already in flight has settled**. The runtime's `close({ timeoutMs })` is built on that promise: it waits for yours, and aborts the signals of the handlers still running only when it has waited too long. A `close` that resolved early would report a drain that did not happen, and the process would exit under a running handler.

## The operations

`get` describes the job or gives back `undefined`, and absence is ordinary — a job is dropped by its retention or by an operator. The `id` on the snapshot is the stored id, and `envelope` is `undefined` when what is stored is not a jubs envelope, which is how a job another producer put on the same queue is reported rather than refused. `attempts` is a **count of the attempts that have ended**, so a job running its second attempt reads 1.

`retry` runs a **failed** job again, in place, under the same id, **from attempt 1**. Resetting the count is the point: a job whose attempts are exhausted would otherwise be refused another run, and the operation would look like it worked while nothing ran. A job in any other state comes back as `not_failed` with that state, and an id nothing answers to as `unknown_job`.

`cancel` answers with what the cancellation reached, and the union is the honest part of it. A job that has not started is `removed`, with the children it waits on. A job already running cannot be removed, so it is marked for the runtime that holds it, and the answer is `aborting` — the abort was asked for, not performed. A job whose children still run is `children_running` and **nothing was cancelled**. A job that settled first is `finished`, with its state.

`takeCancelled` is the other half of that mark. The runtime hands it the deliveries it is running right now, every 250 milliseconds while it holds any, and it gives back **the very objects it was given** so the caller aborts what it already holds. A mark is matched against the delivery it was asked against, by `attemptsStarted`, so one left behind by a delivery that ended kills no later delivery of the same job.

```ts
import type { JobDriver } from "@juicerq/jubs"

const marks = new Map<string, number>()

const takeCancelled: JobDriver["takeCancelled"] = async (queue, running) =>
	running.filter((delivery) => marks.get(`${queue}:${delivery.id}`) === delivery.attemptsStarted)
```

`pause` and `resume` stop and restart the fetching, not the jobs already fetched. `redisDriver` keeps that state in Redis, so a pause reaches every worker on the queue in every process, not only the one that asked. A driver whose state is local pauses only itself.

## Schedules

`reconcileSchedules` is called once per started queue at boot, with the whole set of schedules the definitions of that queue declare. It is a reconciliation, not an upsert: a recurrence deleted from the code stops firing because the driver removes what is no longer declared. `redisDriver` removes only the schedulers it recognises as its own, so a scheduler another producer put on the same queue survives.

A `ScheduleUpsert` carries the `Recurrence` — either `{ everyMs }` or `{ pattern }` — an optional `timezone`, and the `Envelope` and `Delivery` each firing is enqueued with.

## The three stores

`dead` keeps the copy of a job that gave up. **Its methods take the live queue name**, and where the copy goes is the driver's decision: `redisDriver` writes it to `<queue>.dead`. `bury` is called by the runtime after it decides a failure is a burial, `list`, `read` and `remove` serve `jobs.dead.list`, `jobs.dead.replay` and `jobs.dead.discard`. The id you answer `list` with names the record, not the job it is a record of — the entry carries the job's own id in `jobId`.

`idempotency` keeps a key in one of three states: absent, held by a running delivery under a lease that expires, and complete with a kept result. `acquire` mints a token that names one possession of the key, and `renew`, `complete` and `release` carry it back — a store acts only while the key still holds that token. That is what stops a worker whose lease expired under a running handler from freeing the lease a second worker took after it. `forget` carries no token, because an operator asks for it and not the delivery that took the key, which is why it refuses a key some delivery holds and answers `running`. `IdempotencyStore` is exported, but the shapes its methods pass and answer with are not, so write the store as one object typed by the interface and let each parameter be typed from there.

`flow` adds a whole tree in one step, so a flow either exists whole or not at all, and answers with the root. `read` answers for one parent with what its children settled into: the `results` of those that finished, the `failures` of those that did not, and `pending`, how many have settled into neither yet. A non-zero `pending` is what ends that delivery before the handler runs — see [Flows](./flows.md).

## Refusing what you do not support

A driver does not have to do everything. It has to be loud about what it does not do.

```ts
import type { JobDriver } from "@juicerq/jubs"

function unsupported(behaviour: string): Error {
	return new Error(`myDriver does not support "${behaviour}" — run that job against redisDriver`)
}

const flow: JobDriver["flow"] = {
	async enqueue() {
		throw unsupported("a job that waits on children")
	},

	async read() {
		throw unsupported("what the children of a job settled into")
	},
}
```

`memoryDriver` is written this way throughout. It throws on `delayMs`, on `unique`, on a queue `limiter`, on a schedule and on anything to do with flows, and the message names the behaviour and sends you to `redisDriver`. The alternative is worse than the throw: a driver that quietly ignores `delayMs` runs the job now, agrees with the test and disagrees with production. Refuse the option you cannot honour, and name it — see [Testing](./testing.md).

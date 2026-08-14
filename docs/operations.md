# Operations

Five operations act on work already in Redis. `get`, `retry` and `cancel` take a job id. `pause` and `resume` take a queue name.

| Operation            | What it does                                                              |
| -------------------- | ------------------------------------------------------------------------- |
| `jobs.get(id)`       | describes the job, or gives back `undefined`                              |
| `jobs.retry(id)`     | runs a failed job again, from attempt 1                                   |
| `jobs.cancel(id)`    | removes a job that has not started, or aborts the signal of one that runs |
| `jobs.pause(queue)`  | stops every worker on that queue from taking new jobs                     |
| `jobs.resume(queue)` | lets them take jobs again                                                 |

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

`jobs.enqueue` gives back `EnqueuedJob | null`, and it is `null` only when an [atomic block](./outbox.md#atomic-block) held the job instead of delivering it. That is not only the code written inside the block: a callback scheduled from inside it inherits the block, so what it enqueues is held too — and refused outright once the block has ended. Narrow, and the id you pass on is the real one.

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
	{ outcome: "retried" } | { outcome: "unknown_job" } | { outcome: "not_failed"; state: JobState }

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

A job that waits on children is removed with those children. They exist to feed the job you cancelled, and would otherwise finish into a parent that is gone — see [Flows](./flows.md).

**A cancellation that reaches a job whose children are still running cancels nothing.** The result is `children_running`. The job cannot be removed while a descendant holds its lock, and it is not running itself, so there is no delivery to abort either. Nothing changed in Redis. Cancel the running child first, or ask again once the children have settled.

**Cancelling a child of a flow kills the parent, whenever it lands.** A child that has not started is `removed`: its branch leaves the parent's dependencies, the parent is dispatched, and the slot that lost it reads short — so the parent is buried with `children_short` before its handler runs. The **same** child, once it is `active`, is aborted instead, which fails it for good and buries the parent with `child_dead`. Same call, same id, two reasons decided by a race, and neither of them lets the parent run over what is left. Cancel the **root** to cancel a whole flow; cancel a child only when you mean to end the parent too — see [Flows](./flows.md).

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

|         | `jobs.retry(id)`                                  | `jobs.dead.replay(id)`                                           |
| ------- | ------------------------------------------------- | ---------------------------------------------------------------- |
| Acts on | the failed job itself, still in Redis             | the copy kept in the dead queue                                  |
| Id      | the live id, from `enqueue` or `context.id`       | the dead id, from `dead.list(queue)`                             |
| Result  | the same job runs again, same id, attempt 1       | a **new** job is enqueued, new id, and the dead entry is dropped |
| Needs   | the job to be inside the `keepFailedCount` window | the definition registered in `createJobs({ definitions })`       |

Retry the job while it is still there. Replay it once the failed window has rolled over it — which is the only reason the dead queue exists.

### `pause` and `resume`

**A pause is on the queue, not on the process that asks.** The state lives in Redis, so every worker on that queue stops taking new jobs, in every process — not only in the one that called. Resuming is the same in reverse.

```ts
await jobs.pause("billing")

await jobs.resume("billing")
```

A pause stops **fetching**, not the jobs already fetched: what is active when you pause runs to its end. Producers are not affected either — `enqueue` keeps working and the jobs pile up, waiting, until you resume. That is the tool for a broken downstream API: pause, fix, resume, and no job is lost.

### What these operations do not reach

**Clearing a queue does not clear the idempotency keys.** They live outside the `bull:<queue>:` namespace, so a purge leaves them where they were, and a job you enqueue again right after it can meet its own complete key and skip the handler in silence. `jobs.idempotency.forget(definition, data)` is what deletes one — see [Forgetting a key](./uniqueness.md#forgetting-a-key).

### The memory driver

`memoryDriver` keeps every job it records, so `get` and `retry` answer for real. Its jobs are only ever `waiting`, `active`, `completed` or `failed` — the four states an inline run passes through — and never `delayed`, `waiting_children` or `unknown`, which need a clock or a queue it does not have. `attempts` counts the runs that ended here, so a job reads `0` before its first run and `1` after it, and climbs only when you run it again — nothing is retried on its own. `retry` puts a failed job back in the pending line, clears its failure and puts the count back to `0`, the way a retry over Redis does.

`cancel` answers with what it knows: a job it has not run is `removed` and stops answering to `get`, a job that finished is `finished`, an unknown id is `unknown_job`, and a job it is running is marked and comes back as `aborting` — a real abort, because the sweep belongs to the runtime, not to a driver. A mark names its delivery here too, so one left behind kills no later delivery. It keeps no clock, so the 30 second expiry is ignored and a mark nobody collects never expires. Nor is there a race between reading a job's state and removing it, since jobs run inline on the caller's stack — so `removed` here really does mean the job never ran. Test the expiry, and a job that turns active under a cancellation, against `redisDriver`.

`pause` and `resume` are simulated where this driver consumes. A paused queue holds its pending jobs back: `runNext` skips them and throws when every pending job sits on a paused queue, and `drain` runs the other queues and counts only what it ran.

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

| Reason               | What died                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------- |
| `attempts_exhausted` | the last attempt failed                                                                     |
| `unrecoverable`      | the handler threw `UnrecoverableError`                                                      |
| `version_ahead`      | the payload was written by a newer deploy than this worker runs                             |
| `cancelled`          | `jobs.cancel(id)` reached the job while it ran — see [Operations](#operations)              |
| `child_dead`         | a child of this job failed every attempt — see [Flows](./flows.md)                          |
| `children_short`     | a slot of this job holds fewer children than it was enqueued with — see [Flows](./flows.md) |

```ts
const dead = await jobs.dead.list("billing")

for (const job of dead) {
	console.log(job.id, job.envelope.name, job.reason, job.error.message)

	if (job.reason === "version_ahead") {
		await jobs.dead.replay(job.id)
		continue
	}

	await jobs.dead.discard(job.id)
}
```

`id` is opaque — it names the queue and the stored job together. Read it from `list` and pass it back; do not build one.

`replay` enqueues the job again from the stored envelope, so it runs with the payload it was first given, then drops the dead entry. It needs the definition, so the client that replays must register it in `createJobs({ definitions })` — the dead entry stores a job name, and only the definition knows the queue and the delivery policy that name gets today. `discard` drops the entry and enqueues nothing.

`replay` refuses a job that was part of a flow, whatever killed it, and names the repair path instead — see [Flows](./flows.md).

A replay ignores `unique`. The key of a dead job is often still taken — by the dead job itself — and honouring it would drop the replay in silence, which is the one outcome a dead queue exists to prevent. For the same reason a replayed `keepLast` job does not sit out its window: you asked for this job now.

Replay and discard are at-least-once, like every delivery here. Two operators acting on the same dead id at the same moment can both succeed — two replays enqueue the job twice, and a replay racing a discard can do both. Nothing is lost, but a handler can run twice, so keep your handlers safe to repeat. `idempotencyKey` is the remedy — see [Idempotency](./uniqueness.md#idempotency).

One narrow window belongs to the replay itself: it forgets the key and then enqueues, so an enqueue Redis refuses leaves the key deleted with nothing enqueued. The dead record survives that failure and replaying again recovers the job — what does not come back is the guard, so another producer of the same payload inside the retention window runs the handler unguarded.

**A replay forgets the job's idempotency key before it enqueues, and refuses while a delivery holds that key** — so it either runs the handler or throws, and never comes back green over work nobody did. The refusal is the ordinary answer right after a `timeoutMs` burial; the dead record survives it, so you replay again once the body has ended — see [Forgetting a key](./uniqueness.md#forgetting-a-key).

Writing to the dead queue never changes a job's outcome. If Redis refuses the write, jubs reports it on `console.error` and the job still fails the way it would have.

`onDead` is separate, and fires whether or not a dead queue is configured. The hook is the page; the dead queue is the copy you replay from.

# Scheduling

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

| Constructor                   | When the job runs                              |
| ----------------------------- | ---------------------------------------------- |
| `every("5 minutes")`          | at that interval — second, minute, hour or day |
| `dailyAt("07:00")`            | every day at that time, in 24 hours            |
| `weeklyOn("monday", "09:00")` | that weekday, at that time                     |
| `monthlyOn(1, "00:00")`       | that day of the month, at that time            |
| `cron("0 */6 * * *")`         | whenever the pattern says                      |

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

The occurrence the scheduler produces does **not** carry `unique`. BullMQ writes the deduplication option onto the scheduler's template and then ignores it — no key is ever taken — so jubs drops it rather than promise what the layer below does not keep. The recurrence still gives every occurrence its own identity: BullMQ produces one job per occurrence, with a deterministic id. That is identity, not exclusion. `unique` keeps working normally when the same definition is enqueued by hand. An occurrence that must run exactly once needs `idempotencyKey` instead — see [Idempotency](./uniqueness.md#idempotency).

**An occurrence that takes longer than the interval overlaps the next one.** The scheduler produces the next occurrence by the clock, without looking at whether the previous one finished: a 3 second handler on `every("1 second")` reaches four runs at the same time. jubs does not prevent it, and `noOverlap` cannot help — it is the very option the scheduler's template drops. The defence is yours: an interval longer than the worst duration, or a lock inside the handler.

A scheduled definition with `delayMs` in its delivery makes `start` throw. A delay postpones one enqueue, and a recurrence has no single enqueue to postpone.

A definition cannot declare `schedule` and `awaits` together, and `defineJob` refuses the pair. A recurrence enqueues one job on its own, with nothing to fill its slots — see [Flows](./flows.md) for what to write instead.

The memory driver refuses a schedule. It does not simulate the clock, so starting a queue whose handlers declare one throws. A started queue that declares no schedule reconciles to nothing and passes. Test a schedule against `redisDriver`.

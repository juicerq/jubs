# Flows

A flow is a job that waits on child jobs and reads their results. The parent runs once, after every child has settled, with what they returned at hand. It nests to any depth and crosses queues.

**A definition declares what it waits on.** `awaits` maps a **slot** name to the definition that fills it. The slot is what names that child everywhere after — in the id Redis stores it under, in the results the handler reads, and in the failure that buries the parent — so two slots holding the same definition stay apart.

```ts
// jobs/definitions.ts
export const fetchRows = defineJob({
	name: "report.fetch",
	queue: "analytics",
	payload: type({ source: "string" }),
	result: type({ rows: "number" }),
})

export const readBudget = defineJob({
	name: "report.budget",
	queue: "analytics",
	payload: type({ month: "string" }),
	result: type({ target: "number" }),
})

export const buildReport = defineJob({
	name: "report.build",
	queue: "reports",
	payload: type({ month: "string" }),
	result: type({ url: "string.url" }),
	awaits: { rows: [fetchRows], budget: readBudget },
})

export const mailReport = defineJob({
	name: "report.mail",
	queue: "reports",
	payload: type({ to: "string" }),
	awaits: { report: buildReport },
})
```

A definition that declares `awaits` makes every enqueue of it a flow. `jobs.enqueue` then takes a third argument: what fills every slot.

```ts
const enqueued = await jobs.enqueue(
	mailReport,
	{ to: "finance@example.com" },
	{
		report: {
			data: { month: "2026-01" },
			awaits: {
				rows: [{ source: "ledger" }, { source: "invoices" }],
				budget: { month: "2026-01" },
			},
		},
	},
)
```

The two `fetchRows` jobs and `readBudget` run first, on `analytics`. `buildReport` runs when all three are done, `mailReport` when `buildReport` is done. `enqueue` gives back the id of the **root** — the job the producer holds — in the same form an ordinary enqueue gives back. The whole tree is added in one step and validated before anything reaches Redis, so a node a rule refuses stops the flow with nothing enqueued.

**The shape of a slot declares how many children it holds.** `rows: [fetchRows]` waits on many `fetchRows`; `budget: readBudget` waits on exactly one. The array holds one definition and means _many of these_ — it is not a list of different jobs. Two definitions in one slot do not typecheck, and the error TypeScript gives for `[a, b]` is `not assignable to type 'undefined'`, which says nothing useful; the rule is one definition per slot, a slot per job you wait on.

The arity carries through: a slot declared as an array is filled with an array and read back as an array, and a slot declared bare is filled with one value and read back as one value.

**A slot is filled with a payload — unless the job in it waits on children of its own.** Then it is filled with `{ data, awaits }`: `data` is that job's payload, `awaits` is what fills _its_ slots. That is the whole of nesting, and it repeats to any depth. In the example above, `report` takes the wrapper because `buildReport` declares `awaits`, while `rows` and `budget` take plain payloads because `fetchRows` and `readBudget` wait on nothing.

`context.children` is how a parent reads what its children returned.

```ts
export const buildReportHandler = defineHandler(buildReport, async (data, context) => {
	const rows = context.children.rows.reduce((sum, part) => sum + part.rows, 0)

	return { url: await reports.render(data.month, rows, context.children.budget.target) }
})
```

It is a plain object, not a call: one entry per slot, typed from `awaits`. Here `children.rows` is `readonly { rows: number }[]` and `children.budget` is `{ target: number }`. Each value comes back through the `result` schema of the definition its slot declares, applied to the JSON Redis kept.

**Every slot is read and validated before the handler runs**, not inside it. So a value a schema refuses fails the job for good, with no handler code having run — the only honest outcome, since the child is done and another attempt would read the very same value. A definition that declares no `awaits` reads an empty object and touches Redis for nothing.

The read is decided by the definition, never by what enqueued the job. A definition that declares `awaits` has its children read on every delivery — one enqueued by hand, one left in Redis by an older deploy, one a schedule produced. There is no path where a job that waits on children reaches its handler over slots nobody filled.

**A flow is for fan-in.** Use it when one job needs the results of several. A job that merely _follows_ another is not a flow: enqueue it at the end of the first job's handler. That way a failure retries the second job alone, where a flow would keep the first job's whole subtree in Redis to say the same thing.

```ts
export const chargeCardHandler = defineHandler(chargeCard, async (data) => {
	const receipt = await gateway.capture(data.orderId)

	await jobs.enqueue(sendReceipt, { orderId: data.orderId, receipt: receipt.id })
})
```

**Uniqueness does not apply inside a flow, at any position.** BullMQ forbids deduplication beside a parent, so `defineJob` refuses `unique` on a definition that declares `awaits`, and the enqueue refuses it again at every node of the tree. The second refusal is not a duplicate: a definition that declares no `awaits` is a perfectly ordinary job on its own, and still cannot be deduplicated once it fills somebody's slot. Drop `unique` from its delivery, or enqueue that job on its own.

**`idempotencyKey` is refused on a definition that declares `awaits`.** A parent is fed by its children as much as by its payload, so a key derived from the payload alone names two different runs: a second flow over different children would meet the complete key and replay the first flow's result. A definition that waits on nothing keeps its key, because it really is its payload alone — including when it fills a slot.

**`schedule` is refused on a definition that declares `awaits`.** A recurrence enqueues one job on its own, with no producer to fill its slots, so every tick would run over children it never had. `defineJob` refuses the pair. Give the `schedule` to a job that waits on nothing, and let that job's handler enqueue the flow with `jobs.enqueue`.

```ts
export const nightlyReport = defineJob({
	name: "report.nightly",
	queue: "reports",
	payload: type({ month: "string" }),
	schedule: dailyAt("02:00", { data: { month: "2026-01" } }),
})

export const nightlyReportHandler = defineHandler(nightlyReport, async (data) => {
	await jobs.enqueue(
		mailReport,
		{ to: "finance@example.com" },
		{
			report: {
				data: { month: data.month },
				awaits: { rows: [{ source: "ledger" }], budget: { month: data.month } },
			},
		},
	)
})
```

**A slot name cannot hold `:` or `~`.** A child is stored under an id built from the slot it fills, `<slot>~<uuid>`, and a job id is cut at its first colon to find the queue. Either character would make that id come apart, so `defineJob` refuses the name up front instead of letting the failure surface as an unreadable id later.

**Every slot is filled at enqueue, and `jobs.enqueue` checks it at run time too.** The types say this already, and they stop saying it wherever a `JobDefinition` is widened to its default type parameters — that erases `Awaits`, and the third argument becomes optional at the type level. So the run-time check is not redundant. It reads the definition alone and refuses, before Redis is touched, an enqueue that passes nothing, one that leaves a slot empty, and one whose arity disagrees with the declaration. Each refusal names the slot.

**A child that fails every attempt buries its parent.** The child does not fail the parent inside Redis — it drops out of the parent's dependencies, the parent runs, finds the failure and is buried with the reason `child_dead` before its handler is ever called. The dead entry keeps `children`: what the children that **did** finish returned, as the raw JSON Redis holds. So a replay is an informed decision rather than a guess. It propagates: a burial for `child_dead` buries the grandparent the same way.

`children` is on the `child_dead` entry alone — a burial for any other reason cannot be written holding them — so narrow on `reason` before you read them.

```ts
const [dead] = await jobs.dead.list("reports")

if (dead?.reason === "child_dead") {
	console.log(dead.error.message) // names each failed child: its slot, its definition, its id
	console.log(dead.children) // what the others returned
}
```

The message names the slot **and** the definition that slot declares, because the slot alone does not say what ran and the definition alone does not say which of two slots it was.

**A flow job cannot be replayed.** `dead.replay` refuses any entry whose `origin` is `flow`, and says so: the replayed parent would be enqueued with no children at all, run over an empty result set and complete green. Put the flow back together instead.

```ts
await jobs.retry(childId) // the id from the parent entry's error message
await jobs.retry(parentId) // dead.jobId — the parent itself, still in Redis

await jobs.dead.discard(childEntry.id) // the records left behind
await jobs.dead.discard(parentEntry.id)
```

`jobs.retry(childId)` returns the child to its parent's dependencies, and the parent then runs over a full set of results.

**A child still running sends the parent back to waiting, spending no attempt.** A parent can reach its dispatch while one of its children is still in flight — a child that was retried after the parent was already queued behind it. The runtime counts the children that have not settled and ends that delivery before the handler, so no handler runs over a slot that is still filling. The parent then returns to `waiting_children`, and the child that settles last releases it, the same way it does the first time around. The attempt count is untouched, so a child slower than the parent's backoff costs the parent nothing.

**A slot that lost a child buries the parent.** Redis records nothing when a child leaves its parent: a cancelled or removed child drops out of the parent's dependencies, and what stays behind is a slot short by exactly the children it lost — byte for byte a slot that was enqueued smaller. So the envelope carries how many children each slot was given at enqueue, one count per slot. The runtime reads those counts against what arrived, and buries the parent with the reason `children_short` when a slot holds fewer. The handler is never called: it would read that slot short and complete green.

Nothing here is retryable, because there is nothing to retry — the missing child was cancelled, removed, or never enqueued at all. Enqueue the flow again from the top with `jobs.enqueue(definition, data, awaits)`, which builds the whole tree. `dead.replay` is not the repair: it enqueues one envelope, and this envelope cannot be replayed into children it never had.

The same burial catches an envelope that carries no counts at all. That envelope was written by a build that did not record them — a flow left in Redis across the deploy that added the counts — or it was enqueued with no children at all. Nothing left in Redis tells the two apart, so the runtime cannot know whether those slots are full or empty, and running the handler would risk reading an empty slot and completing green. It is buried as `children_short` too, and the message tells the two burials apart: a slot that went short names its counts, a missing record names the absent counts.

```ts
const [dead] = await jobs.dead.list("reports")

console.log(dead?.reason) // "children_short"
console.log(dead?.error.message) // names each short slot: what it was given, what arrived
```

**A cancellation that reaches a parent whose children are still running cancels nothing.** The result is `children_running`, and nothing changed in Redis: the job cannot be removed while a descendant holds a lock, and it is not running itself, so there is no delivery to abort either. Cancel the running child first, or ask again once the children have settled. Cancelling a parent that is merely waiting does remove it, together with its children — see [Operations](./operations.md) for what cancelling a single child does instead.

**Keep a child's `result` schema idempotent.** It runs twice: once when the child finishes, on what the handler returned, and once when the parent reads it, on the JSON Redis kept. A transform that is not idempotent under JSON passes the first and fails the second — `string.numeric.parse` stores `500` and then refuses `500`, because `500` is no longer a string. The parent dies for good on that, with no further attempt: the child is done, so running the parent again would read the very same value.

The memory driver does not simulate flows. Enqueueing a definition that declares `awaits` throws there, and so does reading what the children of such a job settled into. Jobs run inline, so nothing ever reaches `waiting_children`, and a parent that ran before its children would agree with your test and disagree with production. An empty answer would be worse than a refusal: the runtime treats empty slots as a burial, so this driver would manufacture a `children_short` no Redis ever produced. Test a flow against `redisDriver`.

### Two sharp edges

**Build the third argument inline, or annotate it.** An input written straight inside the `enqueue` call is checked slot by slot, so a mistake points at the leaf that is wrong. The same input built in a `const` first is checked as a whole afterwards, and the error becomes a wall as deep as the tree, with the real cause at the bottom. `AwaitsInput<typeof definition>` as an explicit annotation puts the leaf error back.

```ts
import type { AwaitsInput } from "@juicerq/jubs"

const input: AwaitsInput<typeof mailReport> = {
	report: {
		data: { month: "2026-01" },
		awaits: {
			rows: [{ source: "ledger" }],
			budget: { month: "2026-01" },
		},
	},
}

await jobs.enqueue(mailReport, { to: "finance@example.com" }, input)
```

**`{ data, awaits }` is the least guessable rule in this API.** Whether a slot takes a payload or that wrapper depends on the job in it, not on the slot, so the same slot changes shape the day its definition starts waiting on something. Get it wrong and the compiler complains that a field of your payload does not exist on `{ data, awaits }` — true, and no help at all in finding the rule. At run time it is worse: a value in neither shape reads as nothing at all, so what you see is that child's own payload refusal, or the refusal of the slots it was never given. If a slot's error talks about a payload you did think you passed, the wrapper is what is missing.

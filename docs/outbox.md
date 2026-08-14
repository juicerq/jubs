# The atomic block and the outbox

## Atomic block

An enqueue lands in Redis the moment you make it. A transaction around it does not hold it back, because Redis knows nothing of your database:

```ts
await db.transaction(async (tx) => {
	const order = await createOrder(tx, cart)

	await jobs.enqueue(chargeCard, { orderId: order.id }) // in Redis already

	await reserveStock(tx, cart) // throws
})
```

The transaction rolls back and the order is gone. The job is not: a worker takes it, reads an order that no row answers to, and fails — or worse, charges a card for an order nobody placed.

`jobs.atomic(fn)` holds every job enqueued inside `fn` and delivers them, in the order they were enqueued, only once `fn` resolves. An `fn` that throws delivers nothing.

```ts
await jobs.atomic(async () => {
	await db.transaction(async (tx) => {
		const order = await createOrder(tx, cart)

		await jobs.enqueue(chargeCard, { orderId: order.id }) // held

		await reserveStock(tx, cart)
	})
})
```

The block wraps the transaction, not the other way around: it has to outlive the commit, because delivering a job the commit then refuses is the very thing it prevents.

It holds every kind of enqueue — a job on its own and a whole flow — and it holds nothing else.

**Only enqueues are held.** `jobs.dead.discard(id)`, `cancel`, `retry`, `pause` and `resume` act the moment you call them, inside a block or out of it. A `discard` inside a block that then throws has already destroyed the dead record. `jobs.dead.replay(id)` is refused outright inside a block, and says so: a replay forgets the payload's idempotency key, enqueues the job and drops the dead record, and no block makes those three one act.

**A worker started inside a block does not join it.** `jobs.start(handlers)` leaves the block before it consumes, so the jobs its handlers enqueue are delivered at once, however long the worker runs.

**A held enqueue gives back `null`.** `jobs.enqueue` answers with `EnqueuedJob | null`, and it is `null` exactly when a block held the job: there is no job yet, so there is nothing to name. Nothing tells a call site which of the two it is — the block is invisible to the code inside it, which is what lets you wrap code that knows nothing of jubs — so every enqueue reads the same type.

**Every enqueue pays for that, block or no block.** `jobs.get(enqueued.id)` no longer compiles on its own, because `enqueued` may be `null`. Narrow once and pass the narrowed id on.

```ts
const enqueued = await jobs.enqueue(chargeCard, { orderId })

if (!enqueued) {
	throw new Error("this enqueue was held by an atomic block")
}

await jobs.get(enqueued.id)
```

Inside a block, do not ask for the id at all: ask after the block, or move the enqueue that needs one out of it.

**A block inside a block joins the outer one.** Only the outermost block delivers, and it delivers once. Two blocks running at once hold their own jobs and never see each other's.

**A delay counts from the flush, not from the call.** A job whose delivery declares `delayMs` starts counting when the block delivers it, so a block that runs for `D` puts that job `D + delayMs` away from the moment you enqueued it.

**Two enqueues raced inside one block may swap places.** The order is the order in which each call reaches the buffer, which is after its payload has been validated. Validation is synchronous for an Arktype schema, so calls made in sequence keep their sequence; only enqueues started together, with `Promise.all` over a schema that validates asynchronously, can land in the other order.

### What a block does not cover

**What decides is where the callback was scheduled, not whether it is detached.** A block is an async context, and a timer, a listener or a promise created inside `fn` inherits it. So a `setImmediate` scheduled from inside the block is still inside the block when it runs — and it runs after `fn` resolved, which is after the block already delivered what it held.

An enqueue that arrives then is **refused**, because it can no longer be held and delivering it would break the promise the block makes:

```ts
await jobs.atomic(async () => {
	setImmediate(async () => {
		await jobs.enqueue(chargeCard, { orderId }) // throws: the block has ended
	})
})
```

The same holds for a promise `fn` starts and never awaits. Await the work inside `fn`, and the block holds its jobs; or enqueue outside the block, and it is delivered at once. A callback registered **outside** the block, even one an event inside the block triggers, never sees the block at all: its enqueue lands in Redis immediately.

**The delivery of a block is not atomic.** The held jobs are delivered one by one. A driver that fails on the third leaves the first two in Redis, and everything after the third is abandoned without being tried. `atomic` rejects with an error naming both counts, carrying the driver's own failure as its `cause`. Rolling the first two back is not on offer: a job already taken by a worker cannot be unenqueued — so running the block again re-delivers the ones that did land.

**The jobs are held in memory.** A process that dies inside a block loses every job it held, and nothing records that they existed. This is the gap an outbox closes, by writing the envelopes inside your own transaction — that is `jobs.atomic(fn, { tx })`, and the next section is all of it.

Use a block for what a rollback would otherwise contradict. For a job that must survive a crash between the commit and the delivery, use the outbox.

## Outbox and relay

An atomic block holds its jobs in memory. A process killed between the commit and the flush takes them with it, and the row it wrote is left with no job to act on it. The **outbox** closes that window: the jobs are written to a table of your own, in the same transaction as the state change, so the two commit together or neither does. The **relay** reads that table afterwards and puts the jobs in the queue.

You own the table, the columns and the queries. This library opens no connection, knows no SQL and never sees your transaction — it hands you envelopes and takes back what you claim.

```ts
import { createJobs, redisDriver } from "@juicerq/jubs"

const jobs = createJobs({
	driver: redisDriver(connection),
	definitions: [chargeCard],
	outbox: kyselyOutbox(db),
})

await db.transaction().execute(async (tx) => {
	await jobs.atomic(async () => {
		const order = await createOrder(tx, cart)

		await jobs.enqueue(chargeCard, { orderId: order.id })

		await reserveStock(tx, cart)
	}, { tx })
})
```

The block enqueues nothing. It collects the envelopes, and when it resolves it hands them to `outbox.save(envelopes, tx)` — one call, inside your transaction. A block that throws saves nothing, and the transaction that would have rolled back rolls back the rows too.

**The two scopes invert.** Without a `tx`, the block wraps the transaction, because it has to outlive the commit. With a `tx`, the block sits *inside* the transaction, because `save` writes on a transaction that is still open. Wrapping a transaction around a block that already holds a `tx` writes the rows and delivers nothing, forever.

**`tx` is opaque.** It travels from your `jobs.atomic` call to your `outbox.save` untouched. Whatever your database library calls a transaction is what arrives, and casting it back is the one cast the adapter makes.

### The relay

```ts
const relay = jobs.startRelay()

process.on("SIGTERM", async () => {
	await relay.close()
})
```

One cycle claims a batch of rows, enqueues a job for each, and marks the rows it delivered. `close()` stops the loop and waits for the cycle under way.

- `limit` — how many rows one cycle claims. 100 by default.
- `intervalMs` — how long the relay waits between two cycles. 1000 by default.
- `onLeftBehind` — runs for every row the relay leaves behind, with the reason it was left behind on. See [what the outbox refuses](#what-the-outbox-refuses).

**One cycle runs at a time.** A cycle that outruns the interval holds the next tick back instead of running beside itself.

**The relay client needs the definitions.** A row keeps the envelope and nothing else: the queue the job goes to and the delivery policy it is enqueued with are read from the definition, the way a replay reads them. Register in `createJobs({ definitions })` every job your outbox may carry. The client that writes and the client that relays need not be the same one, and usually are not — every producer writes, and one process relays.

**The producer's enqueue answers `null`,** as it does in any block. There is no job yet — there is a row.

### A row delivered twice is one job

The relay dies between the enqueue and the mark, and the rows it delivered are claimed again. That is the normal end of a deploy, not an accident, and it produces no second job: the job is stored under an id derived from the row, `outbox-<row id>`, and BullMQ's `add` refuses to store a second job under an id it already keeps. It hands back the one that is there, without overwriting its data. The job runs once.

The same rule sets what your row ids may be. **A row id carrying a colon is refused**, and so is an empty one: BullMQ reads a custom id with colons as a queue-and-id pair, and answers an empty id by making one up — which would mark the row delivered for a job nothing can name. A `bigserial` or a `uuid` is what to name rows with.

**The ceiling of the guarantee is Redis's retention.** The id only collides while the job is still in Redis, and every promise above holds up to that line and no further. A completed job leaves on time — `keepCompletedForMs`, one hour by default. A failed job leaves on volume: `keepFailedCount` keeps the last 200 failures of the queue, so a busy queue drops a failure in minutes and a quiet one keeps it for weeks. Past either line the id answers to nothing, and the same row delivered again enqueues a job that runs a second time. Keep the completed window wider than the time a dead relay may take to come back, and read [delivery policy](./delivery.md#delivery-policy) for both settings.

**A row whose job died for good is a silent no-op.** The failed job is still retained, so the `add` hands it back and nothing runs — and the relay is told nothing, because from Redis's side that is an ordinary answer. Replay it from the dead queue with `jobs.dead.replay(id)`.

### What the outbox refuses

**A flow is refused inside `jobs.atomic(fn, { tx })`,** and the enqueue throws. A row holds one envelope; a flow is a tree, and what each slot was given would be lost on the way through the table. Enqueue the flow outside the block, or hold it with `jobs.atomic(fn)` alone.

**A `tx` inside a block that is already open is refused,** and throws before the block runs. The outer block delivers what it holds while the inner one would write it, and the inner block does not get to decide for both.

**`jobs.dead.replay(id)` is refused in any block,** with or without a `tx`.

**A job that declares `unique` is refused,** and the enqueue throws. Uniqueness does not survive the outbox: BullMQ reads the derived id first and the deduplication key second, so a key already pointing at another job answers with *that* job's id — the row would be marked delivered by a job that is not its own, and the job the row stands for would never exist. Passing it through in silence is the one thing not on offer. Drop `unique` from the definition, or enqueue that job outside the block, where uniqueness works and the transaction does not cover it. A job that must not run **twice** wants [`idempotencyKey`](./uniqueness.md#idempotency) instead, which travels through the outbox untouched.

**A row the relay cannot deliver is left behind, and the rows around it go on.** A job name no definition on that client answers to, or a payload its schema refuses, takes that row out of the cycle and nothing else: the rows before and after it are enqueued and marked, and only the bad one stays unmarked. The failure names the row, through `console.error`, once per cycle.

**That row does not fix itself.** Every cycle claims it again and fails on it again, so it holds a place in each batch and writes a line in each log. Two acts end it, and both are outside the relay: register the definition the failure names on the relaying client, or take the row out of the claim — mark it delivered by hand, or delete it. The adapter below automates the second one: it counts how often a row has been claimed and stops claiming it after ten, so a poisoned row leaves the batch on its own. Read what it gave up on:

```sql
select id, envelope, claims from jubs_outbox where delivered_at is null and claims >= 10;
```

**A hook says why the row was left behind.** The relay classifies the failure and hands it to `onLeftBehind`, so your table can keep a status of its own:

```ts
const relay = jobs.startRelay({
	async onLeftBehind({ rowId, name, reason, error }) {
		await db
			.updateTable("jubs_outbox")
			.set({ status: reason, failure: `${name}: ${error.message}` })
			.where("id", "=", rowId)
			.execute()
	},
})
```

`reason` is one of three. `driver_failed` is a delivery that failed inside the driver: the row is good and its job is not lost. `version_ahead` is a payload a newer deploy wrote: the row is good, and a process running that version delivers it. `unrecoverable` is a row this process cannot deliver until something outside the relay changes — an unregistered definition, a definition that waits on children, a row id no job id can be derived from, a payload its schema refuses, or a migration of yours that threw. The two good reasons are the rows to leave in the claim; only `unrecoverable` is the row an operator takes out of it.

The console report happens either way, and the hook is what you add on top of it. It may be async, and the cycle awaits it before it marks the rows it delivered. A hook that throws is reported to the console and changes nothing else: the row stays left behind, and the rows around it are delivered and marked as usual.

### Running more than one relay

Nothing in this library keeps two relays off one row. **The claim does**, and the claim is yours: lock the rows you read and skip the rows another relay holds. The adapter below does it with `for update skip locked`, and leases the rows it claims, so a relay killed mid-cycle releases its rows after a minute instead of holding them until someone notices.

**The lease is part of the contract, not a nicety of that adapter.** A cycle marks the rows it delivered and leaves the rest claimed — the row nothing could deliver, and every row of the batch a killed relay never reached. Those rows come back to a later cycle only because the claim expires. A `claim` of your own that holds a row for good hands it out once and never again: the job that row carries never runs, and nothing tells you — no failure, no line in the log, a row sitting undelivered in a table nobody reads. Every promise here about a row being claimed again rests on your claim releasing it.

### A Kysely adapter

The table:

```sql
create table jubs_outbox (
	id bigserial primary key,
	envelope jsonb not null,
	claims integer not null default 0,
	claimed_at timestamptz,
	delivered_at timestamptz,
	status text,
	failure text,
	created_at timestamptz not null default now()
);

create index jubs_outbox_claimable on jubs_outbox (id) where delivered_at is null;
```

`id` is a `bigserial`, so it carries no colon and is never empty. The partial index is what keeps the claim off the rows already delivered, however long you keep them. `claims` counts how many times a row has been handed to a relay, and is what lets the claim give up on a row it cannot get past. `status` and `failure` are what the `onLeftBehind` hook above writes: the reason the relay classified, and the failure it was left behind on. The relay never reads them, so they stay null until a row is left behind, and dropping both columns costs you only that hook.

```ts
import type { Envelope, Outbox } from "@juicerq/jubs"
import type { ColumnType, Generated, Kysely, Transaction } from "kysely"

export interface Database {
	jubs_outbox: {
		id: Generated<string>
		envelope: ColumnType<Envelope, string, string>
		claims: Generated<number>
		claimed_at: Date | null
		delivered_at: Date | null
		status: string | null
		failure: string | null
		created_at: Generated<Date>
	}
}

const CLAIM_LEASE_MS = 60_000

const MAX_CLAIMS = 10

export function kyselyOutbox(db: Kysely<Database>): Outbox {
	return {
		async save(envelopes, tx) {
			await (tx as Transaction<Database>)
				.insertInto("jubs_outbox")
				.values(envelopes.map((envelope) => ({ envelope: JSON.stringify(envelope) })))
				.execute()
		},

		async claim(limit) {
			const rows = await db
				.updateTable("jubs_outbox")
				.set((eb) => ({ claimed_at: new Date(), claims: eb("claims", "+", 1) }))
				.where((eb) =>
					eb(
						"id",
						"in",
						eb
							.selectFrom("jubs_outbox as o")
							.select("o.id")
							.where("o.delivered_at", "is", null)
							.where("o.claims", "<", MAX_CLAIMS)
							.where((claimable) =>
								claimable.or([
									claimable("o.claimed_at", "is", null),
									claimable("o.claimed_at", "<", new Date(Date.now() - CLAIM_LEASE_MS)),
								]),
							)
							.orderBy("o.id")
							.limit(limit)
							.forUpdate()
							.skipLocked(),
					),
				)
				.returning(["id", "envelope"])
				.execute()

			return rows
		},

		async markDelivered(ids) {
			await db
				.updateTable("jubs_outbox")
				.set({ delivered_at: new Date() })
				.where("id", "in", ids)
				.execute()
		},
	}
}
```

The claim is one statement: the inner select locks the rows it reads and skips the ones another relay holds, and the update stamps `claimed_at` on exactly those. A row is claimable again once its lease has run out, which is what makes a killed relay's rows move on their own.

`MAX_CLAIMS` is where the poisoned row is dealt with, and it belongs here rather than in the library — how many attempts a row deserves is a policy about your table. A row handed out ten times and never marked stops being claimed, so it stops taking a place in every batch. Nothing is lost by giving up: the row is still there, with its envelope and its count, and the query above is what reads it. **Read it.** A row counts a claim whether it failed on its own or a relay died holding it, so a healthy row can reach ten as well — and a row that stopped being claimed is a job that will never run until you act.

**A `version_ahead` row is the healthy row that reaches ten.** During a rolling deploy an old pod claims a row a new pod wrote, cannot read it, and leaves it behind; at the default interval and a one-minute lease it counts ten claims in about ten minutes, and the claim gives up on it. The row is good, and a new pod would deliver it. Do not mark it delivered and do not delete it — that loses a job committed with the state change beside it. Let the deploy finish, and if it ran past ten claims, put the row back in the claim by zeroing its count: `update jubs_outbox set claims = 0 where id = $1`.

`markDelivered` stamps a date rather than deleting, so the table doubles as the record of what was enqueued and when. Delete instead if you would rather not sweep it; the relay reads neither column.

The column types are `pg`'s: a `jsonb` comes back parsed and goes in as a string, and a `bigserial` comes back as a string. Another driver reads them its own way, and `ColumnType` is where you say so — the envelope has to come back out of `claim` as the object `save` was given.

### Testing the outbox

`memoryDriver` honours a derived id the way Redis does: a second enqueue under an id it already keeps gives the kept job back, and stores nothing. A test can kill a relay between the delivery and the mark, run it again, and see one job.

It is **more** faithful than Redis in one way, and that way is the ceiling above: it keeps every job it ever recorded, so it never loses an id to a retention sweep. Test the sweep against `redisDriver`.


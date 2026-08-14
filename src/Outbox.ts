import type { JobDefinition } from "@/Definition"
import { resolveDeliveryWithoutUniqueness } from "@/Delivery"
import type { EnqueueRequest, JobDriver } from "@/Driver"
import type { Envelope } from "@/Envelope"
import { slotsOf } from "@/Flow"
import { migrateEnvelope, VersionAheadError } from "@/Migration"
import { validatePayload } from "@/Payload"

/**
 * One row of the outbox, as the relay reads it back: the id the table stores it
 * under, and the envelope an atomic block wrote inside the caller's
 * transaction.
 *
 * A row keeps the envelope and nothing else. The queue the job goes to and the
 * delivery policy it is enqueued with are read from its definition when the
 * relay delivers it, the way a replay reads them — so a policy changed between
 * the write and the delivery is the policy that applies.
 */
export interface ClaimedEnvelope {
	readonly id: string
	readonly envelope: Envelope
}

/**
 * The table `jobs.atomic(fn, { tx })` writes its jobs to, and the relay reads
 * them back from. You own the table and the queries; this library touches no
 * database and holds no connection.
 */
export interface Outbox {
	/**
	 * Writes the envelopes of one atomic block, inside the transaction that block
	 * was given.
	 *
	 * `tx` is whatever your database library calls a transaction. It travels
	 * through this library untouched and arrives here as the very value the
	 * caller passed to `jobs.atomic(fn, { tx })`, so cast it to your own type and
	 * run the insert on it. A row is written undelivered, and the relay is what
	 * delivers it.
	 *
	 * The list is never empty: a block that enqueued nothing does not call this
	 * at all.
	 */
	save(envelopes: readonly Envelope[], tx: unknown): Promise<void>
	/**
	 * Takes up to `limit` rows that are not delivered yet, and hands them to this
	 * relay alone.
	 *
	 * Two relays claiming one row would enqueue it twice, so the claim locks what
	 * it reads and skips what another relay holds — `FOR UPDATE SKIP LOCKED` over
	 * Postgres, and its equivalent elsewhere. The outbox guide,
	 * `docs/outbox.md`, carries the whole Kysely adapter.
	 *
	 * A row claimed and never marked is claimed again, which is the point rather
	 * than a fault: the job is stored under an id derived from the row, so a
	 * second delivery of one row meets the job the first one stored and runs
	 * nothing — for as long as Redis still keeps that job. Past its retention the
	 * id names nothing, and the same row delivered again runs a second time.
	 *
	 * A claim has to expire. A cycle marks the rows it delivered and leaves the
	 * rest claimed, on purpose: the row nothing could deliver stays where it is,
	 * and a relay killed mid-cycle holds every row of its batch. Those rows come
	 * back only because the claim releases them. A claim that holds a row for good
	 * hands it out once and never again — the job that row carries never runs, and
	 * nothing says so. The adapter in the outbox guide leases a claim for a minute.
	 */
	claim(limit: number): Promise<readonly ClaimedEnvelope[]>
	/**
	 * Marks the rows whose jobs are in the queue, so the next claim passes over
	 * them. Deleting them is a valid implementation, and so is setting a
	 * `delivered_at`.
	 *
	 * The list is never empty: a cycle that delivered no row does not call this
	 * at all.
	 */
	markDelivered(ids: readonly string[]): Promise<void>
}

const DEFAULT_LIMIT = 100

const DEFAULT_INTERVAL_MS = 1_000

/**
 * The id the job of one outbox row is stored under.
 *
 * It is derived from the row and from nothing else, which is what makes a
 * re-delivery harmless: the second `add` of the same row meets the job the
 * first one stored and gives it back, storing nothing and running nothing
 * twice. That holds while Redis keeps the job, and no longer — a row delivered
 * again after the retention has swept its job stores a new one, which runs.
 *
 * The id opens with a word so it is never a whole number, which BullMQ refuses
 * as a custom id. Two row ids are refused rather than composed, because both
 * would break the derivation in silence. An empty id would let BullMQ make an
 * id up, and the row would be marked delivered while the job it enqueued
 * answers to nothing — a re-delivery would then enqueue a second one. An id
 * carrying a colon is refused by BullMQ unless its colons cut the id in exactly
 * three parts, and this library cuts a job id at its first colon to find the
 * queue.
 */
function outboxJobId(rowId: string): string {
	if (rowId.length === 0) {
		throw new Error(
			"jubs: the outbox claimed a row with an empty id, and a job id cannot be derived from it — BullMQ would make an id up instead, the row would be marked delivered for a job nothing can name, and the next claim of that row would enqueue a second one. Give every row of your outbox table an id, and hand it back from `claim` as `id`.",
		)
	}

	if (rowId.includes(":")) {
		throw new Error(
			`jubs: the outbox claimed the row "${rowId}", whose id carries a colon, and a job id cannot be derived from it — BullMQ refuses a custom id whose colons do not cut it in exactly three parts, and this library cuts a job id at its first colon to find the queue it is on. Name your rows with an id that holds no colon, a bigserial or a uuid.`,
		)
	}

	return `outbox-${rowId}`
}

/**
 * Why the relay left a row behind.
 *
 * `driver_failed` and `version_ahead` are the two rows that are good: the
 * delivery failed inside the driver, or a newer deploy wrote the payload, and
 * the next cycle delivers them as soon as the driver answers or the deploy
 * finishes. `unrecoverable` is a row this process cannot deliver until
 * something outside the relay changes — a definition it does not know, a
 * definition that waits on slots, a row id a job id cannot be derived from, a
 * payload its schema refuses, or a migration of yours that threw.
 */
export type LeftBehindReason = "driver_failed" | "version_ahead" | "unrecoverable"

export interface LeftBehindEvent {
	/** The id the outbox handed back from `claim`, as `ClaimedEnvelope.id`. */
	readonly rowId: string
	/** The name of the job the row carries. */
	readonly name: string
	readonly reason: LeftBehindReason
	/**
	 * The failure the row was left behind on, as it was thrown — never the report
	 * the relay writes to the console around it.
	 */
	readonly error: Error
}

export interface RelayOptions {
	/** How many rows one cycle claims. 100 by default. */
	readonly limit?: number
	/** How long the relay waits between two cycles. 1000ms by default. */
	readonly intervalMs?: number
	/**
	 * Runs once for every row the relay leaves behind, with the reason it was
	 * left behind on. The console report happens either way, so this hook is
	 * where an adapter writes a status of its own onto the row.
	 */
	readonly onLeftBehind?: (event: LeftBehindEvent) => void | Promise<void>
}

export interface JobsRelay {
	/**
	 * Stops the loop, and resolves once the cycle under way has ended. The rows
	 * that cycle delivered are marked before it ends, and the rows it never
	 * reached are claimed by the next relay to run.
	 */
	close(): Promise<void>
}

interface RelayConfig extends RelayOptions {
	readonly outbox: Outbox
	readonly driver: JobDriver
	readonly definitions: readonly JobDefinition[] | undefined
}

function definitionOf(config: RelayConfig, claimed: ClaimedEnvelope): JobDefinition {
	const definition = config.definitions?.find(
		(candidate) => candidate.name === claimed.envelope.name,
	)

	if (!definition) {
		throw new Error(
			`jubs: the outbox row "${claimed.id}" runs "${claimed.envelope.name}", which this client does not know — register its definition in createJobs({ definitions }) on the client that starts the relay.`,
		)
	}

	if (slotsOf(definition).length > 0) {
		throw new Error(
			`jubs: the outbox row "${claimed.id}" runs "${claimed.envelope.name}", which waits on children, and a row holds one envelope and no tree — delivering it would enqueue that job with no children at all, and the first delivery would find its slots short and bury it. A flow is refused inside jobs.atomic(fn, { tx }) for that reason, so this row was written by something else: drop it, and enqueue the flow with jobs.enqueue(definition, data, awaits) outside the block.`,
		)
	}

	return definition
}

function leftBehindEvent(
	row: ClaimedEnvelope,
	reason: LeftBehindReason,
	failure: unknown,
): LeftBehindEvent {
	return {
		rowId: row.id,
		name: row.envelope.name,
		reason,
		error: failure instanceof Error ? failure : new Error(String(failure)),
	}
}

/**
 * Enqueues one claimed row, under the id that row derives, and answers with the
 * event of a row it left behind — `undefined` for a row that reached the queue.
 *
 * Preparing the request and handing it to the driver are two `try` blocks
 * because the split is the classification: everything that fails before the
 * driver is this process's own doing, and only what fails inside `enqueue` is
 * the driver's. Deriving the job id sits on the preparation side for that
 * reason — a row id no id can be derived from is a broken row, not a driver
 * that stopped answering, and reporting it as one would tell an operator to
 * wait for a connection that is already up.
 *
 * The delivery is resolved **without** the uniqueness the definition declares,
 * the way a replay resolves it. BullMQ reads the derived id first and the
 * deduplication key second, so a key already pointing at another job would
 * answer with that job's id: the row's own job would never be created, and
 * nothing would say so.
 */
async function deliver(
	config: RelayConfig,
	claimed: ClaimedEnvelope,
): Promise<LeftBehindEvent | undefined> {
	let request: EnqueueRequest

	try {
		const definition = definitionOf(config, claimed)
		const migrated = await migrateEnvelope(definition, claimed.envelope)
		const validated = await validatePayload(definition, migrated)

		request = {
			queue: definition.queue,
			envelope: claimed.envelope,
			delivery: resolveDeliveryWithoutUniqueness(definition, validated),
			jobId: outboxJobId(claimed.id),
		}
	} catch (failure) {
		const reason = failure instanceof VersionAheadError ? "version_ahead" : "unrecoverable"

		return leftBehindEvent(claimed, reason, failure)
	}

	try {
		await config.driver.enqueue(request)
	} catch (failure) {
		return leftBehindEvent(claimed, "driver_failed", failure)
	}

	return undefined
}

const ADVICE: Record<LeftBehindReason, string> = {
	driver_failed:
		"that row is not broken and its job is not lost: the delivery failed inside the driver, so nothing of it reached the queue, and the next cycle claims the row again and delivers it as soon as the driver answers. Look at Redis and at the connection the driver holds, and leave the row in the claim: marking it delivered by hand or deleting it loses a job that was committed together with the state change beside it. An outbox that counts claims gives up on the row before the connection comes back — the adapter in the outbox guide stops claiming a row after ten — so read that count, and reset it once the driver answers again.",
	version_ahead:
		"that row is not broken, and this process alone cannot deliver it: a newer deploy wrote its payload, and a process running that version delivers it as soon as it claims it. Wait for the deploy to finish, and leave the row in the claim: marking it delivered by hand or deleting it loses a job that was committed together with the state change beside it. An outbox that counts claims gives up on the row before the deploy does — the adapter in the outbox guide stops claiming a row after ten — so read that count, and reset it if the deploy takes long.",
	unrecoverable:
		"that row alone is not enqueued and stays unmarked, so every cycle claims it again and fails on it again until something outside the relay changes it. Register on the relaying client the definition the failure names, or take the row out of the claim: mark it delivered by hand, or delete it.",
}

/**
 * Names the row the relay left behind, and says the relay went on without it.
 *
 * One row nothing can deliver holds no other row back: the cycle carries on
 * through the rows around it and marks those delivered. The row itself stays
 * where it is, and every cycle claims it again and fails on it again, so the
 * failure names the row an operator has to act on, and what ends it: the two
 * acts that take a broken row out of the claim, or — for a row a newer deploy
 * wrote, and for a row whose delivery failed inside the driver — the deploy
 * finishing and the driver answering again, since those rows are good and
 * taking them out of the claim would lose the jobs they hold.
 */
function leftBehind(event: LeftBehindEvent): Error {
	return new Error(
		`jubs: the outbox row "${event.rowId}" was left behind, and the relay went on with the rows around it — ${ADVICE[event.reason]} The failure it was left behind on: ${event.error.message}`,
		{ cause: event.error },
	)
}

async function reportLeftBehind(config: RelayConfig, event: LeftBehindEvent): Promise<void> {
	console.error(leftBehind(event))

	if (!config.onLeftBehind) {
		return
	}

	try {
		await config.onLeftBehind(event)
	} catch (error) {
		console.error(
			`jubs: the onLeftBehind hook threw for the outbox row "${event.rowId}"; the row is left behind either way, and the rows around it are delivered and marked`,
			error,
		)
	}
}

/**
 * Claims a batch, delivers its rows one by one, and marks the ones that were
 * delivered.
 *
 * A row that cannot be delivered is reported and left where it is, and the rows
 * around it go on — a batch is a hundred rows of every job the outbox carries,
 * and one row nothing can deliver would otherwise hold the ninety-nine beside
 * it. The mark is still one call, so a relay that dies after the delivery
 * leaves those rows claimable: the next cycle enqueues the same jobs under the
 * same ids, which creates nothing new while Redis still keeps them.
 */
async function runRelayCycle(config: RelayConfig): Promise<void> {
	const claimed = await config.outbox.claim(config.limit ?? DEFAULT_LIMIT)
	const delivered: string[] = []

	for (const row of claimed) {
		const event = await deliver(config, row)

		if (!event) {
			delivered.push(row.id)
			continue
		}

		await reportLeftBehind(config, event)
	}

	if (delivered.length === 0) {
		return
	}

	await config.outbox.markDelivered(delivered)
}

/**
 * Runs the claim → deliver → mark cycle until it is closed, one cycle at a
 * time: a cycle that outruns the interval holds the next tick back rather than
 * running beside it, since two relays over one batch would enqueue it twice.
 *
 * A cycle that fails is reported and the loop goes on, because the rows it left
 * behind are still there to claim.
 */
export function startRelayLoop(config: RelayConfig): JobsRelay {
	let cycle: Promise<void> | undefined

	function tick(): void {
		if (cycle) {
			return
		}

		cycle = runRelayCycle(config)
			.catch((error: unknown) => {
				console.error(
					"jubs: a relay cycle failed on the outbox itself, claiming rows or marking them delivered — the rows it delivered before that stay unmarked and are enqueued again next cycle, which creates no second job while Redis still keeps the first",
					error,
				)
			})
			.finally(() => {
				cycle = undefined
			})
	}

	const ticking = setInterval(tick, config.intervalMs ?? DEFAULT_INTERVAL_MS)

	tick()

	return {
		async close() {
			clearInterval(ticking)

			await cycle
		},
	}
}

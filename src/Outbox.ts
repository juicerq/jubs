import type { JobDefinition } from "@/Definition"
import { resolveDeliveryWithoutUniqueness } from "@/Delivery"
import type { JobDriver } from "@/Driver"
import type { Envelope } from "@/Envelope"
import { slotsOf } from "@/Flow"
import { migrateEnvelope } from "@/Migration"
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
	 * Postgres, and its equivalent elsewhere. The README carries the whole Kysely
	 * adapter.
	 *
	 * A row claimed and never marked is claimed again, which is the point rather
	 * than a fault: the job is stored under an id derived from the row, so a
	 * second delivery of one row meets the job the first one stored and runs
	 * nothing — for as long as Redis still keeps that job. Past its retention the
	 * id names nothing, and the same row delivered again runs a second time.
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

export interface RelayOptions {
	/** How many rows one cycle claims. 100 by default. */
	readonly limit?: number
	/** How long the relay waits between two cycles. 1000ms by default. */
	readonly intervalMs?: number
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

/**
 * Enqueues one claimed row, under the id that row derives.
 *
 * The delivery is resolved **without** the uniqueness the definition declares,
 * the way a replay resolves it. BullMQ reads the derived id first and the
 * deduplication key second, so a key already pointing at another job would
 * answer with that job's id: the row's own job would never be created, and
 * nothing would say so.
 */
async function deliver(config: RelayConfig, claimed: ClaimedEnvelope): Promise<void> {
	const definition = definitionOf(config, claimed)
	const migrated = await migrateEnvelope(definition, claimed.envelope)
	const validated = await validatePayload(definition, migrated)

	await config.driver.enqueue({
		queue: definition.queue,
		envelope: claimed.envelope,
		delivery: resolveDeliveryWithoutUniqueness(definition, validated),
		jobId: outboxJobId(claimed.id),
	})
}

/**
 * Names the row the relay left behind, and says the relay went on without it.
 *
 * One row nothing can deliver holds no other row back: the cycle carries on
 * through the rows around it and marks those delivered. The row itself stays
 * where it is, and every cycle claims it again and fails on it again, so the
 * failure names the row an operator has to act on, and the two acts that end
 * it.
 */
function leftBehind(row: ClaimedEnvelope, failure: unknown): Error {
	const reason = failure instanceof Error ? failure.message : String(failure)

	return new Error(
		`jubs: the outbox row "${row.id}" was left behind, and the relay went on with the rows around it — that row alone is not enqueued and stays unmarked, so every cycle claims it again and fails on it again until something outside the relay changes it. Register on the relaying client the definition the failure names, or take the row out of the claim: mark it delivered by hand, or delete it. The failure it was left behind on: ${reason}`,
		{ cause: failure },
	)
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
		await deliver(config, row)
			.then(() => {
				delivered.push(row.id)
			})
			.catch((failure: unknown) => {
				console.error(leftBehind(row, failure))
			})
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

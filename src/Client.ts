import type { StandardSchemaV1 } from "@standard-schema/spec"
import { type DeadJob, deadQueueName, liveQueueName } from "@/Dead"
import {
	type AwaitsMap,
	type EnqueueAwaits,
	type JobDefinition,
	type JobHandler,
	payloadVersion,
} from "@/Definition"
import { resolveDelivery, resolveDeliveryWithoutUniqueness } from "@/Delivery"
import type { CancelResult, EnqueuedJob, JobDriver, JobSnapshot, RetryResult } from "@/Driver"
import { composeFlow, slotsOf } from "@/Flow"
import type { JobHooks } from "@/Hooks"
import { composeJobId, readJobId } from "@/JobId"
import { migrateEnvelope } from "@/Migration"
import { validatePayload } from "@/Payload"
import { type JobsRuntime, type StartOptions, startRuntime } from "@/Runtime"
import { assertTimezone } from "@/Schedule"

export interface JobsConfig {
	readonly driver: JobDriver
	readonly definitions?: readonly JobDefinition[]
	readonly deadQueues?: readonly string[]
	readonly timezone?: string
	readonly hooks?: JobHooks
}

export interface JobsClient {
	/**
	 * Enqueues one job, and gives back the id it answers to.
	 *
	 * A definition that declares `awaits` takes a third argument: what fills every
	 * one of its slots, nesting to any depth and across queues. The whole tree is
	 * added in one step, and the id that comes back is the parent's — the job the
	 * producer holds. Annotate a pre-built input with `AwaitsInput<typeof
	 * definition>`, so a mistake names the slot it is in.
	 *
	 * Inside the parent's handler, `context.children` reads what those children
	 * returned, slot by slot. A child that fails every attempt does not fail the
	 * parent inside Redis: the parent runs, finds the failure, and is buried with
	 * the reason `child_dead`, which buries its own parent in turn.
	 *
	 * A definition declaring `unique` cannot take part in a flow, at any position.
	 */
	enqueue<
		Payload extends StandardSchemaV1,
		Queue extends string,
		Result extends StandardSchemaV1 | undefined,
		Awaits extends AwaitsMap | undefined,
	>(
		definition: JobDefinition<Payload, Queue, Result, Awaits>,
		data: StandardSchemaV1.InferInput<Payload>,
		...awaits: EnqueueAwaits<Awaits>
	): Promise<EnqueuedJob>
	start<Queue extends string>(
		handlers: JobHandler<Queue>[],
		options?: StartOptions<Queue>,
	): Promise<JobsRuntime>
	/**
	 * Describes the job an id names, and gives back `undefined` when no job answers
	 * to it — a job Redis has already dropped, one that never existed, or an id
	 * that names a dead queue, which `jobs.dead.list(queue)` reads instead.
	 */
	get(id: string): Promise<JobSnapshot | undefined>
	/**
	 * Runs a failed job again, from attempt 1. The result says whether it was
	 * retried, whether no job answers to the id, or which state holds a job that
	 * is not failed.
	 *
	 * An id naming a dead queue reads `unknown_job`: a dead job is replayed with
	 * `jobs.dead.replay(id)`, which rebuilds it from its envelope alone and so only
	 * serves a job that runs alone: a dead job that is part of a flow, or one whose
	 * definition waits on children, is refused.
	 */
	retry(id: string): Promise<RetryResult>
	/**
	 * Stops a job. A job that has not started is removed, together with the
	 * children it waits on; a job already running
	 * has its handler's signal aborted — by whichever process runs it, not by the
	 * one that asks — and then dies without another attempt, with the failure
	 * hooks and a dead queue entry whose reason is `cancelled`.
	 *
	 * Two limits come from the signal itself, the same two a shutdown has. A
	 * handler that ignores its signal runs to the end, because nothing can stop a
	 * running function. A handler that notices the abort and *returns* counts as a
	 * success, because the contract is to throw: rethrow `context.signal.reason`.
	 *
	 * An id naming a dead queue reads `unknown_job` and leaves the dead entry
	 * where it is: a dead job runs nothing, so there is nothing to stop, and
	 * `jobs.dead.discard(id)` is what drops the record.
	 */
	cancel(id: string): Promise<CancelResult>
	pause(queue: string): Promise<void>
	resume(queue: string): Promise<void>
	readonly dead: {
		list(queue: string): Promise<DeadJob[]>
		replay(id: string): Promise<EnqueuedJob>
		discard(id: string): Promise<void>
	}
}

/**
 * Reads the live queue and the stored id out of an id `jobs.dead.list(queue)`
 * composed. A dead job is named after the dead queue that keeps it, and the
 * `DeadStore` is asked in the name of the live queue, so the suffix comes off
 * here — and an id that carries no dead queue is the caller's mistake.
 */
function readDeadJobId(id: string): { queue: string; storedId: string } {
	const { queue, storedId } = readJobId(id)
	const live = liveQueueName(queue)

	if (live === undefined) {
		throw new Error(
			`jubs: "${id}" is not a dead job id — pass an id returned by jobs.dead.list(queue)`,
		)
	}

	return { queue: live, storedId }
}

/**
 * Reads the queue and the stored id out of an id that must name a live job, and
 * gives back `undefined` when the id names a dead queue instead.
 *
 * A dead job is not a job: it is a record of one. `get`, `retry` and `cancel`
 * speak of the live queues only, and a dead id is an absence to them, not a
 * mistake — `jobs.dead.replay(id)` and `jobs.dead.discard(id)` are what read it.
 * Answering a dead id here would be worse than useless: `cancel` would call
 * `remove` on the dead record and destroy it.
 */
function readLiveJobId(id: string): { queue: string; storedId: string } | undefined {
	const { queue, storedId } = readJobId(id)

	if (liveQueueName(queue) !== undefined) {
		return undefined
	}

	return { queue, storedId }
}

function assertEveryDeadQueueIsUsed(config: JobsConfig): void {
	if (!config.definitions) {
		return
	}

	const used = new Set(config.definitions.map((definition) => definition.queue))
	const stray = config.deadQueues?.find((queue) => !used.has(queue))

	if (!stray) {
		return
	}

	const names = [...used].map((queue) => `"${queue}"`).join(", ")

	throw new Error(
		`jubs: createJobs was given the dead queue "${stray}", which no registered definition uses — the queues its definitions use are ${names}`,
	)
}

export function createJobs(config: JobsConfig): JobsClient {
	assertEveryDeadQueueIsUsed(config)

	if (config.timezone) {
		assertTimezone(config.timezone)
	}

	return {
		dead: {
			async list(queue) {
				const buried = await config.driver.dead.list(queue)

				return buried.map((job) => ({
					...job.entry,
					id: composeJobId(deadQueueName(queue), job.id),
				}))
			},

			async replay(id) {
				const { queue, storedId } = readDeadJobId(id)
				const entry = await config.driver.dead.read(queue, storedId)

				if (!entry) {
					throw new Error(
						`jubs: no dead job "${id}" is kept — it was replayed or discarded already`,
					)
				}

				if (entry.envelope.origin === "flow") {
					throw new Error(
						`jubs: the dead job "${id}" is part of a flow, and replaying it would enqueue "${entry.envelope.name}" with no children at all — it would run over an empty result set and complete green. Put the flow back together instead: jobs.retry(id) on each child that failed, which returns it to its parent's dependencies, then jobs.retry("${entry.jobId}") on the parent. The ids of the children that failed are named in this entry's error. Both dead records stay where they are — drop them with jobs.dead.discard(id).`,
					)
				}

				const definition = config.definitions?.find(
					(candidate) => candidate.name === entry.envelope.name,
				)

				if (!definition) {
					throw new Error(
						`jubs: the dead job "${id}" runs "${entry.envelope.name}", which this client does not know — register its definition in createJobs({ definitions }) to replay it`,
					)
				}

				if (slotsOf(definition).length > 0) {
					throw new Error(
						`jubs: the dead job "${id}" runs "${entry.envelope.name}", which waits on children, and replaying it would enqueue that job alone, with no children at all — the first delivery would find its slots short and bury it again with the reason children_short. Enqueue the flow again with jobs.enqueue(definition, data, awaits), which builds the whole flow. This dead record stays where it is — drop it with jobs.dead.discard(id).`,
					)
				}

				const migrated = await migrateEnvelope(definition, entry.envelope)
				const validated = await validatePayload(definition, migrated)

				const enqueued = await config.driver.enqueue({
					queue: definition.queue,
					envelope: entry.envelope,
					delivery: resolveDeliveryWithoutUniqueness(definition, validated),
				})

				await config.driver.dead.remove(queue, storedId)

				return { id: composeJobId(definition.queue, enqueued.id) }
			},

			async discard(id) {
				const { queue, storedId } = readDeadJobId(id)
				const dropped = await config.driver.dead.remove(queue, storedId)

				if (dropped) {
					return
				}

				throw new Error(`jubs: no dead job "${id}" is kept — it was replayed or discarded already`)
			},
		},

		async enqueue(definition, data, ...awaits) {
			if (slotsOf(definition).length > 0) {
				const root = await composeFlow(definition, data, awaits[0])
				const flowed = await config.driver.flow.enqueue(root)

				return { id: composeJobId(definition.queue, flowed.id) }
			}

			const validated = await validatePayload(definition, data)

			const enqueued = await config.driver.enqueue({
				queue: definition.queue,
				envelope: { v: payloadVersion(definition), name: definition.name, data, origin: "direct" },
				delivery: resolveDelivery(definition, validated),
			})

			return { id: composeJobId(definition.queue, enqueued.id) }
		},

		start(handlers, options) {
			return startRuntime(config, handlers, options)
		},

		async get(id) {
			const live = readLiveJobId(id)

			if (!live) {
				return undefined
			}

			const snapshot = await config.driver.get(live.queue, live.storedId)

			if (!snapshot) {
				return undefined
			}

			return { ...snapshot, id: composeJobId(live.queue, snapshot.id) }
		},

		async retry(id) {
			const live = readLiveJobId(id)

			if (!live) {
				return { outcome: "unknown_job" }
			}

			return config.driver.retry(live.queue, live.storedId)
		},

		async cancel(id) {
			const live = readLiveJobId(id)

			if (!live) {
				return { outcome: "unknown_job" }
			}

			return config.driver.cancel(live.queue, live.storedId)
		},

		pause(queue) {
			return config.driver.pause(queue)
		},

		resume(queue) {
			return config.driver.resume(queue)
		},
	}
}

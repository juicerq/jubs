import type { StandardSchemaV1 } from "@standard-schema/spec"
import { holdOrDeliver, inAtomicBlock, outsideAtomicBlock, runAtomicBlock } from "@/Atomic"
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
import { idempotencyKeyFor } from "@/Idempotency"
import { composeJobId, readJobId } from "@/JobId"
import { migrateEnvelope } from "@/Migration"
import { validatePayload } from "@/Payload"
import { type JobsRuntime, type StartOptions, startRuntime } from "@/Runtime"
import { assertTimezone } from "@/Schedule"

/**
 * What forgetting an idempotency key reached. `forgotten` deleted a complete
 * key, so the next delivery runs the handler again; `not_found` met no key at
 * all; `running` met a key a delivery holds right now, which is refused and
 * deletes nothing; `not_guarded` is a definition that declares no
 * `idempotencyKey`, which names no key to forget.
 */
export type ForgetResult =
	| { readonly outcome: "forgotten" }
	| { readonly outcome: "not_found" }
	| { readonly outcome: "running" }
	| { readonly outcome: "not_guarded" }

export interface JobsConfig {
	readonly driver: JobDriver
	readonly definitions?: readonly JobDefinition[]
	readonly deadQueues?: readonly string[]
	readonly timezone?: string
	readonly hooks?: JobHooks
}

export interface JobsClient {
	/**
	 * Enqueues one job, and gives back the id it answers to — or `null` when an
	 * atomic block holds the enqueue back, because no job exists yet to be named.
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
	): Promise<EnqueuedJob | null>
	/**
	 * Runs a block that holds every job it enqueues, and delivers them, in the
	 * order they were enqueued, only once the block resolves. A block that throws
	 * delivers nothing, and an enqueue a block holds gives back `null`, because no
	 * job exists yet.
	 *
	 * A block opened inside another block joins the outer one, so the outermost
	 * block is the only one that delivers. Two blocks running at once never see
	 * each other's held jobs.
	 *
	 * What the block holds is every enqueue, and only those: `dead.discard`,
	 * `cancel`, `retry`, `pause` and `resume` act at once, and `dead.replay` is
	 * refused. The jobs are held in memory, so a process that dies mid-block loses
	 * them, and an enqueue made after the block has ended — from a callback it
	 * scheduled and never waited for — is refused rather than lost. A runtime
	 * `start` opens inside a block leaves the block first, so what its handlers
	 * enqueue is never held.
	 *
	 * A delivery that fails mid-flush leaves the jobs before it enqueued and
	 * abandons the ones after it: the failure names both counts.
	 *
	 * `options.tx` **throws synchronously**, before the block runs, for as long as
	 * no outbox is configured on this client.
	 */
	atomic<Result>(run: () => Promise<Result>, options?: { readonly tx?: unknown }): Promise<Result>
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
		/**
		 * Enqueues a dead job again from the envelope it was buried with, and drops
		 * the dead record. Called inside an atomic block it throws: the replay is
		 * three effects that no block makes one.
		 *
		 * The idempotency key that payload names is forgotten first, so the replay
		 * runs the handler instead of replaying a result kept from an earlier run. A
		 * job buried on its `timeoutMs` whose detached body then completed the key
		 * would otherwise be replayed green with nothing run at all.
		 *
		 * A key a delivery holds right now is refused, and nothing is enqueued. That
		 * is the normal state right after a `timeoutMs` burial, because the body
		 * that outlived the attempt keeps the key held and renewed until it returns.
		 * A replay enqueued over it would meet the held key, be rescheduled, and
		 * then hand back the result that body kept — green, with nothing run, and
		 * the dead record already dropped. Refusing before the enqueue is what
		 * leaves the record where it is, for a replay that runs.
		 */
		replay(id: string): Promise<EnqueuedJob>
		discard(id: string): Promise<void>
	}
	readonly idempotency: {
		/**
		 * Deletes the idempotency key one payload names, so the next delivery of
		 * that payload runs the handler again instead of replaying the kept result.
		 *
		 * The payload is validated first, and the key is computed from what the
		 * schema gave back — the very key a delivery of the same payload computes.
		 * An invalid payload fails the call and deletes nothing.
		 *
		 * **A key a delivery holds right now is refused**, with the outcome
		 * `running`. Deleting it would take the lease from under a running handler,
		 * and the next delivery would find the key free and run a second body beside
		 * the first. Wait for that delivery to end, then ask again.
		 */
		forget<Payload extends StandardSchemaV1>(
			definition: JobDefinition<Payload>,
			data: StandardSchemaV1.InferInput<Payload>,
		): Promise<ForgetResult>
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

/**
 * Deletes the idempotency key a validated payload names, and says what that
 * reached. A definition that declares no `idempotencyKey` names no key at all,
 * which is an answer of its own rather than a failure — so a caller reads one
 * union instead of guarding the definition first.
 */
async function forgetKey(
	driver: JobDriver,
	definition: JobDefinition,
	validated: unknown,
): Promise<ForgetResult["outcome"]> {
	const key = idempotencyKeyFor(definition, validated)

	if (!key) {
		return "not_guarded"
	}

	return driver.idempotency.forget(key)
}

/**
 * Stores one job and names it after the queue it went to, or gives back `null`
 * when an atomic block held the store back. The id a driver answers with is the
 * stored one, and the id this library speaks is the queue and that one together,
 * so the two always travel as a pair.
 */
async function heldOrNamed(
	queue: string,
	store: () => Promise<EnqueuedJob>,
): Promise<EnqueuedJob | null> {
	const stored = await holdOrDeliver(store)

	if (!stored) {
		return null
	}

	return { id: composeJobId(queue, stored.id) }
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
				if (inAtomicBlock()) {
					throw new Error(
						`jubs: jobs.dead.replay("${id}") was called inside an atomic block, and a replay is not one enqueue — it forgets the payload's idempotency key, enqueues the job and drops the dead record, and those three never become one act. Holding only the last two would let a block that then failed leave the key forgotten with nothing enqueued, and the record listed for a second replay of the same job. Replay outside the block.`,
					)
				}

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

				if ((await forgetKey(config.driver, definition, validated)) === "running") {
					throw new Error(
						`jubs: the dead job "${id}" runs "${entry.envelope.name}", whose idempotency key is held by a delivery running right now — the body that outlived this job's last attempt is still going, and it completes that key with its own result when it returns. Replaying now would enqueue a job that meets the held key and is rescheduled, and the delivery after that would hand back the kept result without ever calling the handler: green, with nothing run. This dead record stays where it is — replay it again once that body has ended.`,
					)
				}

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

		idempotency: {
			async forget(definition, data) {
				const validated = await validatePayload(definition, data)

				return { outcome: await forgetKey(config.driver, definition, validated) }
			},
		},

		async enqueue(definition, data, ...awaits) {
			if (slotsOf(definition).length > 0) {
				const root = await composeFlow(definition, data, awaits[0])

				return heldOrNamed(definition.queue, () => config.driver.flow.enqueue(root))
			}

			const validated = await validatePayload(definition, data)

			return heldOrNamed(definition.queue, () =>
				config.driver.enqueue({
					queue: definition.queue,
					envelope: {
						v: payloadVersion(definition),
						name: definition.name,
						data,
						origin: "direct",
					},
					delivery: resolveDelivery(definition, validated),
				}),
			)
		},

		atomic(run, options) {
			if (options?.tx !== undefined) {
				throw new Error(
					"jubs: jobs.atomic was given a tx, and this client has no outbox configured — a tx exists to write the envelopes inside your own database transaction, and nothing here can accept one. Call jobs.atomic(fn) without it: the block still holds every enqueue until it resolves, but it holds them in memory, so a process that dies mid-block loses them.",
				)
			}

			return runAtomicBlock(run)
		},

		start(handlers, options) {
			return outsideAtomicBlock(() => startRuntime(config, handlers, options))
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

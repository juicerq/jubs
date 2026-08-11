import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { DeadEntry } from "@/Dead"
import type { JobDefinition } from "@/Definition"
import type { ConsumeRequest, JobDriver } from "@/Driver"
import type { Envelope } from "@/Envelope"

const ACCEPTED_DELIVERY = [
	"attempts",
	"backoff",
	"priority",
	"keepCompletedForMs",
	"keepFailedCount",
]

function unsupported(behaviour: string): Error {
	return new Error(
		`juibs: memoryDriver does not simulate "${behaviour}"; test that behaviour against redisDriver`,
	)
}

interface MemoryJob {
	readonly id: string
	readonly queue: string
	readonly maxAttempts: number
	readonly envelope: Envelope
}

interface MemoryDeadQueue {
	readonly entries: Map<string, DeadEntry>
	deaths: number
}

export interface MemoryDriver extends JobDriver {
	enqueued<Payload extends StandardSchemaV1>(
		definition: JobDefinition<Payload>,
	): readonly StandardSchemaV1.InferInput<Payload>[]
	runNext(): Promise<void>
	drain(): Promise<number>
}

/**
 * Records enqueues and runs handlers inline, with no Redis.
 *
 * It does not simulate the clock, delays, backoff, retries, priority ordering,
 * uniqueness windows, schedules or stalled recovery. Jobs run first in, first out,
 * always on attempt 1, and a failing handler propagates its error to the caller
 * instead of being retried. Anything time-dependent is only testable against
 * `redisDriver`. The dead queue is simulated, because keeping, listing, replaying
 * and discarding a dead job depends on no clock.
 *
 * It accepts `attempts`, `backoff`, `priority`, `keepCompletedForMs` and
 * `keepFailedCount`, but only `attempts` reaches the handler, as `maxAttempts`.
 * `delayMs` is time-dependent, so it throws instead: test a delay against
 * `redisDriver`. `unique` throws for the same reason and one more — uniqueness
 * is decided atomically inside Redis, and an inline imitation of it would agree
 * with your test and disagree with production. Any delivery option outside the
 * accepted set throws on enqueue
 * and names itself, so a behaviour this driver never learns to simulate fails
 * loudly instead of passing a test it would fail in production.
 *
 * Per-queue `concurrency` is accepted and ignored — jobs run inline, one at a
 * time. A `limiter` throws, for the same reason a delay does.
 */
export function memoryDriver(): MemoryDriver {
	const recorded: MemoryJob[] = []
	const pending: MemoryJob[] = []
	const consumers = new Map<string, ConsumeRequest["run"]>()
	const buried = new Map<string, MemoryDeadQueue>()

	function deadQueueFor(queue: string): MemoryDeadQueue {
		const open = buried.get(queue)

		if (open) {
			return open
		}

		const opened: MemoryDeadQueue = { entries: new Map(), deaths: 0 }

		buried.set(queue, opened)

		return opened
	}

	async function runNext(): Promise<void> {
		const next = pending[0]

		if (!next) {
			throw new Error("juibs: memoryDriver has no pending job to run")
		}

		const run = consumers.get(next.queue)

		if (!run) {
			throw new Error(
				`juibs: memoryDriver has no consumer on queue "${next.queue}" — call jobs.start(handlers) before running jobs`,
			)
		}

		pending.shift()

		await run({
			id: next.id,
			attempt: 1,
			maxAttempts: next.maxAttempts,
			envelope: next.envelope,
		})
	}

	return {
		async enqueue(request) {
			const rejected = Object.keys(request.delivery).find((key) => !ACCEPTED_DELIVERY.includes(key))

			if (rejected) {
				throw unsupported(rejected)
			}

			const job: MemoryJob = {
				id: String(recorded.length + 1),
				queue: request.queue,
				maxAttempts: request.delivery.attempts,
				envelope: JSON.parse(JSON.stringify(request.envelope)) as Envelope,
			}

			recorded.push(job)
			pending.push(job)

			return { id: job.id }
		},

		async consume(request) {
			if (request.limiter) {
				throw unsupported("limiter")
			}

			consumers.set(request.queue, request.run)

			return {
				async close() {
					consumers.delete(request.queue)
				},
			}
		},

		dead: {
			async bury(queue, entry) {
				const dead = deadQueueFor(queue)

				dead.deaths += 1
				dead.entries.set(String(dead.deaths), JSON.parse(JSON.stringify(entry)) as DeadEntry)
			},

			async list(queue) {
				const dead = buried.get(queue)

				if (!dead) {
					return []
				}

				return [...dead.entries].map(([id, entry]) => ({ id, entry }))
			},

			async read(queue, id) {
				return buried.get(queue)?.entries.get(id)
			},

			async remove(queue, id) {
				return buried.get(queue)?.entries.delete(id) ?? false
			},
		},

		enqueued<Payload extends StandardSchemaV1>(definition: JobDefinition<Payload>) {
			return recorded
				.filter((job) => job.envelope.name === definition.name)
				.map((job) => job.envelope.data as StandardSchemaV1.InferInput<Payload>)
		},

		runNext,

		async drain() {
			let ran = 0

			while (pending.length > 0) {
				await runNext()
				ran += 1
			}

			return ran
		},
	}
}

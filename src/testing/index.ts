import type { StandardSchemaV1 } from "@standard-schema/spec"
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

interface MemoryJob {
	readonly id: string
	readonly queue: string
	readonly maxAttempts: number
	readonly envelope: Envelope
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
 * `redisDriver`.
 *
 * It accepts every option of the base `Delivery`, but only `attempts` reaches the
 * handler, as `maxAttempts`. `backoff` and `priority` are accepted and ignored.
 * Any option outside that set throws on enqueue and names itself, so a delivery
 * behaviour this driver never learns to simulate fails loudly instead of passing
 * a test it would fail in production.
 */
export function memoryDriver(): MemoryDriver {
	const recorded: MemoryJob[] = []
	const pending: MemoryJob[] = []
	const consumers = new Map<string, ConsumeRequest["run"]>()

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
			const unsupported = Object.keys(request.delivery).find(
				(key) => !ACCEPTED_DELIVERY.includes(key),
			)

			if (unsupported) {
				throw new Error(
					`juibs: memoryDriver does not simulate "${unsupported}"; test that behaviour against redisDriver`,
				)
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
			consumers.set(request.queue, request.run)

			return {
				async close() {
					consumers.delete(request.queue)
				},
			}
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

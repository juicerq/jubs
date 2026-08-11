import { type ConnectionOptions, type JobsOptions, Queue, Worker, type WorkerOptions } from "bullmq"
import type { Delivery } from "@/Delivery"
import type { ConsumeRequest, JobDriver } from "@/Driver"

const BLOCKING_CONNECTION_FIX =
	"juibs: the Redis connection passed to redisDriver must be created with `maxRetriesPerRequest: null`, because a BullMQ worker opens a blocking connection — new Redis(url, { maxRetriesPerRequest: null })"

interface ConnectionShape {
	readonly isCluster?: boolean
	readonly options?: {
		readonly maxRetriesPerRequest?: unknown
		readonly redisOptions?: { readonly maxRetriesPerRequest?: unknown }
	}
}

function blockingOptions(connection: ConnectionOptions) {
	const client = connection as ConnectionShape

	if (client.isCluster) {
		return client.options?.redisOptions
	}

	return client.options
}

export function assertBlockingConnection(connection: ConnectionOptions): void {
	if (!blockingOptions(connection)?.maxRetriesPerRequest) {
		return
	}

	throw new Error(BLOCKING_CONNECTION_FIX)
}

function toJobsOptions(delivery: Delivery): JobsOptions {
	const options: JobsOptions = {
		attempts: delivery.attempts,
		backoff: { type: delivery.backoff.type, delay: delivery.backoff.delayMs },
		priority: delivery.priority,
		removeOnComplete: { age: Math.round(delivery.keepCompletedForMs / 1_000) },
		removeOnFail: { count: delivery.keepFailedCount },
	}

	if (delivery.delayMs === undefined) {
		return options
	}

	return { ...options, delay: delivery.delayMs }
}

function toWorkerOptions(request: ConsumeRequest, connection: ConnectionOptions): WorkerOptions {
	const options: WorkerOptions = { connection, concurrency: request.concurrency }

	if (!request.limiter) {
		return options
	}

	return {
		...options,
		limiter: { max: request.limiter.max, duration: request.limiter.durationMs },
	}
}

export function redisDriver(connection: ConnectionOptions): JobDriver {
	const queues = new Map<string, Queue>()

	function queueFor(name: string): Queue {
		const open = queues.get(name)

		if (open) {
			return open
		}

		const queue = new Queue(name, { connection })

		queues.set(name, queue)

		return queue
	}

	return {
		async enqueue(request) {
			const job = await queueFor(request.queue).add(
				request.envelope.name,
				request.envelope,
				toJobsOptions(request.delivery),
			)

			if (!job.id) {
				throw new Error(`juibs: redis stored job "${request.envelope.name}" without an id`)
			}

			return { id: job.id }
		},

		async consume(request) {
			assertBlockingConnection(connection)

			const worker = new Worker(
				request.queue,
				async (job) => {
					if (!job.id) {
						throw new Error(`juibs: redis delivered job "${job.name}" without an id`)
					}

					return request.run({
						id: job.id,
						attempt: job.attemptsMade + 1,
						maxAttempts: job.opts.attempts ?? 1,
						envelope: job.data,
					})
				},
				toWorkerOptions(request, connection),
			)

			await worker.waitUntilReady()

			return {
				async close() {
					await worker.close()
				},
			}
		},
	}
}

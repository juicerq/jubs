import {
	type ConnectionOptions,
	type DeduplicationOptions,
	type JobSchedulerTemplateOptions,
	type JobsOptions,
	Queue,
	type RepeatOptions,
	Worker,
	type WorkerOptions,
} from "bullmq"
import { type DeadEntry, deadQueueName } from "@/Dead"
import type { Delivery, ResolvedUnique } from "@/Delivery"
import type { ConsumeRequest, JobDriver, ScheduleUpsert } from "@/Driver"

const SCHEDULER_PREFIX = "juibs."

function schedulerId(jobName: string): string {
	return `${SCHEDULER_PREFIX}${jobName}`
}

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

function toDeduplication(unique: ResolvedUnique): DeduplicationOptions {
	if (unique.mode === "noOverlap") {
		return { id: unique.key, keepLastIfActive: true }
	}

	if (unique.mode === "keepLast") {
		return { id: unique.key, ttl: unique.ttlMs, extend: true, replace: true }
	}

	if (unique.ttlMs === undefined) {
		return { id: unique.key }
	}

	return { id: unique.key, ttl: unique.ttlMs }
}

function toTemplateOptions(delivery: Delivery): JobSchedulerTemplateOptions {
	return {
		attempts: delivery.attempts,
		backoff: { type: delivery.backoff.type, delay: delivery.backoff.delayMs },
		priority: delivery.priority,
		removeOnComplete: { age: Math.round(delivery.keepCompletedForMs / 1_000) },
		removeOnFail: { count: delivery.keepFailedCount },
	}
}

function toRepeatOptions(schedule: ScheduleUpsert): RepeatOptions {
	if ("everyMs" in schedule.recurrence) {
		return { every: schedule.recurrence.everyMs }
	}

	if (!schedule.timezone) {
		return { pattern: schedule.recurrence.pattern }
	}

	return { pattern: schedule.recurrence.pattern, tz: schedule.timezone }
}

function refusedRecurrence(schedule: ScheduleUpsert): string {
	if ("everyMs" in schedule.recurrence) {
		return `the interval of ${schedule.recurrence.everyMs}ms`
	}

	return `the pattern "${schedule.recurrence.pattern}"`
}

function refusedSchedule(schedule: ScheduleUpsert, error: unknown): Error {
	const reason = error instanceof Error ? error.message : String(error)

	return new Error(
		`juibs: redis refused the schedule of the job "${schedule.envelope.name}" — correct ${refusedRecurrence(schedule)} its definition declares, or the time zone it runs in — ${reason}`,
		{ cause: error },
	)
}

export function toJobsOptions(delivery: Delivery): JobsOptions {
	const base: JobsOptions = toTemplateOptions(delivery)

	const options = delivery.unique
		? { ...base, deduplication: toDeduplication(delivery.unique) }
		: base

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

		async reconcileSchedules({ queue, declared }) {
			assertBlockingConnection(connection)

			const handle = queueFor(queue)
			const existing = await handle.getJobSchedulers()
			const kept = new Set(declared.map((schedule) => schedulerId(schedule.envelope.name)))

			const stale = existing
				.map((scheduler) => scheduler.key)
				.filter((key) => key.startsWith(SCHEDULER_PREFIX) && !kept.has(key))

			await Promise.all(stale.map((key) => handle.removeJobScheduler(key)))

			for (const schedule of declared) {
				await handle
					.upsertJobScheduler(schedulerId(schedule.envelope.name), toRepeatOptions(schedule), {
						name: schedule.envelope.name,
						data: schedule.envelope,
						opts: toTemplateOptions(schedule.delivery),
					})
					.catch((error: unknown) => {
						throw refusedSchedule(schedule, error)
					})
			}
		},

		dead: {
			async bury(queue, entry) {
				await queueFor(deadQueueName(queue)).add(entry.envelope.name, entry)
			},

			async list(queue) {
				const waiting = await queueFor(deadQueueName(queue)).getWaiting()

				return waiting.flatMap((job) =>
					job.id ? [{ id: job.id, entry: job.data as DeadEntry }] : [],
				)
			},

			async read(queue, id) {
				const job = await queueFor(deadQueueName(queue)).getJob(id)

				if (!job) {
					return undefined
				}

				return job.data as DeadEntry
			},

			async remove(queue, id) {
				const job = await queueFor(deadQueueName(queue)).getJob(id)

				if (!job) {
					return false
				}

				await job.remove()

				return true
			},
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

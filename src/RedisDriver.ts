import { randomUUID } from "node:crypto"
import {
	type ConnectionOptions,
	type DeduplicationOptions,
	DelayedError,
	type Job,
	type JobSchedulerTemplateOptions,
	type JobsOptions,
	Queue,
	type RedisClient,
	type RepeatOptions,
	Worker,
	type WorkerOptions,
} from "bullmq"
import { type DeadEntry, deadQueueName } from "@/Dead"
import type { Delivery, ResolvedUnique } from "@/Delivery"
import type { ConsumeRequest, JobDriver, ScheduleUpsert } from "@/Driver"
import {
	type IdempotencyLease,
	type IdempotencyStore,
	type KeptResult,
	LeaseHeldError,
} from "@/Idempotency"
import { ShutdownAbortError } from "@/Shutdown"

const SCHEDULER_PREFIX = "juibs."

const IDEMPOTENCY_QUEUE = "juibs.idempotency"

export const IDEMPOTENCY_KEY_PREFIX = "juibs:idem:"

export const RUNNING_PREFIX = "running:"

const ACQUIRE_COMMAND = "juibsIdempotencyAcquire"

const RENEW_COMMAND = "juibsIdempotencyRenew"

const COMPLETE_COMMAND = "juibsIdempotencyComplete"

const RELEASE_COMMAND = "juibsIdempotencyRelease"

const ACQUIRE_LUA = `
local current = redis.call("GET", KEYS[1])

if current then
  return { current, redis.call("PTTL", KEYS[1]) }
end

redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[2]))

return {}
`

const RENEW_LUA = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))

  return 1
end

return 0
`

const COMPLETE_LUA = `
local current = redis.call("GET", KEYS[1])

if current and current ~= ARGV[1] then
  return 0
end

redis.call("SET", KEYS[1], ARGV[2], "PX", tonumber(ARGV[3]))

return 1
`

const RELEASE_LUA = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("DEL", KEYS[1])

  return 1
end

return 0
`

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

export const HELD_RETRY_MS = 1_000

async function openIdempotencyClient(connection: ConnectionOptions): Promise<RedisClient> {
	const handle = new Queue(IDEMPOTENCY_QUEUE, { connection, skipMetasUpdate: true })
	const client = await handle.getBackend().client

	client.defineCommand(ACQUIRE_COMMAND, { numberOfKeys: 1, lua: ACQUIRE_LUA })
	client.defineCommand(RENEW_COMMAND, { numberOfKeys: 1, lua: RENEW_LUA })
	client.defineCommand(COMPLETE_COMMAND, { numberOfKeys: 1, lua: COMPLETE_LUA })
	client.defineCommand(RELEASE_COMMAND, { numberOfKeys: 1, lua: RELEASE_LUA })

	return client
}

export function readLease(reply: unknown, token: string): IdempotencyLease {
	const [current, ttl] = Array.isArray(reply) ? reply : []

	if (typeof current !== "string") {
		return { state: "acquired", token }
	}

	if (current.startsWith(RUNNING_PREFIX)) {
		return { state: "held", retryInMs: typeof ttl === "number" && ttl > 0 ? ttl : HELD_RETRY_MS }
	}

	return { state: "complete", kept: JSON.parse(current) as KeptResult }
}

/**
 * Keeps the three idempotency states in Redis, under the `juibs:idem:` prefix.
 *
 * Every operation is a Lua script registered once per client, so reading the key
 * and acting on it is one atomic step, and the narrow `IRedisClient` interface —
 * which declares no `SET NX` and no `PTTL` — never limits what the store can do.
 *
 * A running lease is stored as `running:<token>`, and a complete key as the JSON
 * kept result, so the two are told apart by the prefix. `renew`, `complete` and
 * `release` compare the stored token before they act, so a worker whose lease
 * expired under a running handler cannot renew, finish or free the lease that a
 * second worker took after it. `complete` still writes when the key is gone,
 * because nobody holds it then.
 *
 * The client comes from a `Queue` handle opened with `skipMetasUpdate`, so the
 * handle writes no key of its own and the store shares the caller's connection.
 * The queue it names holds no job and takes no worker.
 */
function redisIdempotency(connection: ConnectionOptions): IdempotencyStore {
	let opening: Promise<RedisClient> | undefined

	function client(): Promise<RedisClient> {
		opening ??= openIdempotencyClient(connection)

		return opening
	}

	function redisKey(key: string): string {
		return `${IDEMPOTENCY_KEY_PREFIX}${key}`
	}

	function heldBy(token: string): string {
		return `${RUNNING_PREFIX}${token}`
	}

	return {
		async acquire({ key, leaseMs }) {
			const token = randomUUID()

			const reply = await (await client()).runCommand(ACQUIRE_COMMAND, [
				redisKey(key),
				heldBy(token),
				leaseMs,
			])

			return readLease(reply, token)
		},

		async renew({ key, token, leaseMs }) {
			await (await client()).runCommand(RENEW_COMMAND, [redisKey(key), heldBy(token), leaseMs])
		},

		async complete({ key, token, kept, retainForMs }) {
			await (await client()).runCommand(COMPLETE_COMMAND, [
				redisKey(key),
				heldBy(token),
				JSON.stringify(kept),
				retainForMs,
			])
		},

		async release({ key, token }) {
			await (await client()).runCommand(RELEASE_COMMAND, [redisKey(key), heldBy(token)])
		},
	}
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

async function reschedule(
	job: Job,
	token: string | undefined,
	postponed: LeaseHeldError | ShutdownAbortError,
): Promise<never> {
	if (!token) {
		throw new Error(
			`juibs: redis delivered job "${job.name}" without a worker token, and this delivery asked to be delivered again instead of failing — juibs cannot reschedule it without the token, so it fails the attempt instead`,
			{ cause: postponed },
		)
	}

	await job.moveToDelayed(Date.now() + postponed.delayMs, token)

	throw new DelayedError(postponed.message)
}

export function redisDriver(connection: ConnectionOptions): JobDriver {
	const queues = new Map<string, Queue>()
	const idempotency = redisIdempotency(connection)

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
		idempotency,

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
				async (job, token) => {
					if (!job.id) {
						throw new Error(`juibs: redis delivered job "${job.name}" without an id`)
					}

					return request
						.run({
							id: job.id,
							attempt: job.attemptsMade + 1,
							maxAttempts: job.opts.attempts ?? 1,
							envelope: job.data,
						})
						.catch((error: unknown) => {
							if (error instanceof LeaseHeldError || error instanceof ShutdownAbortError) {
								return reschedule(job, token, error)
							}

							throw error
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

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
import { CANCEL_MARK_TTL_MS, type RunningDelivery } from "@/Cancellation"
import { type DeadEntry, deadQueueName } from "@/Dead"
import type { Delivery, ResolvedUnique } from "@/Delivery"
import type { CancelResult, ConsumeRequest, JobDriver, JobState, ScheduleUpsert } from "@/Driver"
import { type Envelope, readEnvelope } from "@/Envelope"
import {
	type IdempotencyLease,
	type IdempotencyStore,
	type KeptResult,
	LeaseHeldError,
} from "@/Idempotency"
import { ShutdownAbortError } from "@/Shutdown"

const SCHEDULER_PREFIX = "juibs."

const SCRIPT_QUEUE = "juibs.scripts"

export const IDEMPOTENCY_KEY_PREFIX = "juibs:idem:"

export const CANCEL_KEY_PREFIX = "juibs:cancel:"

export const RUNNING_PREFIX = "running:"

const ACQUIRE_COMMAND = "juibsIdempotencyAcquire"

const RENEW_COMMAND = "juibsIdempotencyRenew"

const COMPLETE_COMMAND = "juibsIdempotencyComplete"

const RELEASE_COMMAND = "juibsIdempotencyRelease"

const TAKE_CANCELLED_COMMAND = "juibsTakeCancelled"

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

const TAKE_CANCELLED_LUA = `
local taken = {}

for index = 1, #ARGV, 2 do
  local key = KEYS[1] .. ARGV[index]

  if redis.call("GET", key) == ARGV[index + 1] then
    redis.call("DEL", key)

    taken[#taken + 1] = (index + 1) / 2
  end
end

return taken
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

const JOB_STATES = {
	waiting: "waiting",
	prioritized: "waiting",
	active: "active",
	delayed: "delayed",
	completed: "completed",
	failed: "failed",
	"waiting-children": "waiting_children",
	unknown: "unknown",
} as const satisfies Record<Awaited<ReturnType<Job["getState"]>>, JobState>

function storedEnvelope(stored: unknown): Envelope | undefined {
	try {
		return readEnvelope(stored)
	} catch {
		return undefined
	}
}

export const HELD_RETRY_MS = 1_000

/**
 * Whether Redis refused to remove a job because a worker holds its lock.
 *
 * A job read as waiting can be taken by a worker before the removal reaches
 * Redis, and BullMQ then reports the lock in the message of the error it
 * throws. That job is running, so the cancellation takes the running path
 * instead of failing the caller.
 */
function lockedByAWorker(error: unknown): boolean {
	return error instanceof Error && error.message.includes("locked by another worker")
}

async function openScriptClient(connection: ConnectionOptions): Promise<RedisClient> {
	const handle = new Queue(SCRIPT_QUEUE, { connection, skipMetasUpdate: true })
	const client = await handle.getBackend().client

	client.defineCommand(ACQUIRE_COMMAND, { numberOfKeys: 1, lua: ACQUIRE_LUA })
	client.defineCommand(RENEW_COMMAND, { numberOfKeys: 1, lua: RENEW_LUA })
	client.defineCommand(COMPLETE_COMMAND, { numberOfKeys: 1, lua: COMPLETE_LUA })
	client.defineCommand(RELEASE_COMMAND, { numberOfKeys: 1, lua: RELEASE_LUA })
	client.defineCommand(TAKE_CANCELLED_COMMAND, { numberOfKeys: 1, lua: TAKE_CANCELLED_LUA })

	return client
}

/**
 * Opens the one client every juibs script runs on, the first time a script
 * needs it.
 *
 * A script juibs registers is not a command BullMQ exposes, so it is declared
 * once per client with `defineCommand` and called with `runCommand`. The client
 * comes from a `Queue` handle opened with `skipMetasUpdate`, so the handle
 * writes no key of its own and every store shares the caller's connection. The
 * queue it names holds no job and takes no worker.
 */
function scriptClient(connection: ConnectionOptions): () => Promise<RedisClient> {
	let opening: Promise<RedisClient> | undefined

	return () => {
		opening ??= openScriptClient(connection)

		return opening
	}
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
 */
function redisIdempotency(client: () => Promise<RedisClient>): IdempotencyStore {
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

/**
 * The prefix every cancellation mark of one queue is written under.
 *
 * The queue name sits in a hash tag, so every mark of one queue lands in one
 * Redis Cluster slot: the marks are read one queue at a time, and the Lua
 * script derives its keys from the single key it declares.
 */
function cancelPrefix(queue: string): string {
	return `${CANCEL_KEY_PREFIX}{${queue}}:`
}

/**
 * Marks one delivery as cancelled, for the runtime that runs it to find.
 *
 * The mark holds the delivery it names, not just a flag, and it expires on its
 * own, so a cancellation nobody collected leaves nothing behind.
 */
async function requestCancel(
	client: () => Promise<RedisClient>,
	queue: string,
	delivery: RunningDelivery,
): Promise<void> {
	await (await client()).set(
		`${cancelPrefix(queue)}${delivery.id}`,
		String(delivery.attemptsStarted),
		{ PX: CANCEL_MARK_TTL_MS },
	)
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
	const client = scriptClient(connection)
	const idempotency = redisIdempotency(client)

	function queueFor(name: string): Queue {
		const open = queues.get(name)

		if (open) {
			return open
		}

		const queue = new Queue(name, { connection })

		queues.set(name, queue)

		return queue
	}

	async function markRunningDelivery(queue: string, id: string): Promise<CancelResult> {
		const job = await queueFor(queue).getJob(id)

		if (!job) {
			return { outcome: "unknown_job" }
		}

		await requestCancel(client, queue, { id, attemptsStarted: job.attemptsStarted })

		return { outcome: "aborting" }
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

		async get(queue, id) {
			const job = await queueFor(queue).getJob(id)

			if (!job) {
				return undefined
			}

			const state = await job.getState()

			return {
				id,
				queue,
				name: job.name,
				state: JOB_STATES[state],
				envelope: storedEnvelope(job.data),
				attempts: job.attemptsMade,
				maxAttempts: job.opts.attempts ?? 1,
				failure: job.failedReason || undefined,
			}
		},

		async retry(queue, id) {
			const job = await queueFor(queue).getJob(id)

			if (!job) {
				return { outcome: "unknown_job" }
			}

			const state = await job.getState()

			if (state !== "failed") {
				return { outcome: "not_failed", state: JOB_STATES[state] }
			}

			await job.retry("failed", { resetAttemptsMade: true })

			return { outcome: "retried" }
		},

		async cancel(queue, id) {
			const job = await queueFor(queue).getJob(id)

			if (!job) {
				return { outcome: "unknown_job" }
			}

			const state = JOB_STATES[await job.getState()]

			if (state === "completed" || state === "failed") {
				return { outcome: "finished", state }
			}

			if (state === "active") {
				await requestCancel(client, queue, { id, attemptsStarted: job.attemptsStarted })

				return { outcome: "aborting" }
			}

			return job.remove({ removeChildren: true }).then(
				() => ({ outcome: "removed" }) as const,
				(refused: unknown) => {
					if (!lockedByAWorker(refused)) {
						throw refused
					}

					return markRunningDelivery(queue, id)
				},
			)
		},

		async takeCancelled(queue, running) {
			if (running.length === 0) {
				return []
			}

			const taken: unknown = await (await client()).runCommand(TAKE_CANCELLED_COMMAND, [
				cancelPrefix(queue),
				...running.flatMap((delivery) => [delivery.id, String(delivery.attemptsStarted)]),
			])

			if (!Array.isArray(taken)) {
				return []
			}

			return taken.flatMap((position) => {
				const delivery = running[Number(position) - 1]

				return delivery ? [delivery] : []
			})
		},

		async pause(queue) {
			await queueFor(queue).pause()
		},

		async resume(queue) {
			await queueFor(queue).resume()
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
							attemptsStarted: job.attemptsStarted,
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

import { randomUUID } from "node:crypto"
import {
	type ConnectionOptions,
	type DeduplicationOptions,
	DelayedError,
	type FlowJobNode,
	FlowProducer,
	type Job,
	type JobSchedulerTemplateOptions,
	type JobsOptions,
	Queue,
	type RedisClient,
	type RepeatOptions,
	WaitingChildrenError,
	Worker,
	type WorkerOptions,
} from "bullmq"
import { CANCEL_MARK_TTL_MS, type RunningDelivery } from "@/Cancellation"
import { deadQueueName, readDeadEntry } from "@/Dead"
import type { Delivery, ResolvedUnique } from "@/Delivery"
import type {
	CancelResult,
	ChildFailure,
	ChildResult,
	ConsumeRequest,
	EnqueueRequest,
	FlowChildNode,
	FlowNode,
	FlowState,
	JobDriver,
	JobState,
	ScheduleUpsert,
} from "@/Driver"
import { type Envelope, readEnvelope } from "@/Envelope"
import { ChildrenPendingError } from "@/Flow"
import {
	type ForgetOutcome,
	heldRetryDelayMs,
	type IdempotencyLease,
	type IdempotencyStore,
	type KeptResult,
	LeaseHeldError,
} from "@/Idempotency"
import { composeChildId, composeJobId, readChildSlot } from "@/JobId"
import { ShutdownAbortError } from "@/Shutdown"

const SCHEDULER_PREFIX = "jubs."

const SCRIPT_QUEUE = "jubs.scripts"

export const IDEMPOTENCY_KEY_PREFIX = "jubs:idem:"

export const CANCEL_KEY_PREFIX = "jubs:cancel:"

export const RUNNING_PREFIX = "running:"

const ACQUIRE_COMMAND = "jubsIdempotencyAcquire"

const RENEW_COMMAND = "jubsIdempotencyRenew"

const COMPLETE_COMMAND = "jubsIdempotencyComplete"

const RELEASE_COMMAND = "jubsIdempotencyRelease"

const FORGET_COMMAND = "jubsIdempotencyForget"

const TAKE_CANCELLED_COMMAND = "jubsTakeCancelled"

const READ_FLOW_COMMAND = "jubsReadFlow"

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

const FORGET_LUA = `
local current = redis.call("GET", KEYS[1])

if not current then
  return 0
end

if string.sub(current, 1, string.len(ARGV[1])) == ARGV[1] then
  return 2
end

redis.call("DEL", KEYS[1])

return 1
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

/**
 * Reads what the children of one flow job settled into, in one round trip.
 *
 * The two hashes BullMQ keeps on a parent are both keyed by the child's full
 * job key, and a child's key holds its id, which holds the slot it fills. So
 * the two hashes say everything, and no child's own hash is read at all — which
 * is what makes this correct rather than merely short: a completed child is
 * swept by its `removeOnComplete` while its value stays in `:processed`, and a
 * read that went to the child's hash would find nothing there.
 *
 * The `:dependencies` set is read in the same trip, because the two hashes only
 * say what has settled. A child that is neither in them nor gone is still in
 * flight, and its slot is empty for a reason that will not last.
 */
const READ_FLOW_LUA = `
local processed = redis.call("HGETALL", KEYS[1] .. ":processed")
local failed = redis.call("HGETALL", KEYS[1] .. ":failed")
local pending = redis.call("SCARD", KEYS[1] .. ":dependencies")

return { processed, failed, pending }
`

function schedulerId(jobName: string): string {
	return `${SCHEDULER_PREFIX}${jobName}`
}

const BLOCKING_CONNECTION_FIX =
	"jubs: the Redis connection passed to redisDriver must be created with `maxRetriesPerRequest: null`, because a BullMQ worker opens a blocking connection — new Redis(url, { maxRetriesPerRequest: null })"

interface ConnectionShape {
	readonly isCluster?: boolean
	readonly maxRetriesPerRequest?: unknown
	readonly redisOptions?: { readonly maxRetriesPerRequest?: unknown }
	readonly options?: {
		readonly maxRetriesPerRequest?: unknown
		readonly redisOptions?: { readonly maxRetriesPerRequest?: unknown }
	}
}

function blockingOptions(connection: ConnectionShape) {
	if (connection.isCluster) {
		return connection.options?.redisOptions ?? connection.redisOptions
	}

	return connection.options ?? connection.redisOptions ?? connection
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
		`jubs: redis refused the schedule of the job "${schedule.envelope.name}" — correct ${refusedRecurrence(schedule)} its definition declares, or the time zone it runs in — ${reason}`,
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

/**
 * The options one enqueue is added with, and the id it is stored under when the
 * caller derived one. BullMQ's `add` refuses to store a second job under an id
 * it already keeps: it gives back the job that is there, without overwriting
 * its data — which is what makes a redelivered outbox row one job, for as long
 * as Redis keeps that job. Retention is the ceiling: a completed job leaves on
 * `keepCompletedForMs`, a failed one leaves once 200 failures are newer than
 * it, and an id whose job has left is an id nothing answers to.
 *
 * `deduplication` is never combined with such an id. The id is looked at first,
 * and a deduplication key pointing at another job would answer with *that*
 * job's id, so the row would be marked delivered by a job it never enqueued.
 * The relay resolves its delivery without uniqueness for that reason.
 */
function toEnqueueOptions(request: EnqueueRequest): JobsOptions {
	const options = toJobsOptions(request.delivery)

	if (request.jobId === undefined) {
		return options
	}

	return { ...options, jobId: request.jobId }
}

/**
 * Maps one node of a flow onto the job BullMQ adds, children and all.
 *
 * Every node but the root carries `ignoreDependencyOnFailure`, so a child that
 * exhausts its attempts drops out of its parent's dependencies and leaves its
 * reason in the parent's `:failed` hash. The parent then runs, finds the
 * failure and is buried by the runtime — where a hook fires and a dead entry is
 * kept. `failParentOnFailure` would fail the parent inside BullMQ's own worker,
 * before the processor is called, so nothing would be buried and no burial
 * would reach the grandparent.
 */
export function toFlowJob(node: FlowNode): FlowJobNode {
	return {
		name: node.envelope.name,
		queueName: node.queue,
		data: node.envelope,
		opts: toJobsOptions(node.delivery),
		children: node.children.map(toDependentFlowJob),
	}
}

/**
 * Every node but the root is stored under an id built from the slot it fills,
 * so its parent can attribute the value it left behind. The producer already
 * assigns a random id to every node, so naming it ourselves changes nothing
 * else.
 */
function toDependentFlowJob(node: FlowChildNode): FlowJobNode {
	const job = toFlowJob(node)

	return {
		...job,
		opts: {
			...job.opts,
			jobId: composeChildId(node.slot),
			ignoreDependencyOnFailure: true,
		},
	}
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

/**
 * What a cancellation Redis refused for a lock actually reached, read from the
 * state the job is in once the refusal came back.
 *
 * A job that turned `active` under the cancellation holds its own lock, so its
 * delivery is marked and aborted. A job in any other state does not: the lock
 * belongs to a descendant of the flow it waits on, which `removeChildren` tried
 * to delete. Nothing was cancelled then, and `aborting` would name a delivery
 * that does not exist — the caller cancels the running child, or asks again.
 */
export function cancelUnderLock(state: JobState): CancelResult {
	if (state === "active") {
		return { outcome: "aborting" }
	}

	return { outcome: "children_running" }
}

async function openScriptClient(connection: ConnectionOptions): Promise<RedisClient> {
	const handle = new Queue(SCRIPT_QUEUE, { connection, skipMetasUpdate: true })
	const client = await handle.getBackend().client

	client.defineCommand(ACQUIRE_COMMAND, { numberOfKeys: 1, lua: ACQUIRE_LUA })
	client.defineCommand(RENEW_COMMAND, { numberOfKeys: 1, lua: RENEW_LUA })
	client.defineCommand(COMPLETE_COMMAND, { numberOfKeys: 1, lua: COMPLETE_LUA })
	client.defineCommand(RELEASE_COMMAND, { numberOfKeys: 1, lua: RELEASE_LUA })
	client.defineCommand(FORGET_COMMAND, { numberOfKeys: 1, lua: FORGET_LUA })
	client.defineCommand(TAKE_CANCELLED_COMMAND, { numberOfKeys: 1, lua: TAKE_CANCELLED_LUA })
	client.defineCommand(READ_FLOW_COMMAND, { numberOfKeys: 1, lua: READ_FLOW_LUA })

	return client
}

/**
 * Opens the one client every jubs script runs on, the first time a script
 * needs it.
 *
 * A script jubs registers is not a command BullMQ exposes, so it is declared
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
	const [current] = Array.isArray(reply) ? reply : []

	if (typeof current !== "string") {
		return { state: "acquired", token }
	}

	if (current.startsWith(RUNNING_PREFIX)) {
		return { state: "held" }
	}

	const kept: KeptResult = JSON.parse(current)

	return { state: "complete", kept }
}

/**
 * What the forget script reached, read out of the one number it replies with.
 * The numbers are the script's own and end here: 1 deleted a complete key, 2
 * met a key a delivery holds, and anything else met no key at all.
 */
function readForgotten(reply: unknown): ForgetOutcome {
	if (reply === 1) {
		return "forgotten"
	}

	if (reply === 2) {
		return "running"
	}

	return "not_found"
}

/**
 * Keeps the three idempotency states in Redis, under the `jubs:idem:` prefix.
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
 * `forget` reads the same prefix instead of a token: a value carrying it is a
 * lease some delivery holds, which it refuses, and anything else is a complete
 * key, which it deletes.
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

			const reply = await (
				await client()
			).runCommand(ACQUIRE_COMMAND, [redisKey(key), heldBy(token), leaseMs])

			return readLease(reply, token)
		},

		async renew({ key, token, leaseMs }) {
			await (await client()).runCommand(RENEW_COMMAND, [redisKey(key), heldBy(token), leaseMs])
		},

		async complete({ key, token, kept, retainForMs }) {
			await (
				await client()
			).runCommand(COMPLETE_COMMAND, [
				redisKey(key),
				heldBy(token),
				JSON.stringify(kept),
				retainForMs,
			])
		},

		async release({ key, token }) {
			await (await client()).runCommand(RELEASE_COMMAND, [redisKey(key), heldBy(token)])
		},

		async forget(key) {
			const reply: unknown = await (
				await client()
			).runCommand(FORGET_COMMAND, [redisKey(key), RUNNING_PREFIX])

			return readForgotten(reply)
		},
	}
}

/**
 * Reads a child out of the job key its parent's hashes are keyed by. A job key
 * is `<prefix>:<queueName>:<jobId>`, so the last two segments are the queue and
 * the stored id.
 */
function readChildKey(key: string): { queue: string; storedId: string } {
	const segments = key.split(":")

	return { queue: segments.at(-2) ?? "", storedId: segments.at(-1) ?? "" }
}

/**
 * What a child returned, as Redis kept it. A child whose handler returned
 * nothing stored nothing readable, and reads back as `undefined` — the parse is
 * our own for that very reason, since BullMQ's `getChildrenValues` throws on it.
 */
function childValue(stored: string): unknown {
	try {
		return JSON.parse(stored)
	} catch {
		return undefined
	}
}

function hashEntries(reply: unknown): { key: string; value: string }[] {
	const flat = Array.isArray(reply) ? reply : []

	return flat.flatMap((_, index) =>
		index % 2 === 0 ? [{ key: String(flat[index]), value: String(flat[index + 1]) }] : [],
	)
}

export function readFlowState(reply: unknown): FlowState {
	const [processed, failed, pending] = Array.isArray(reply) ? reply : []

	const results: ChildResult[] = hashEntries(processed).map((entry) => {
		const { storedId } = readChildKey(entry.key)

		return { slot: readChildSlot(storedId), value: childValue(entry.value) }
	})

	const failures: ChildFailure[] = hashEntries(failed).map((entry) => {
		const { queue, storedId } = readChildKey(entry.key)

		return {
			id: composeJobId(queue, storedId),
			slot: readChildSlot(storedId),
			reason: entry.value,
		}
	})

	return { results, failures, pending: typeof pending === "number" ? pending : 0 }
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
	await (
		await client()
	).set(`${cancelPrefix(queue)}${delivery.id}`, String(delivery.attemptsStarted), {
		PX: CANCEL_MARK_TTL_MS,
	})
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
			`jubs: redis delivered job "${job.name}" without a worker token, and this delivery asked to be delivered again instead of failing — jubs cannot reschedule it without the token, so it fails the attempt instead`,
			{ cause: postponed },
		)
	}

	const delayMs =
		postponed instanceof LeaseHeldError ? heldRetryDelayMs(job.attemptsStarted) : postponed.delayMs

	await job.moveToDelayed(Date.now() + delayMs, token)

	throw new DelayedError(postponed.message)
}

/**
 * Puts a parent delivered while one of its children still runs back into
 * `waiting-children`, instead of spending an attempt on a refusal it cannot
 * avoid.
 *
 * The worker reads a `WaitingChildrenError` before it fails anything, so no
 * attempt is made, no failure event is emitted, and the completion of the last
 * child moves the parent back to `wait` on its own. Without this the parent is
 * delivered again on backoff and refused again, and a child slower than the
 * attempts allowed kills a parent that nothing was wrong with.
 *
 * The move only writes when the job is active, held under this token, and still
 * has dependencies, and it answers `false` when the last child settled between
 * the read and the call. Throwing anyway would leave the job stuck in `active`
 * until the stalled check reclaims it, so a `false` lets the original refusal
 * take the ordinary failure path: the attempt after it reads a full set.
 */
async function waitForChildren(
	job: Job,
	token: string | undefined,
	pending: ChildrenPendingError,
): Promise<never> {
	if (!token) {
		throw new Error(
			`jubs: redis delivered job "${job.name}" without a worker token, and this delivery asked to wait on its children again instead of failing — jubs cannot move it back to waiting-children without the token, so it fails the attempt instead`,
			{ cause: pending },
		)
	}

	if (!(await job.moveToWaitingChildren(token))) {
		throw pending
	}

	throw new WaitingChildrenError(pending.message)
}

export function redisDriver(connection: ConnectionOptions): JobDriver {
	const queues = new Map<string, Queue>()
	const client = scriptClient(connection)
	const idempotency = redisIdempotency(client)

	let flows: FlowProducer | undefined

	function queueFor(name: string): Queue {
		const open = queues.get(name)

		if (open) {
			return open
		}

		const queue = new Queue(name, { connection })

		queues.set(name, queue)

		return queue
	}

	/**
	 * Opens the one producer every flow is added through, the first time a flow
	 * needs it.
	 *
	 * It takes the connection and the key prefix the queues take, so a flow lands
	 * on the very queues `enqueue` writes to. Nothing closes it, because nothing
	 * closes the queues either: the caller owns the connection they all share.
	 */
	function flowProducer(): FlowProducer {
		flows ??= new FlowProducer({ connection })

		return flows
	}

	async function cancelRefusedByALock(queue: string, id: string): Promise<CancelResult> {
		const job = await queueFor(queue).getJob(id)

		if (!job) {
			return { outcome: "unknown_job" }
		}

		const reached = cancelUnderLock(JOB_STATES[await job.getState()])

		if (reached.outcome === "aborting") {
			await requestCancel(client, queue, { id, attemptsStarted: job.attemptsStarted })
		}

		return reached
	}

	return {
		idempotency,

		async enqueue(request) {
			const job = await queueFor(request.queue).add(
				request.envelope.name,
				request.envelope,
				toEnqueueOptions(request),
			)

			if (!job.id) {
				throw new Error(`jubs: redis stored job "${request.envelope.name}" without an id`)
			}

			return { id: job.id }
		},

		async get(queue, id) {
			const job = await queueFor(queue).getJob(id)

			if (!job) {
				return
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

					return cancelRefusedByALock(queue, id)
				},
			)
		},

		async takeCancelled(queue, running) {
			if (running.length === 0) {
				return []
			}

			const taken: unknown = await (
				await client()
			).runCommand(TAKE_CANCELLED_COMMAND, [
				cancelPrefix(queue),
				...running.flatMap((delivery) => [delivery.id, String(delivery.attemptsStarted)]),
			])

			if (!Array.isArray(taken)) {
				return []
			}

			return taken.flatMap((position) => {
				const delivery = running[Number(position) - 1]

				if (!delivery) {
					return []
				}

				return [delivery]
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

		flow: {
			async enqueue(root) {
				const tree = await flowProducer().add(toFlowJob(root))
				const id = tree?.job.id

				if (!id) {
					throw new Error(`jubs: redis stored the flow "${root.envelope.name}" without an id`)
				}

				return { id }
			},

			async read(queue, id) {
				const reply: unknown = await (
					await client()
				).runCommand(READ_FLOW_COMMAND, [queueFor(queue).toKey(id)])

				return readFlowState(reply)
			},
		},

		dead: {
			async bury(queue, entry) {
				await queueFor(deadQueueName(queue)).add(entry.envelope.name, entry)
			},

			async list(queue) {
				const dead = deadQueueName(queue)
				const waiting = await queueFor(dead).getWaiting()

				return waiting.flatMap((job) => {
					if (!job.id) {
						return []
					}

					try {
						return [{ id: job.id, entry: readDeadEntry(job.data) }]
					} catch (unreadable: unknown) {
						console.error(
							`jubs: the dead queue "${dead}" holds a record this library cannot read, and it is left out of jobs.dead.list("${queue}") so the records beside it stay reachable — drop it with jobs.dead.discard("${composeJobId(dead, job.id)}")`,
							unreadable,
						)

						return []
					}
				})
			},

			async read(queue, id) {
				const job = await queueFor(deadQueueName(queue)).getJob(id)

				if (!job) {
					return
				}

				return readDeadEntry(job.data)
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
						throw new Error(`jubs: redis delivered job "${job.name}" without an id`)
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

							if (error instanceof ChildrenPendingError) {
								return waitForChildren(job, token, error)
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

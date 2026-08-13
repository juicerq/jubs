import { randomUUID } from "node:crypto"
import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { RunningDelivery } from "@/Cancellation"
import type { DeadEntry } from "@/Dead"
import type { JobDefinition } from "@/Definition"
import type { ConsumeRequest, JobDriver, JobState } from "@/Driver"
import type { Envelope } from "@/Envelope"
import { type IdempotencyStore, type KeptResult, LeaseHeldError } from "@/Idempotency"
import { composeJobId } from "@/JobId"

const ACCEPTED_DELIVERY = [
	"attempts",
	"backoff",
	"priority",
	"keepCompletedForMs",
	"keepFailedCount",
]

function unsupported(behaviour: string): Error {
	return new Error(
		`jubs: memoryDriver does not simulate "${behaviour}"; test that behaviour against redisDriver`,
	)
}

interface MemoryJob {
	readonly id: string
	readonly queue: string
	readonly maxAttempts: number
	readonly envelope: Envelope
	state: JobState
	attempts: number
	attemptsStarted: number
	failure: string | undefined
}

/**
 * What a store over Redis would give back, since it writes the kept result as
 * JSON and reads it back the same way. A result the runtime keeps is always
 * serialisable, because `keptResultOf` checks it first; a caller reaching the
 * store on its own is given back what it passed, so its own mistake is not
 * turned into a throw from a driver.
 */
function asJsonKeeps(kept: KeptResult): KeptResult {
	try {
		return JSON.parse(JSON.stringify(kept)) as KeptResult
	} catch {
		return kept
	}
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
 *
 * Enqueuing a definition that declares `awaits` throws, and so does delivering
 * one. Jobs run inline here, so nothing ever reaches `waiting-children`, and a
 * parent that ran before its children would agree with your test and disagree
 * with production. Reading what a flow settled into throws for the same reason,
 * and for one more: an empty answer is not the truth about a flow but a shape
 * the runtime refuses, so this driver would manufacture a burial no Redis ever
 * produced. Test a job that waits on children against `redisDriver`.
 *
 * Starting a queue whose definitions declare a schedule throws, because a
 * recurrence is nothing but a clock: an inline imitation would fire when your
 * test asked it to and never again in production. A started queue that declares
 * no schedule reconciles to nothing and passes.
 *
 * The idempotency store keeps the absent and the complete state for real, so a
 * repeated delivery of a completed key skips the handler and gives back the kept
 * result — as the JSON projection of it, the way a store over Redis gives it
 * back, so a `Date` a result schema produced comes back a string here too. It
 * keeps no clock, so `leaseMs` and the result retention are ignored
 * and a lease never expires on its own. A delivery that meets a held lease has
 * to be rescheduled, which needs a clock, so it throws instead: test a held
 * lease, an expired lease and a killed worker against `redisDriver`.
 *
 * It keeps every job it records, so `jobs.get` and `jobs.retry` answer for real.
 * A job it keeps is `waiting`, `active`, `completed` or `failed` — the four states
 * an inline run passes through — and never `delayed`, `waiting_children` or
 * `unknown`, which need a clock or a queue this driver does not have. Its
 * `attempts` counts the runs that ended here, so a job reads 0 before its first
 * run and 1 after it, and climbs only when `runNext` or `drain` runs it again —
 * a failed job is never retried on its own. `jobs.retry` puts a failed job back
 * in the pending line, clears its failure and puts its count back to 0, the way
 * a retry over Redis does; a job in any other state is refused with the state it
 * is in, and an id no job answers to comes back as `unknown_job`.
 *
 * `jobs.cancel` answers with what this driver knows. A job it has not run is
 * removed and stops answering to `jobs.get`; a job that already finished comes
 * back with the state it settled in; an id no job answers to comes back as
 * `unknown_job`. A job it is running right now is marked and comes back as
 * `aborting`, and that abort is real: the marks are a map in memory, from job to
 * the delivery cancelled, and the sweep that reads them belongs to the runtime,
 * not to a driver. A mark is handed out only to the delivery it names, so one
 * left behind by a delivery that ended kills no later delivery — the same rule
 * `redisDriver` follows. The marks keep no clock, so `CANCEL_MARK_TTL_MS` is
 * ignored and a mark nobody collects never expires on its own — test the expiry
 * against `redisDriver`. Nor is there a
 * race between reading a job's state and removing it, because a job runs inline
 * on the caller's stack: test a job that turns active under a cancellation
 * against `redisDriver`.
 *
 * `jobs.pause` and `jobs.resume` are simulated where this driver consumes: a
 * paused queue holds its pending jobs back, so `runNext` skips them and fails
 * when every pending job sits on a paused queue, and `drain` runs the other
 * queues and counts only what it ran.
 *
 * `context.signal` and `timeoutMs` are enforced by the runtime, not by a driver,
 * so both work here: a handler that outruns its `timeoutMs` has its signal
 * aborted and its delivery rejected, and the rejection reaches the caller of
 * `runNext` or `drain`. A timed-out handler whose definition declares an
 * `idempotencyKey` keeps its key held until its body ends, so the next delivery
 * meets a held lease — which this driver cannot reschedule, so it throws.
 *
 * Shutdown is where this driver stops. It runs jobs inline on the caller's own
 * stack, so it satisfies `Consumer.close()` only for as long as the caller
 * awaits `runNext` or `drain`: nothing is in flight when `close` runs, the
 * timeout of `close({ timeoutMs })` never expires, and no signal is ever aborted
 * by a shutdown. The abort path is not reachable here. Test a deploy that cuts a
 * running handler short against `redisDriver`.
 */
export function memoryDriver(): MemoryDriver {
	const recorded: MemoryJob[] = []
	const pending: MemoryJob[] = []
	const consumers = new Map<string, ConsumeRequest["run"]>()
	const paused = new Set<string>()
	const buried = new Map<string, MemoryDeadQueue>()
	const leased = new Map<string, string>()
	const kept = new Map<string, KeptResult>()
	const marked = new Map<string, number>()

	function deadQueueFor(queue: string): MemoryDeadQueue {
		const open = buried.get(queue)

		if (open) {
			return open
		}

		const opened: MemoryDeadQueue = { entries: new Map(), deaths: 0 }

		buried.set(queue, opened)

		return opened
	}

	function jobAt(queue: string, id: string): MemoryJob | undefined {
		return recorded.find((candidate) => candidate.queue === queue && candidate.id === id)
	}

	function runnable(): MemoryJob | undefined {
		return pending.find((job) => !paused.has(job.queue))
	}

	async function runNext(): Promise<void> {
		const next = runnable()

		if (!next) {
			if (pending.length > 0) {
				throw new Error(
					"jubs: memoryDriver has every pending job on a paused queue — call jobs.resume(queue) to run them",
				)
			}

			throw new Error("jubs: memoryDriver has no pending job to run")
		}

		const run = consumers.get(next.queue)

		if (!run) {
			throw new Error(
				`jubs: memoryDriver has no consumer on queue "${next.queue}" — call jobs.start(handlers) before running jobs`,
			)
		}

		pending.splice(pending.indexOf(next), 1)
		next.state = "active"
		next.attemptsStarted += 1

		await run({
			id: next.id,
			attempt: next.attempts + 1,
			attemptsStarted: next.attemptsStarted,
			maxAttempts: next.maxAttempts,
			envelope: next.envelope,
		})
			.then(() => {
				next.state = "completed"
				next.attempts += 1
			})
			.catch((error: unknown) => {
				if (error instanceof LeaseHeldError) {
					next.state = "waiting"
					pending.unshift(next)

					throw unsupported("rescheduling a delivery whose idempotency lease is held")
				}

				next.state = "failed"
				next.attempts += 1
				next.failure = error instanceof Error ? error.message : String(error)

				throw error
			})
	}

	const idempotency: IdempotencyStore = {
		async acquire({ key, leaseMs }) {
			const complete = kept.get(key)

			if (complete) {
				return { state: "complete", kept: complete }
			}

			if (leased.has(key)) {
				return { state: "held", retryInMs: leaseMs }
			}

			const token = randomUUID()

			leased.set(key, token)

			return { state: "acquired", token }
		},

		async renew() {},

		async complete({ key, token, kept: result }) {
			const held = leased.get(key)

			if (held !== undefined && held !== token) {
				return
			}

			leased.delete(key)
			kept.set(key, asJsonKeeps(result))
		},

		async release({ key, token }) {
			if (leased.get(key) !== token) {
				return
			}

			leased.delete(key)
		},
	}

	function requestCancel(queue: string, delivery: RunningDelivery): void {
		marked.set(composeJobId(queue, delivery.id), delivery.attemptsStarted)
	}

	function takeCancellation(queue: string, delivery: RunningDelivery): boolean {
		const key = composeJobId(queue, delivery.id)

		if (marked.get(key) !== delivery.attemptsStarted) {
			return false
		}

		marked.delete(key)

		return true
	}

	return {
		idempotency,

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
				state: "waiting",
				attempts: 0,
				attemptsStarted: 0,
				failure: undefined,
			}

			recorded.push(job)
			pending.push(job)

			return { id: job.id }
		},

		async get(queue, id) {
			const job = jobAt(queue, id)

			if (!job) {
				return undefined
			}

			return {
				id: job.id,
				queue: job.queue,
				name: job.envelope.name,
				state: job.state,
				envelope: job.envelope,
				attempts: job.attempts,
				maxAttempts: job.maxAttempts,
				failure: job.failure,
			}
		},

		async retry(queue, id) {
			const job = jobAt(queue, id)

			if (!job) {
				return { outcome: "unknown_job" }
			}

			if (job.state !== "failed") {
				return { outcome: "not_failed", state: job.state }
			}

			job.state = "waiting"
			job.attempts = 0
			job.failure = undefined
			pending.push(job)

			return { outcome: "retried" }
		},

		async cancel(queue, id) {
			const job = jobAt(queue, id)

			if (!job) {
				return { outcome: "unknown_job" }
			}

			if (job.state === "completed" || job.state === "failed") {
				return { outcome: "finished", state: job.state }
			}

			if (job.state === "active") {
				requestCancel(queue, job)

				return { outcome: "aborting" }
			}

			pending.splice(pending.indexOf(job), 1)
			recorded.splice(recorded.indexOf(job), 1)

			return { outcome: "removed" }
		},

		async takeCancelled(queue, running) {
			return running.filter((delivery) => takeCancellation(queue, delivery))
		},

		async pause(queue) {
			paused.add(queue)
		},

		async resume(queue) {
			paused.delete(queue)
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

		async reconcileSchedules(request) {
			if (request.declared.length === 0) {
				return
			}

			throw unsupported("schedule")
		},

		flow: {
			async enqueue() {
				throw unsupported("a job that waits on children")
			},

			async read() {
				throw unsupported("what the children of a job that waits on children settled into")
			},
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

			while (runnable()) {
				await runNext()
				ran += 1
			}

			return ran
		},
	}
}

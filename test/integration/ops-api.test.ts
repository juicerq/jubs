import { afterAll, describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import { CANCEL_SWEEP_MS } from "@/Cancellation"
import {
	createJobs,
	DELIVERY_DEFAULTS,
	defineHandler,
	defineJob,
	every,
	type JobFailureEvent,
	redisDriver,
} from "@/index"
import { CANCEL_KEY_PREFIX } from "@/RedisDriver"
import { scoped, storedId } from "./namespace"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

async function waitFor(ready: () => Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 4_000

	while (Date.now() < deadline) {
		if (await ready()) {
			return
		}

		await Bun.sleep(25)
	}

	throw new Error("the condition did not hold in time")
}

/**
 * A handler body that obeys its signal: it rejects with `signal.reason` the
 * moment the runtime aborts, and otherwise returns once `settleInMs` passes —
 * or never, when no delay is given.
 */
function obedient(signal: AbortSignal, settleInMs?: number): Promise<void> {
	return new Promise((settle, reject) => {
		const settling = settleInMs === undefined ? undefined : setTimeout(settle, settleInMs)

		signal.addEventListener("abort", () => {
			clearTimeout(settling)
			reject(signal.reason)
		})
	})
}

const ROOT = join(import.meta.dir, "..", "..")

/**
 * Writes a worker that runs one job in a process of its own, counts the runs it
 * starts and the aborts its signal receives, and obeys the abort by rethrowing.
 *
 * The cancellation under test is asked for by the test process, which runs no
 * worker on this queue, so only a channel that crosses processes can reach this
 * handler.
 */
async function writeCancelWorker(
	job: { name: string; queue: string },
	startedKey: string,
	abortedKey: string,
): Promise<string> {
	const source = `
import IORedis from ${JSON.stringify(Bun.resolveSync("ioredis", ROOT))}
import { type } from ${JSON.stringify(Bun.resolveSync("arktype", ROOT))}
import { createJobs, defineHandler, defineJob, redisDriver } from ${JSON.stringify(join(ROOT, "src", "index.ts"))}

const connection = new IORedis(${JSON.stringify(REDIS_URL)}, { maxRetriesPerRequest: null })

const renderReport = defineJob({
	name: ${JSON.stringify(job.name)},
	queue: ${JSON.stringify(job.queue)},
	payload: type({ reportId: "string" }),
	delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 10 } },
})

const jobs = createJobs({ driver: redisDriver(connection), deadQueues: [${JSON.stringify(job.queue)}] })

await jobs.start([
	defineHandler(renderReport, async (_data, context) => {
		await connection.incr(${JSON.stringify(startedKey)})

		await new Promise((_keep, reject) => {
			context.signal.addEventListener("abort", () => {
				connection.incr(${JSON.stringify(abortedKey)}).then(() => reject(context.signal.reason))
			})
		})
	}),
])
`

	const path = join(tmpdir(), `juibs-cancel-worker-${Bun.randomUUIDv7()}.mjs`)

	await Bun.write(path, source)

	return path
}

async function counter(key: string): Promise<number> {
	return Number((await inspectorConnection.get(key)) ?? 0)
}

async function scrubMarks(queue: string): Promise<void> {
	const marks = await inspectorConnection.keys(`${CANCEL_KEY_PREFIX}{${queue}}:*`)

	if (marks.length) {
		await inspectorConnection.del(...marks)
	}
}

async function scrub(queue: Queue): Promise<Queue> {
	await queue.obliterate({ force: true })
	await scrubMarks(queue.name)

	return queue
}

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.resume()))
	await Promise.all(opened.map(scrub))
	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("reading a job over redis", () => {
	test("describes a job waiting on a queue nobody consumes", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-waiting"),
			payload: type({ cents: "string.numeric.parse" }),
		})

		await scrub(inspect(chargeCard.queue))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await jobs.get(enqueued.id)).toEqual({
			id: enqueued.id,
			queue: chargeCard.queue,
			name: chargeCard.name,
			state: "waiting",
			envelope: {
				v: 1,
				name: chargeCard.name,
				data: { cents: "500" },
				origin: "direct",
			},
			attempts: 0,
			maxAttempts: DELIVERY_DEFAULTS.attempts,
			failure: undefined,
		})
	})

	test("carries the failure and the attempt of a job that failed every attempt", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-failed"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 2, backoff: { type: "exponential", delayMs: 10 } },
		})

		const queue = await scrub(inspect(chargeCard.queue))
		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card was declined")
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitFor(async () => (await queue.getJob(storedId(enqueued)))?.finishedOn !== undefined)

		const snapshot = await jobs.get(enqueued.id)

		expect(snapshot?.state).toBe("failed")
		expect(snapshot?.attempts).toBe(2)
		expect(snapshot?.maxAttempts).toBe(2)
		expect(snapshot?.failure).toBe("the card was declined")
		expect(snapshot?.envelope?.data).toEqual({ cents: "500" })

		await runtime.close()
	})

	test("reads a dead job's id on the dead queue, not on the live one", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-dead-id"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 1 },
		})

		const queue = await scrub(inspect(chargeCard.queue))

		await scrub(inspect(`${chargeCard.queue}.dead`))

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [chargeCard.queue],
		})

		let refusing = false

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				if (refusing) {
					throw new Error("the card was declined")
				}
			}),
		])

		const survivor = await jobs.enqueue(chargeCard, { cents: "100" })

		await waitFor(async () => (await jobs.get(survivor.id))?.state === "completed")

		refusing = true

		const doomed = await jobs.enqueue(chargeCard, { cents: "300" })

		await waitFor(async () => (await queue.getJob(storedId(doomed)))?.finishedOn !== undefined)
		await waitFor(async () => (await jobs.dead.list(chargeCard.queue)).length === 1)

		const [dead] = await jobs.dead.list(chargeCard.queue)

		expect(dead?.id).toBe(`${chargeCard.queue}.dead:1`)
		expect(await jobs.get(dead?.id ?? "")).toBeUndefined()
		expect(await jobs.retry(dead?.id ?? "")).toEqual({ outcome: "unknown_job" })
		expect(await jobs.cancel(dead?.id ?? "")).toEqual({ outcome: "unknown_job" })
		expect(await jobs.dead.list(chargeCard.queue)).toHaveLength(1)

		await runtime.close()
	})

	test("answers the id a scheduled job's handler is given, colons and all", async () => {
		const sendDigest = defineJob({
			name: scoped("reports.digest"),
			queue: scoped("juibs.test.ops-schedule-id"),
			payload: type({ ledger: "string" }),
			schedule: every("1 second", { data: { ledger: "main" } }),
		})

		await scrub(inspect(sendDigest.queue))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran = Promise.withResolvers<string>()

		const runtime = await jobs.start([
			defineHandler(sendDigest, async (_data, context) => {
				ran.resolve(context.id)
			}),
		])

		const id = await ran.promise
		const snapshot = await jobs.get(id)

		expect(storedId({ id })).toStartWith("repeat:")
		expect(snapshot?.queue).toBe(sendDigest.queue)
		expect(snapshot?.envelope?.origin).toBe("schedule")

		await runtime.close()
	})

	test("gives back nothing for an id redis keeps no job under", async () => {
		const missing = inspect(scoped("juibs.test.ops-missing"))
		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		expect(await jobs.get(`${missing.name}:404`)).toBeUndefined()
	})
})

describe("retrying a job over redis", () => {
	test("runs a job that failed every attempt again, from attempt 1", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-retry"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 1 },
		})

		const queue = await scrub(inspect(chargeCard.queue))
		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const attempts: number[] = []
		let refusing = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (_data, context) => {
				attempts.push(context.attempt)

				if (refusing) {
					throw new Error("the card was declined")
				}
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitFor(async () => (await queue.getJob(storedId(enqueued)))?.finishedOn !== undefined)

		refusing = false

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "retried" })

		await waitFor(async () => (await jobs.get(enqueued.id))?.state === "completed")

		const snapshot = await jobs.get(enqueued.id)

		expect(attempts).toEqual([1, 1])
		expect(snapshot?.attempts).toBe(1)
		expect(snapshot?.failure).toBeUndefined()

		await runtime.close()
	})

	test("names the state of a job that is not failed, and the id no job answers to", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-not-failed"),
			payload: type({ cents: "string.numeric.parse" }),
		})

		await scrub(inspect(chargeCard.queue))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "not_failed", state: "waiting" })
		expect(await jobs.retry(`${chargeCard.queue}:404`)).toEqual({ outcome: "unknown_job" })
	})
})

describe("cancelling a job over redis", () => {
	test("removes a job no worker has taken yet", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-cancel-waiting"),
			payload: type({ cents: "string.numeric.parse" }),
		})

		const queue = await scrub(inspect(chargeCard.queue))
		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "removed" })
		expect(await jobs.get(enqueued.id)).toBeUndefined()
		expect(await queue.getJobCountByTypes("waiting")).toBe(0)
	})

	test("names the state of a job that already finished, and the id no job answers to", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-cancel-finished"),
			payload: type({ cents: "string.numeric.parse" }),
		})

		await scrub(inspect(chargeCard.queue))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const runtime = await jobs.start([defineHandler(chargeCard, async () => {})])
		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitFor(async () => (await jobs.get(enqueued.id))?.state === "completed")

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "finished", state: "completed" })
		expect(await jobs.cancel(`${chargeCard.queue}:404`)).toEqual({ outcome: "unknown_job" })

		await runtime.close()
	})

	test("a cancelled job dies without another attempt, and lands in the dead queue", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-cancel-dead"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 10 } },
		})

		await scrub(inspect(chargeCard.queue))
		await scrub(inspect(`${chargeCard.queue}.dead`))

		const dead: JobFailureEvent[] = []
		const started = Promise.withResolvers<void>()
		let runs = 0

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [chargeCard.queue],
			hooks: {
				onDead: (event) => {
					dead.push(event)
				},
			},
		})

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (_data, context) => {
				runs += 1
				started.resolve()

				await obedient(context.signal)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await started.promise

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })

		await waitFor(async () => (await jobs.dead.list(chargeCard.queue)).length === 1)

		const [buried] = await jobs.dead.list(chargeCard.queue)

		expect(buried?.reason).toBe("cancelled")
		expect(buried?.error.name).toBe("CancelledError")
		expect(dead[0]?.id).toBe(enqueued.id)
		expect((await jobs.get(enqueued.id))?.state).toBe("failed")
		expect(runs).toBe(1)

		await runtime.close()
	})

	test("leaves no mark behind, so a retry of the cancelled job runs to the end", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-cancel-retry"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 1 },
		})

		await scrub(inspect(chargeCard.queue))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const started = Promise.withResolvers<void>()
		const runs: number[] = []
		let cancelling = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (data, context) => {
				runs.push(data.cents)

				if (!cancelling) {
					await obedient(context.signal, CANCEL_SWEEP_MS * 3)

					return
				}

				started.resolve()

				await obedient(context.signal)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await started.promise
		await jobs.cancel(enqueued.id)
		await waitFor(async () => (await jobs.get(enqueued.id))?.state === "failed")

		cancelling = false

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "retried" })

		await waitFor(async () => (await jobs.get(enqueued.id))?.state === "completed")

		expect(runs).toEqual([500, 500])

		await runtime.close()
	})

	test("lets a cancellation nobody collected die with its delivery, never killing the next one", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-cancel-stale"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 1 },
		})

		await scrub(inspect(chargeCard.queue))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const started = Promise.withResolvers<void>()
		const release = Promise.withResolvers<void>()
		const runs: number[] = []
		let failing = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (data, context) => {
				runs.push(data.cents)

				if (failing) {
					started.resolve()

					await release.promise

					throw new Error("the card was declined")
				}

				await obedient(context.signal, CANCEL_SWEEP_MS * 3)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await started.promise

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })

		release.resolve()

		await waitFor(async () => (await jobs.get(enqueued.id))?.state === "failed")

		failing = false

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "retried" })
		await waitFor(async () => (await jobs.get(enqueued.id))?.state === "completed")

		expect(runs).toEqual([500, 500])

		await runtime.close()
	})

	test("a cancellation asked for by another process aborts the handler that runs the job", async () => {
		const renderReport = defineJob({
			name: scoped("reports.render"),
			queue: scoped("juibs.test.ops-cancel-cross"),
			payload: type({ reportId: "string" }),
			delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 10 } },
		})

		const startedKey = scoped("juibs:test:ops:cancel:started")
		const abortedKey = scoped("juibs:test:ops:cancel:aborted")

		await scrub(inspect(renderReport.queue))
		await scrub(inspect(`${renderReport.queue}.dead`))
		await inspectorConnection.del(startedKey, abortedKey)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const enqueued = await jobs.enqueue(renderReport, { reportId: "rep-1" })

		const workerPath = await writeCancelWorker(renderReport, startedKey, abortedKey)
		const child = Bun.spawn(["bun", workerPath], { stdout: "inherit", stderr: "inherit" })

		try {
			await waitFor(async () => (await counter(startedKey)) === 1)

			expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })

			await waitFor(async () => (await counter(abortedKey)) === 1)
			await waitFor(async () => (await jobs.dead.list(renderReport.queue)).length === 1)

			const [buried] = await jobs.dead.list(renderReport.queue)

			expect(buried?.reason).toBe("cancelled")
			expect(await counter(startedKey)).toBe(1)
		} finally {
			child.kill("SIGKILL")

			await child.exited
			await Bun.file(workerPath).delete()
			await inspectorConnection.del(startedKey, abortedKey)
		}
	}, 60_000)
})

describe("pausing a queue over redis", () => {
	test("stops a running worker from taking jobs until the queue resumes", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("juibs.test.ops-pause"),
			payload: type({ cents: "string.numeric.parse" }),
		})

		const queue = await scrub(inspect(chargeCard.queue))
		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const charged: number[] = []

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (data) => {
				charged.push(data.cents)
			}),
		])

		await jobs.pause(chargeCard.queue)

		expect(await queue.isPaused()).toBe(true)

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await Bun.sleep(500)

		expect(charged).toEqual([])
		expect((await jobs.get(enqueued.id))?.state).toBe("waiting")

		await jobs.resume(chargeCard.queue)

		await waitFor(async () => charged.length === 1)

		expect(charged).toEqual([500])
		expect(await queue.isPaused()).toBe(false)

		await runtime.close()
	})
})

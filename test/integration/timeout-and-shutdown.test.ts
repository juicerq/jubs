import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { type Job, Queue } from "bullmq"
import IORedis from "ioredis"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { IDEMPOTENCY_KEY_PREFIX, RUNNING_PREFIX } from "@/RedisDriver"
import { scoped, storedId } from "./namespace"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

async function freshQueue(queue: string): Promise<Queue> {
	const live = new Queue(queue, { connection: inspectorConnection })
	const dead = new Queue(`${queue}.dead`, { connection: inspectorConnection })

	opened.push(live, dead)

	await live.obliterate({ force: true })
	await dead.obliterate({ force: true })

	return live
}

async function waitForFinished(queue: Queue, id: string): Promise<Job> {
	const deadline = Date.now() + 8_000

	while (Date.now() < deadline) {
		const job = await queue.getJob(id)

		if (job?.finishedOn) {
			return job
		}

		await Bun.sleep(25)
	}

	throw new Error(`job ${id} on ${queue.name} did not finish in time`)
}

const leased: string[] = []

function leaseKey(job: string, id: string): string {
	const key = `${IDEMPOTENCY_KEY_PREFIX}${job}:${id}`

	leased.push(key)

	return key
}

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))

	if (leased.length > 0) {
		await inspectorConnection.del(...leased)
	}

	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("handler timeout over redis", () => {
	test("retries a timed-out attempt and buries the job once its attempts run out", async () => {
		const renderReport = defineJob({
			name: scoped("reports.render"),
			queue: scoped("jubs.test.timeout-dead"),
			payload: type({ id: "string" }),
			delivery: { attempts: 2, backoff: { type: "exponential", delayMs: 1 } },
			timeoutMs: 100,
		})

		const live = await freshQueue(renderReport.queue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [renderReport.queue],
		})

		const entered: number[] = []

		const runtime = await jobs.start([
			defineHandler(renderReport, async (_data, context) => {
				entered.push(context.attempt)

				await Bun.sleep(1_500)
			}),
		])

		const enqueued = await jobs.enqueue(renderReport, { id: scoped("rep-1") })
		const finished = await waitForFinished(live, storedId(enqueued))

		expect(entered).toEqual([1, 2])
		expect(finished.failedReason).toContain("timeoutMs")

		const dead = await jobs.dead.list(renderReport.queue)

		expect(dead).toHaveLength(1)
		expect(dead[0]?.reason).toBe("attempts_exhausted")
		expect(dead[0]?.error.message).toContain("timeoutMs")

		await runtime.close()
	})

	test("holds the idempotency key of a timed-out body, so no second body runs beside it", async () => {
		const settlePayment = defineJob({
			name: scoped("payments.settle"),
			queue: scoped("jubs.test.timeout-idem"),
			payload: type({ id: "string" }),
			delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 1 } },
			timeoutMs: 100,
			idempotencyKey: (data) => data.id,
		})

		const live = await freshQueue(settlePayment.queue)
		const paymentId = scoped("pay-1")

		await inspectorConnection.del(leaseKey(settlePayment.name, paymentId))

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [settlePayment.queue],
		})

		const bodies: number[] = []

		const runtime = await jobs.start([
			defineHandler(settlePayment, async (_data, context) => {
				bodies.push(context.attempt)

				await Bun.sleep(1_500)
			}),
		])

		const enqueued = await jobs.enqueue(settlePayment, { id: paymentId })

		await Bun.sleep(700)

		const stored = await live.getJob(storedId(enqueued))

		expect(bodies).toEqual([1])
		expect(await stored?.getState()).toBe("delayed")
		expect(stored?.attemptsMade).toBe(1)
		expect(await jobs.dead.list(settlePayment.queue)).toEqual([])

		await runtime.close()
	})
})

describe("shutdown over redis", () => {
	test("resolves close within its timeout when a handler ignores its signal", async () => {
		const renderReport = defineJob({
			name: scoped("reports.render"),
			queue: scoped("jubs.test.shutdown-deaf"),
			payload: type({ id: "string" }),
			delivery: { attempts: 1 },
		})

		await freshQueue(renderReport.queue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const started = Promise.withResolvers<void>()
		const done = Promise.withResolvers<void>()

		const runtime = await jobs.start([
			defineHandler(renderReport, async () => {
				started.resolve()

				await Bun.sleep(2_000)

				done.resolve()
			}),
		])

		await jobs.enqueue(renderReport, { id: scoped("rep-1") })
		await started.promise

		const at = Date.now()

		await runtime.close({ timeoutMs: 300 })

		const elapsed = Date.now() - at

		expect(elapsed).toBeGreaterThanOrEqual(250)
		expect(elapsed).toBeLessThan(1_500)

		await done.promise
		await Bun.sleep(200)
	})

	test("aborts the signal of a handler still running when the close timeout expires", async () => {
		const renderReport = defineJob({
			name: scoped("reports.render"),
			queue: scoped("jubs.test.shutdown-listening"),
			payload: type({ id: "string" }),
			delivery: { attempts: 1 },
		})

		await freshQueue(renderReport.queue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const started = Promise.withResolvers<void>()
		const aborted = Promise.withResolvers<boolean>()

		const runtime = await jobs.start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				expect(context.signal.aborted).toBe(false)

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				aborted.resolve(context.signal.aborted)
			}),
		])

		await jobs.enqueue(renderReport, { id: scoped("rep-1") })
		await started.promise

		await runtime.close({ timeoutMs: 300 })

		expect(await aborted.promise).toBe(true)

		await Bun.sleep(200)
	})

	test("keeps the idempotency key held when close aborts a body before its own timeoutMs", async () => {
		const settlePayment = defineJob({
			name: scoped("payments.settle"),
			queue: scoped("jubs.test.shutdown-idem"),
			payload: type({ id: "string" }),
			delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 1 } },
			timeoutMs: 300,
			idempotencyKey: (data) => data.id,
		})

		const live = await freshQueue(settlePayment.queue)
		const paymentId = scoped("pay-2")
		const key = leaseKey(settlePayment.name, paymentId)

		await inspectorConnection.del(key)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [settlePayment.queue],
		})

		const started = Promise.withResolvers<void>()
		const bodies: number[] = []

		const runtime = await jobs.start([
			defineHandler(settlePayment, async (_data, context) => {
				bodies.push(context.attempt)
				started.resolve()

				await Bun.sleep(1_200)

				return { receipt: "late" }
			}),
		])

		const enqueued = await jobs.enqueue(settlePayment, { id: paymentId })

		await started.promise
		await runtime.close({ timeoutMs: 50 })
		await Bun.sleep(500)

		expect(await inspectorConnection.get(key)).toStartWith(RUNNING_PREFIX)

		const stored = await live.getJob(storedId(enqueued))

		expect(stored?.attemptsMade).toBe(0)
		expect(await jobs.dead.list(settlePayment.queue)).toEqual([])

		await Bun.sleep(900)

		expect(await inspectorConnection.get(key)).toBe(JSON.stringify({ result: { receipt: "late" } }))
		expect(bodies).toEqual([1])
	})

	test("delivers a job aborted by close again, spending no attempt and burying nothing", async () => {
		const renderReport = defineJob({
			name: scoped("reports.render"),
			queue: scoped("jubs.test.shutdown-redeliver"),
			payload: type({ id: "string" }),
			delivery: { attempts: 1 },
		})

		const live = await freshQueue(renderReport.queue)

		const paged: string[] = []

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [renderReport.queue],
			hooks: {
				onDead: (event) => {
					paged.push(event.name)
				},
			},
		})

		const started = Promise.withResolvers<void>()

		const runtime = await jobs.start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				throw context.signal.reason
			}),
		])

		const enqueued = await jobs.enqueue(renderReport, { id: scoped("rep-1") })

		await started.promise
		await runtime.close({ timeoutMs: 300 })
		await Bun.sleep(400)

		const stored = await live.getJob(storedId(enqueued))

		expect(stored).toBeDefined()
		expect(stored?.attemptsMade).toBe(0)
		expect(stored?.finishedOn).toBeUndefined()
		expect(await jobs.dead.list(renderReport.queue)).toEqual([])
		expect(paged).toEqual([])
	})
})

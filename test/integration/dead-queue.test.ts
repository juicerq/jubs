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
const leased: string[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

function leaseKey(jobName: string, key: string): string {
	const lease = `${IDEMPOTENCY_KEY_PREFIX}${jobName}:${key}`

	leased.push(lease)

	return lease
}

async function keySettled(lease: string): Promise<boolean> {
	const stored = await inspectorConnection.get(lease)

	return !!stored && !stored.startsWith(RUNNING_PREFIX)
}

async function waitFor(reached: () => boolean | Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 8_000

	while (Date.now() < deadline) {
		if (await reached()) {
			return
		}

		await Bun.sleep(25)
	}

	throw new Error("the executions expected by this test did not arrive in time")
}

async function waitForFinished(queue: Queue, id: string): Promise<Job> {
	const deadline = Date.now() + 4_000

	while (Date.now() < deadline) {
		const job = await queue.getJob(id)

		if (job?.finishedOn) {
			return job
		}

		await Bun.sleep(25)
	}

	throw new Error(`job ${id} on ${queue.name} did not finish in time`)
}

function chargeCardOn(queue: string) {
	return defineJob({
		name: scoped("billing.charge"),
		queue,
		payload: type({ cents: "string.numeric.parse" }),
		delivery: { attempts: 1 },
	})
}

async function freshQueues(queue: string): Promise<{ live: Queue; dead: Queue }> {
	const live = inspect(queue)
	const dead = inspect(`${queue}.dead`)

	await live.obliterate({ force: true })
	await dead.obliterate({ force: true })

	return { live, dead }
}

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))

	if (leased.length) {
		await inspectorConnection.del(...leased)
	}

	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("dead queue over redis", () => {
	test("keeps a job that failed every attempt where no worker consumes it", async () => {
		const chargeCard = chargeCardOn(scoped("jubs.test.dead-keep"))
		const { live, dead } = await freshQueues(chargeCard.queue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [chargeCard.queue],
		})

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card was declined")
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitForFinished(live, storedId(enqueued))

		const waiting = await dead.getWaiting()

		expect(waiting).toHaveLength(1)
		expect(waiting[0]?.name).toBe(chargeCard.name)
		expect(waiting[0]?.data.envelope).toEqual({
			v: 1,
			name: chargeCard.name,
			data: { cents: "500" },
			origin: "direct",
		})
		expect(waiting[0]?.data.error.message).toBe("the card was declined")
		expect(waiting[0]?.data.reason).toBe("attempts_exhausted")

		await Bun.sleep(300)

		expect(await dead.getWaitingCount()).toBe(1)
		expect(await waiting[0]?.getState()).toBe("waiting")

		await runtime.close()
	})

	test("lists a dead job and replays it with the payload it was enqueued with", async () => {
		const chargeCard = chargeCardOn(scoped("jubs.test.dead-replay"))
		const { live } = await freshQueues(chargeCard.queue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [chargeCard.queue],
			definitions: [chargeCard],
		})

		const charged = Promise.withResolvers<unknown>()
		let refusing = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (data) => {
				if (refusing) {
					throw new Error("the card was declined")
				}

				charged.resolve(data)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitForFinished(live, storedId(enqueued))

		const dead = await jobs.dead.list(chargeCard.queue)

		expect(dead).toHaveLength(1)
		expect(dead[0]?.reason).toBe("attempts_exhausted")
		expect(dead[0]?.envelope.data).toEqual({ cents: "500" })

		refusing = false

		const replayed = await jobs.dead.replay(dead[0]?.id ?? "")

		expect(await charged.promise).toEqual({ cents: 500 })
		expect(replayed.id).not.toBe(enqueued.id)
		expect(await jobs.dead.list(chargeCard.queue)).toEqual([])

		await runtime.close()
	})

	test("replays a job whose uniqueness key is still taken", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("jubs.test.dead-unique"),
			payload: type({ cents: "string.numeric.parse" }),
			delivery: {
				attempts: 1,
				unique: { key: (data) => String(data.cents), mode: "keepFirst", ttlMs: 60_000 },
			},
		})

		const { live } = await freshQueues(chargeCard.queue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [chargeCard.queue],
			definitions: [chargeCard],
		})

		const charged = Promise.withResolvers<unknown>()
		let refusing = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (data) => {
				if (refusing) {
					throw new Error("the card was declined")
				}

				charged.resolve(data)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitForFinished(live, storedId(enqueued))

		const [dead] = await jobs.dead.list(chargeCard.queue)

		refusing = false

		const replayed = await jobs.dead.replay(dead?.id ?? "")

		expect(replayed.id).not.toBe(enqueued.id)
		expect(await charged.promise).toEqual({ cents: 500 })
		expect(await jobs.dead.list(chargeCard.queue)).toEqual([])

		await runtime.close()
	})

	test("replays a job buried on its deadline whose body completed its key, and runs the handler", async () => {
		const settlePayment = defineJob({
			name: scoped("billing.replay-forget"),
			queue: scoped("jubs.test.dead-forget"),
			payload: type({ paymentId: "string" }),
			idempotencyKey: (data) => data.paymentId,
			delivery: { attempts: 1 },
			timeoutMs: 50,
		})

		const { live } = await freshQueues(settlePayment.queue)

		const lease = leaseKey(settlePayment.name, "pay-1")
		await inspectorConnection.del(lease)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [settlePayment.queue],
			definitions: [settlePayment],
		})

		const settled: string[] = []

		const runtime = await jobs.start([
			defineHandler(settlePayment, async (data) => {
				settled.push(data.paymentId)

				await Bun.sleep(300)

				return { receipt: "receipt-pay-1" }
			}),
		])

		const enqueued = await jobs.enqueue(settlePayment, { paymentId: "pay-1" })

		await waitForFinished(live, storedId(enqueued))
		await waitFor(() => keySettled(lease))

		const [dead] = await jobs.dead.list(settlePayment.queue)

		expect(dead?.reason).toBe("attempts_exhausted")
		expect(settled).toEqual(["pay-1"])

		await jobs.dead.replay(dead?.id ?? "")

		await waitFor(() => settled.length === 2)
		await waitFor(() => keySettled(lease))

		expect(settled).toEqual(["pay-1", "pay-1"])

		await runtime.close()
	})

	test("refuses to replay a job whose key the body that outlived it still holds", async () => {
		const settlePayment = defineJob({
			name: scoped("billing.replay-held"),
			queue: scoped("jubs.test.dead-held"),
			payload: type({ paymentId: "string" }),
			idempotencyKey: (data) => data.paymentId,
			delivery: { attempts: 1 },
			timeoutMs: 50,
		})

		const { live } = await freshQueues(settlePayment.queue)

		const lease = leaseKey(settlePayment.name, "held-1")
		await inspectorConnection.del(lease)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [settlePayment.queue],
			definitions: [settlePayment],
		})

		const settled: string[] = []

		const runtime = await jobs.start([
			defineHandler(settlePayment, async (data) => {
				settled.push(data.paymentId)

				await Bun.sleep(1_500)

				return { receipt: "receipt-held-1" }
			}),
		])

		const enqueued = await jobs.enqueue(settlePayment, { paymentId: "held-1" })

		await waitForFinished(live, storedId(enqueued))

		expect(await inspectorConnection.get(lease)).toStartWith(RUNNING_PREFIX)

		const [dead] = await jobs.dead.list(settlePayment.queue)
		const failure = await jobs.dead.replay(dead?.id ?? "").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("held by a delivery running right now")
		expect(await jobs.dead.list(settlePayment.queue)).toHaveLength(1)
		expect(await live.getJobCountByTypes("waiting", "delayed", "active")).toBe(0)
		expect(settled).toEqual(["held-1"])

		await waitFor(() => keySettled(lease))

		await runtime.close()
	})

	test("discards a dead job, and refuses an id no dead job answers to", async () => {
		const chargeCard = chargeCardOn(scoped("jubs.test.dead-discard"))
		const { live, dead } = await freshQueues(chargeCard.queue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [chargeCard.queue],
		})

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card was declined")
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await waitForFinished(live, storedId(enqueued))

		const [buried] = await jobs.dead.list(chargeCard.queue)
		const id = buried?.id ?? ""

		await jobs.dead.discard(id)

		expect(await jobs.dead.list(chargeCard.queue)).toEqual([])
		expect(await dead.getWaitingCount()).toBe(0)

		const failure = await jobs.dead.discard(id).catch((error: unknown) => error)

		expect((failure as Error).message).toContain(id)

		await runtime.close()
	})
})

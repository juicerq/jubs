import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { IDEMPOTENCY_KEY_PREFIX } from "@/RedisDriver"
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

async function scrub(queue: Queue): Promise<void> {
	await queue.pause()
	await queue.obliterate({ force: true })
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

afterAll(async () => {
	await Promise.all(opened.map(scrub))

	if (leased.length) {
		await inspectorConnection.del(...leased)
	}

	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("a result schema over redis", () => {
	test("keeps the validated value, so a repeated key replays it instead of the raw one", async () => {
		const settleInvoice = defineJob({
			name: scoped("billing.settle"),
			queue: scoped("jubs.test.result.replay"),
			payload: type({ invoiceId: "string" }),
			result: type({ total: "string.numeric.parse" }),
			idempotencyKey: (data) => data.invoiceId,
		})

		const queue = inspect(settleInvoice.queue)
		await scrub(queue)
		await inspectorConnection.del(leaseKey(settleInvoice.name, "inv-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []

		const runtime = await jobs.start([
			defineHandler(settleInvoice, async (data) => {
				ran.push(data.invoiceId)

				return { total: "500" }
			}),
		])

		const first = await jobs.enqueue(settleInvoice, { invoiceId: "inv-1" })
		await waitFor(async () => !!(await queue.getJob(storedId(first)))?.finishedOn)

		const second = await jobs.enqueue(settleInvoice, { invoiceId: "inv-1" })
		await waitFor(async () => !!(await queue.getJob(storedId(second)))?.finishedOn)

		const settled = await queue.getJob(storedId(first))
		const replayed = await queue.getJob(storedId(second))

		expect(settled).toBeDefined()
		expect(replayed).toBeDefined()
		expect(ran).toEqual(["inv-1"])
		expect(settled?.returnvalue).toEqual({ total: 500 })
		expect(replayed?.returnvalue).toEqual({ total: 500 })

		await runtime.close()
	})
})

import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { type Job, Queue } from "bullmq"
import IORedis from "ioredis"
import {
	createJobs,
	DELIVERY_DEFAULTS,
	defineHandler,
	defineJob,
	type HandlerContext,
	redisDriver,
} from "@/index"
import { liveId } from "../support/JobIds"
import { scoped, storedId } from "./namespace"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const producerConnection = new IORedis(REDIS_URL)
const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
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

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([
		producerConnection.quit(),
		workerConnection.quit(),
		inspectorConnection.quit(),
	])
})

describe("job-core over redis", () => {
	test("a job enqueued by one client runs in a worker started by another", async () => {
		const sendEmail = defineJob({
			name: scoped("email.send"),
			queue: scoped("jubs.test.two-process"),
			payload: type({ to: "string.email", subject: "string" }),
		})

		const queue = inspect(sendEmail.queue)
		await queue.obliterate({ force: true })

		const producer = createJobs({ driver: redisDriver(producerConnection) })
		const consumer = createJobs({ driver: redisDriver(workerConnection) })
		const ran = Promise.withResolvers<{ data: unknown; context: HandlerContext }>()

		const runtime = await consumer.start([
			defineHandler(sendEmail, async (data, context) => {
				ran.resolve({ data, context })
			}),
		])

		const enqueued = await producer.enqueue(sendEmail, {
			to: "ada@example.com",
			subject: "welcome",
		})

		const seen = await ran.promise

		expect(seen.data).toEqual({ to: "ada@example.com", subject: "welcome" })
		expect(seen.context).toMatchObject({
			id: liveId(enqueued),
			attempt: 1,
			maxAttempts: DELIVERY_DEFAULTS.attempts,
			origin: "direct",
		})
		expect(seen.context.signal.aborted).toBe(false)

		const stored = await queue.getJob(storedId(enqueued))

		expect(stored?.name).toBe(sendEmail.name)
		expect(stored?.data).toEqual({
			v: 1,
			name: sendEmail.name,
			data: { to: "ada@example.com", subject: "welcome" },
			origin: "direct",
		})

		await runtime.close()
	})

	test("a transforming schema stores the raw input and hands the handler the output", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("jubs.test.transform"),
			payload: type({ cents: "string.numeric.parse" }),
		})

		const queue = inspect(chargeCard.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran = Promise.withResolvers<unknown>()

		const runtime = await jobs.start([
			defineHandler(chargeCard, async (data) => {
				ran.resolve(data)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await ran.promise).toEqual({ cents: 500 })

		const stored = await queue.getJob(storedId(enqueued))

		expect(stored?.data).toEqual({
			v: 1,
			name: chargeCard.name,
			data: { cents: "500" },
			origin: "direct",
		})

		await runtime.close()
	})

	test("an envelope no handler owns fails on its first attempt", async () => {
		const queueName = scoped("jubs.test.unknown-name")

		const kept = defineJob({
			name: scoped("email.send"),
			queue: queueName,
			payload: type({ to: "string.email" }),
		})

		const retired = defineJob({
			name: scoped("email.retired"),
			queue: queueName,
			payload: type({ to: "string.email" }),
		})

		const queue = inspect(queueName)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const runtime = await jobs.start([defineHandler(kept, async () => {})])
		const enqueued = await jobs.enqueue(retired, { to: "ada@example.com" })
		const finished = await waitForFinished(queue, storedId(enqueued))

		expect(finished.attemptsMade).toBe(1)
		expect(finished.failedReason).toContain(retired.name)

		await runtime.close()
	})

	test("a stored payload that no longer validates fails on its first attempt", async () => {
		const queueName = scoped("jubs.test.invalid-payload")

		const sendEmail = defineJob({
			name: scoped("email.send"),
			queue: queueName,
			payload: type({ to: "string.email" }),
		})

		const queue = inspect(queueName)
		await queue.obliterate({ force: true })

		let ran = false
		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const runtime = await jobs.start([
			defineHandler(sendEmail, async () => {
				ran = true
			}),
		])

		const stale = await queue.add(
			sendEmail.name,
			{ v: 1, name: sendEmail.name, data: { to: "not-an-email" }, origin: "direct" },
			{ attempts: DELIVERY_DEFAULTS.attempts },
		)

		const finished = await waitForFinished(queue, stale.id ?? "")

		expect(finished.attemptsMade).toBe(1)
		expect(finished.failedReason).toContain(sendEmail.name)
		expect(ran).toBe(false)

		await runtime.close()
	})

	test("start refuses a connection that retries requests, before opening a worker", async () => {
		const retrying = new IORedis(REDIS_URL, { maxRetriesPerRequest: 20, lazyConnect: true })

		const guarded = defineJob({
			name: scoped("email.send"),
			queue: scoped("jubs.test.guarded"),
			payload: type({ to: "string.email" }),
		})

		const jobs = createJobs({ driver: redisDriver(retrying) })

		const failure = await jobs
			.start([defineHandler(guarded, async () => {})])
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(Error)
		expect((failure as Error).message).toContain("maxRetriesPerRequest: null")
		expect(retrying.status).toBe("wait")

		retrying.disconnect()
	})
})

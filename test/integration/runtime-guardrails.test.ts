import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import {
	createJobs,
	defineHandler,
	defineJob,
	type JobEvent,
	type JobFailureEvent,
	redisDriver,
} from "@/index"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("runtime-guardrails over redis", () => {
	test("reports every failed attempt, and reports the job dead once", async () => {
		const chargeCard = defineJob({
			name: "billing.charge",
			queue: "juibs.test.hooks",
			payload: type({ cents: "number" }),
			delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 10 } },
		})

		const queue = inspect(chargeCard.queue)
		await queue.obliterate({ force: true })

		const failed: JobFailureEvent[] = []
		const dead: JobFailureEvent[] = []
		const started: JobEvent[] = []
		const buried = Promise.withResolvers<void>()

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			hooks: {
				onStart: (event) => {
					started.push(event)
				},
				onAttemptFailed: (event) => {
					failed.push(event)
				},
				onDead: (event) => {
					dead.push(event)
					buried.resolve()
				},
			},
		})

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card was declined")
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: 500 })

		await buried.promise
		await Bun.sleep(100)

		expect(started.map((event) => event.attempt)).toEqual([1, 2, 3])
		expect(failed.map((event) => event.attempt)).toEqual([1, 2, 3])
		expect(dead).toHaveLength(1)

		expect(dead[0]?.attempt).toBe(3)
		expect(dead[0]?.id).toBe(enqueued.id)
		expect(dead[0]?.queue).toBe(chargeCard.queue)
		expect(dead[0]?.name).toBe("billing.charge")
		expect(dead[0]?.origin).toBe("direct")
		expect(dead[0]?.error.message).toBe("the card was declined")

		await runtime.close()
	})

	test("holds a job with delayMs until its delay has passed", async () => {
		const sendEmail = defineJob({
			name: "email.send",
			queue: "juibs.test.delay",
			payload: type({ to: "string.email" }),
			delivery: { delayMs: 700 },
		})

		const queue = inspect(sendEmail.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran = Promise.withResolvers<number>()
		const enqueuedAt = Date.now()

		const runtime = await jobs.start([
			defineHandler(sendEmail, async () => {
				ran.resolve(Date.now() - enqueuedAt)
			}),
		])

		const enqueued = await jobs.enqueue(sendEmail, { to: "ada@example.com" })
		const stored = await queue.getJob(enqueued.id)

		expect(stored?.opts.delay).toBe(700)
		expect(await ran.promise).toBeGreaterThanOrEqual(600)

		await runtime.close()
	})
})

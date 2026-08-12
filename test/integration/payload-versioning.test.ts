import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { type Job, Queue } from "bullmq"
import IORedis from "ioredis"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { scoped, storedId } from "./namespace"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

const QUEUE = scoped("jubs.test.version-ahead")

const JOB_NAME = scoped("contact.sync")

const contactPayload = type({ email: "string" })

const runningContact = defineJob({
	name: JOB_NAME,
	queue: QUEUE,
	payload: contactPayload,
	delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 10 } },
})

const aheadContact = defineJob({
	name: JOB_NAME,
	queue: QUEUE,
	payload: contactPayload,
	version: 2,
	migrations: { 1: (data) => data },
	delivery: { attempts: 3, backoff: { type: "exponential", delayMs: 10 } },
})

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
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("a payload version ahead of this worker, over redis", () => {
	test("burns one attempt, is not retried, and is kept with reason version_ahead", async () => {
		const live = inspect(QUEUE)
		const dead = inspect(`${QUEUE}.dead`)

		await live.obliterate({ force: true })
		await dead.obliterate({ force: true })

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			definitions: [runningContact],
			deadQueues: [QUEUE],
		})

		let ran = false

		const runtime = await jobs.start([
			defineHandler(runningContact, async () => {
				ran = true
			}),
		])

		const enqueued = await jobs.enqueue(aheadContact, { email: "ada@example.com" })

		const finished = await waitForFinished(live, storedId(enqueued))

		expect(finished.attemptsMade).toBe(1)
		expect(await finished.getState()).toBe("failed")
		expect(ran).toBe(false)

		await Bun.sleep(300)

		const settled = await live.getJob(storedId(enqueued))

		expect(settled?.attemptsMade).toBe(1)
		expect(await settled?.getState()).toBe("failed")

		const buried = await jobs.dead.list(QUEUE)

		expect(buried).toHaveLength(1)
		expect(buried[0]?.reason).toBe("version_ahead")
		expect(buried[0]?.envelope.v).toBe(2)

		await runtime.close()
	})
})

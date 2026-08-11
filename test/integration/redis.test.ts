import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { Queue } from "bullmq"
import IORedis from "ioredis"

const connection = new IORedis(process.env.REDIS_URL || "redis://127.0.0.1:6379", {
	maxRetriesPerRequest: null,
})

const queue = new Queue("juibs.smoke", { connection })

beforeAll(async () => {
	await queue.waitUntilReady()
	await queue.obliterate({ force: true })
})

afterAll(async () => {
	await queue.obliterate({ force: true })
	await queue.close()
	await connection.quit()
})

describe("redis service", () => {
	test("a queue reaches redis", async () => {
		await queue.add("placeholder", { ok: true })

		expect(await queue.getWaitingCount()).toBe(1)
	})
})

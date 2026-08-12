import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { scoped } from "./namespace"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

async function waitFor(reached: () => boolean): Promise<void> {
	const deadline = Date.now() + 4_000

	while (Date.now() < deadline) {
		if (reached()) {
			return
		}

		await Bun.sleep(25)
	}

	throw new Error("the executions expected by this test did not arrive in time")
}

afterAll(async () => {
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("uniqueness over redis", () => {
	test("ten keepLast enqueues inside the window run once, carrying the tenth payload", async () => {
		const rebuildIndex = defineJob({
			name: scoped("search.rebuild"),
			queue: scoped("jubs.test.unique.keep-last"),
			payload: type({ entityId: "string", version: "number" }),
			delivery: {
				unique: { key: (data) => `search:${data.entityId}`, mode: "keepLast", ttlMs: 400 },
			},
		})

		const queue = inspect(rebuildIndex.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: number[] = []

		const runtime = await jobs.start([
			defineHandler(rebuildIndex, async (data) => {
				ran.push(data.version)
			}),
		])

		for (let version = 1; version <= 10; version += 1) {
			await jobs.enqueue(rebuildIndex, { entityId: "42", version })
		}

		await waitFor(() => ran.length > 0)
		await Bun.sleep(600)

		expect(ran).toEqual([10])

		await runtime.close()
	})

	test("three noOverlap enqueues run twice, the second carrying the latest payload", async () => {
		const syncAccount = defineJob({
			name: scoped("account.sync"),
			queue: scoped("jubs.test.unique.no-overlap"),
			payload: type({ accountId: "string", version: "number" }),
			delivery: { unique: { key: (data) => `sync:${data.accountId}`, mode: "noOverlap" } },
		})

		const queue = inspect(syncAccount.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: number[] = []
		const holding = Promise.withResolvers<void>()

		const runtime = await jobs.start([
			defineHandler(syncAccount, async (data) => {
				ran.push(data.version)

				if (data.version === 1) {
					await holding.promise
				}
			}),
		])

		await jobs.enqueue(syncAccount, { accountId: "42", version: 1 })
		await waitFor(() => ran.length === 1)

		await jobs.enqueue(syncAccount, { accountId: "42", version: 2 })
		await jobs.enqueue(syncAccount, { accountId: "42", version: 3 })

		holding.resolve()

		await waitFor(() => ran.length === 2)
		await Bun.sleep(300)

		expect(ran).toEqual([1, 3])

		await runtime.close()
	})

	test("without unique, the same payload enqueued twice becomes two jobs", async () => {
		const sendEmail = defineJob({
			name: scoped("email.send"),
			queue: scoped("jubs.test.unique.absent"),
			payload: type({ to: "string.email" }),
		})

		const queue = inspect(sendEmail.queue)
		await queue.obliterate({ force: true })

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []

		const runtime = await jobs.start([
			defineHandler(sendEmail, async (data) => {
				ran.push(data.to)
			}),
		])

		const first = await jobs.enqueue(sendEmail, { to: "ada@example.com" })
		const second = await jobs.enqueue(sendEmail, { to: "ada@example.com" })

		await waitFor(() => ran.length === 2)

		expect(first.id).not.toBe(second.id)
		expect(ran).toEqual(["ada@example.com", "ada@example.com"])

		await runtime.close()
	})
})

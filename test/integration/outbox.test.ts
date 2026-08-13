import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import {
	createJobs,
	defineHandler,
	defineJob,
	type Envelope,
	type Origin,
	type Outbox,
	redisDriver,
} from "@/index"
import { scoped } from "./namespace"
import { REDIS_URL } from "./redis"

const producerConnection = new IORedis(REDIS_URL)
const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

interface OutboxRow {
	readonly id: string
	readonly envelope: Envelope
	delivered: boolean
}

/**
 * An outbox that keeps its rows in memory. The table is the caller's in
 * production and a real one proves nothing more here: what this suite proves is
 * what Redis does with a row delivered twice, and `marking` is how a relay is
 * killed between the delivery and the mark.
 */
interface MemoryOutbox extends Outbox {
	readonly rows: OutboxRow[]
	marking: boolean
}

function memoryOutbox(): MemoryOutbox {
	const rows: OutboxRow[] = []

	const outbox: MemoryOutbox = {
		rows,
		marking: true,

		async save(envelopes) {
			for (const envelope of envelopes) {
				rows.push({ id: String(rows.length + 1), envelope, delivered: false })
			}
		},

		async claim(limit) {
			return rows
				.filter((row) => !row.delivered)
				.slice(0, limit)
				.map((row) => ({ id: row.id, envelope: row.envelope }))
		},

		async markDelivered(ids) {
			if (!outbox.marking) {
				return
			}

			for (const row of rows) {
				if (ids.includes(row.id)) {
					row.delivered = true
				}
			}
		},
	}

	return outbox
}

async function waitFor(reached: () => boolean | Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 4_000

	while (Date.now() < deadline) {
		if (await reached()) {
			return
		}

		await Bun.sleep(25)
	}

	throw new Error("the outbox suite waited too long for what it expected")
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

describe("the outbox relay over redis", () => {
	test("re-delivers a row its relay never marked, and the job runs once", async () => {
		const settleOrder = defineJob({
			name: scoped("orders.settle"),
			queue: scoped("jubs.test.outbox"),
			payload: type({ orderId: "string" }),
		})

		const queue = inspect(settleOrder.queue)
		await queue.obliterate({ force: true })

		const outbox = memoryOutbox()
		const producer = createJobs({
			driver: redisDriver(producerConnection),
			outbox,
			definitions: [settleOrder],
		})
		const consumer = createJobs({
			driver: redisDriver(workerConnection),
			definitions: [settleOrder],
		})
		const ran: Origin[] = []

		const runtime = await consumer.start([
			defineHandler(settleOrder, async (_data, context) => {
				ran.push(context.origin)
			}),
		])

		await producer.atomic(
			async () => {
				await producer.enqueue(settleOrder, { orderId: "order-1" })
			},
			{ tx: { name: "the caller's transaction" } },
		)

		expect(outbox.rows).toHaveLength(1)
		expect(await queue.getJobCounts("waiting")).toMatchObject({ waiting: 0 })

		outbox.marking = false

		await producer.startRelay().close()

		expect(outbox.rows[0]?.delivered).toBe(false)

		await waitFor(() => ran.length > 0)

		const delivered = await queue.getJob("outbox-1")

		expect(delivered?.data).toMatchObject({ name: settleOrder.name, origin: "relay" })

		outbox.marking = true

		await producer.startRelay().close()

		expect(outbox.rows[0]?.delivered).toBe(true)

		await Bun.sleep(500)

		expect(ran).toEqual(["relay"])
		expect(await queue.getJobCounts("waiting", "active", "completed")).toMatchObject({
			waiting: 0,
			active: 0,
			completed: 1,
		})

		await runtime.close()
	})

	test("runs a row a second time once redis has swept the job its id named", async () => {
		const settleOrder = defineJob({
			name: scoped("orders.settle-swept"),
			queue: scoped("jubs.test.outbox.swept"),
			payload: type({ orderId: "string" }),
		})

		const queue = inspect(settleOrder.queue)
		await queue.obliterate({ force: true })

		const outbox = memoryOutbox()
		const producer = createJobs({
			driver: redisDriver(producerConnection),
			outbox,
			definitions: [settleOrder],
		})
		const consumer = createJobs({
			driver: redisDriver(workerConnection),
			definitions: [settleOrder],
		})
		const ran: string[] = []

		const runtime = await consumer.start([
			defineHandler(settleOrder, async (data) => {
				ran.push(data.orderId)
			}),
		])

		await producer.atomic(
			async () => {
				await producer.enqueue(settleOrder, { orderId: "order-1" })
			},
			{ tx: {} },
		)

		outbox.marking = false

		await producer.startRelay().close()
		await waitFor(async () => !!(await queue.getJob("outbox-1"))?.finishedOn)

		const stored = await queue.getJob("outbox-1")

		await stored?.remove()

		expect(await queue.getJob("outbox-1")).toBeUndefined()

		await producer.startRelay().close()
		await waitFor(() => ran.length > 1)

		expect(ran).toEqual(["order-1", "order-1"])
		expect(outbox.rows[0]?.delivered).toBe(false)

		await runtime.close()
	})
})

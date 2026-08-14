import { afterAll, describe, expect, test } from "bun:test"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { type } from "arktype"
import { Queue } from "bullmq"
import IORedis from "ioredis"
import { HELD_RETRY_FLOOR_MS, HELD_RETRY_MS, IDEMPOTENCY_MAX_RESULT_BYTES } from "@/Idempotency"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { IDEMPOTENCY_KEY_PREFIX, RUNNING_PREFIX } from "@/RedisDriver"
import { waitFor } from "../support/Wait"
import { scoped, storedId } from "./namespace"
import { REDIS_URL } from "./redis"

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

const ROOT = join(import.meta.dir, "..", "..")

async function writeCrashWorker(job: { name: string; queue: string }, startedKey: string) {
	const source = `
import IORedis from ${JSON.stringify(Bun.resolveSync("ioredis", ROOT))}
import { type } from ${JSON.stringify(Bun.resolveSync("arktype", ROOT))}
import { createJobs, defineHandler, defineJob, redisDriver } from ${JSON.stringify(join(ROOT, "src", "index.ts"))}

const connection = new IORedis(${JSON.stringify(REDIS_URL)}, { maxRetriesPerRequest: null })

const settlePayment = defineJob({
	name: ${JSON.stringify(job.name)},
	queue: ${JSON.stringify(job.queue)},
	payload: type({ paymentId: "string" }),
	idempotencyKey: (data) => data.paymentId,
})

const jobs = createJobs({ driver: redisDriver(connection) })

await jobs.start([
	defineHandler(settlePayment, async () => {
		await connection.incr(${JSON.stringify(startedKey)})

		await new Promise(() => {})
	}),
])
`

	const path = join(tmpdir(), `jubs-idem-crash-worker-${Bun.randomUUIDv7()}.mjs`)

	await Bun.write(path, source)

	return path
}

async function counter(key: string): Promise<number> {
	return Number((await inspectorConnection.get(key)) ?? 0)
}

const DELAYED_SCORE_PER_MS = 0x1000

async function requestedDelaysOf(queue: string, jobId: string, samples: number): Promise<number[]> {
	const delayedKey = `bull:${queue}:delayed`
	const requested: number[] = []
	let scheduledFor = 0

	await waitFor(async () => {
		const score = await inspectorConnection.zscore(delayedKey, jobId)

		if (score) {
			const wakeAt = Math.floor(Number(score) / DELAYED_SCORE_PER_MS)

			if (wakeAt !== scheduledFor) {
				scheduledFor = wakeAt
				requested.push(wakeAt - Date.now())
			}
		}

		return requested.length >= samples
	})

	return requested
}

afterAll(async () => {
	await Promise.all(opened.map(scrub))

	if (leased.length) {
		await inspectorConnection.del(...leased)
	}

	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("idempotency over redis", () => {
	test("a delivery whose key is complete skips the handler and returns the kept result", async () => {
		const chargeInvoice = defineJob({
			name: scoped("billing.charge"),
			queue: scoped("jubs.test.idem.complete"),
			payload: type({ invoiceId: "string" }),
			idempotencyKey: (data) => data.invoiceId,
		})

		const queue = inspect(chargeInvoice.queue)
		await scrub(queue)
		await inspectorConnection.del(leaseKey(chargeInvoice.name, "inv-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []

		const runtime = await jobs.start([
			defineHandler(chargeInvoice, async (data) => {
				ran.push(data.invoiceId)

				return { receipt: `receipt-${data.invoiceId}` }
			}),
		])

		const first = await jobs.enqueue(chargeInvoice, { invoiceId: "inv-1" })
		await waitFor(async () => !!(await queue.getJob(storedId(first)))?.finishedOn)

		const second = await jobs.enqueue(chargeInvoice, { invoiceId: "inv-1" })
		await waitFor(async () => !!(await queue.getJob(storedId(second)))?.finishedOn)

		const kept = await queue.getJob(storedId(second))

		expect(ran).toEqual(["inv-1"])
		expect(kept?.returnvalue).toEqual({ receipt: "receipt-inv-1" })

		await runtime.close()
	})

	test("a delivery whose lease is held is rescheduled, and spends no attempt on the wait", async () => {
		const syncLedger = defineJob({
			name: scoped("ledger.sync"),
			queue: scoped("jubs.test.idem.held"),
			payload: type({ ledgerId: "string", version: "number" }),
			idempotencyKey: (data) => data.ledgerId,
		})

		const queue = inspect(syncLedger.queue)
		await scrub(queue)
		await inspectorConnection.del(leaseKey(syncLedger.name, "led-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: number[] = []
		const holding = Promise.withResolvers<void>()

		const runtime = await jobs.start(
			[
				defineHandler(syncLedger, async (data) => {
					ran.push(data.version)

					await holding.promise
				}),
			],
			{ queues: { [syncLedger.queue]: { concurrency: 2 } } },
		)

		const first = await jobs.enqueue(syncLedger, { ledgerId: "led-1", version: 1 })
		await waitFor(() => ran.length === 1)

		await inspectorConnection.pexpire(leaseKey(syncLedger.name, "led-1"), 1_500)

		const second = await jobs.enqueue(syncLedger, { ledgerId: "led-1", version: 2 })
		const enqueued = await queue.getJob(storedId(second))

		await waitFor(
			async () => (await (await queue.getJob(storedId(second)))?.getState()) === "delayed",
		)

		const waiting = await queue.getJob(storedId(second))

		expect(enqueued?.attemptsMade).toBe(0)
		expect(waiting?.attemptsMade).toBe(0)
		expect(waiting?.failedReason).toBeUndefined()

		holding.resolve()

		await waitFor(async () => !!(await queue.getJob(storedId(first)))?.finishedOn)
		await waitFor(
			async () => (await (await queue.getJob(storedId(second)))?.getState()) === "completed",
		)

		expect(ran).toEqual([1])

		await runtime.close()
	})

	test("a duplicate delivered behind a running job settles on the retry cadence, not the lease", async () => {
		const chargeCard = defineJob({
			name: scoped("billing.dupe"),
			queue: scoped("jubs.test.idem.dupe"),
			payload: type({ orderId: "string" }),
			idempotencyKey: (data) => data.orderId,
		})

		const queue = inspect(chargeCard.queue)
		await scrub(queue)
		await inspectorConnection.del(leaseKey(chargeCard.name, "dup-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []
		const holding = Promise.withResolvers<void>()

		const runtime = await jobs.start(
			[
				defineHandler(chargeCard, async (data) => {
					ran.push(data.orderId)

					await holding.promise

					return { receipt: "receipt-dup-1" }
				}),
			],
			{ queues: { [chargeCard.queue]: { concurrency: 2 } } },
		)

		const first = await jobs.enqueue(chargeCard, { orderId: "dup-1" })
		await waitFor(() => ran.length === 1)

		const second = await jobs.enqueue(chargeCard, { orderId: "dup-1" })
		const enqueuedBehind = Date.now()

		await Bun.sleep(250)
		holding.resolve()

		await waitFor(
			async () => (await (await queue.getJob(storedId(second)))?.getState()) === "completed",
		)

		const kept = await queue.getJob(storedId(second))

		expect(ran).toEqual(["dup-1"])
		expect(kept?.returnvalue).toEqual({ receipt: "receipt-dup-1" })
		expect(Date.now() - enqueuedBehind).toBeLessThan(2_500)

		await waitFor(async () => !!(await queue.getJob(storedId(first)))?.finishedOn)

		await runtime.close()
	}, 20_000)

	test("a duplicate held behind a running job climbs from the floor to the ceiling", async () => {
		const settleOrder = defineJob({
			name: scoped("billing.ladder"),
			queue: scoped("jubs.test.idem.ladder"),
			payload: type({ orderId: "string" }),
			idempotencyKey: (data) => data.orderId,
		})

		const queue = inspect(settleOrder.queue)
		await scrub(queue)
		await inspectorConnection.del(leaseKey(settleOrder.name, "lad-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []
		const holding = Promise.withResolvers<void>()

		const runtime = await jobs.start(
			[
				defineHandler(settleOrder, async (data) => {
					ran.push(data.orderId)

					await holding.promise
				}),
			],
			{ queues: { [settleOrder.queue]: { concurrency: 2 } } },
		)

		const first = await jobs.enqueue(settleOrder, { orderId: "lad-1" })
		await waitFor(() => ran.length === 1)

		const second = await jobs.enqueue(settleOrder, { orderId: "lad-1" })
		const climbing = await requestedDelaysOf(settleOrder.queue, storedId(second), 5)

		expect(climbing.at(0)).toBeLessThan(HELD_RETRY_FLOOR_MS * 2.5)
		expect(climbing.some((delayMs) => delayMs > HELD_RETRY_MS * 0.75)).toBe(true)

		holding.resolve()

		await waitFor(async () => !!(await queue.getJob(storedId(first)))?.finishedOn)
		await waitFor(
			async () => (await (await queue.getJob(storedId(second)))?.getState()) === "completed",
		)

		expect(ran).toEqual(["lad-1"])

		await runtime.close()
	}, 30_000)

	test("a delivery whose lease expired runs the handler again", async () => {
		const shipOrder = defineJob({
			name: scoped("orders.ship"),
			queue: scoped("jubs.test.idem.expired"),
			payload: type({ orderId: "string" }),
			idempotencyKey: (data) => data.orderId,
		})

		const queue = inspect(shipOrder.queue)
		await scrub(queue)

		const stuck = leaseKey(shipOrder.name, "ord-1")
		await inspectorConnection.set(stuck, `${RUNNING_PREFIX}dead-worker`, "PX", 2_000)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []

		const runtime = await jobs.start([
			defineHandler(shipOrder, async (data) => {
				ran.push(data.orderId)
			}),
		])

		const delivery = await jobs.enqueue(shipOrder, { orderId: "ord-1" })

		await waitFor(
			async () => (await (await queue.getJob(storedId(delivery)))?.getState()) === "delayed",
		)

		expect(ran).toEqual([])

		await waitFor(async () => !!(await queue.getJob(storedId(delivery)))?.finishedOn)

		expect(ran).toEqual(["ord-1"])

		await runtime.close()
	})

	test("a worker killed under the handler makes the job run again, and finish exactly once", async () => {
		const settlePayment = defineJob({
			name: scoped("payments.settle"),
			queue: scoped("jubs.test.idem.crash"),
			payload: type({ paymentId: "string" }),
			idempotencyKey: (data) => data.paymentId,
		})

		const startedKey = scoped("jubs:test:idem:crash:started")
		const finishedKey = scoped("jubs:test:idem:crash:finished")

		const queue = inspect(settlePayment.queue)
		await scrub(queue)
		await inspectorConnection.del(
			leaseKey(settlePayment.name, "pay-1"),
			startedKey,
			finishedKey,
			`bull:${settlePayment.queue}:stalled-check`,
		)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const delivery = await jobs.enqueue(settlePayment, { paymentId: "pay-1" })

		const workerPath = await writeCrashWorker(settlePayment, startedKey)
		const child = Bun.spawn(["bun", workerPath], { stdout: "inherit", stderr: "inherit" })

		await waitFor(async () => (await counter(startedKey)) === 1)

		child.kill("SIGKILL")
		await child.exited
		await Bun.file(workerPath).delete()

		await inspectorConnection.del(
			`bull:${settlePayment.queue}:${storedId(delivery)}:lock`,
			`bull:${settlePayment.queue}:stalled-check`,
		)
		await inspectorConnection.pexpire(leaseKey(settlePayment.name, "pay-1"), 1)

		const settleOnce = defineHandler(settlePayment, async () => {
			await inspectorConnection.incr(startedKey)
			await inspectorConnection.incr(finishedKey)
		})

		const runtime = await jobs.start([settleOnce])

		await waitFor(
			async () =>
				(await inspectorConnection.sismember(
					`bull:${settlePayment.queue}:stalled`,
					storedId(delivery),
				)) === 1,
		)

		await inspectorConnection.del(`bull:${settlePayment.queue}:stalled-check`)

		const sweeper = await jobs.start([settleOnce])

		await waitFor(
			async () => (await (await queue.getJob(storedId(delivery)))?.getState()) === "completed",
		)
		await Bun.sleep(500)

		expect(await counter(startedKey)).toBe(2)
		expect(await counter(finishedKey)).toBe(1)

		await sweeper.close()
		await runtime.close()
		await inspectorConnection.del(startedKey, finishedKey)
	}, 60_000)

	test("forgetting a key a delivery holds is refused, and the key stays held", async () => {
		const settleInvoice = defineJob({
			name: scoped("billing.forget"),
			queue: scoped("jubs.test.idem.forget"),
			payload: type({ invoiceId: "string" }),
			idempotencyKey: (data) => data.invoiceId,
		})

		const queue = inspect(settleInvoice.queue)
		await scrub(queue)

		const lease = leaseKey(settleInvoice.name, "for-1")
		await inspectorConnection.del(lease)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []
		const holding = Promise.withResolvers<void>()

		const runtime = await jobs.start([
			defineHandler(settleInvoice, async (data) => {
				ran.push(data.invoiceId)

				await holding.promise

				return { receipt: "receipt-for-1" }
			}),
		])

		const delivery = await jobs.enqueue(settleInvoice, { invoiceId: "for-1" })
		await waitFor(() => ran.length === 1)

		const refused = await jobs.idempotency.forget(settleInvoice, { invoiceId: "for-1" })

		expect(refused.outcome).toBe("running")
		expect(await inspectorConnection.get(lease)).toStartWith(RUNNING_PREFIX)

		holding.resolve()
		await waitFor(async () => !!(await queue.getJob(storedId(delivery)))?.finishedOn)

		const forgotten = await jobs.idempotency.forget(settleInvoice, { invoiceId: "for-1" })
		const again = await jobs.idempotency.forget(settleInvoice, { invoiceId: "for-1" })

		expect(forgotten.outcome).toBe("forgotten")
		expect(again.outcome).toBe("not_found")
		expect(await inspectorConnection.get(lease)).toBeNull()

		await runtime.close()
	})

	test("a result above the byte limit keeps the marker alone, so the second delivery returns nothing", async () => {
		const renderReport = defineJob({
			name: scoped("reports.render"),
			queue: scoped("jubs.test.idem.oversized"),
			payload: type({ reportId: "string" }),
			idempotencyKey: (data) => data.reportId,
		})

		const queue = inspect(renderReport.queue)
		await scrub(queue)
		await inspectorConnection.del(leaseKey(renderReport.name, "rep-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []

		const runtime = await jobs.start([
			defineHandler(renderReport, async (data) => {
				ran.push(data.reportId)

				return { page: "p".repeat(IDEMPOTENCY_MAX_RESULT_BYTES + 1) }
			}),
		])

		const first = await jobs.enqueue(renderReport, { reportId: "rep-1" })
		await waitFor(async () => !!(await queue.getJob(storedId(first)))?.finishedOn)

		const second = await jobs.enqueue(renderReport, { reportId: "rep-1" })
		await waitFor(async () => !!(await queue.getJob(storedId(second)))?.finishedOn)

		const kept = await queue.getJob(storedId(second))

		expect(ran).toEqual(["rep-1"])
		expect(kept?.returnvalue ?? undefined).toBeUndefined()

		await runtime.close()
	})
})

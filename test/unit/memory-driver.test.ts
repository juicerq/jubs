import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import {
	createJobs,
	DELIVERY_DEFAULTS,
	type Delivery,
	defineHandler,
	defineJob,
	every,
} from "@/index"
import { memoryDriver } from "@/testing/index"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "string.numeric.parse" }),
})

const sendEmail = defineJob({
	name: "email.send",
	queue: "mail",
	payload: type({ to: "string.email" }),
})

describe("memoryDriver", () => {
	test("runs a job end to end through createJobs, with the payload validated on both sides", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const charged: number[] = []

		await jobs.start([
			defineHandler(chargeCard, async (data) => {
				charged.push(data.cents)
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })

		expect(charged).toEqual([])
		expect(driver.enqueued(chargeCard)).toEqual([{ cents: "500" }])

		expect(await driver.drain()).toBe(1)
		expect(charged).toEqual([500])
	})

	test("gives the handler the job id, attempt 1 and the delivery's attempts", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const contexts: { id: string; attempt: number; maxAttempts: number }[] = []

		await jobs.start([
			defineHandler(chargeCard, async (_data, context) => {
				contexts.push({
					id: context.id,
					attempt: context.attempt,
					maxAttempts: context.maxAttempts,
				})
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "1" })
		await driver.runNext()

		expect(contexts).toEqual([{ id: "1", attempt: 1, maxAttempts: DELIVERY_DEFAULTS.attempts }])
	})

	test("rejects an invalid payload before it reaches the driver", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.enqueue(sendEmail, { to: "not-an-email" }).catch(() => {})

		expect(driver.enqueued(sendEmail)).toEqual([])
	})

	test("enqueued reports only the payloads of the definition it is given", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.enqueue(chargeCard, { cents: "500" })
		await jobs.enqueue(sendEmail, { to: "ada@example.com" })
		await jobs.enqueue(chargeCard, { cents: "700" })

		expect(driver.enqueued(chargeCard)).toEqual([{ cents: "500" }, { cents: "700" }])
		expect(driver.enqueued(sendEmail)).toEqual([{ to: "ada@example.com" }])
	})

	test("runNext fails when nothing is pending", async () => {
		const failure = await memoryDriver()
			.runNext()
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("no pending job")
	})

	test("runNext fails when no consumer is open on the job's queue", async () => {
		const driver = memoryDriver()

		await createJobs({ driver }).enqueue(chargeCard, { cents: "500" })

		const failure = await driver.runNext().catch((error: unknown) => error)

		expect((failure as Error).message).toContain('no consumer on queue "billing"')
		expect((failure as Error).message).toContain("jobs.start(handlers)")
	})

	test("enqueue fails on a delivery option it does not simulate", async () => {
		const driver = memoryDriver()

		const failure = await driver
			.enqueue({
				queue: "billing",
				envelope: { v: 1, name: "billing.charge", data: { cents: "500" }, origin: "direct" },
				delivery: { ...DELIVERY_DEFAULTS, delayMs: 5_000 } as Delivery,
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe(
			'juibs: memoryDriver does not simulate "delayMs"; test that behaviour against redisDriver',
		)
		expect(driver.enqueued(chargeCard)).toEqual([])
	})

	test("enqueue fails on a unique policy, which only a real queue enforces", async () => {
		const driver = memoryDriver()

		const failure = await driver
			.enqueue({
				queue: "billing",
				envelope: { v: 1, name: "billing.charge", data: { cents: "500" }, origin: "direct" },
				delivery: {
					...DELIVERY_DEFAULTS,
					unique: { mode: "keepFirst", key: "charge:500" },
				} as Delivery,
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe(
			'juibs: memoryDriver does not simulate "unique"; test that behaviour against redisDriver',
		)
		expect(driver.enqueued(chargeCard)).toEqual([])
	})

	test("drops a payload key holding undefined, the way Redis does", async () => {
		const driver = memoryDriver()
		const loose = defineJob({
			name: "email.loose",
			queue: "mail",
			payload: type({ subject: "string | undefined" }),
		})

		await createJobs({ driver }).enqueue(loose, { subject: undefined })

		expect(driver.enqueued(loose).map((payload) => Object.keys(payload))).toEqual([[]])
	})

	test("propagates a handler failure out of drain", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card was declined")
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })

		const failure = await driver.drain().catch((error: unknown) => error)

		expect((failure as Error).message).toBe("the card was declined")
	})

	test("drain runs the jobs a running handler enqueues", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const sent: string[] = []

		await jobs.start([
			defineHandler(chargeCard, async () => {
				await jobs.enqueue(sendEmail, { to: "ada@example.com" })
			}),
			defineHandler(sendEmail, async (data) => {
				sent.push(data.to)
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await driver.drain()).toBe(2)
		expect(sent).toEqual(["ada@example.com"])
	})

	test("closing the runtime leaves the queue with no consumer", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		const runtime = await jobs.start([defineHandler(chargeCard, async () => {})])

		await jobs.enqueue(chargeCard, { cents: "500" })
		await runtime.close()

		const failure = await driver.runNext().catch((error: unknown) => error)

		expect((failure as Error).message).toContain('no consumer on queue "billing"')
	})

	test("drain returns 0 when nothing is pending", async () => {
		expect(await memoryDriver().drain()).toBe(0)
	})

	test("start fails on a declared schedule, before any consumer opens", async () => {
		const driver = memoryDriver()

		const sweepSessions = defineJob({
			name: "session.sweep",
			queue: "maintenance",
			payload: type({ olderThanDays: "number" }),
			schedule: every("5 minutes", { data: { olderThanDays: 30 } }),
		})

		const jobs = createJobs({ driver, definitions: [sweepSessions] })

		await jobs.enqueue(sweepSessions, { olderThanDays: 30 })

		const failure = await jobs
			.start([defineHandler(sweepSessions, async () => {})])
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe(
			'juibs: memoryDriver does not simulate "schedule"; test that behaviour against redisDriver',
		)

		const orphan = await driver.runNext().catch((error: unknown) => error)

		expect((orphan as Error).message).toContain('no consumer on queue "maintenance"')
	})

	test("start accepts a queue that declares no schedule", async () => {
		const driver = memoryDriver()

		await createJobs({ driver, definitions: [chargeCard] }).start([
			defineHandler(chargeCard, async () => {}),
		])

		await createJobs({ driver }).enqueue(chargeCard, { cents: "500" })

		expect(await driver.drain()).toBe(1)
	})
})

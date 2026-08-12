import { describe, expect, jest, test } from "bun:test"
import { type } from "arktype"
import {
	IDEMPOTENCY_MAX_RESULT_BYTES,
	IDEMPOTENCY_RENEW_MS,
	idempotencyKeyFor,
} from "@/Idempotency"
import {
	createJobs,
	defineHandler,
	defineJob,
	type JobDelivery,
	type JobFailureEvent,
	LeaseHeldError,
} from "@/index"
import { memoryDriver } from "@/testing/index"
import { recordingDriver } from "./support/RecordingDriver"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ orderId: "string", cents: "number" }),
	idempotencyKey: (data) => data.orderId,
})

function keyOf(orderId: string): string {
	const key = idempotencyKeyFor(chargeCard, { orderId, cents: 500 })

	if (!key) {
		throw new Error("the charge definition declares no idempotency key")
	}

	return key
}

function deliveryOf(orderId: string): JobDelivery {
	return {
		id: "42",
		attempt: 1,
		maxAttempts: 5,
		envelope: {
			v: 1,
			name: "billing.charge",
			data: { orderId, cents: 500 },
			origin: "direct",
		},
	}
}

async function settleMicrotasks(): Promise<void> {
	for (let turn = 0; turn < 20; turn += 1) {
		await Promise.resolve()
	}
}

describe("idempotency", () => {
	test("a delivery whose key is complete skips the handler and returns the kept result", async () => {
		const driver = recordingDriver()
		let ran = 0

		await createJobs({ driver }).start([
			defineHandler(chargeCard, async () => {
				ran += 1

				return { receipt: "fresh" }
			}),
		])

		driver.keepResult(keyOf("order-1"), { result: { receipt: "kept" } })

		const result = await driver.deliver("billing", deliveryOf("order-1"))

		expect(result).toEqual({ receipt: "kept" })
		expect(ran).toBe(0)
	})

	test("a delivery whose key kept only the completion marker gives back nothing", async () => {
		const driver = recordingDriver()
		let ran = 0

		await createJobs({ driver }).start([
			defineHandler(chargeCard, async () => {
				ran += 1

				return { receipt: "fresh" }
			}),
		])

		driver.keepResult(keyOf("order-1"), {})

		const result = await driver.deliver("billing", deliveryOf("order-1"))

		expect(result).toBeUndefined()
		expect(ran).toBe(0)
	})

	test("a result over the size limit keeps the completion marker alone", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([
			defineHandler(chargeCard, async () => "x".repeat(IDEMPOTENCY_MAX_RESULT_BYTES)),
		])

		await driver.deliver("billing", deliveryOf("order-1"))

		expect(driver.completed).toEqual([{ key: keyOf("order-1"), kept: {} }])
	})

	test("a result JSON cannot serialise keeps the completion marker alone", async () => {
		const driver = recordingDriver()
		const circular: { self?: unknown } = {}
		circular.self = circular

		await createJobs({ driver }).start([defineHandler(chargeCard, async () => circular)])

		await driver.deliver("billing", deliveryOf("order-1"))

		expect(driver.completed).toEqual([{ key: keyOf("order-1"), kept: {} }])
		expect(driver.released).toEqual([])
	})

	test("a result carrying a BigInt keeps the completion marker alone", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([defineHandler(chargeCard, async () => ({ cents: 500n }))])

		await driver.deliver("billing", deliveryOf("order-1"))

		expect(driver.completed).toEqual([{ key: keyOf("order-1"), kept: {} }])
		expect(driver.released).toEqual([])
	})

	test("a result under the size limit is kept whole", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([
			defineHandler(chargeCard, async () => ({ receipt: "fresh" })),
		])

		await driver.deliver("billing", deliveryOf("order-1"))

		expect(driver.completed).toEqual([
			{ key: keyOf("order-1"), kept: { result: { receipt: "fresh" } } },
		])
	})

	test("a handler slower than the renew interval keeps its lease renewed, and stops on the last one", async () => {
		const driver = recordingDriver()
		const running = Promise.withResolvers<void>()

		await createJobs({ driver }).start([defineHandler(chargeCard, () => running.promise)])

		jest.useFakeTimers()

		const delivery = driver.deliver("billing", deliveryOf("order-1"))

		await settleMicrotasks()
		jest.advanceTimersByTime(IDEMPOTENCY_RENEW_MS * 3)
		await settleMicrotasks()

		expect(driver.renewed).toEqual([keyOf("order-1"), keyOf("order-1"), keyOf("order-1")])

		running.resolve()
		await delivery

		jest.advanceTimersByTime(IDEMPOTENCY_RENEW_MS * 3)
		await settleMicrotasks()

		expect(driver.renewed).toHaveLength(3)

		jest.useRealTimers()
	})

	test("a handler that throws releases the lease and keeps no result", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card processor is down")
			}),
		])

		const failure = await driver
			.deliver("billing", deliveryOf("order-1"))
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe("the card processor is down")
		expect(driver.released).toEqual([keyOf("order-1")])
		expect(driver.completed).toEqual([])
	})

	test("a delivery that meets a held lease reports no failure and buries nothing", async () => {
		const driver = recordingDriver()
		const failed: JobFailureEvent[] = []
		const dead: JobFailureEvent[] = []
		let ran = 0

		await createJobs({
			driver,
			deadQueues: ["billing"],
			definitions: [chargeCard],
			hooks: {
				onAttemptFailed: (event) => {
					failed.push(event)
				},
				onDead: (event) => {
					dead.push(event)
				},
			},
		}).start([
			defineHandler(chargeCard, async () => {
				ran += 1
			}),
		])

		driver.holdLease(keyOf("order-1"), 250)

		const signal = await driver
			.deliver("billing", { ...deliveryOf("order-1"), attempt: 5, maxAttempts: 5 })
			.catch((error: unknown) => error)

		expect(signal).toBeInstanceOf(LeaseHeldError)
		expect((signal as LeaseHeldError).delayMs).toBe(250)
		expect(ran).toBe(0)
		expect(failed).toEqual([])
		expect(dead).toEqual([])
	})

	test("a delivery skipped by a complete key succeeds", async () => {
		const driver = recordingDriver()
		const succeeded: string[] = []

		await createJobs({
			driver,
			hooks: {
				onSuccess: (event) => {
					succeeded.push(event.name)
				},
			},
		}).start([defineHandler(chargeCard, async () => ({ receipt: "fresh" }))])

		driver.keepResult(keyOf("order-1"), { result: { receipt: "kept" } })

		await driver.deliver("billing", deliveryOf("order-1"))

		expect(succeeded).toEqual(["billing.charge"])
	})

	test("a definition that declares no idempotency key never reaches the store", async () => {
		const driver = recordingDriver()

		const sendEmail = defineJob({
			name: "email.send",
			queue: "mail",
			payload: type({ to: "string.email" }),
		})

		await createJobs({ driver }).start([defineHandler(sendEmail, async () => {})])

		await driver.deliver("mail", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: { v: 1, name: "email.send", data: { to: "ada@example.com" }, origin: "direct" },
		})

		expect(driver.acquired).toEqual([])
		expect(driver.completed).toEqual([])
		expect(driver.released).toEqual([])
	})

	test("a repeated delivery of the same key runs the handler once", async () => {
		const driver = memoryDriver()
		const charged: number[] = []

		const jobs = createJobs({ driver })

		await jobs.start([
			defineHandler(chargeCard, async (data) => {
				charged.push(data.cents)
			}),
		])

		await jobs.enqueue(chargeCard, { orderId: "order-1", cents: 500 })
		await jobs.enqueue(chargeCard, { orderId: "order-1", cents: 500 })

		expect(await driver.drain()).toBe(2)
		expect(charged).toEqual([500])
	})
})

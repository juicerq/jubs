import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { createJobs, DELIVERY_DEFAULTS, type Delivery, defineJob } from "@/index"
import { memoryDriver } from "@/testing/index"
import { recordingDriver } from "./support/RecordingDriver"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "string.numeric.parse" }),
	delivery: { attempts: 2, delayMs: 5_000 },
})

describe("delivery", () => {
	test("a plain policy overrides only the keys it names", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).enqueue(chargeCard, { cents: "500" })

		expect(driver.enqueued[0]?.delivery).toEqual({
			...DELIVERY_DEFAULTS,
			attempts: 2,
			delayMs: 5_000,
		})
	})

	test("a function policy reads the validated payload and the defaults it may spread", async () => {
		const driver = recordingDriver()
		const seen: { data: unknown; options: Delivery }[] = []

		const charge = defineJob({
			name: "billing.charge",
			queue: "billing",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: (input) => {
				seen.push(input)

				return { priority: input.data.cents > 100 ? 1 : input.options.priority }
			},
		})

		await createJobs({ driver }).enqueue(charge, { cents: "500" })

		expect(seen).toEqual([{ data: { cents: 500 }, options: DELIVERY_DEFAULTS }])
		expect(driver.enqueued[0]?.delivery).toEqual({ ...DELIVERY_DEFAULTS, priority: 1 })
	})

	test("drops a key the policy left undefined, so a driver never sees it", async () => {
		const driver = memoryDriver()

		const charge = defineJob({
			name: "billing.charge",
			queue: "billing",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: () => ({ delayMs: undefined }),
		})

		await createJobs({ driver }).enqueue(charge, { cents: "500" })

		expect(driver.enqueued(charge)).toEqual([{ cents: "500" }])
	})

	test("a definition with no policy enqueues with the defaults", async () => {
		const driver = recordingDriver()

		const charge = defineJob({
			name: "billing.charge",
			queue: "billing",
			payload: type({ cents: "string.numeric.parse" }),
		})

		await createJobs({ driver }).enqueue(charge, { cents: "500" })

		expect(driver.enqueued[0]?.delivery).toEqual(DELIVERY_DEFAULTS)
	})
})

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

	test("applies the unique key to the validated payload", async () => {
		const driver = recordingDriver()

		const charge = defineJob({
			name: "billing.charge",
			queue: "billing",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { unique: { key: (data) => `charge:${data.cents}`, mode: "keepFirst" } },
		})

		await createJobs({ driver }).enqueue(charge, { cents: "500" })

		expect(driver.enqueued[0]?.delivery).toEqual({
			...DELIVERY_DEFAULTS,
			unique: { key: "charge:500", mode: "keepFirst" },
		})
	})

	test("refuses keepLast without a window", async () => {
		const rebuild = defineJob({
			name: "search.rebuild",
			queue: "search",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { unique: { key: () => "search", mode: "keepLast" } },
		})

		const failure = await createJobs({ driver: recordingDriver() })
			.enqueue(rebuild, { cents: "500" })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("keepLast")
		expect((failure as Error).message).toContain("ttlMs")
	})

	test("holds a keepLast job for the window, keeping the longer of window and delay", async () => {
		const driver = recordingDriver()

		const shortDelay = defineJob({
			name: "search.rebuild",
			queue: "search",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: {
				delayMs: 1_000,
				unique: { key: () => "search", mode: "keepLast", ttlMs: 4_000 },
			},
		})

		const longDelay = defineJob({
			name: "search.reindex",
			queue: "search",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: {
				delayMs: 9_000,
				unique: { key: () => "search", mode: "keepLast", ttlMs: 4_000 },
			},
		})

		const jobs = createJobs({ driver })

		await jobs.enqueue(shortDelay, { cents: "500" })
		await jobs.enqueue(longDelay, { cents: "500" })

		expect(driver.enqueued[0]?.delivery.delayMs).toBe(4_000)
		expect(driver.enqueued[1]?.delivery.delayMs).toBe(9_000)
	})

	test("drops the window on noOverlap, which runs one job at a time instead", async () => {
		const driver = recordingDriver()

		const sync = defineJob({
			name: "account.sync",
			queue: "sync",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { unique: { key: () => "account", mode: "noOverlap", ttlMs: 4_000 } },
		})

		await createJobs({ driver }).enqueue(sync, { cents: "500" })

		expect(driver.enqueued[0]?.delivery.unique).toEqual({ key: "account", mode: "noOverlap" })
	})

	test("keeps keepFirst without a window when the policy names none", async () => {
		const driver = recordingDriver()

		const welcome = defineJob({
			name: "email.welcome",
			queue: "mail",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { unique: { key: () => "ada", mode: "keepFirst" } },
		})

		await createJobs({ driver }).enqueue(welcome, { cents: "500" })

		expect(Object.keys(driver.enqueued[0]?.delivery.unique ?? {})).toEqual(["mode", "key"])
	})

	test("a policy naming no unique leaves the key off the resolved delivery", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).enqueue(chargeCard, { cents: "500" })

		expect(Object.keys(driver.enqueued[0]?.delivery ?? {})).not.toContain("unique")
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

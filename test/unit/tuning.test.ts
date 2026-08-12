import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { createJobs, defineHandler, defineJob, type JobHandler } from "@/index"
import { DEFAULT_CONCURRENCY } from "@/Runtime"
import { memoryDriver } from "@/testing/index"
import { recordingDriver } from "./support/RecordingDriver"

const sendEmail = defineJob({
	name: "email.send",
	queue: "mail",
	payload: type({ to: "string.email" }),
})

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "number" }),
})

describe("per-queue tuning", () => {
	test("consumes every queue with the default concurrency and no limiter", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([defineHandler(sendEmail, async () => {})])

		expect(driver.consumed[0]?.concurrency).toBe(DEFAULT_CONCURRENCY)
		expect(driver.consumed[0]?.limiter).toBeUndefined()
	})

	test("passes each queue its own concurrency and limiter", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start(
			[defineHandler(sendEmail, async () => {}), defineHandler(chargeCard, async () => {})],
			{ queues: { mail: { concurrency: 25, limiter: { max: 10, durationMs: 1_000 } } } },
		)

		const mail = driver.consumed.find((request) => request.queue === "mail")
		const billing = driver.consumed.find((request) => request.queue === "billing")

		expect(mail?.concurrency).toBe(25)
		expect(mail?.limiter).toEqual({ max: 10, durationMs: 1_000 })
		expect(billing?.concurrency).toBe(DEFAULT_CONCURRENCY)
		expect(billing?.limiter).toBeUndefined()
	})

	test("refuses tuning aimed at a queue no handler starts, naming it", async () => {
		const driver = recordingDriver()
		const handlers: JobHandler[] = [defineHandler(sendEmail, async () => {})]

		const failure = await createJobs({ driver })
			.start(handlers, { queues: { reprots: { concurrency: 2 } } })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("reprots")
		expect((failure as Error).message).toContain("mail")
		expect(driver.consuming).toEqual([])
	})

	test("refuses tuning whose queue name is only known at runtime", async () => {
		const driver = recordingDriver()
		const queue: string = ["rep", "rots"].join("")

		const failure = await createJobs({ driver })
			.start([defineHandler(sendEmail, async () => {})], {
				queues: { [queue]: { concurrency: 2 } },
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("reprots")
		expect(driver.consuming).toEqual([])
	})

	test("memoryDriver accepts concurrency and refuses a limiter", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([defineHandler(chargeCard, async () => {})], {
			queues: { billing: { concurrency: 4 } },
		})

		const failure = await jobs
			.start([defineHandler(sendEmail, async () => {})], {
				queues: { mail: { limiter: { max: 10, durationMs: 1_000 } } },
			})
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe(
			'jubs: memoryDriver does not simulate "limiter"; test that behaviour against redisDriver',
		)
	})

	test("closes the consumers it already opened when a later queue refuses to consume", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.enqueue(chargeCard, { cents: 500 })

		const failure = await jobs
			.start(
				[defineHandler(chargeCard, async () => {}), defineHandler(sendEmail, async () => {})],
				{
					queues: { mail: { limiter: { max: 10, durationMs: 1_000 } } },
				},
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain('does not simulate "limiter"')

		const orphaned = await driver.runNext().catch((error: unknown) => error)

		expect((orphaned as Error).message).toContain('no consumer on queue "billing"')
	})
})

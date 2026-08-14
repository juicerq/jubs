import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { idempotencyKeyFor } from "@/Idempotency"
import {
	createJobs,
	defineHandler,
	defineJob,
	type JobDefinition,
	type JobFailureEvent,
} from "@/index"
import { memoryDriver } from "@/testing/index"
import { liveId } from "../support/JobIds"
import { errorOf } from "../support/Failures"
import { recordingDriver } from "./support/RecordingDriver"

const renderReport = defineJob({
	name: "reports.render",
	queue: "reports",
	payload: type({ reportId: "string" }),
	result: type({ url: "string.url" }),
})

function keyOf(definition: JobDefinition, data: unknown): string {
	const key = idempotencyKeyFor(definition, data)

	if (!key) {
		throw new Error(`the ${definition.name} definition declares no idempotency key`)
	}

	return key
}

describe("a definition that declares a result schema", () => {
	test("runs a handler whose return value the schema accepts", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([
			defineHandler(renderReport, async (data) => ({
				url: `https://reports.example.com/${data.reportId}`,
			})),
		])

		await jobs.enqueue(renderReport, { reportId: "rep-1" })

		expect(await driver.drain()).toBe(1)
	})

	test("fails the attempt unrecoverably when the schema rejects the return value", async () => {
		const driver = memoryDriver()
		const dead: JobFailureEvent[] = []
		let ran = 0

		const jobs = createJobs({
			driver,
			deadQueues: [renderReport.queue],
			hooks: {
				onDead: (event) => {
					dead.push(event)
				},
			},
		})

		await jobs.start([
			defineHandler(renderReport, async () => {
				ran += 1

				return { url: "not a url at all" }
			}),
		])

		const enqueued = await jobs.enqueue(renderReport, { reportId: "rep-1" })
		const failure = await driver.runNext().catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("result")
		expect(ran).toBe(1)

		const buried = await driver.dead.list(renderReport.queue)

		expect(buried.map(({ entry }) => entry.reason)).toEqual(["unrecoverable"])
		expect(dead).toHaveLength(1)

		const stored = await jobs.get(liveId(enqueued))

		expect(stored?.state).toBe("failed")
		expect(stored?.attempts).toBe(1)
		expect(stored?.maxAttempts).toBe(5)
	})

	test("stores and replays the value the schema returns, not the one the handler did", async () => {
		const settleInvoice = defineJob({
			name: "billing.settle",
			queue: "billing",
			payload: type({ invoiceId: "string" }),
			result: type({ total: "string.numeric.parse" }),
			idempotencyKey: (data) => data.invoiceId,
		})

		const driver = recordingDriver()

		await createJobs({ driver }).start([
			defineHandler(settleInvoice, async () => ({ total: "500" })),
		])

		const delivered = await driver.deliver("billing", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: {
				v: 1,
				name: settleInvoice.name,
				data: { invoiceId: "inv-1" },
				origin: "direct",
			},
		})

		const key = keyOf(settleInvoice, { invoiceId: "inv-1" })

		expect(delivered).toEqual({ total: 500 })
		expect(driver.completed).toEqual([{ key, kept: { result: { total: 500 } } }])
	})

	test("keeps the JSON projection of the validated value, the way redis keeps it", async () => {
		const stampBackup = defineJob({
			name: "backups.stamp",
			queue: "backups",
			payload: type({ backupId: "string" }),
			result: type({ at: "string.date.iso.parse" }),
			idempotencyKey: (data) => data.backupId,
		})

		const taken = "2024-05-01T10:00:00.000Z"
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([defineHandler(stampBackup, async () => ({ at: taken }))])

		await jobs.enqueue(stampBackup, { backupId: "bak-1" })

		expect(await driver.drain()).toBe(1)

		const key = keyOf(stampBackup, { backupId: "bak-1" })
		const replayed = await driver.idempotency.acquire({ key, leaseMs: 1_000 })

		expect(replayed).toEqual({ state: "complete", kept: { result: { at: taken } } })
	})

	test("refuses a handler whose return value the schema does not describe", () => {
		const handler = defineHandler(
			renderReport,
			// @ts-expect-error the result schema declares `url` as a string
			async () => ({ url: 42 }),
		)

		expect(handler.definition).toBe(renderReport)
	})
})

describe("a definition that declares no result schema", () => {
	const sweepReports = defineJob({
		name: "reports.sweep",
		queue: "reports.sweep",
		payload: type({ olderThanDays: "number" }),
	})

	test("takes whatever the handler returns and validates nothing", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([
			defineHandler(sweepReports, async () => ({ swept: 3, at: new Date(), of: [1, "two"] })),
		])

		await jobs.enqueue(sweepReports, { olderThanDays: 30 })

		expect(await driver.drain()).toBe(1)
	})
})

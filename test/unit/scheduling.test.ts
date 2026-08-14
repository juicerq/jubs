import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { DEFAULT_PAYLOAD_VERSION } from "@/Definition"
import {
	createJobs,
	DELIVERY_DEFAULTS,
	dailyAt,
	defineHandler,
	defineJob,
	every,
	type JobDefinition,
	type ScheduleUpsert,
} from "@/index"
import { errorOf } from "../support/Failures"
import { recordingDriver } from "./support/RecordingDriver"

const sweepSessions = defineJob({
	name: "session.sweep",
	queue: "maintenance",
	payload: type({ olderThanDays: "number" }),
	schedule: every("5 minutes", { data: { olderThanDays: 30 } }),
})

const billDaily = defineJob({
	name: "billing.daily",
	queue: "billing",
	payload: type({ cents: "string.numeric.parse" }),
	schedule: dailyAt("09:30", { data: { cents: "500" } }),
})

async function declaredBy(
	definition: JobDefinition,
	timezone?: string,
): Promise<readonly ScheduleUpsert[]> {
	const driver = recordingDriver()
	const definitions = [definition]

	const jobs = createJobs(timezone ? { driver, definitions, timezone } : { driver, definitions })

	await jobs.start([defineHandler(definition, async () => {})])

	return driver.reconciled[0]?.declared ?? []
}

describe("start reconciles the declared schedules", () => {
	test("declares the scheduled job of a started queue", async () => {
		const driver = recordingDriver()

		await createJobs({ driver, definitions: [sweepSessions] }).start([
			defineHandler(sweepSessions, async () => {}),
		])

		expect(driver.reconciled).toEqual([
			{
				queue: "maintenance",
				declared: [
					{
						recurrence: { everyMs: 300_000 },
						envelope: {
							v: DEFAULT_PAYLOAD_VERSION,
							name: "session.sweep",
							data: { olderThanDays: 30 },
							origin: "schedule",
						},
						delivery: { ...DELIVERY_DEFAULTS, attempts: 1 },
					},
				],
			},
		])
	})

	test("keeps the schedule's data as written, so a schema that transforms still validates it", async () => {
		const [scheduled] = await declaredBy(billDaily)

		expect(scheduled?.envelope.data).toEqual({ cents: "500" })
	})

	test("declares the template on the payload version its definition names", async () => {
		const sweepTwice = defineJob({
			name: "session.sweep",
			queue: "maintenance",
			payload: type({ olderThanDays: "number" }),
			version: 2,
			migrations: { 1: (data) => data },
			schedule: every("5 minutes", { data: { olderThanDays: 30 } }),
		})

		const [scheduled] = await declaredBy(sweepTwice)

		expect(scheduled?.envelope.v).toBe(2)
	})

	test("a started queue that declares no schedule still reconciles, with nothing declared", async () => {
		const driver = recordingDriver()

		const sendEmail = defineJob({
			name: "email.send",
			queue: "mail",
			payload: type({ to: "string.email" }),
		})

		await createJobs({ driver, definitions: [sendEmail] }).start([
			defineHandler(sendEmail, async () => {}),
		])

		expect(driver.reconciled).toEqual([{ queue: "mail", declared: [] }])
	})

	test("reads the schedules off the started handlers, not off the client's definitions", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([defineHandler(sweepSessions, async () => {})])

		expect(driver.reconciled[0]?.declared.map((scheduled) => scheduled.envelope.name)).toEqual([
			"session.sweep",
		])
	})

	test("reconciles before opening a consumer, so a refused reconcile leaves none open", async () => {
		const driver = recordingDriver()

		driver.refuseSchedules()

		const failure = await createJobs({ driver })
			.start([defineHandler(sweepSessions, async () => {})])
			.catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("refuse the schedules")
		expect(driver.consuming).toEqual([])
	})
})

describe("the time zone of a scheduled job", () => {
	test("is UTC when neither the schedule nor the client names one", async () => {
		const [scheduled] = await declaredBy(billDaily)

		expect(scheduled?.timezone).toBe("UTC")
	})

	test("is the client's when the schedule names none", async () => {
		const [scheduled] = await declaredBy(billDaily, "America/Sao_Paulo")

		expect(scheduled?.timezone).toBe("America/Sao_Paulo")
	})

	test("is the schedule's, which beats the client's", async () => {
		const closeBooks = defineJob({
			name: "billing.close",
			queue: "billing",
			payload: type({ ledger: "string" }),
			schedule: dailyAt("23:00", { data: { ledger: "main" }, timezone: "Europe/Lisbon" }),
		})

		const [scheduled] = await declaredBy(closeBooks, "America/Sao_Paulo")

		expect(scheduled?.timezone).toBe("Europe/Lisbon")
	})

	test("is absent on an interval recurrence, which no time zone shifts", async () => {
		const [scheduled] = await declaredBy(sweepSessions, "America/Sao_Paulo")

		expect(scheduled?.timezone).toBeUndefined()
	})

	test("is refused by createJobs when it is not an IANA name", () => {
		expect(() => createJobs({ driver: recordingDriver(), timezone: "GMT-3" })).toThrow("GMT-3")
	})
})

describe("start boot checks on a schedule", () => {
	test("refuses a schedule whose data the payload schema rejects", async () => {
		const driver = recordingDriver()

		const nightlyReport = defineJob({
			name: "report.nightly",
			queue: "reports",
			payload: type({ range: "string" }),
			schedule: dailyAt("02:00"),
		})

		const failure = await createJobs({ driver, definitions: [nightlyReport] })
			.start([defineHandler(nightlyReport, async () => {})])
			.catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("report.nightly")
		expect(errorOf(failure).message).toContain("`data`")
		expect(driver.consuming).toEqual([])
		expect(driver.reconciled).toEqual([])
	})

	test("refuses a scheduled job whose delivery delays the enqueue", async () => {
		const driver = recordingDriver()

		const digestReport = defineJob({
			name: "report.digest",
			queue: "reports",
			payload: type({ range: "string" }),
			delivery: { delayMs: 5_000 },
			schedule: dailyAt("02:00", { data: { range: "day" } }),
		})

		const failure = await createJobs({ driver, definitions: [digestReport] })
			.start([defineHandler(digestReport, async () => {})])
			.catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("report.digest")
		expect(errorOf(failure).message).toContain("`delayMs`")
		expect(driver.consuming).toEqual([])
		expect(driver.reconciled).toEqual([])
	})
})

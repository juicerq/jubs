import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { dailyAt, defineHandler, defineJob } from "@/index"

const emailPayload = type({ to: "string.email", subject: "string" })

const sendEmail = defineJob({ name: "email.send", queue: "mail", payload: emailPayload })

describe("defineJob", () => {
	test("returns a plain data object carrying name, queue and payload", () => {
		expect(Object.getPrototypeOf(sendEmail)).toBe(Object.prototype)
		expect(sendEmail.name).toBe("email.send")
		expect(sendEmail.queue).toBe("mail")
		expect(sendEmail.payload).toBe(emailPayload)
	})

	test("leaves the schedule key off a definition nobody schedules", () => {
		expect(Object.keys(sendEmail)).toEqual(["name", "queue", "payload"])
	})

	test("carries the schedule and the data every occurrence runs with", () => {
		const digest = defineJob({
			name: "email.digest",
			queue: "mail",
			payload: emailPayload,
			schedule: dailyAt("07:00", {
				data: { to: "ada@example.com", subject: "digest" },
				timezone: "America/Sao_Paulo",
			}),
		})

		expect(digest.schedule).toEqual({
			recurrence: { pattern: "0 7 * * *" },
			data: { to: "ada@example.com", subject: "digest" },
			timezone: "America/Sao_Paulo",
		})
	})

	test("takes a schedule that names no data, whatever the payload schema is", () => {
		const sweep = defineJob({
			name: "email.sweep",
			queue: "mail",
			payload: emailPayload,
			schedule: dailyAt("07:00", { timezone: "UTC" }),
		})

		expect(sweep.schedule).toEqual({ recurrence: { pattern: "0 7 * * *" }, timezone: "UTC" })
	})

	test("refuses a schedule beside awaits, because a tick fills no slot", () => {
		expect(() =>
			defineJob({
				name: "email.recap",
				queue: "mail",
				payload: emailPayload,
				schedule: dailyAt("07:00", { data: { to: "ada@example.com", subject: "recap" } }),
				awaits: { sent: sendEmail },
			}),
		).toThrow("declares `schedule` and `awaits`")
	})

	test("leaves the version and migration keys off a definition that names no version", () => {
		expect(Object.keys(sendEmail)).not.toContain("version")
		expect(Object.keys(sendEmail)).not.toContain("migrations")
	})

	test("carries the version and the migrations that reach it", () => {
		const step = (data: unknown) => data

		const versioned = defineJob({
			name: "email.versioned",
			queue: "mail",
			payload: emailPayload,
			version: 2,
			migrations: { 1: step },
		})

		expect(versioned.version).toBe(2)
		expect(versioned.migrations?.[1]).toBe(step)
	})

	test("refuses a version that is not a whole number of 1 or more", () => {
		const refused = [0, -1, 1.5]

		for (const version of refused) {
			expect(() =>
				defineJob({ name: "email.send", queue: "mail", payload: emailPayload, version }),
			).toThrow("version")
		}
	})

	test("refuses a migration from a version the definition never runs through", () => {
		expect(() =>
			defineJob({
				name: "email.send",
				queue: "mail",
				payload: emailPayload,
				version: 2,
				migrations: { 2: (data) => data },
			}),
		).toThrow("2")
	})

	test("refuses a migration on a definition that names no version", () => {
		expect(() =>
			defineJob({
				name: "email.send",
				queue: "mail",
				payload: emailPayload,
				migrations: { 1: (data) => data },
			}),
		).toThrow("email.send")
	})

	test("takes a version whose earlier steps are missing, so a first version needs no identity step", () => {
		const versioned = defineJob({
			name: "email.send",
			queue: "mail",
			payload: emailPayload,
			version: 3,
			migrations: { 2: (data) => data },
		})

		expect(versioned.version).toBe(3)
	})

	test("rejects schedule data the payload schema does not accept", () => {
		defineJob({
			name: "email.digest",
			queue: "mail",
			payload: emailPayload,
			// @ts-expect-error the payload schema declares no `body` field
			schedule: dailyAt("07:00", { data: { to: "ada@example.com", body: "digest" } }),
		})
	})

	test("refuses a timeoutMs that is not a positive number", () => {
		expect(() =>
			defineJob({ name: "email.send", queue: "mail", payload: emailPayload, timeoutMs: 0 }),
		).toThrow("email.send")

		expect(() =>
			defineJob({ name: "email.send", queue: "mail", payload: emailPayload, timeoutMs: -5 }),
		).toThrow("timeoutMs")

		expect(() =>
			defineJob({
				name: "email.send",
				queue: "mail",
				payload: emailPayload,
				timeoutMs: Number.NaN,
			}),
		).toThrow("timeoutMs")
	})

	test("keeps a timeoutMs it accepts", () => {
		const timed = defineJob({
			name: "email.send",
			queue: "mail",
			payload: emailPayload,
			timeoutMs: 30_000,
		})

		expect(timed.timeoutMs).toBe(30_000)
	})
})

describe("defineHandler", () => {
	test("binds a handler to its definition and infers data from the payload schema", async () => {
		const seen: string[] = []

		const handler = defineHandler(sendEmail, async (data) => {
			seen.push(data.subject)
		})

		expect(handler.definition).toBe(sendEmail)

		await handler.run(
			{ to: "ada@example.com", subject: "welcome" },
			{
				id: "1",
				attempt: 1,
				maxAttempts: 5,
				origin: "direct",
				children: {},
				signal: new AbortController().signal,
			},
		)

		expect(seen).toEqual(["welcome"])
	})

	test("rejects a field the payload schema does not declare", () => {
		const handler = defineHandler(sendEmail, async (data) => {
			// @ts-expect-error the payload schema declares no `body` field
			return data.body
		})

		expect(handler.definition).toBe(sendEmail)
	})
})

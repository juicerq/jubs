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

	test("rejects schedule data the payload schema does not accept", () => {
		defineJob({
			name: "email.digest",
			queue: "mail",
			payload: emailPayload,
			// @ts-expect-error the payload schema declares no `body` field
			schedule: dailyAt("07:00", { data: { to: "ada@example.com", body: "digest" } }),
		})
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
			{ id: "1", attempt: 1, maxAttempts: 5, origin: "direct" },
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

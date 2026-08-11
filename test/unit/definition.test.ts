import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { defineHandler, defineJob } from "@/index"

const emailPayload = type({ to: "string.email", subject: "string" })

const sendEmail = defineJob({ name: "email.send", queue: "mail", payload: emailPayload })

describe("defineJob", () => {
	test("returns a plain data object carrying name, queue and payload", () => {
		expect(Object.getPrototypeOf(sendEmail)).toBe(Object.prototype)
		expect(sendEmail.name).toBe("email.send")
		expect(sendEmail.queue).toBe("mail")
		expect(sendEmail.payload).toBe(emailPayload)
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

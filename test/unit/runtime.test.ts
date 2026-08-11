import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { createJobs, defineHandler, defineJob, type Envelope, type HandlerContext } from "@/index"
import { recordingDriver } from "./support/RecordingDriver"

const sendEmail = defineJob({
	name: "email.send",
	queue: "mail",
	payload: type({ to: "string.email", subject: "string" }),
})

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "number" }),
})

function envelopeFor(name: string, data: unknown): Envelope {
	return { v: 1, name, data, origin: "direct" }
}

describe("start", () => {
	test("opens one consumer per queue in use and dispatches by envelope name", async () => {
		const driver = recordingDriver()
		const seen: string[] = []

		const runtime = await createJobs({ driver }).start([
			defineHandler(sendEmail, async (data) => {
				seen.push(`mail:${data.subject}`)
			}),
			defineHandler(chargeCard, async (data) => {
				seen.push(`billing:${data.cents}`)
			}),
		])

		expect(driver.consuming.toSorted()).toEqual(["billing", "mail"])

		await driver.deliver("mail", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: envelopeFor("email.send", { to: "ada@example.com", subject: "welcome" }),
		})

		await driver.deliver("billing", {
			id: "43",
			attempt: 1,
			maxAttempts: 5,
			envelope: envelopeFor("billing.charge", { cents: 500 }),
		})

		expect(seen).toEqual(["mail:welcome", "billing:500"])

		await runtime.close()

		expect(driver.consuming).toEqual([])
	})

	test("gives the handler a 1-based attempt, the job id, maxAttempts and the origin", async () => {
		const driver = recordingDriver()
		const contexts: HandlerContext[] = []

		await createJobs({ driver }).start([
			defineHandler(sendEmail, async (_data, context) => {
				contexts.push(context)
			}),
		])

		await driver.deliver("mail", {
			id: "42",
			attempt: 3,
			maxAttempts: 5,
			envelope: envelopeFor("email.send", { to: "ada@example.com", subject: "welcome" }),
		})

		expect(contexts).toEqual([{ id: "42", attempt: 3, maxAttempts: 5, origin: "direct" }])
	})

	test("fails unrecoverably when no handler owns the envelope name", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([defineHandler(sendEmail, async () => {})])

		const failure = await driver
			.deliver("mail", {
				id: "42",
				attempt: 1,
				maxAttempts: 5,
				envelope: envelopeFor("email.retired", {}),
			})
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect((failure as Error).message).toContain("email.retired")
	})

	test("fails unrecoverably when the stored payload no longer validates", async () => {
		const driver = recordingDriver()
		let ran = false

		await createJobs({ driver }).start([
			defineHandler(sendEmail, async () => {
				ran = true
			}),
		])

		const failure = await driver
			.deliver("mail", {
				id: "42",
				attempt: 1,
				maxAttempts: 5,
				envelope: envelopeFor("email.send", { to: "not-an-email", subject: "welcome" }),
			})
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect(ran).toBe(false)
	})

	test("fails unrecoverably when the stored value is not an envelope", async () => {
		const driver = recordingDriver()

		await createJobs({ driver }).start([defineHandler(sendEmail, async () => {})])

		const failure = await driver
			.deliver("mail", { id: "42", attempt: 1, maxAttempts: 5, envelope: { nope: true } })
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(UnrecoverableError)
	})
})

import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { createJobs, DELIVERY_DEFAULTS, defineJob, PayloadError } from "@/index"
import { recordingDriver } from "./support/RecordingDriver"

const sendEmail = defineJob({
	name: "email.send",
	queue: "mail",
	payload: type({ to: "string.email", subject: "string" }),
})

describe("enqueue", () => {
	test("wraps the payload in an envelope and adds it to the definition's queue", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const enqueued = await jobs.enqueue(sendEmail, {
			to: "ada@example.com",
			subject: "welcome",
		})

		expect(enqueued.id).toBe("mail:1")
		expect(driver.enqueued).toEqual([
			{
				queue: "mail",
				envelope: {
					v: 1,
					name: "email.send",
					data: { to: "ada@example.com", subject: "welcome" },
					origin: "direct",
				},
				delivery: DELIVERY_DEFAULTS,
			},
		])
	})

	test("rejects a payload the schema does not describe", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		// @ts-expect-error `subject` is a string in the payload schema
		await jobs.enqueue(sendEmail, { to: "ada@example.com", subject: 1 }).catch(() => {})

		expect(driver.enqueued).toEqual([])
	})

	test("rejects an invalid payload before the driver is contacted", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(sendEmail, { to: "not-an-email", subject: "welcome" })
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(PayloadError)
		expect((failure as PayloadError).message).toContain("email.send")
		expect(driver.enqueued).toEqual([])
	})
})

describe("createJobs", () => {
	test("two clients on different drivers keep their jobs apart", async () => {
		const first = recordingDriver()
		const second = recordingDriver()

		await createJobs({ driver: first }).enqueue(sendEmail, {
			to: "ada@example.com",
			subject: "first",
		})

		expect(first.enqueued).toHaveLength(1)
		expect(second.enqueued).toHaveLength(0)
	})
})

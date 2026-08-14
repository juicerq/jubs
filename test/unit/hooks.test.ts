import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import {
	createJobs,
	defineHandler,
	defineJob,
	type Envelope,
	type JobEvent,
	type JobFailureEvent,
	type JobHooks,
} from "@/index"
import { recordingDriver } from "./support/RecordingDriver"

const sendEmail = defineJob({
	name: "email.send",
	queue: "mail",
	payload: type({ to: "string.email" }),
})

function envelopeFor(name: string, data: unknown): Envelope {
	return { v: 1, name, data, origin: "direct" }
}

function recordHooks(fired: string[], events: (JobEvent | JobFailureEvent)[]): JobHooks {
	return {
		onStart: (event) => {
			fired.push("onStart")
			events.push(event)
		},
		onSuccess: (event) => {
			fired.push("onSuccess")
			events.push(event)
		},
		onAttemptFailed: (event) => {
			fired.push("onAttemptFailed")
			events.push(event)
		},
		onDead: (event) => {
			fired.push("onDead")
			events.push(event)
		},
	}
}

function failureOf(event: JobEvent | JobFailureEvent | undefined): JobFailureEvent {
	if (event && "error" in event) {
		return event
	}

	throw new Error(`jubs: ${JSON.stringify(event)} is not a failure event, so it carries no error`)
}

describe("hooks", () => {
	test("fires onStart then onSuccess, naming the job, its queue, id, attempt and origin", async () => {
		const driver = recordingDriver()
		const fired: string[] = []
		const events: JobEvent[] = []

		await createJobs({ driver, hooks: recordHooks(fired, events) }).start([
			defineHandler(sendEmail, async () => {}),
		])

		await driver.deliver("mail", {
			id: "42",
			attempt: 2,
			maxAttempts: 5,
			envelope: envelopeFor("email.send", { to: "ada@example.com" }),
		})

		expect(fired).toEqual(["onStart", "onSuccess"])
		expect(events[0]).toEqual({
			name: "email.send",
			queue: "mail",
			id: "mail:42",
			attempt: 2,
			origin: "direct",
		})
	})

	test("fires onAttemptFailed with the serialised error while attempts remain", async () => {
		const driver = recordingDriver()
		const fired: string[] = []
		const events: (JobEvent | JobFailureEvent)[] = []

		await createJobs({ driver, hooks: recordHooks(fired, events) }).start([
			defineHandler(sendEmail, async () => {
				throw new TypeError("the mailer refused the address")
			}),
		])

		await driver
			.deliver("mail", {
				id: "42",
				attempt: 2,
				maxAttempts: 5,
				envelope: envelopeFor("email.send", { to: "ada@example.com" }),
			})
			.catch(() => {})

		expect(fired).toEqual(["onStart", "onAttemptFailed"])

		const failure = failureOf(events.at(-1))

		expect(failure.name).toBe("email.send")
		expect(failure.attempt).toBe(2)
		expect(failure.error.name).toBe("TypeError")
		expect(failure.error.message).toBe("the mailer refused the address")
	})

	test("fires onDead once, on the attempt that exhausts the job's attempts", async () => {
		const driver = recordingDriver()
		const fired: string[] = []
		const events: (JobEvent | JobFailureEvent)[] = []

		await createJobs({ driver, hooks: recordHooks(fired, events) }).start([
			defineHandler(sendEmail, async () => {
				throw new Error("the mailer is down")
			}),
		])

		for (const attempt of [1, 2, 3]) {
			await driver
				.deliver("mail", {
					id: "42",
					attempt,
					maxAttempts: 3,
					envelope: envelopeFor("email.send", { to: "ada@example.com" }),
				})
				.catch(() => {})
		}

		expect(fired.filter((hook) => hook === "onAttemptFailed")).toHaveLength(3)
		expect(fired.filter((hook) => hook === "onDead")).toHaveLength(1)
		expect(fired.at(-1)).toBe("onDead")
	})

	test("fires onDead on the first attempt when the handler gives up unrecoverably", async () => {
		const driver = recordingDriver()
		const fired: string[] = []

		await createJobs({ driver, hooks: recordHooks(fired, []) }).start([
			defineHandler(sendEmail, async () => {
				throw new UnrecoverableError("the address is retired")
			}),
		])

		await driver
			.deliver("mail", {
				id: "42",
				attempt: 1,
				maxAttempts: 5,
				envelope: envelopeFor("email.send", { to: "ada@example.com" }),
			})
			.catch(() => {})

		expect(fired).toEqual(["onStart", "onAttemptFailed", "onDead"])
	})

	test("reports a job no handler owns as failed and dead, without ever starting it", async () => {
		const driver = recordingDriver()
		const fired: string[] = []
		const events: (JobEvent | JobFailureEvent)[] = []

		await createJobs({ driver, hooks: recordHooks(fired, events) }).start([
			defineHandler(sendEmail, async () => {}),
		])

		await driver
			.deliver("mail", {
				id: "42",
				attempt: 1,
				maxAttempts: 5,
				envelope: envelopeFor("email.retired", {}),
			})
			.catch(() => {})

		expect(fired).toEqual(["onAttemptFailed", "onDead"])

		const failure = failureOf(events[0])

		expect(failure.name).toBe("email.retired")
		expect(failure.origin).toBe("direct")
		expect(failure.error.message).toContain("email.retired")
	})

	test("fires nothing for a stored value that is not an envelope", async () => {
		const driver = recordingDriver()
		const fired: string[] = []

		await createJobs({ driver, hooks: recordHooks(fired, []) }).start([
			defineHandler(sendEmail, async () => {}),
		])

		await driver
			.deliver("mail", { id: "42", attempt: 1, maxAttempts: 5, envelope: { nope: true } })
			.catch(() => {})

		expect(fired).toEqual([])
	})

	test("fires onStart before the stored payload is validated", async () => {
		const driver = recordingDriver()
		const fired: string[] = []

		await createJobs({ driver, hooks: recordHooks(fired, []) }).start([
			defineHandler(sendEmail, async () => {}),
		])

		await driver
			.deliver("mail", {
				id: "42",
				attempt: 1,
				maxAttempts: 5,
				envelope: envelopeFor("email.send", { to: "not-an-email" }),
			})
			.catch(() => {})

		expect(fired).toEqual(["onStart", "onAttemptFailed", "onDead"])
	})

	test("keeps the job's outcome when a hook throws", async () => {
		const driver = recordingDriver()
		let ran = false

		await createJobs({
			driver,
			hooks: {
				onStart: () => {
					throw new Error("the metrics client is down")
				},
				onSuccess: async () => {
					throw new Error("the metrics client is down")
				},
			},
		}).start([
			defineHandler(sendEmail, async () => {
				ran = true

				return "sent"
			}),
		])

		const result = await driver.deliver("mail", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: envelopeFor("email.send", { to: "ada@example.com" }),
		})

		expect(ran).toBe(true)
		expect(result).toBe("sent")
	})

	test("awaits an async hook before the execution ends", async () => {
		const driver = recordingDriver()
		const fired: string[] = []

		await createJobs({
			driver,
			hooks: {
				onSuccess: async () => {
					await Bun.sleep(5)
					fired.push("onSuccess")
				},
			},
		}).start([defineHandler(sendEmail, async () => {})])

		await driver.deliver("mail", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: envelopeFor("email.send", { to: "ada@example.com" }),
		})

		expect(fired).toEqual(["onSuccess"])
	})
})

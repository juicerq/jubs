import { describe, expect, jest, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { idempotencyKeyFor } from "@/Idempotency"
import {
	createJobs,
	defineHandler,
	defineJob,
	type Envelope,
	type HandlerContext,
	type JobFailureEvent,
} from "@/index"
import { VersionAheadError } from "@/Migration"
import { ShutdownAbortError } from "@/Shutdown"
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

		expect(contexts).toMatchObject([{ id: "42", attempt: 3, maxAttempts: 5, origin: "direct" }])
	})

	test("gives the handler a signal that stays unaborted through a normal run", async () => {
		const driver = recordingDriver()
		const signals: AbortSignal[] = []

		await createJobs({ driver }).start([
			defineHandler(sendEmail, async (_data, context) => {
				signals.push(context.signal)

				expect(context.signal.aborted).toBe(false)
			}),
		])

		await driver.deliver("mail", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: envelopeFor("email.send", { to: "ada@example.com", subject: "welcome" }),
		})

		expect(signals).toHaveLength(1)
		expect(signals[0]?.aborted).toBe(false)
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

describe("start boot checks", () => {
	test("refuses two handlers sharing a job name, before opening a consumer", async () => {
		const driver = recordingDriver()

		const twin = defineJob({
			name: "email.send",
			queue: "mail.retry",
			payload: type({ to: "string.email", subject: "string" }),
		})

		const failure = await createJobs({ driver })
			.start([defineHandler(sendEmail, async () => {}), defineHandler(twin, async () => {})])
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("email.send")
		expect(driver.consuming).toEqual([])
	})

	test("refuses a registered definition that no handler runs on a started queue", async () => {
		const driver = recordingDriver()

		const forgotten = defineJob({
			name: "email.digest",
			queue: "mail",
			payload: type({ to: "string.email" }),
		})

		const failure = await createJobs({ driver, definitions: [sendEmail, forgotten] })
			.start([defineHandler(sendEmail, async () => {})])
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("email.digest")
		expect(driver.consuming).toEqual([])
	})

	test("ignores a registered definition whose queue no handler started", async () => {
		const driver = recordingDriver()

		await createJobs({ driver, definitions: [sendEmail, chargeCard] }).start([
			defineHandler(sendEmail, async () => {}),
		])

		expect(driver.consuming).toEqual(["mail"])
	})
})

describe("handler timeout", () => {
	const slowReport = defineJob({
		name: "reports.render",
		queue: "reports",
		payload: type({ id: "string" }),
		timeoutMs: 20,
	})

	function renderEnvelope(): Envelope {
		return envelopeFor("reports.render", { id: "rep-1" })
	}

	test("aborts the handler signal and fails the attempt when timeoutMs expires", async () => {
		const driver = recordingDriver()
		const aborted = Promise.withResolvers<boolean>()

		await createJobs({ driver }).start([
			defineHandler(slowReport, async (_data, context) => {
				context.signal.addEventListener("abort", () => aborted.resolve(true))

				await Bun.sleep(2_000)
			}),
		])

		const failure = await driver
			.deliver("reports", {
				id: "42",
				attempt: 1,
				maxAttempts: 5,
				envelope: renderEnvelope(),
			})
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(Error)
		expect(failure).not.toBeInstanceOf(UnrecoverableError)
		expect((failure as Error).message).toContain("reports.render")
		expect(await aborted.promise).toBe(true)
	})

	test("leaves the handler signal unaborted when it finishes inside timeoutMs", async () => {
		const driver = recordingDriver()
		const signals: AbortSignal[] = []

		await createJobs({ driver }).start([
			defineHandler(slowReport, async (_data, context) => {
				signals.push(context.signal)
			}),
		])

		await driver.deliver("reports", {
			id: "42",
			attempt: 1,
			maxAttempts: 5,
			envelope: renderEnvelope(),
		})

		await Bun.sleep(60)

		expect(signals[0]?.aborted).toBe(false)
	})

	test("gives the aborted signal the timeout as its reason", async () => {
		const driver = recordingDriver()
		const reason = Promise.withResolvers<unknown>()

		await createJobs({ driver }).start([
			defineHandler(slowReport, async (_data, context) => {
				context.signal.addEventListener("abort", () => reason.resolve(context.signal.reason))

				await Bun.sleep(2_000)
			}),
		])

		await driver
			.deliver("reports", { id: "42", attempt: 1, maxAttempts: 5, envelope: renderEnvelope() })
			.catch(() => {})

		const aborted = await reason.promise

		expect(aborted).toBeInstanceOf(Error)
		expect(aborted).not.toBeInstanceOf(ShutdownAbortError)
		expect((aborted as Error).message).toContain("timeoutMs")
	})
})

describe("close", () => {
	const renderReport = defineJob({
		name: "reports.render",
		queue: "reports",
		payload: type({ id: "string" }),
	})

	function renderEnvelope(): Envelope {
		return envelopeFor("reports.render", { id: "rep-1" })
	}

	test("aborts a job still running with a shutdown reason, and reports no failure", async () => {
		const driver = recordingDriver()
		const failed: JobFailureEvent[] = []
		const dead: JobFailureEvent[] = []
		const started = Promise.withResolvers<void>()
		const reason = Promise.withResolvers<unknown>()

		const runtime = await createJobs({
			driver,
			deadQueues: ["reports"],
			hooks: {
				onAttemptFailed: (event) => {
					failed.push(event)
				},
				onDead: (event) => {
					dead.push(event)
				},
			},
		}).start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				reason.resolve(context.signal.reason)

				throw context.signal.reason
			}),
		])

		const delivered = driver
			.deliver("reports", { id: "42", attempt: 5, maxAttempts: 5, envelope: renderEnvelope() })
			.catch((error: unknown) => error)

		await started.promise
		await runtime.close({ timeoutMs: 20 })

		expect(await reason.promise).toBeInstanceOf(ShutdownAbortError)
		expect(await delivered).toBeInstanceOf(ShutdownAbortError)
		expect(failed).toEqual([])
		expect(dead).toEqual([])
	})

	test("keeps the handler failure it replaced as the cause of the abort", async () => {
		const driver = recordingDriver()
		const started = Promise.withResolvers<void>()
		const failure = new Error("DB constraint violated: duplicate charge")

		const runtime = await createJobs({ driver }).start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				throw failure
			}),
		])

		const delivered = driver
			.deliver("reports", { id: "42", attempt: 1, maxAttempts: 5, envelope: renderEnvelope() })
			.catch((error: unknown) => error)

		await started.promise
		await runtime.close({ timeoutMs: 20 })

		const aborted = await delivered

		expect(aborted).toBeInstanceOf(ShutdownAbortError)
		expect((aborted as ShutdownAbortError).cause).toBe(failure)
	})

	test("never makes the abort its own cause when the handler rethrows the signal reason", async () => {
		const driver = recordingDriver()
		const started = Promise.withResolvers<void>()

		const runtime = await createJobs({ driver }).start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				throw context.signal.reason
			}),
		])

		const delivered = driver
			.deliver("reports", { id: "42", attempt: 1, maxAttempts: 5, envelope: renderEnvelope() })
			.catch((error: unknown) => error)

		await started.promise
		await runtime.close({ timeoutMs: 20 })

		const aborted = await delivered

		expect(aborted).toBeInstanceOf(ShutdownAbortError)
		expect((aborted as ShutdownAbortError).cause).not.toBe(aborted)
	})

	test("buries an UnrecoverableError thrown inside the shutdown window", async () => {
		const driver = recordingDriver()
		const failed: JobFailureEvent[] = []
		const dead: JobFailureEvent[] = []
		const started = Promise.withResolvers<void>()

		const runtime = await createJobs({
			driver,
			hooks: {
				onAttemptFailed: (event) => {
					failed.push(event)
				},
				onDead: (event) => {
					dead.push(event)
				},
			},
		}).start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				throw new UnrecoverableError("this payload can never succeed")
			}),
		])

		const delivered = driver
			.deliver("reports", { id: "42", attempt: 1, maxAttempts: 5, envelope: renderEnvelope() })
			.catch((error: unknown) => error)

		await started.promise
		await runtime.close({ timeoutMs: 20 })

		expect(await delivered).toBeInstanceOf(UnrecoverableError)
		expect(failed).toHaveLength(1)
		expect(dead).toHaveLength(1)
	})

	test("buries a VersionAheadError thrown inside the shutdown window", async () => {
		const driver = recordingDriver()
		const dead: JobFailureEvent[] = []
		const started = Promise.withResolvers<void>()

		const runtime = await createJobs({
			driver,
			hooks: {
				onDead: (event) => {
					dead.push(event)
				},
			},
		}).start([
			defineHandler(renderReport, async (_data, context) => {
				started.resolve()

				await new Promise<void>((resolve) => {
					context.signal.addEventListener("abort", () => resolve())
				})

				throw new VersionAheadError("reports.render", 2, 1)
			}),
		])

		const delivered = driver
			.deliver("reports", { id: "42", attempt: 1, maxAttempts: 5, envelope: renderEnvelope() })
			.catch((error: unknown) => error)

		await started.promise
		await runtime.close({ timeoutMs: 20 })

		expect(await delivered).toBeInstanceOf(VersionAheadError)
		expect(dead).toHaveLength(1)
	})

	test("holds the idempotency key of a body a shutdown abort cut short", async () => {
		const settlePayment = defineJob({
			name: "payments.settle",
			queue: "payments",
			payload: type({ id: "string" }),
			timeoutMs: 60,
			idempotencyKey: (data) => data.id,
		})

		const key = idempotencyKeyFor(settlePayment, { id: "pay-1" })

		if (!key) {
			throw new Error("the settle definition declares no idempotency key")
		}

		const driver = recordingDriver()
		const started = Promise.withResolvers<void>()
		const body = Promise.withResolvers<{ receipt: string }>()

		const runtime = await createJobs({ driver }).start([
			defineHandler(settlePayment, () => {
				started.resolve()

				return body.promise
			}),
		])

		const delivered = driver
			.deliver("payments", {
				id: "42",
				attempt: 1,
				maxAttempts: 3,
				envelope: envelopeFor("payments.settle", { id: "pay-1" }),
			})
			.catch((error: unknown) => error)

		await started.promise
		await runtime.close({ timeoutMs: 10 })

		expect(await delivered).toBeInstanceOf(ShutdownAbortError)
		expect(driver.released).toEqual([])
		expect(driver.completed).toEqual([])

		body.resolve({ receipt: "late" })

		for (let turn = 0; turn < 20; turn += 1) {
			await Promise.resolve()
		}

		expect(driver.released).toEqual([])
		expect(driver.completed).toEqual([{ key, kept: { result: { receipt: "late" } } }])
	})

	test("clears its timer when the consumers refuse to close", async () => {
		const driver = recordingDriver()
		const cleared = jest.spyOn(globalThis, "clearTimeout")

		const runtime = await createJobs({ driver }).start([
			defineHandler(renderReport, async () => {}),
		])

		driver.refuseClose()

		const failure = await runtime.close({ timeoutMs: 20 }).catch((error: unknown) => error)

		expect((failure as Error).message).toContain("recordingDriver")
		expect(cleared).toHaveBeenCalled()

		cleared.mockRestore()
	})
})

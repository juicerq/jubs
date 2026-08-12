import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { CANCEL_SWEEP_MS } from "@/Cancellation"
import {
	CancelledError,
	createJobs,
	DELIVERY_DEFAULTS,
	defineHandler,
	defineJob,
	type JobEvent,
	type JobFailureEvent,
	type JobSnapshot,
} from "@/index"
import { memoryDriver } from "@/testing/index"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "string.numeric.parse" }),
	delivery: { attempts: 1 },
})

const charged = defineHandler(chargeCard, async () => {})

const declined = defineHandler(chargeCard, async () => {
	throw new Error("the card was declined")
})

describe("reading a job", () => {
	test("describes a job nobody has run yet", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([charged])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await jobs.get(enqueued.id)).toEqual({
			id: "billing:1",
			queue: "billing",
			name: "billing.charge",
			state: "waiting",
			envelope: {
				v: 1,
				name: "billing.charge",
				data: { cents: "500" },
				origin: "direct",
			},
			attempts: 0,
			maxAttempts: 1,
			failure: undefined,
		})
	})

	test("describes a job that ran to the end", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([charged])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await driver.drain()

		const snapshot = await jobs.get(enqueued.id)

		expect(snapshot?.state).toBe("completed")
		expect(snapshot?.attempts).toBe(1)
		expect(snapshot?.failure).toBeUndefined()
	})

	test("carries the failure message of a job whose handler threw", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([declined])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await driver.drain().catch(() => {})

		const snapshot = await jobs.get(enqueued.id)

		expect(snapshot?.state).toBe("failed")
		expect(snapshot?.attempts).toBe(1)
		expect(snapshot?.failure).toBe("the card was declined")
	})

	test("reports the maxAttempts of the delivery the job was enqueued with", async () => {
		const driver = memoryDriver()
		const patient = defineJob({
			name: "billing.settle",
			queue: "billing",
			payload: type({ cents: "string" }),
		})

		const enqueued = await createJobs({ driver }).enqueue(patient, { cents: "500" })
		const snapshot = await createJobs({ driver }).get(enqueued.id)

		expect(snapshot?.maxAttempts).toBe(DELIVERY_DEFAULTS.attempts)
	})

	test("never answers a dead job's id with the live job that shares its number", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })
		let refusing = false

		await jobs.start([
			defineHandler(chargeCard, async () => {
				if (refusing) {
					throw new Error("the card was declined")
				}
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "100" })
		await jobs.enqueue(chargeCard, { cents: "200" })
		await driver.drain()

		refusing = true

		await jobs.enqueue(chargeCard, { cents: "300" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list("billing")

		expect(dead?.envelope.data).toEqual({ cents: "300" })
		expect(await jobs.get(dead?.id ?? "")).toBeUndefined()
	})

	test("refuses a dead job's id, in the three calls that speak of a live job", async () => {
		const jobs = createJobs({ driver: memoryDriver(), deadQueues: ["billing"] })

		expect(await jobs.get("billing.dead:1")).toBeUndefined()
		expect(await jobs.retry("billing.dead:1")).toEqual({ outcome: "unknown_job" })
		expect(await jobs.cancel("billing.dead:1")).toEqual({ outcome: "unknown_job" })
	})

	test("answers the id a handler and a hook are given", async () => {
		const driver = memoryDriver()
		const seen: (JobSnapshot | undefined)[] = []
		const events: JobEvent[] = []

		const jobs = createJobs({
			driver,
			hooks: {
				onStart: (event) => {
					events.push(event)
				},
			},
		})

		await jobs.start([
			defineHandler(chargeCard, async (_data, context) => {
				seen.push(await jobs.get(context.id))
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await driver.runNext()

		expect(seen[0]?.id).toBe(enqueued.id)
		expect(seen[0]?.state).toBe("active")
		expect(events[0]?.id).toBe(enqueued.id)
	})

	test("gives back nothing for an id no job answers to", async () => {
		const jobs = createJobs({ driver: memoryDriver() })

		expect(await jobs.get("billing:7")).toBeUndefined()
	})

	test("refuses an id that names no queue", async () => {
		const jobs = createJobs({ driver: memoryDriver() })

		const failure = await jobs.get("7").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("is not a job id")
		expect((failure as Error).message).toContain("jobs.enqueue")
		expect((failure as Error).message).toContain("jobs.dead.list(queue)")
	})
})

describe("retrying a job", () => {
	test("runs a failed job again", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const seen: number[] = []
		let refusing = true

		await jobs.start([
			defineHandler(chargeCard, async (data) => {
				seen.push(data.cents)

				if (refusing) {
					throw new Error("the card was declined")
				}
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await driver.drain().catch(() => {})

		refusing = false

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "retried" })
		expect(await driver.drain()).toBe(1)
		expect(seen).toEqual([500, 500])

		const snapshot = await jobs.get(enqueued.id)

		expect(snapshot?.state).toBe("completed")
		expect(snapshot?.failure).toBeUndefined()
	})

	test("names the state of a job that has not failed", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([charged])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "not_failed", state: "waiting" })

		await driver.drain()

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "not_failed", state: "completed" })
	})

	test("reports an id no job answers to instead of throwing", async () => {
		const jobs = createJobs({ driver: memoryDriver() })

		expect(await jobs.retry("billing:7")).toEqual({ outcome: "unknown_job" })
	})

	test("refuses an id that names no queue", async () => {
		const jobs = createJobs({ driver: memoryDriver() })

		const failure = await jobs.retry("7").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("is not a job id")
	})
})

/**
 * A handler body that obeys its signal: it rejects with `signal.reason` the
 * moment the runtime aborts, and otherwise returns once `settleInMs` passes —
 * or never, when no delay is given.
 */
function obedient(signal: AbortSignal, settleInMs?: number): Promise<void> {
	return new Promise((settle, reject) => {
		const settling = settleInMs === undefined ? undefined : setTimeout(settle, settleInMs)

		signal.addEventListener("abort", () => {
			clearTimeout(settling)
			reject(signal.reason)
		})
	})
}

describe("cancelling a job", () => {
	test("removes a job nobody has run yet", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([charged])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "removed" })
		expect(await jobs.get(enqueued.id)).toBeUndefined()
		expect(await driver.drain()).toBe(0)
	})

	test("aborts a running handler, and buries the job as cancelled without another attempt", async () => {
		const driver = memoryDriver()
		const failed: JobFailureEvent[] = []
		const dead: JobFailureEvent[] = []

		const jobs = createJobs({
			driver,
			deadQueues: ["billing"],
			hooks: {
				onAttemptFailed: (event) => {
					failed.push(event)
				},
				onDead: (event) => {
					dead.push(event)
				},
			},
		})

		const started = Promise.withResolvers<void>()

		await jobs.start([
			defineHandler(chargeCard, async (_data, context) => {
				started.resolve()

				await obedient(context.signal)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })
		const running = driver.runNext().catch((error: unknown) => error)

		await started.promise

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })
		expect(await running).toBeInstanceOf(CancelledError)

		const [buried] = await jobs.dead.list("billing")

		expect(buried?.reason).toBe("cancelled")
		expect(failed).toHaveLength(1)
		expect(dead).toHaveLength(1)
		expect(dead[0]?.id).toBe(enqueued.id)
		expect((await jobs.get(enqueued.id))?.state).toBe("failed")
	})

	test("leaves no mark behind, so a retry of the cancelled job runs to the end", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const runs: number[] = []
		const started = Promise.withResolvers<void>()
		let cancelling = true

		await jobs.start([
			defineHandler(chargeCard, async (data, context) => {
				runs.push(data.cents)

				if (!cancelling) {
					await obedient(context.signal, CANCEL_SWEEP_MS * 3)

					return
				}

				started.resolve()

				await obedient(context.signal)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })
		const running = driver.runNext().catch(() => {})

		await started.promise
		await jobs.cancel(enqueued.id)
		await running

		cancelling = false

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "retried" })
		expect(await driver.drain()).toBe(1)
		expect(runs).toEqual([500, 500])
		expect((await jobs.get(enqueued.id))?.state).toBe("completed")
	})

	test("lets a cancellation nobody collected die with its delivery, never killing the next one", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const runs: number[] = []
		const started = Promise.withResolvers<void>()
		const release = Promise.withResolvers<void>()
		let failing = true

		await jobs.start([
			defineHandler(chargeCard, async (data, context) => {
				runs.push(data.cents)

				if (failing) {
					started.resolve()

					await release.promise

					throw new Error("the card was declined")
				}

				await obedient(context.signal, CANCEL_SWEEP_MS * 3)
			}),
		])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })
		const running = driver.runNext().catch(() => {})

		await started.promise

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })

		release.resolve()

		await running

		failing = false

		expect(await jobs.retry(enqueued.id)).toEqual({ outcome: "retried" })
		expect(await driver.drain()).toBe(1)
		expect(runs).toEqual([500, 500])
		expect((await jobs.get(enqueued.id))?.state).toBe("completed")
	})

	test("holds the idempotency lease when the body of a cancelled handler runs past its timeout", async () => {
		const slowCharge = defineJob({
			name: "billing.charge.slow",
			queue: "billing",
			payload: type({ orderId: "string" }),
			idempotencyKey: (data) => data.orderId,
			timeoutMs: CANCEL_SWEEP_MS * 3,
			delivery: { attempts: 1 },
		})

		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const bodies: string[] = []
		const started = Promise.withResolvers<void>()

		await jobs.start([
			defineHandler(slowCharge, async (data) => {
				bodies.push(data.orderId)
				started.resolve()

				await Bun.sleep(CANCEL_SWEEP_MS * 20)
			}),
		])

		const enqueued = await jobs.enqueue(slowCharge, { orderId: "order-1" })

		await jobs.enqueue(slowCharge, { orderId: "order-1" })

		const running = driver.runNext().catch((error: unknown) => error)

		await started.promise

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })
		expect(await running).toBeInstanceOf(CancelledError)

		const refused = await driver.runNext().catch((error: unknown) => error)

		expect((refused as Error).message).toContain("idempotency lease is held")
		expect(bodies).toEqual(["order-1"])
	})

	test("names the state of a job that already finished", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([charged])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await driver.drain()

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "finished", state: "completed" })
	})

	test("reports an id no job answers to instead of throwing", async () => {
		const jobs = createJobs({ driver: memoryDriver() })

		expect(await jobs.cancel("billing:7")).toEqual({ outcome: "unknown_job" })
	})

	test("refuses an id that names no queue", async () => {
		const jobs = createJobs({ driver: memoryDriver() })

		const failure = await jobs.cancel("7").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("is not a job id")
	})
})

describe("pausing a queue", () => {
	test("holds every job of a paused queue back, and runs them once it resumes", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const seen: number[] = []

		await jobs.start([
			defineHandler(chargeCard, async (data) => {
				seen.push(data.cents)
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })
		await jobs.pause("billing")

		expect(await driver.drain()).toBe(0)
		expect(seen).toEqual([])

		await jobs.resume("billing")

		expect(await driver.drain()).toBe(1)
		expect(seen).toEqual([500])
	})

	test("runs the jobs of the queues that are not paused", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const sent: string[] = []

		const sendEmail = defineJob({
			name: "email.send",
			queue: "mail",
			payload: type({ to: "string" }),
		})

		await jobs.start([
			charged,
			defineHandler(sendEmail, async (data) => {
				sent.push(data.to)
			}),
		])

		await jobs.pause("billing")
		await jobs.enqueue(chargeCard, { cents: "500" })
		await jobs.enqueue(sendEmail, { to: "ada@example.com" })

		expect(await driver.drain()).toBe(1)
		expect(sent).toEqual(["ada@example.com"])
	})

	test("runNext says a paused queue is what holds the pending jobs back", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		await jobs.start([charged])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await jobs.pause("billing")

		const failure = await driver.runNext().catch((error: unknown) => error)

		expect((failure as Error).message).toContain("paused queue")
		expect((failure as Error).message).toContain("jobs.resume(queue)")
	})
})

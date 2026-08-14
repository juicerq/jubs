import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { burialFor, readDeadEntry } from "@/Dead"
import { ChildDeadError, ChildrenShortError } from "@/Flow"
import {
	CancelledError,
	createJobs,
	defineHandler,
	defineJob,
	type JobDelivery,
	VersionAheadError,
} from "@/index"
import { type MemoryDriver, memoryDriver } from "@/testing/index"
import { liveId } from "../support/JobIds"
import { errorOf } from "../support/Failures"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "string.numeric.parse" }),
	delivery: { attempts: 1 },
})

const declined = defineHandler(chargeCard, async () => {
	throw new Error("the card was declined")
})

describe("burying a dead job", () => {
	test("keeps the envelope, the serialised error and the reason of a job that failed every attempt", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([declined])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const buried = await jobs.dead.list("billing")

		expect(buried).toHaveLength(1)
		expect(buried[0]?.envelope).toEqual({
			v: 1,
			name: "billing.charge",
			data: { cents: "500" },
			origin: "direct",
		})
		expect(buried[0]?.error.message).toBe("the card was declined")
		expect(buried[0]?.reason).toBe("attempts_exhausted")
	})

	test("names the job it buried, apart from the id of the record that keeps it", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([declined])

		const enqueued = await jobs.enqueue(chargeCard, { cents: "500" })

		await driver.drain().catch(() => {})

		const buried = await jobs.dead.list("billing")

		expect(buried[0]?.jobId).toBe(liveId(enqueued))
		expect(buried[0]?.id).toBe("billing.dead:1")
	})

	test("marks a job the handler gave up on as unrecoverable", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new UnrecoverableError("the card is closed")
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const buried = await jobs.dead.list("billing")

		expect(buried[0]?.reason).toBe("unrecoverable")
	})

	test("buries nothing for a queue with no dead queue, and still fires onDead", async () => {
		const driver = memoryDriver()
		const fired: string[] = []

		const jobs = createJobs({
			driver,
			hooks: {
				onDead: (event) => {
					fired.push(event.name)
				},
			},
		})

		await jobs.start([declined])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		expect(fired).toEqual(["billing.charge"])
		expect(await jobs.dead.list("billing")).toEqual([])
	})

	test("counts the dead jobs of each queue on its own", async () => {
		const driver = memoryDriver()

		const sendEmail = defineJob({
			name: "email.send",
			queue: "mail",
			payload: type({ to: "string" }),
			delivery: { attempts: 1 },
		})

		const jobs = createJobs({ driver, deadQueues: ["billing", "mail"] })

		await jobs.start([
			declined,
			defineHandler(sendEmail, async () => {
				throw new Error("the mailer is down")
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})
		await jobs.enqueue(sendEmail, { to: "ada@example.com" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list("billing")
		const [mailed] = await jobs.dead.list("mail")

		expect(dead?.id).toBe("billing.dead:1")
		expect(mailed?.id).toBe("mail.dead:1")
	})
})

describe("opting a queue into a dead queue", () => {
	test("refuses a dead queue no definition uses", () => {
		expect(() =>
			createJobs({
				driver: memoryDriver(),
				definitions: [chargeCard],
				deadQueues: ["biling"],
			}),
		).toThrow('"biling"')
	})

	test("accepts any dead queue when the client registers no definitions", () => {
		expect(() => createJobs({ driver: memoryDriver(), deadQueues: ["biling"] })).not.toThrow()
	})

	test("refuses to start a worker on a queue that is another queue's dead queue", async () => {
		const swallowing = defineJob({
			name: "billing.audit",
			queue: "billing.dead",
			payload: type({ cents: "string" }),
		})

		const jobs = createJobs({ driver: memoryDriver(), deadQueues: ["billing"] })

		const failure = await jobs
			.start([defineHandler(swallowing, async () => {})])
			.catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("billing.dead")
		expect(errorOf(failure).message).toContain("consumed by nobody")
	})
})

describe("inspecting and replaying a dead job", () => {
	test("list names each dead job by an id its queue and its storage share", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([declined])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const dead = await jobs.dead.list("billing")

		expect(dead).toHaveLength(1)
		expect(dead[0]?.id).toBe("billing.dead:1")
		expect(dead[0]?.reason).toBe("attempts_exhausted")
		expect(dead[0]?.envelope.data).toEqual({ cents: "500" })
	})

	test("replay enqueues the stored payload again and clears the dead job", async () => {
		const driver = memoryDriver()

		const jobs = createJobs({
			driver,
			deadQueues: ["billing"],
			definitions: [chargeCard],
		})

		const charged: number[] = []
		let refusing = true

		await jobs.start([
			defineHandler(chargeCard, async (data) => {
				charged.push(data.cents)

				if (refusing) {
					throw new Error("the card was declined")
				}
			}),
		])

		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list("billing")

		refusing = false

		const replayed = await jobs.dead.replay(dead?.id ?? "")

		expect(await jobs.dead.list("billing")).toEqual([])
		expect(driver.enqueued(chargeCard)).toEqual([{ cents: "500" }, { cents: "500" }])
		expect(replayed.id).toBe("billing:2")
		expect(await driver.drain()).toBe(1)
		expect(charged).toEqual([500, 500])
	})

	test("replays a job whose policy is unique and delayed, carrying neither", async () => {
		const driver = memoryDriver()

		const rebuildIndex = defineJob({
			name: "search.rebuild",
			queue: "search",
			payload: type({ entityId: "string" }),
			delivery: { unique: { key: (data) => data.entityId, mode: "keepLast", ttlMs: 4_000 } },
		})

		const jobs = createJobs({ driver, deadQueues: ["search"], definitions: [rebuildIndex] })

		await driver.dead.bury("search", {
			jobId: "search:1",
			envelope: { v: 1, name: "search.rebuild", data: { entityId: "e_1" }, origin: "direct" },
			error: { name: "Error", message: "the index refused the write" },
			reason: "attempts_exhausted",
		})

		const [dead] = await jobs.dead.list("search")

		await jobs.dead.replay(dead?.id ?? "")

		expect(driver.enqueued(rebuildIndex)).toEqual([{ entityId: "e_1" }])
	})

	test("replay refuses an id no dead job answers to", async () => {
		const jobs = createJobs({ driver: memoryDriver(), deadQueues: ["billing"] })

		const failure = await jobs.dead.replay("billing.dead:7").catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("billing.dead:7")
		expect(errorOf(failure).message).toContain("replayed or discarded already")
	})

	test("replay and discard refuse an id that names a live queue", async () => {
		const jobs = createJobs({ driver: memoryDriver(), deadQueues: ["billing"] })

		const replayed = await jobs.dead.replay("billing:1").catch((error: unknown) => error)
		const discarded = await jobs.dead.discard("billing:1").catch((error: unknown) => error)

		for (const failure of [replayed, discarded]) {
			expect(errorOf(failure).message).toContain("is not a dead job id")
			expect(errorOf(failure).message).toContain("jobs.dead.list(queue)")
		}
	})

	test("replay refuses a job whose definition the client does not know", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([declined])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list("billing")
		const failure = await jobs.dead.replay(dead?.id ?? "").catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("billing.charge")
		expect(errorOf(failure).message).toContain("createJobs({ definitions })")
	})

	test("replay refuses a job whose definition waits on children, whatever its envelope says", async () => {
		const driver = memoryDriver()

		const signRow = defineJob({
			name: "nfe.sign-row",
			queue: "signing",
			payload: type({ row: "number" }),
			result: type({ signed: "boolean" }),
		})

		const buildManifest = defineJob({
			name: "nfe.manifest",
			queue: "documents",
			payload: type({ run_id: "string" }),
			awaits: { row: signRow },
		})

		const jobs = createJobs({ driver, deadQueues: ["documents"], definitions: [buildManifest] })

		await driver.dead.bury("documents", {
			jobId: "documents:1",
			envelope: { v: 1, name: "nfe.manifest", data: { run_id: "r_1" }, origin: "direct" },
			error: { name: "ChildrenShortError", message: "a slot holds fewer children" },
			reason: "children_short",
		})

		const [dead] = await jobs.dead.list("documents")
		const failure = await jobs.dead.replay(dead?.id ?? "").catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("nfe.manifest")
		expect(errorOf(failure).message).toContain("children_short")
		expect(errorOf(failure).message).toContain("jobs.enqueue(definition, data, awaits)")
		expect(await jobs.dead.list("documents")).toHaveLength(1)
	})

	const settlePayment = defineJob({
		name: "billing.settle",
		queue: "billing",
		payload: type({ paymentId: "string" }),
		idempotencyKey: (data) => data.paymentId,
		delivery: { attempts: 1 },
		timeoutMs: 20,
	})

	/**
	 * Buries the job on its deadline and gives back the client, with the handler's
	 * body still running under the key it holds — the state every job that pairs
	 * `timeoutMs` with `idempotencyKey` passes through on its way to being buried.
	 */
	async function buryOnItsDeadline(driver: MemoryDriver, bodyMs: number) {
		const jobs = createJobs({
			driver,
			deadQueues: ["billing"],
			definitions: [settlePayment],
		})

		const settled: string[] = []

		await jobs.start([
			defineHandler(settlePayment, async (data) => {
				settled.push(data.paymentId)

				await Bun.sleep(bodyMs)
			}),
		])

		await jobs.enqueue(settlePayment, { paymentId: "pay-1" })
		await driver.drain().catch(() => {})

		return { jobs, settled }
	}

	test("replay forgets the idempotency key, so a job buried on its deadline runs again", async () => {
		const driver = memoryDriver()
		const { jobs, settled } = await buryOnItsDeadline(driver, 60)

		await Bun.sleep(120)

		const [dead] = await jobs.dead.list("billing")

		expect(dead?.reason).toBe("attempts_exhausted")

		await jobs.dead.replay(dead?.id ?? "")
		await driver.drain().catch(() => {})

		expect(settled).toEqual(["pay-1", "pay-1"])
	})

	test("replay refuses a job whose idempotency key a running body still holds", async () => {
		const driver = memoryDriver()
		const { jobs } = await buryOnItsDeadline(driver, 200)

		const [dead] = await jobs.dead.list("billing")
		const failure = await jobs.dead.replay(dead?.id ?? "").catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("held by a delivery running right now")
		expect(errorOf(failure).message).toContain("This dead record stays where it is")
		expect(await jobs.dead.list("billing")).toHaveLength(1)
		expect(driver.enqueued(settlePayment)).toEqual([{ paymentId: "pay-1" }])
	})

	test("discard drops the dead job, and refuses an id no dead job answers to", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([declined])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list("billing")

		await jobs.dead.discard(dead?.id ?? "")

		expect(await jobs.dead.list("billing")).toEqual([])

		const failure = await jobs.dead.discard("billing.dead:1").catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain("billing.dead:1")
	})
})

const reconcileLedger = defineJob({
	name: "billing.reconcile",
	queue: "billing",
	payload: type({ day: "string" }),
})

const closeBooks = defineJob({
	name: "billing.close",
	queue: "billing",
	payload: type({ month: "string" }),
	awaits: { ledger: reconcileLedger },
})

const ledgerResult = { slot: "ledger", value: { rows: 2 } }

const childIsDead = new ChildDeadError(closeBooks, {
	results: [ledgerResult],
	failures: [{ id: "billing:ledger~ab", slot: "ledger", reason: "the ledger is locked" }],
	pending: 0,
})

const midway: JobDelivery = {
	id: "1",
	attempt: 1,
	attemptsStarted: 1,
	maxAttempts: 3,
	envelope: {},
}

const lastAttempt: JobDelivery = { ...midway, attempt: 3 }

function unrecoverable(error: Error): UnrecoverableError {
	const failure = new UnrecoverableError(error.message)

	failure.cause = error

	return failure
}

describe("burialFor", () => {
	test("reads a cancelled delivery as cancelled", () => {
		expect(burialFor(midway, new CancelledError("billing.charge"))).toEqual({ reason: "cancelled" })
	})

	test("reads a payload a newer deploy wrote as version_ahead", () => {
		expect(burialFor(midway, new VersionAheadError("billing.charge", 2, 1))).toEqual({
			reason: "version_ahead",
		})
	})

	test("reads a child that failed every attempt as child_dead, raw and wrapped", () => {
		const burial = { reason: "child_dead", children: [ledgerResult] } as const

		expect(burialFor(midway, childIsDead)).toEqual(burial)
		expect(burialFor(midway, unrecoverable(childIsDead))).toEqual(burial)
	})

	test("reads a slot holding fewer children as children_short, raw and wrapped", () => {
		const short = new ChildrenShortError("a slot holds fewer children")

		expect(burialFor(midway, short)).toEqual({ reason: "children_short" })
		expect(burialFor(midway, unrecoverable(short))).toEqual({ reason: "children_short" })
	})

	test("reads a handler that gave up as unrecoverable", () => {
		expect(burialFor(midway, new UnrecoverableError("the card is closed"))).toEqual({
			reason: "unrecoverable",
		})
	})

	test("reads the last attempt of an ordinary failure as attempts_exhausted", () => {
		expect(burialFor(lastAttempt, new Error("the card was declined"))).toEqual({
			reason: "attempts_exhausted",
		})
	})

	test("reads an ordinary failure with attempts left as no burial at all", () => {
		expect(burialFor(midway, new Error("the card was declined"))).toBeUndefined()
	})

	test("keeps no children on a burial no child caused, raw and wrapped", () => {
		const declined = new Error("the card was declined")

		expect(burialFor(lastAttempt, declined)).not.toHaveProperty("children")
		expect(burialFor(lastAttempt, unrecoverable(declined))).not.toHaveProperty("children")
	})
})

const storedEntry = {
	jobId: "billing:1",
	envelope: { v: 1, name: "billing.charge", data: { cents: "500" }, origin: "direct" },
	error: { name: "Error", message: "the card was declined" },
	reason: "attempts_exhausted",
}

function refusal(stored: unknown): string {
	try {
		readDeadEntry(stored)
	} catch (error: unknown) {
		return errorOf(error).message
	}

	throw new Error(`readDeadEntry read ${JSON.stringify(stored)} instead of refusing it`)
}

describe("readDeadEntry", () => {
	test("reads back a record this library wrote, whole", () => {
		expect(readDeadEntry(structuredClone(storedEntry))).toEqual({
			jobId: "billing:1",
			envelope: { v: 1, name: "billing.charge", data: { cents: "500" }, origin: "direct" },
			error: { name: "Error", message: "the card was declined" },
			reason: "attempts_exhausted",
		})
	})

	test("reads back the children of a child_dead record, whole", () => {
		const stored = { ...storedEntry, reason: "child_dead", children: [ledgerResult] }

		expect(readDeadEntry(stored)).toEqual({
			jobId: "billing:1",
			envelope: { v: 1, name: "billing.charge", data: { cents: "500" }, origin: "direct" },
			error: { name: "Error", message: "the card was declined" },
			reason: "child_dead",
			children: [ledgerResult],
		})
	})

	test("refuses what is not an object at all", () => {
		expect(refusal("billing:1")).toContain("expected an object, got string")
		expect(refusal(null)).toContain("expected an object, got object")
	})

	test("refuses a record naming no job id", () => {
		expect(refusal({ ...storedEntry, jobId: undefined })).toContain("its job id is missing")
		expect(refusal({ ...storedEntry, jobId: 7 })).toContain("its job id is missing")
		expect(refusal({ ...storedEntry, jobId: "" })).toContain("its job id is missing")
	})

	test("refuses a reason no job is buried for", () => {
		expect(refusal({ ...storedEntry, reason: "expired" })).toContain(
			'its reason "expired" is not one a job is buried for',
		)
		expect(refusal({ ...storedEntry, reason: undefined })).toContain("its reason undefined")
	})

	test("refuses an envelope that is not one, in the envelope's own words", () => {
		expect(refusal({ ...storedEntry, envelope: { name: "billing.charge" } })).toContain(
			"its payload version `v` is missing",
		)
	})

	test("refuses an error that is not a serialized one", () => {
		expect(refusal({ ...storedEntry, error: "the card was declined" })).toContain(
			'its error is "the card was declined", which is not a serialized error',
		)
		expect(refusal({ ...storedEntry, error: { message: "the card was declined" } })).toContain(
			"its error carries no error name",
		)
		expect(refusal({ ...storedEntry, error: { name: "Error" } })).toContain(
			"its error carries no error message",
		)
		expect(refusal({ ...storedEntry, error: { ...storedEntry.error, stack: 3 } })).toContain(
			"its error carries a stack that is not text",
		)
	})

	test("refuses an error whose cause is not a serialized one", () => {
		const error = { ...storedEntry.error, cause: { message: "the acquirer timed out" } }

		expect(refusal({ ...storedEntry, error })).toContain("its error cause carries no error name")
	})

	test("refuses a child_dead record holding no children", () => {
		expect(refusal({ ...storedEntry, reason: "child_dead" })).toContain(
			'it was buried for "child_dead" and its children are undefined, which is not a list',
		)
	})

	test("refuses a child that names no slot it filled", () => {
		const stored = { ...storedEntry, reason: "child_dead", children: [{ value: { rows: 2 } }] }

		expect(refusal(stored)).toContain('its child {"value":{"rows":2}} names no slot it filled')
	})

	test("refuses children on a burial no child caused", () => {
		expect(refusal({ ...storedEntry, children: [ledgerResult] })).toContain(
			'it was buried for "attempts_exhausted" and still carries children',
		)
	})

	test("names the one thing an operator can do with a record it refuses", () => {
		expect(refusal({ ...storedEntry, reason: "expired" })).toContain("jobs.dead.discard(id)")
	})
})

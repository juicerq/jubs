import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { createJobs, defineHandler, defineJob } from "@/index"
import { type MemoryDriver, memoryDriver } from "@/testing/index"

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

		expect(buried[0]?.jobId).toBe(enqueued.id)
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

		expect((failure as Error).message).toContain("billing.dead")
		expect((failure as Error).message).toContain("consumed by nobody")
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

		expect((failure as Error).message).toContain("billing.dead:7")
		expect((failure as Error).message).toContain("replayed or discarded already")
	})

	test("replay and discard refuse an id that names a live queue", async () => {
		const jobs = createJobs({ driver: memoryDriver(), deadQueues: ["billing"] })

		const replayed = await jobs.dead.replay("billing:1").catch((error: unknown) => error)
		const discarded = await jobs.dead.discard("billing:1").catch((error: unknown) => error)

		for (const failure of [replayed, discarded]) {
			expect((failure as Error).message).toContain("is not a dead job id")
			expect((failure as Error).message).toContain("jobs.dead.list(queue)")
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

		expect((failure as Error).message).toContain("billing.charge")
		expect((failure as Error).message).toContain("createJobs({ definitions })")
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

		expect((failure as Error).message).toContain("nfe.manifest")
		expect((failure as Error).message).toContain("children_short")
		expect((failure as Error).message).toContain("jobs.enqueue(definition, data, awaits)")
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

		expect((failure as Error).message).toContain("held by a delivery running right now")
		expect((failure as Error).message).toContain("This dead record stays where it is")
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

		expect((failure as Error).message).toContain("billing.dead:1")
	})
})

import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { createJobs, defineHandler, defineJob } from "@/index"
import { memoryDriver } from "@/testing/index"

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

		expect(dead?.id).toBe("billing:1")
		expect(mailed?.id).toBe("mail:1")
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
		expect(dead[0]?.id).toBe("billing:1")
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
		expect(replayed.id).toBe("2")
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

		const failure = await jobs.dead.replay("billing:7").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("billing:7")
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

	test("discard drops the dead job, and refuses an id no dead job answers to", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["billing"] })

		await jobs.start([declined])
		await jobs.enqueue(chargeCard, { cents: "500" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list("billing")

		await jobs.dead.discard(dead?.id ?? "")

		expect(await jobs.dead.list("billing")).toEqual([])

		const failure = await jobs.dead.discard("billing:1").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("billing:1")
	})

	test("carries a dead job through list and discard when its queue name holds a colon", async () => {
		const driver = memoryDriver()

		const scoped = defineJob({
			name: "billing.charge",
			queue: "tenant:acme:billing",
			payload: type({ cents: "string.numeric.parse" }),
			delivery: { attempts: 1 },
		})

		const jobs = createJobs({ driver, deadQueues: [scoped.queue] })

		await jobs.start([
			defineHandler(scoped, async () => {
				throw new Error("the card was declined")
			}),
		])

		await jobs.enqueue(scoped, { cents: "500" })
		await driver.drain().catch(() => {})

		const [dead] = await jobs.dead.list(scoped.queue)

		expect(dead?.id).toBe("tenant:acme:billing:1")

		await jobs.dead.discard(dead?.id ?? "")

		expect(await jobs.dead.list(scoped.queue)).toEqual([])
	})
})

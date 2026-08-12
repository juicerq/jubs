import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { type Job, type JobType, Queue } from "bullmq"
import IORedis from "ioredis"
import { childJob, createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { composeJobId } from "@/JobId"
import { CANCEL_KEY_PREFIX, IDEMPOTENCY_KEY_PREFIX } from "@/RedisDriver"
import { scoped, storedId } from "./namespace"

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379"

const workerConnection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })
const inspectorConnection = new IORedis(REDIS_URL)

const opened: Queue[] = []
const leased: string[] = []

function leaseKey(jobName: string, key: string): string {
	const lease = `${IDEMPOTENCY_KEY_PREFIX}${jobName}:${key}`

	leased.push(lease)

	return lease
}

function inspect(queue: string): Queue {
	const handle = new Queue(queue, { connection: inspectorConnection })

	opened.push(handle)

	return handle
}

async function freshQueues(...queues: string[]): Promise<Queue[]> {
	const handles = queues.flatMap((queue) => [inspect(queue), inspect(`${queue}.dead`)])

	for (const handle of handles) {
		await handle.obliterate({ force: true })
	}

	return handles
}

const WAITING: JobType[] = ["wait", "prioritized"]

let flowReads = 0

function countingDriver() {
	const driver = redisDriver(workerConnection)

	return {
		...driver,
		flow: {
			...driver.flow,
			read(queue: string, id: string) {
				flowReads += 1

				return driver.flow.read(queue, id)
			},
		},
	}
}

async function waitFor(reached: () => boolean | Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 10_000

	while (Date.now() < deadline) {
		if (await reached()) {
			return
		}

		await Bun.sleep(25)
	}

	throw new Error("the executions expected by this test did not arrive in time")
}

async function waitForFinished(queue: Queue, id: string): Promise<Job> {
	let finished: Job | undefined

	await waitFor(async () => {
		finished = await queue.getJob(id)

		return !!finished?.finishedOn
	})

	if (!finished) {
		throw new Error(`job ${id} on ${queue.name} did not finish in time`)
	}

	return finished
}

afterAll(async () => {
	if (leased.length) {
		await inspectorConnection.del(...leased)
	}

	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))
	await Promise.all([workerConnection.quit(), inspectorConnection.quit()])
})

describe("a flow over redis", () => {
	test("runs the parent only after every child, with their results at hand", async () => {
		const reportQueue = scoped("jubs.test.flow.report")

		const fetchRows = defineJob({
			name: scoped("report.fetch"),
			queue: reportQueue,
			payload: type({ source: "string" }),
			result: type({ at: "string.date.parse", rows: "number" }),
		})

		const buildReport = defineJob({
			name: scoped("report.build"),
			queue: reportQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(reportQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []
		const read = Promise.withResolvers<{ at: Date; rows: number }[]>()

		const runtime = await jobs.start([
			defineHandler(fetchRows, async (data) => {
				ran.push(data.source)

				return { at: "2026-01-02T03:04:05.000Z", rows: data.source === "ledger" ? 2 : 7 }
			}),
			defineHandler(buildReport, async (_data, context) => {
				ran.push("build")
				read.resolve([...(await context.children(fetchRows))])
			}),
		])

		const enqueued = await jobs.flow(
			buildReport,
			{ id: "rep-1" },
			{
				children: [
					childJob(fetchRows, { source: "ledger" }),
					childJob(fetchRows, { source: "invoices" }),
				],
			},
		)

		const parent = await waitForFinished(live as Queue, storedId(enqueued))
		const results = (await read.promise).sort((one, other) => one.rows - other.rows)

		expect(ran).toEqual(expect.arrayContaining(["ledger", "invoices"]))
		expect(ran[2]).toBe("build")
		expect(await parent.getState()).toBe("completed")
		expect(results).toHaveLength(2)
		expect(results[0]?.rows).toBe(2)
		expect(results[1]?.rows).toBe(7)
		expect(results[0]?.at).toBeInstanceOf(Date)
		expect(results[0]?.at.toISOString()).toBe("2026-01-02T03:04:05.000Z")

		await runtime.close()
	}, 30_000)

	test("carries a grandchild's result up to its own parent, across two queues", async () => {
		const topQueue = scoped("jubs.test.flow.top")
		const midQueue = scoped("jubs.test.flow.mid")

		const readLeaf = defineJob({
			name: scoped("chain.leaf"),
			queue: topQueue,
			payload: type({ id: "string" }),
			result: type({ cents: "number" }),
		})

		const sumMid = defineJob({
			name: scoped("chain.mid"),
			queue: midQueue,
			payload: type({ id: "string" }),
			result: type({ total: "number" }),
		})

		const closeTop = defineJob({
			name: scoped("chain.top"),
			queue: topQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(topQueue, midQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const closed = Promise.withResolvers<{ total: number }[]>()

		const runtime = await jobs.start([
			defineHandler(readLeaf, async () => ({ cents: 250 })),
			defineHandler(sumMid, async (_data, context) => {
				const leaves = await context.children(readLeaf)

				return { total: leaves.reduce((sum, leaf) => sum + leaf.cents, 0) }
			}),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve([...(await context.children(sumMid))])
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "chain-1" },
			{
				children: [
					childJob(
						sumMid,
						{ id: "chain-1" },
						{
							children: [
								childJob(readLeaf, { id: "leaf-a" }),
								childJob(readLeaf, { id: "leaf-b" }),
							],
						},
					),
				],
			},
		)

		const parent = await waitForFinished(live as Queue, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual([{ total: 500 }])

		await runtime.close()
	}, 30_000)

	test("keeps two child definitions sharing one queue apart", async () => {
		const parentQueue = scoped("jubs.test.flow.shared-parent")
		const sharedQueue = scoped("jubs.test.flow.shared")

		const chargeCard = defineJob({
			name: scoped("shared.charge"),
			queue: sharedQueue,
			payload: type({ id: "string" }),
			result: type({ paid: "number" }),
		})

		const reserveFunds = defineJob({
			name: scoped("shared.reserve"),
			queue: sharedQueue,
			payload: type({ id: "string" }),
			result: type({ held: "number" }),
		})

		const settle = defineJob({
			name: scoped("shared.settle"),
			queue: parentQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(parentQueue, sharedQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const settled = Promise.withResolvers<{ charged: unknown[]; reserved: unknown[] }>()

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => ({ paid: 900 })),
			defineHandler(reserveFunds, async () => ({ held: 100 })),
			defineHandler(settle, async (_data, context) => {
				settled.resolve({
					charged: [...(await context.children(chargeCard))],
					reserved: [...(await context.children(reserveFunds))],
				})
			}),
		])

		const enqueued = await jobs.flow(
			settle,
			{ id: "set-1" },
			{
				children: [
					childJob(chargeCard, { id: "set-1" }),
					childJob(reserveFunds, { id: "set-1" }),
					childJob(reserveFunds, { id: "set-2" }),
				],
			},
		)

		await waitForFinished(live as Queue, storedId(enqueued))

		expect(await settled.promise).toEqual({
			charged: [{ paid: 900 }],
			reserved: [{ held: 100 }, { held: 100 }],
		})

		await runtime.close()
	}, 30_000)

	test("fails a child whose returned value its own result schema refuses, without another attempt", async () => {
		const parentQueue = scoped("jubs.test.flow.refused-parent")
		const childQueue = scoped("jubs.test.flow.refused")

		const renderPdf = defineJob({
			name: scoped("refused.render"),
			queue: childQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
			delivery: { attempts: 3 },
		})

		const sendInvoice = defineJob({
			name: scoped("refused.send"),
			queue: parentQueue,
			payload: type({ id: "string" }),
		})

		const [, , children] = await freshQueues(parentQueue, childQueue)
		const childrenQueue = children as Queue

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [childQueue],
		})

		let rendered = 0

		const runtime = await jobs.start([
			defineHandler(renderPdf, async () => {
				rendered += 1

				return { url: "not a url" }
			}),
			defineHandler(sendInvoice, async () => undefined),
		])

		await jobs.flow(
			sendInvoice,
			{ id: "inv-1" },
			{ children: [childJob(renderPdf, { id: "inv-1" })] },
		)

		await waitFor(async () => (await childrenQueue.getFailed()).length === 1)

		const [failed] = await childrenQueue.getFailed()
		const buried = await jobs.dead.list(childQueue)

		expect(rendered).toBe(1)
		expect(failed?.attemptsMade).toBe(1)
		expect(failed?.failedReason).toContain(renderPdf.name)
		expect(buried).toHaveLength(1)
		expect(buried[0]?.reason).toBe("unrecoverable")

		await runtime.close()
	}, 30_000)

	test("buries the parent with the reason child_dead, keeping what the other children returned", async () => {
		const flowQueue = scoped("jubs.test.flow.dead")

		const chargeCard = defineJob({
			name: scoped("dead.charge"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			delivery: { attempts: 1 },
		})

		const renderPdf = defineJob({
			name: scoped("dead.render"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
		})

		const sendInvoice = defineJob({
			name: scoped("dead.send"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
		})

		let sent = 0

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				throw new Error("the card was declined")
			}),
			defineHandler(renderPdf, async () => ({ url: "https://a.example/inv-1.pdf" })),
			defineHandler(sendInvoice, async () => {
				sent += 1
			}),
		])

		const enqueued = await jobs.flow(
			sendInvoice,
			{ id: "inv-1" },
			{ children: [childJob(chargeCard, { id: "inv-1" }), childJob(renderPdf, { id: "inv-1" })] },
		)

		const parent = await waitForFinished(live as Queue, storedId(enqueued))
		const buried = await jobs.dead.list(flowQueue)
		const parentEntry = buried.find((entry) => entry.envelope.name === sendInvoice.name)

		expect(sent).toBe(0)
		expect(await parent.getState()).toBe("failed")
		expect(parentEntry?.jobId).toBe(enqueued.id)
		expect(parentEntry?.reason).toBe("child_dead")
		expect(parentEntry?.error.message).toContain(chargeCard.name)
		expect(parentEntry?.children).toEqual([
			{ queue: flowQueue, name: renderPdf.name, value: { url: "https://a.example/inv-1.pdf" } },
		])

		await runtime.close()
	}, 30_000)

	test("buries the parent and the grandparent when a grandchild fails for good", async () => {
		const flowQueue = scoped("jubs.test.flow.nested-dead")

		const readLeaf = defineJob({
			name: scoped("nested.leaf"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			delivery: { attempts: 1 },
		})

		const sumMid = defineJob({
			name: scoped("nested.mid"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const closeTop = defineJob({
			name: scoped("nested.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
		})

		const runtime = await jobs.start([
			defineHandler(readLeaf, async () => {
				throw new Error("the ledger is closed")
			}),
			defineHandler(sumMid, async () => undefined),
			defineHandler(closeTop, async () => undefined),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "chain-1" },
			{
				children: [
					childJob(sumMid, { id: "chain-1" }, { children: [childJob(readLeaf, { id: "leaf-a" })] }),
				],
			},
		)

		await waitForFinished(live as Queue, storedId(enqueued))
		await waitFor(async () => (await jobs.dead.list(flowQueue)).length === 3)

		const buried = await jobs.dead.list(flowQueue)
		const byName = new Map(buried.map((entry) => [entry.envelope.name, entry]))

		expect(byName.get(readLeaf.name)?.reason).toBe("attempts_exhausted")
		expect(byName.get(sumMid.name)?.reason).toBe("child_dead")
		expect(byName.get(closeTop.name)?.reason).toBe("child_dead")
		expect(byName.get(sumMid.name)?.error.message).toContain(readLeaf.name)
		expect(byName.get(closeTop.name)?.error.message).toContain(sumMid.name)

		await runtime.close()
	}, 30_000)

	test("refuses a definition declaring unique before anything reaches redis", async () => {
		const parentQueue = scoped("jubs.test.flow.unique-parent")
		const childQueue = scoped("jubs.test.flow.unique")

		const reconcileOnce = defineJob({
			name: scoped("unique.reconcile"),
			queue: childQueue,
			payload: type({ id: "string" }),
			delivery: { unique: { key: (data) => data.id, mode: "keepFirst" } },
		})

		const sendInvoice = defineJob({
			name: scoped("unique.send"),
			queue: parentQueue,
			payload: type({ id: "string" }),
		})

		const [parents, , children] = await freshQueues(parentQueue, childQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const failure = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [childJob(reconcileOnce, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain(reconcileOnce.name)
		expect((failure as Error).message).toContain("uniqueness does not apply inside a flow")
		expect(await (parents as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
		expect(await (children as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
	}, 30_000)

	test("reads no children, and no flow state, for a job enqueued outside a flow", async () => {
		const plainQueue = scoped("jubs.test.flow.plain")

		const renderPdf = defineJob({
			name: scoped("plain.render"),
			queue: plainQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
		})

		const sendInvoice = defineJob({
			name: scoped("plain.send"),
			queue: plainQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(plainQueue)

		const jobs = createJobs({ driver: countingDriver() })
		const read = Promise.withResolvers<unknown[]>()

		const runtime = await jobs.start([
			defineHandler(renderPdf, async () => ({ url: "https://a.example/inv-1.pdf" })),
			defineHandler(sendInvoice, async (_data, context) => {
				read.resolve([...(await context.children(renderPdf))])
			}),
		])

		flowReads = 0

		const enqueued = await jobs.enqueue(sendInvoice, { id: "inv-1" })

		await waitForFinished(live as Queue, storedId(enqueued))

		expect(await read.promise).toEqual([])
		expect(flowReads).toBe(0)

		await runtime.close()
	}, 30_000)
})

describe("cancelling over redis", () => {
	test("refuses a flow root whose descendant is running, and cancels nothing", async () => {
		const flowQueue = scoped("jubs.test.flow.cancel-busy")

		const holdLine = defineJob({
			name: scoped("cancel.hold"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ held: "number" }),
		})

		const closeTop = defineJob({
			name: scoped("cancel.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const running = Promise.withResolvers<AbortSignal>()
		const release = Promise.withResolvers<void>()
		const closed = Promise.withResolvers<{ held: number }[]>()

		const runtime = await jobs.start([
			defineHandler(holdLine, async (_data, context) => {
				running.resolve(context.signal)

				await release.promise

				return { held: 1 }
			}),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve([...(await context.children(holdLine))])
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "hold-1" },
			{ children: [childJob(holdLine, { id: "hold-1" })] },
		)

		const signal = await running.promise
		const reached = await jobs.cancel(enqueued.id)
		const marks = await inspectorConnection.keys(`${CANCEL_KEY_PREFIX}{${flowQueue}}:*`)

		expect(reached).toEqual({ outcome: "children_running" })
		expect(signal.aborted).toBe(false)
		expect((await jobs.get(enqueued.id))?.state).toBe("waiting_children")
		expect(marks).toEqual([])

		release.resolve()

		const parent = await waitForFinished(live as Queue, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual([{ held: 1 }])

		await runtime.close()
	}, 30_000)

	test("removes a flow root no descendant is running, with the children it waits on", async () => {
		const flowQueue = scoped("jubs.test.flow.cancel-idle")

		const holdLine = defineJob({
			name: scoped("idle.hold"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const closeTop = defineJob({
			name: scoped("idle.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "idle-1" },
			{ children: [childJob(holdLine, { id: "idle-1" })] },
		)

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "removed" })
		expect(await jobs.get(enqueued.id)).toBeUndefined()
		expect(await (live as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
	}, 30_000)

	test("aborts an ordinary job that is already running", async () => {
		const plainQueue = scoped("jubs.test.flow.cancel-plain")

		const holdLine = defineJob({
			name: scoped("plain.hold"),
			queue: plainQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(plainQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const running = Promise.withResolvers<AbortSignal>()
		const release = Promise.withResolvers<void>()

		const runtime = await jobs.start([
			defineHandler(holdLine, async (_data, context) => {
				running.resolve(context.signal)

				await release.promise

				if (context.signal.aborted) {
					throw context.signal.reason
				}
			}),
		])

		const enqueued = await jobs.enqueue(holdLine, { id: "plain-1" })
		const signal = await running.promise

		expect(await jobs.cancel(enqueued.id)).toEqual({ outcome: "aborting" })

		await waitFor(() => signal.aborted)

		release.resolve()

		const finished = await waitForFinished(live as Queue, storedId(enqueued))

		expect(await finished.getState()).toBe("failed")

		await runtime.close()
	}, 30_000)
})

describe("an idempotencyKey inside a flow over redis", () => {
	test("is refused on a node that waits on children, before anything reaches redis", async () => {
		const parentQueue = scoped("jubs.test.flow.idem-parent")
		const childQueue = scoped("jubs.test.flow.idem-child")

		const renderPdf = defineJob({
			name: scoped("idem.render"),
			queue: childQueue,
			payload: type({ id: "string" }),
		})

		const sendInvoice = defineJob({
			name: scoped("idem.send"),
			queue: parentQueue,
			payload: type({ id: "string" }),
			idempotencyKey: (data) => data.id,
		})

		const [parents, , children] = await freshQueues(parentQueue, childQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const failure = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [childJob(renderPdf, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain(sendInvoice.name)
		expect((failure as Error).message).toContain("waits on children")
		expect(await (parents as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
		expect(await (children as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
	}, 30_000)

	test("stays in force on a leaf, and replays its result for a later job", async () => {
		const flowQueue = scoped("jubs.test.flow.idem-leaf")

		const settleInvoice = defineJob({
			name: scoped("idem.settle"),
			queue: flowQueue,
			payload: type({ invoiceId: "string" }),
			result: type({ total: "number" }),
			idempotencyKey: (data) => data.invoiceId,
		})

		const closeTop = defineJob({
			name: scoped("idem.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)
		await inspectorConnection.del(leaseKey(settleInvoice.name, "inv-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []
		const closed = Promise.withResolvers<{ total: number }[]>()

		const runtime = await jobs.start([
			defineHandler(settleInvoice, async (data) => {
				ran.push(data.invoiceId)

				return { total: 500 }
			}),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve([...(await context.children(settleInvoice))])
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "inv-1" },
			{ children: [childJob(settleInvoice, { invoiceId: "inv-1" })] },
		)

		await waitForFinished(live as Queue, storedId(enqueued))

		expect(await closed.promise).toEqual([{ total: 500 }])

		const repeated = await jobs.enqueue(settleInvoice, { invoiceId: "inv-1" })
		const replayed = await waitForFinished(live as Queue, storedId(repeated))

		expect(ran).toEqual(["inv-1"])
		expect(replayed.returnvalue).toEqual({ total: 500 })

		await runtime.close()
	}, 30_000)
})

describe("a buried flow job over redis", () => {
	test("refuses a replay, and is repaired by retrying the child and then the parent", async () => {
		const flowQueue = scoped("jubs.test.flow.repair")

		const chargeCard = defineJob({
			name: scoped("repair.charge"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ paid: "number" }),
			delivery: { attempts: 1 },
		})

		const sendInvoice = defineJob({
			name: scoped("repair.send"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
			definitions: [chargeCard, sendInvoice],
		})

		const sent: unknown[][] = []
		let declining = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				if (declining) {
					throw new Error("the card was declined")
				}

				return { paid: 900 }
			}),
			defineHandler(sendInvoice, async (_data, context) => {
				sent.push([...(await context.children(chargeCard))])
			}),
		])

		const enqueued = await jobs.flow(
			sendInvoice,
			{ id: "inv-1" },
			{ children: [childJob(chargeCard, { id: "inv-1" })] },
		)

		await waitForFinished(live as Queue, storedId(enqueued))
		await waitFor(async () => (await jobs.dead.list(flowQueue)).length === 2)

		const buried = await jobs.dead.list(flowQueue)
		const parentEntry = buried.find((entry) => entry.envelope.name === sendInvoice.name)
		const before = await (live as Queue).getJobCounts()

		const refusal = await jobs.dead.replay(parentEntry?.id ?? "").catch((error: unknown) => error)

		expect((refusal as Error).message).toContain(sendInvoice.name)
		expect((refusal as Error).message).toContain("part of a flow")
		expect(await jobs.dead.list(flowQueue)).toHaveLength(2)
		expect(await (live as Queue).getJobCounts()).toEqual(before)
		expect(sent).toEqual([])

		declining = false

		const childId = parentEntry?.error.message.match(/\(([^)]+)\)/)?.[1] ?? ""

		expect(childId).toContain(chargeCard.name)
		expect(await jobs.retry(childId)).toEqual({ outcome: "retried" })

		await waitFor(async () => (await jobs.get(childId))?.state === "completed")

		expect(await jobs.retry(parentEntry?.jobId ?? "")).toEqual({ outcome: "retried" })

		await waitFor(() => sent.length > 0)

		expect(sent[0]).toEqual([{ paid: 900 }])

		await runtime.close()
	}, 30_000)
})

describe("a child swept by removeOnComplete over redis", () => {
	test("still reaches its parent after redis dropped its hash", async () => {
		const flowQueue = scoped("jubs.test.flow.swept")

		const readFast = defineJob({
			name: scoped("swept.fast"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ rows: "number" }),
			delivery: { keepCompletedForMs: 1_000 },
		})

		const readSlow = defineJob({
			name: scoped("swept.slow"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ rows: "number" }),
		})

		const sweepQueue = defineJob({
			name: scoped("swept.sweep"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			delivery: { keepCompletedForMs: 1_000 },
		})

		const closeTop = defineJob({
			name: scoped("swept.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const fast = Promise.withResolvers<string>()
		const release = Promise.withResolvers<void>()
		const closed = Promise.withResolvers<{ fast: unknown[]; slow: unknown[] }>()

		const runtime = await jobs.start([
			defineHandler(readFast, async (_data, context) => {
				fast.resolve(context.id)

				return { rows: 2 }
			}),
			defineHandler(readSlow, async () => {
				await release.promise

				return { rows: 7 }
			}),
			defineHandler(sweepQueue, async () => undefined),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve({
					fast: [...(await context.children(readFast))],
					slow: [...(await context.children(readSlow))],
				})
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "swept-1" },
			{ children: [childJob(readFast, { id: "fast-1" }), childJob(readSlow, { id: "slow-1" })] },
		)

		const fastId = storedId({ id: await fast.promise })

		await waitForFinished(live as Queue, fastId)
		await Bun.sleep(1_100)

		const sweeper = await jobs.enqueue(sweepQueue, { id: "sweep-1" })

		await waitForFinished(live as Queue, storedId(sweeper))
		await waitFor(async () => !(await (live as Queue).getJob(fastId)))

		expect(await (live as Queue).getJob(fastId)).toBeUndefined()

		release.resolve()

		const parent = await waitForFinished(live as Queue, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual({ fast: [{ rows: 2 }], slow: [{ rows: 7 }] })

		await runtime.close()
	}, 30_000)
})

describe("a definition name inside a flow over redis", () => {
	test("is refused for a child when it holds a character the child id cannot carry", async () => {
		const parentQueue = scoped("jubs.test.flow.name-parent")
		const childQueue = scoped("jubs.test.flow.name-child")

		const sendInvoice = defineJob({
			name: scoped("name.send"),
			queue: parentQueue,
			payload: type({ id: "string" }),
		})

		const colonName = defineJob({
			name: scoped("name:render"),
			queue: childQueue,
			payload: type({ id: "string" }),
		})

		const markerName = defineJob({
			name: scoped("name~render"),
			queue: childQueue,
			payload: type({ id: "string" }),
		})

		const [parents, , children] = await freshQueues(parentQueue, childQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const colon = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [childJob(colonName, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		const marker = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [childJob(markerName, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((colon as Error).message).toContain(colonName.name)
		expect((colon as Error).message).toContain('holds ":"')
		expect((marker as Error).message).toContain(markerName.name)
		expect((marker as Error).message).toContain('holds "~"')
		expect(await (parents as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
		expect(await (children as Queue).getJobCounts()).toMatchObject({
			waiting: 0,
			active: 0,
			delayed: 0,
			completed: 0,
			failed: 0,
			"waiting-children": 0,
		})
	}, 30_000)

	test("is left alone for a root, which carries no id of ours", async () => {
		const flowQueue = scoped("jubs.test.flow.name-root")

		const renderPdf = defineJob({
			name: scoped("root.render"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
		})

		const sendInvoice = defineJob({
			name: scoped("root:send"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const closed = Promise.withResolvers<unknown[]>()

		const runtime = await jobs.start([
			defineHandler(renderPdf, async () => ({ url: "https://a.example/inv-1.pdf" })),
			defineHandler(sendInvoice, async (_data, context) => {
				closed.resolve([...(await context.children(renderPdf))])
			}),
		])

		const enqueued = await jobs.flow(
			sendInvoice,
			{ id: "inv-1" },
			{ children: [childJob(renderPdf, { id: "inv-1" })] },
		)

		const parent = await waitForFinished(live as Queue, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual([{ url: "https://a.example/inv-1.pdf" }])

		await runtime.close()
	}, 30_000)
})

describe("cancelling one child of a flow over redis", () => {
	test("removes a child that has not started, and the parent runs short by it", async () => {
		const parentQueue = scoped("jubs.test.flow.branch-parent")
		const childQueue = scoped("jubs.test.flow.branch-child")

		const readRows = defineJob({
			name: scoped("branch.read"),
			queue: childQueue,
			payload: type({ source: "string" }),
			result: type({ rows: "number" }),
		})

		const holdWorker = defineJob({
			name: scoped("branch.hold"),
			queue: childQueue,
			payload: type({ id: "string" }),
		})

		const buildReport = defineJob({
			name: scoped("branch.build"),
			queue: parentQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
		})

		const closeTop = defineJob({
			name: scoped("branch.top"),
			queue: parentQueue,
			payload: type({ id: "string" }),
		})

		const [parents, , children] = await freshQueues(parentQueue, childQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [parentQueue, childQueue],
		})

		const release = Promise.withResolvers<void>()
		const closed = Promise.withResolvers<{ read: unknown[]; built: unknown[] }>()

		const runtime = await jobs.start(
			[
				defineHandler(readRows, async () => ({ rows: 2 })),
				defineHandler(holdWorker, async () => {
					await release.promise
				}),
				defineHandler(buildReport, async () => ({ url: "https://a.example/rep-1.pdf" })),
				defineHandler(closeTop, async (_data, context) => {
					closed.resolve({
						read: [...(await context.children(readRows))],
						built: [...(await context.children(buildReport))],
					})
				}),
			],
			{ queues: { [childQueue]: { concurrency: 1 } } },
		)

		const hog = await jobs.enqueue(holdWorker, { id: "hog-1" })

		await waitFor(async () => (await jobs.get(hog.id))?.state === "active")

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "rep-1" },
			{
				children: [
					childJob(buildReport, { id: "rep-1" }),
					childJob(readRows, { source: "ledger" }),
				],
			},
		)

		await waitFor(async () => (await (children as Queue).getJobs(WAITING)).length === 1)

		const [waiting] = await (children as Queue).getJobs(WAITING)
		const victim = composeJobId(childQueue, waiting?.id ?? "")

		expect(waiting?.name).toBe(readRows.name)
		expect((await jobs.get(victim))?.state).toBe("waiting")
		expect(await jobs.cancel(victim)).toEqual({ outcome: "removed" })

		release.resolve()

		const parent = await waitForFinished(parents as Queue, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual({
			read: [],
			built: [{ url: "https://a.example/rep-1.pdf" }],
		})
		expect(await jobs.dead.list(parentQueue)).toEqual([])
		expect(await jobs.dead.list(childQueue)).toEqual([])

		await runtime.close()
	}, 30_000)

	test("aborts the same child once it is active, and buries the parent with child_dead", async () => {
		const parentQueue = scoped("jubs.test.flow.branch-active-parent")
		const childQueue = scoped("jubs.test.flow.branch-active-child")

		const readRows = defineJob({
			name: scoped("active.read"),
			queue: childQueue,
			payload: type({ source: "string" }),
			result: type({ rows: "number" }),
		})

		const buildReport = defineJob({
			name: scoped("active.build"),
			queue: parentQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
		})

		const closeTop = defineJob({
			name: scoped("active.top"),
			queue: parentQueue,
			payload: type({ id: "string" }),
		})

		const [parents] = await freshQueues(parentQueue, childQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [parentQueue, childQueue],
		})

		const running = Promise.withResolvers<{ id: string; signal: AbortSignal }>()
		const release = Promise.withResolvers<void>()
		let closed = 0

		const runtime = await jobs.start([
			defineHandler(readRows, async (_data, context) => {
				running.resolve({ id: context.id, signal: context.signal })

				await release.promise

				if (context.signal.aborted) {
					throw context.signal.reason
				}

				return { rows: 2 }
			}),
			defineHandler(buildReport, async () => ({ url: "https://a.example/rep-1.pdf" })),
			defineHandler(closeTop, async () => {
				closed += 1
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "rep-1" },
			{
				children: [
					childJob(buildReport, { id: "rep-1" }),
					childJob(readRows, { source: "ledger" }),
				],
			},
		)

		const victim = await running.promise

		expect((await jobs.get(victim.id))?.state).toBe("active")
		expect(await jobs.cancel(victim.id)).toEqual({ outcome: "aborting" })

		await waitFor(() => victim.signal.aborted)

		release.resolve()

		const parent = await waitForFinished(parents as Queue, storedId(enqueued))
		const [childEntry] = await jobs.dead.list(childQueue)
		const [parentEntry] = await jobs.dead.list(parentQueue)

		expect(closed).toBe(0)
		expect(await parent.getState()).toBe("failed")
		expect(childEntry?.envelope.name).toBe(readRows.name)
		expect(childEntry?.reason).toBe("cancelled")
		expect(parentEntry?.envelope.name).toBe(closeTop.name)
		expect(parentEntry?.reason).toBe("child_dead")
		expect(parentEntry?.error.message).toContain(readRows.name)

		await runtime.close()
	}, 30_000)
})

describe("a child result schema over redis", () => {
	test("runs twice, so a transform json does not survive kills the parent for good", async () => {
		const flowQueue = scoped("jubs.test.flow.twice")

		const countRows = defineJob({
			name: scoped("twice.count"),
			queue: flowQueue,
			payload: type({ source: "string" }),
			result: type({ rows: "string.numeric.parse" }),
		})

		const closeTop = defineJob({
			name: scoped("twice.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			delivery: { attempts: 3 },
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
		})

		const read: unknown[] = []

		const runtime = await jobs.start([
			defineHandler(countRows, async () => ({ rows: "500" })),
			defineHandler(closeTop, async (_data, context) => {
				read.push(...(await context.children(countRows)))
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "rep-1" },
			{ children: [childJob(countRows, { source: "ledger" })] },
		)

		const parent = await waitForFinished(live as Queue, storedId(enqueued))
		const completed = await (live as Queue).getCompleted()
		const counted = completed.find((job) => job.name === countRows.name)
		const [buried] = await jobs.dead.list(flowQueue)

		expect(counted?.name).toBe(countRows.name)
		expect(counted?.returnvalue).toEqual({ rows: 500 })
		expect(read).toEqual([])
		expect(await parent.getState()).toBe("failed")
		expect(parent.attemptsMade).toBe(1)
		expect(parent.failedReason).toContain(countRows.name)
		expect(buried?.envelope.name).toBe(closeTop.name)
		expect(buried?.reason).toBe("unrecoverable")

		await runtime.close()
	}, 30_000)

	test("passes both times when the transform survives json", async () => {
		const flowQueue = scoped("jubs.test.flow.twice-idempotent")

		const countRows = defineJob({
			name: scoped("idempotent.count"),
			queue: flowQueue,
			payload: type({ source: "string" }),
			result: type({ at: "string.date.parse" }),
		})

		const closeTop = defineJob({
			name: scoped("idempotent.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
		})

		const [live] = await freshQueues(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
		})

		const closed = Promise.withResolvers<{ at: Date }[]>()

		const runtime = await jobs.start([
			defineHandler(countRows, async () => ({ at: "2026-01-02T03:04:05.000Z" })),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve([...(await context.children(countRows))])
			}),
		])

		const enqueued = await jobs.flow(
			closeTop,
			{ id: "rep-1" },
			{ children: [childJob(countRows, { source: "ledger" })] },
		)

		const parent = await waitForFinished(live as Queue, storedId(enqueued))
		const completed = await (live as Queue).getCompleted()
		const counted = completed.find((job) => job.name === countRows.name)
		const read = await closed.promise

		expect(counted?.name).toBe(countRows.name)
		expect(counted?.returnvalue).toEqual({ at: "2026-01-02T03:04:05.000Z" })
		expect(read[0]?.at).toBeInstanceOf(Date)
		expect(read[0]?.at.toISOString()).toBe("2026-01-02T03:04:05.000Z")
		expect(await parent.getState()).toBe("completed")
		expect(await jobs.dead.list(flowQueue)).toEqual([])

		await runtime.close()
	}, 30_000)
})

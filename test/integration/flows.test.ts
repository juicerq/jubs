import { afterAll, describe, expect, test } from "bun:test"
import { type } from "arktype"
import { type JobType, Queue } from "bullmq"
import IORedis from "ioredis"
import { deadQueueName } from "@/Dead"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { composeJobId } from "@/JobId"
import { CANCEL_KEY_PREFIX, IDEMPOTENCY_KEY_PREFIX } from "@/RedisDriver"
import { errorOf } from "../support/Failures"
import { liveId } from "../support/JobIds"
import { waitFor, waitForFinished } from "../support/Wait"
import { scoped, storedId } from "./namespace"
import { REDIS_URL } from "./redis"

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

async function freshQueue(queue: string): Promise<Queue> {
	const live = inspect(queue)
	const dead = inspect(deadQueueName(queue))

	await live.obliterate({ force: true })
	await dead.obliterate({ force: true })

	return live
}

const WAITING: JobType[] = ["wait", "prioritized"]

const EMPTY_COUNTS = {
	waiting: 0,
	active: 0,
	delayed: 0,
	completed: 0,
	failed: 0,
	"waiting-children": 0,
}

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

/** The id of the child a failed parent's error names, so a test can retry it. */
function failedChildId(message: string | undefined): string {
	return message?.match(/\(([^)]+)\)/)?.[1] ?? ""
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
	test("runs the parent only after every child, with the slot it declared at hand", async () => {
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
			awaits: { sources: [fetchRows] },
		})

		const live = await freshQueue(reportQueue)

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
				read.resolve([...context.children.sources])
			}),
		])

		const enqueued = await jobs.enqueue(
			buildReport,
			{ id: "rep-1" },
			{ sources: [{ source: "ledger" }, { source: "invoices" }] },
		)

		const parent = await waitForFinished(live, storedId(enqueued))
		const results = (await read.promise).sort((one, other) => one.rows - other.rows)
		const completed = await live.getCompleted()
		const fetched = completed.find((job) => job.name === fetchRows.name)

		expect(ran).toEqual(expect.arrayContaining(["ledger", "invoices"]))
		expect(ran[2]).toBe("build")
		expect(await parent.getState()).toBe("completed")
		expect(results).toHaveLength(2)
		expect(results[0]?.rows).toBe(2)
		expect(results[1]?.rows).toBe(7)
		expect(results[0]?.at).toBeInstanceOf(Date)
		expect(results[0]?.at.toISOString()).toBe("2026-01-02T03:04:05.000Z")
		expect(fetched?.returnvalue).toMatchObject({ at: "2026-01-02T03:04:05.000Z" })

		await runtime.close()
	}, 30_000)

	test("keeps two slots holding the same definition apart, from the child id up to the handler", async () => {
		const mergeQueue = scoped("jubs.test.flow.merge")
		const splitQueue = scoped("jubs.test.flow.split")

		const fetchRows = defineJob({
			name: scoped("split.fetch"),
			queue: splitQueue,
			payload: type({ source: "string" }),
			result: type({ source: "string", rows: "number" }),
		})

		const mergeRows = defineJob({
			name: scoped("split.merge"),
			queue: mergeQueue,
			payload: type({ id: "string" }),
			awaits: { ledger: fetchRows, invoices: [fetchRows] },
		})

		const [parents, children] = await Promise.all([freshQueue(mergeQueue), freshQueue(splitQueue)])

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const rowsOf: Record<string, number> = { ledger: 2, "invoices-a": 7, "invoices-b": 9 }

		const merged = Promise.withResolvers<{
			ledger: { source: string; rows: number }
			invoices: { source: string; rows: number }[]
		}>()

		const runtime = await jobs.start([
			defineHandler(fetchRows, async (data) => ({
				source: data.source,
				rows: rowsOf[data.source] ?? 0,
			})),
			defineHandler(mergeRows, async (_data, context) => {
				merged.resolve({
					ledger: context.children.ledger,
					invoices: [...context.children.invoices].sort((one, other) => one.rows - other.rows),
				})
			}),
		])

		const enqueued = await jobs.enqueue(
			mergeRows,
			{ id: "merge-1" },
			{
				ledger: { source: "ledger" },
				invoices: [{ source: "invoices-a" }, { source: "invoices-b" }],
			},
		)

		const parent = await waitForFinished(parents, storedId(enqueued))
		const read = await merged.promise
		const ids = (await children.getCompleted()).map((job) => job.id ?? "")

		expect(await parent.getState()).toBe("completed")
		expect(read.ledger).toEqual({ source: "ledger", rows: 2 })
		expect(read.invoices).toEqual([
			{ source: "invoices-a", rows: 7 },
			{ source: "invoices-b", rows: 9 },
		])
		expect(ids.filter((id) => id.startsWith("ledger~"))).toHaveLength(1)
		expect(ids.filter((id) => id.startsWith("invoices~"))).toHaveLength(2)
		expect(ids.filter((id) => id.includes(fetchRows.name))).toEqual([])

		await runtime.close()
	}, 30_000)

	test("carries a grandchild's result to the top, three levels deep and across queues", async () => {
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
			awaits: { leaves: [readLeaf] },
		})

		const closeTop = defineJob({
			name: scoped("chain.top"),
			queue: topQueue,
			payload: type({ id: "string" }),
			awaits: { mid: sumMid },
		})

		const [live] = await Promise.all([freshQueue(topQueue), freshQueue(midQueue)])

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const closed = Promise.withResolvers<{ total: number }>()

		const runtime = await jobs.start([
			defineHandler(readLeaf, async () => ({ cents: 250 })),
			defineHandler(sumMid, async (_data, context) => ({
				total: context.children.leaves.reduce((sum, leaf) => sum + leaf.cents, 0),
			})),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve(context.children.mid)
			}),
		])

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "chain-1" },
			{
				mid: { data: { id: "chain-1" }, awaits: { leaves: [{ id: "leaf-a" }, { id: "leaf-b" }] } },
			},
		)

		const parent = await waitForFinished(live, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual({ total: 500 })

		await runtime.close()
	}, 30_000)

	test("reads no flow state at all for a job whose definition waits on nothing", async () => {
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

		const closeTop = defineJob({
			name: scoped("plain.top"),
			queue: plainQueue,
			payload: type({ id: "string" }),
			awaits: { render: renderPdf },
		})

		const live = await freshQueue(plainQueue)

		const jobs = createJobs({ driver: countingDriver() })
		const origins: string[] = []
		const plain = Promise.withResolvers<Record<string, never>>()

		const runtime = await jobs.start([
			defineHandler(renderPdf, async () => ({ url: "https://a.example/inv-1.pdf" })),
			defineHandler(sendInvoice, async (_data, context) => {
				origins.push(context.origin)
				plain.resolve(context.children)
			}),
			defineHandler(closeTop, async (_data, context) => {
				origins.push(context.origin)
			}),
		])

		flowReads = 0

		const ordinary = await jobs.enqueue(sendInvoice, { id: "inv-1" })

		await waitForFinished(live, storedId(ordinary))

		const readsAfterOrdinary = flowReads

		expect(await plain.promise).toEqual({})
		expect(origins).toEqual(["direct"])
		expect(readsAfterOrdinary).toBe(0)

		const flowed = await jobs.enqueue(closeTop, { id: "inv-2" }, { render: { id: "inv-2" } })

		await waitForFinished(live, storedId(flowed))

		expect(origins).toEqual(["direct", "flow"])
		expect(flowReads).toBeGreaterThan(readsAfterOrdinary)

		await runtime.close()
	}, 30_000)

	test("refuses a flow whose child declares unique, before anything reaches redis", async () => {
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
			awaits: { reconcile: reconcileOnce },
		})

		const [parents, children] = await Promise.all([freshQueue(parentQueue), freshQueue(childQueue)])

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const failure = await jobs
			.enqueue(sendInvoice, { id: "inv-1" }, { reconcile: { id: "inv-1" } })
			.catch((error: unknown) => error)

		expect(errorOf(failure).message).toContain(reconcileOnce.name)
		expect(errorOf(failure).message).toContain("uniqueness does not apply inside a flow")
		expect(await parents.getJobCounts()).toMatchObject(EMPTY_COUNTS)
		expect(await children.getJobCounts()).toMatchObject(EMPTY_COUNTS)
	}, 30_000)

	test("runs children whose definition names hold the characters a child id is cut on", async () => {
		const flowQueue = scoped("jubs.test.flow.name")

		const colonName = defineJob({
			name: scoped("name:render"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ url: "string.url" }),
		})

		const markerName = defineJob({
			name: scoped("name~count"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ rows: "number" }),
		})

		const sendInvoice = defineJob({
			name: scoped("name:send"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			awaits: { rendered: colonName, counted: markerName },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const closed = Promise.withResolvers<{ rendered: { url: string }; counted: { rows: number } }>()

		const runtime = await jobs.start([
			defineHandler(colonName, async () => ({ url: "https://a.example/inv-1.pdf" })),
			defineHandler(markerName, async () => ({ rows: 3 })),
			defineHandler(sendInvoice, async (_data, context) => {
				closed.resolve({ rendered: context.children.rendered, counted: context.children.counted })
			}),
		])

		const enqueued = await jobs.enqueue(
			sendInvoice,
			{ id: "inv-1" },
			{ rendered: { id: "inv-1" }, counted: { id: "inv-1" } },
		)

		const parent = await waitForFinished(live, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual({
			rendered: { url: "https://a.example/inv-1.pdf" },
			counted: { rows: 3 },
		})

		await runtime.close()
	}, 30_000)
})

describe("a child that fails every attempt over redis", () => {
	test("buries the parent with child_dead, keeping the other slots and naming the failed child", async () => {
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
			awaits: { charge: chargeCard, render: renderPdf },
		})

		const live = await freshQueue(flowQueue)

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

		const enqueued = await jobs.enqueue(
			sendInvoice,
			{ id: "inv-1" },
			{ charge: { id: "inv-1" }, render: { id: "inv-1" } },
		)

		const parent = await waitForFinished(live, storedId(enqueued))

		await waitFor(async () => (await jobs.dead.list(flowQueue)).length === 2)

		const buried = await jobs.dead.list(flowQueue)
		const parentEntry = buried.find((entry) => entry.envelope.name === sendInvoice.name)

		if (parentEntry?.reason !== "child_dead") {
			throw new Error(`the parent was buried for ${parentEntry?.reason}, not over its children`)
		}

		const childId = failedChildId(parentEntry.error.message)

		expect(sent).toBe(0)
		expect(await parent.getState()).toBe("failed")
		expect(parentEntry.jobId).toBe(liveId(enqueued))
		expect(parentEntry.error.message).toContain(`"charge", which runs "${chargeCard.name}"`)
		expect(parentEntry.children).toEqual([
			{ slot: "render", value: { url: "https://a.example/inv-1.pdf" } },
		])
		expect(childId).toStartWith(`${flowQueue}:charge~`)
		expect((await jobs.get(childId))?.state).toBe("failed")

		await runtime.close()
	}, 30_000)

	test("buries the parent and the grandparent, each naming the slot below it", async () => {
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
			awaits: { leaf: readLeaf },
		})

		const closeTop = defineJob({
			name: scoped("nested.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			awaits: { mid: sumMid },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
		})

		const runtime = await jobs.start([
			defineHandler(readLeaf, async () => {
				throw new Error("the ledger is closed")
			}),
			defineHandler(sumMid, async () => {}),
			defineHandler(closeTop, async () => {}),
		])

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "chain-1" },
			{ mid: { data: { id: "chain-1" }, awaits: { leaf: { id: "leaf-a" } } } },
		)

		await waitForFinished(live, storedId(enqueued))
		await waitFor(async () => (await jobs.dead.list(flowQueue)).length === 3)

		const buried = await jobs.dead.list(flowQueue)
		const byName = new Map(buried.map((entry) => [entry.envelope.name, entry]))

		expect(byName.get(readLeaf.name)?.reason).toBe("attempts_exhausted")
		expect(byName.get(sumMid.name)?.reason).toBe("child_dead")
		expect(byName.get(closeTop.name)?.reason).toBe("child_dead")
		expect(byName.get(sumMid.name)?.error.message).toContain(
			`"leaf", which runs "${readLeaf.name}"`,
		)
		expect(byName.get(closeTop.name)?.error.message).toContain(`"mid", which runs "${sumMid.name}"`)
		expect(failedChildId(byName.get(closeTop.name)?.error.message)).toStartWith(`${flowQueue}:mid~`)

		await runtime.close()
	}, 30_000)

	test("fails a child whose own result schema refuses what it returned, without another attempt", async () => {
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
			awaits: { render: renderPdf },
		})

		const [, children] = await Promise.all([freshQueue(parentQueue), freshQueue(childQueue)])

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
			defineHandler(sendInvoice, async () => {}),
		])

		await jobs.enqueue(sendInvoice, { id: "inv-1" }, { render: { id: "inv-1" } })

		await waitFor(async () => (await children.getFailed()).length === 1)

		const [failed] = await children.getFailed()
		const buried = await jobs.dead.list(childQueue)

		expect(rendered).toBe(1)
		expect(failed?.attemptsMade).toBe(1)
		expect(failed?.failedReason).toContain(renderPdf.name)
		expect(buried).toHaveLength(1)
		expect(buried[0]?.reason).toBe("unrecoverable")

		await runtime.close()
	}, 30_000)

	test("fails the parent for good over a value its slot refuses, with the handler never entered", async () => {
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
			awaits: { counted: countRows },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
		})

		let entered = 0

		const runtime = await jobs.start([
			defineHandler(countRows, async () => ({ rows: "500" })),
			defineHandler(closeTop, async () => {
				entered += 1
			}),
		])

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "rep-1" },
			{ counted: { source: "ledger" } },
		)

		const parent = await waitForFinished(live, storedId(enqueued))
		const completed = await live.getCompleted()
		const counted = completed.find((job) => job.name === countRows.name)
		const [buried] = await jobs.dead.list(flowQueue)

		expect(counted?.returnvalue).toEqual({ rows: 500 })
		expect(entered).toBe(0)
		expect(await parent.getState()).toBe("failed")
		expect(parent.attemptsMade).toBe(1)
		expect(parent.failedReason).toContain(countRows.name)
		expect(buried?.envelope.name).toBe(closeTop.name)
		expect(buried?.reason).toBe("unrecoverable")

		await runtime.close()
	}, 30_000)
})

describe("a child swept by removeOnComplete over redis", () => {
	test("still fills the slot it was enqueued under after redis dropped its hash", async () => {
		const flowQueue = scoped("jubs.test.flow.swept")

		const readRows = defineJob({
			name: scoped("swept.read"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ rows: "number" }),
			delivery: { keepCompletedForMs: 1_000 },
		})

		const sweepAlong = defineJob({
			name: scoped("swept.sweep"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			delivery: { keepCompletedForMs: 1_000 },
		})

		const closeTop = defineJob({
			name: scoped("swept.top"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			awaits: { fast: readRows, slow: readRows },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const fast = Promise.withResolvers<string>()
		const release = Promise.withResolvers<void>()
		const closed = Promise.withResolvers<{ fast: { rows: number }; slow: { rows: number } }>()

		const runtime = await jobs.start([
			defineHandler(readRows, async (data, context) => {
				if (data.id === "slow-1") {
					await release.promise

					return { rows: 7 }
				}

				fast.resolve(context.id)

				return { rows: 2 }
			}),
			defineHandler(sweepAlong, async () => {}),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve({ fast: context.children.fast, slow: context.children.slow })
			}),
		])

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "swept-1" },
			{ fast: { id: "fast-1" }, slow: { id: "slow-1" } },
		)

		const fastId = storedId({ id: await fast.promise })

		expect(fastId).toStartWith("fast~")

		await waitForFinished(live, fastId)
		await Bun.sleep(1_100)

		const sweeper = await jobs.enqueue(sweepAlong, { id: "sweep-1" })

		await waitForFinished(live, storedId(sweeper))
		await waitFor(async () => !(await live.getJob(fastId)))

		expect(await live.getJob(fastId)).toBeUndefined()

		release.resolve()

		const parent = await waitForFinished(live, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual({ fast: { rows: 2 }, slow: { rows: 7 } })

		await runtime.close()
	}, 30_000)
})

describe("cancelling a flow over redis", () => {
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
			awaits: { hold: holdLine },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const running = Promise.withResolvers<AbortSignal>()
		const release = Promise.withResolvers<void>()
		const closed = Promise.withResolvers<{ held: number }>()

		const runtime = await jobs.start([
			defineHandler(holdLine, async (_data, context) => {
				running.resolve(context.signal)

				await release.promise

				return { held: 1 }
			}),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve(context.children.hold)
			}),
		])

		const enqueued = await jobs.enqueue(closeTop, { id: "hold-1" }, { hold: { id: "hold-1" } })
		const id = liveId(enqueued)

		const signal = await running.promise
		const reached = await jobs.cancel(id)
		const marks = await inspectorConnection.keys(`${CANCEL_KEY_PREFIX}{${flowQueue}}:*`)

		expect(reached).toEqual({ outcome: "children_running" })
		expect(signal.aborted).toBe(false)
		expect((await jobs.get(id))?.state).toBe("waiting_children")
		expect(marks).toEqual([])

		release.resolve()

		const parent = await waitForFinished(live, storedId(enqueued))

		expect(await parent.getState()).toBe("completed")
		expect(await closed.promise).toEqual({ held: 1 })

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
			awaits: { hold: holdLine },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({ driver: redisDriver(workerConnection) })

		const enqueued = await jobs.enqueue(closeTop, { id: "idle-1" }, { hold: { id: "idle-1" } })
		const id = liveId(enqueued)

		expect(await jobs.cancel(id)).toEqual({ outcome: "removed" })
		expect(await jobs.get(id)).toBeUndefined()
		expect(await live.getJobCounts()).toMatchObject(EMPTY_COUNTS)
	}, 30_000)

	test("removes one child that has not started, and the parent is buried for running short", async () => {
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
			awaits: { built: buildReport, read: [readRows] },
		})

		const [parents, children] = await Promise.all([freshQueue(parentQueue), freshQueue(childQueue)])

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [parentQueue, childQueue],
		})

		const release = Promise.withResolvers<void>()
		let closed = 0

		const runtime = await jobs.start(
			[
				defineHandler(readRows, async () => ({ rows: 2 })),
				defineHandler(holdWorker, async () => {
					await release.promise
				}),
				defineHandler(buildReport, async () => ({ url: "https://a.example/rep-1.pdf" })),
				defineHandler(closeTop, async () => {
					closed += 1
				}),
			],
			{ queues: { [childQueue]: { concurrency: 1 } } },
		)

		const hog = await jobs.enqueue(holdWorker, { id: "hog-1" })

		await waitFor(async () => (await jobs.get(liveId(hog)))?.state === "active")

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "rep-1" },
			{ built: { id: "rep-1" }, read: [{ source: "ledger" }] },
		)

		await waitFor(async () => (await children.getJobs(WAITING)).length === 1)

		const [waiting] = await children.getJobs(WAITING)
		const victim = composeJobId(childQueue, waiting?.id ?? "")

		expect(waiting?.name).toBe(readRows.name)
		expect(waiting?.id).toStartWith("read~")
		expect((await jobs.get(victim))?.state).toBe("waiting")
		expect(await jobs.cancel(victim)).toEqual({ outcome: "removed" })

		release.resolve()

		const parent = await waitForFinished(parents, storedId(enqueued))

		await waitFor(async () => (await jobs.dead.list(parentQueue)).length === 1)

		const [buried] = await jobs.dead.list(parentQueue)

		expect(closed).toBe(0)
		expect(await parent.getState()).toBe("failed")
		expect(parent.attemptsMade).toBe(1)
		expect(buried?.envelope.name).toBe(closeTop.name)
		expect(buried?.reason).toBe("children_short")
		expect(buried?.error.message).toContain('"read" was given 1 and 0 arrived')
		expect(await jobs.dead.list(childQueue)).toEqual([])

		await runtime.close()
	}, 30_000)

	test("aborts one child once it is active, and buries the parent with child_dead", async () => {
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
			awaits: { built: buildReport, read: readRows },
		})

		const [parents] = await Promise.all([freshQueue(parentQueue), freshQueue(childQueue)])

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

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "rep-1" },
			{ built: { id: "rep-1" }, read: { source: "ledger" } },
		)

		const victim = await running.promise

		expect((await jobs.get(victim.id))?.state).toBe("active")
		expect(await jobs.cancel(victim.id)).toEqual({ outcome: "aborting" })

		await waitFor(() => victim.signal.aborted)

		release.resolve()

		const parent = await waitForFinished(parents, storedId(enqueued))
		const [childEntry] = await jobs.dead.list(childQueue)
		const [parentEntry] = await jobs.dead.list(parentQueue)

		expect(closed).toBe(0)
		expect(await parent.getState()).toBe("failed")
		expect(childEntry?.envelope.name).toBe(readRows.name)
		expect(childEntry?.reason).toBe("cancelled")
		expect(parentEntry?.envelope.name).toBe(closeTop.name)
		expect(parentEntry?.reason).toBe("child_dead")
		expect(parentEntry?.error.message).toContain(`"read", which runs "${readRows.name}"`)

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
			awaits: { charge: chargeCard },
		})

		const live = await freshQueue(flowQueue)

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
			definitions: [chargeCard, sendInvoice],
		})

		const sent: unknown[] = []
		let declining = true

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				if (declining) {
					throw new Error("the card was declined")
				}

				return { paid: 900 }
			}),
			defineHandler(sendInvoice, async (_data, context) => {
				sent.push(context.children.charge)
			}),
		])

		const enqueued = await jobs.enqueue(sendInvoice, { id: "inv-1" }, { charge: { id: "inv-1" } })

		await waitForFinished(live, storedId(enqueued))
		await waitFor(async () => (await jobs.dead.list(flowQueue)).length === 2)

		const buried = await jobs.dead.list(flowQueue)
		const parentEntry = buried.find((entry) => entry.envelope.name === sendInvoice.name)
		const before = await live.getJobCounts()

		const refusal = await jobs.dead.replay(parentEntry?.id ?? "").catch((error: unknown) => error)

		expect(errorOf(refusal).message).toContain(sendInvoice.name)
		expect(errorOf(refusal).message).toContain("part of a flow")
		expect(errorOf(refusal).message).toContain("jobs.retry")
		expect(errorOf(refusal).message).toContain("jobs.dead.discard")
		expect(await jobs.dead.list(flowQueue)).toHaveLength(2)
		expect(await live.getJobCounts()).toEqual(before)
		expect(sent).toEqual([])

		declining = false

		const childId = failedChildId(parentEntry?.error.message)

		expect(childId).toStartWith(`${flowQueue}:charge~`)
		expect(await jobs.retry(childId)).toEqual({ outcome: "retried" })

		await waitFor(async () => (await jobs.get(childId))?.state === "completed")

		expect(await jobs.retry(parentEntry?.jobId ?? "")).toEqual({ outcome: "retried" })

		await waitFor(() => sent.length > 0)

		expect(sent[0]).toEqual({ paid: 900 })

		await runtime.close()
	}, 30_000)
})

describe("a retried child slower than the parent's attempts over redis", () => {
	test("waits for it instead of spending the parent's attempts, and runs the handler once", async () => {
		const flowQueue = scoped("jubs.test.flow.slow-retry")

		const chargeCard = defineJob({
			name: scoped("slow.charge"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			result: type({ paid: "number" }),
			delivery: { attempts: 1 },
		})

		const sendInvoice = defineJob({
			name: scoped("slow.send"),
			queue: flowQueue,
			payload: type({ id: "string" }),
			delivery: { attempts: 2, backoff: { type: "exponential", delayMs: 250 } },
			awaits: { charge: chargeCard },
		})

		const live = await freshQueue(flowQueue)

		const sent: unknown[] = []
		const attemptsFailed: string[] = []
		let declining = true

		const jobs = createJobs({
			driver: redisDriver(workerConnection),
			deadQueues: [flowQueue],
			definitions: [chargeCard, sendInvoice],
			hooks: {
				onAttemptFailed: (event) => {
					attemptsFailed.push(event.name)
				},
			},
		})

		const runtime = await jobs.start([
			defineHandler(chargeCard, async () => {
				if (declining) {
					throw new Error("the card was declined")
				}

				await Bun.sleep(4_000)

				return { paid: 900 }
			}),
			defineHandler(sendInvoice, async (_data, context) => {
				sent.push(context.children.charge)
			}),
		])

		const enqueued = await jobs.enqueue(sendInvoice, { id: "inv-1" }, { charge: { id: "inv-1" } })

		await waitForFinished(live, storedId(enqueued))
		await waitFor(async () => (await jobs.dead.list(flowQueue)).length === 2)

		const parentEntry = (await jobs.dead.list(flowQueue)).find(
			(entry) => entry.envelope.name === sendInvoice.name,
		)

		const childId = failedChildId(parentEntry?.error.message)
		const reportedBefore = [...attemptsFailed]

		declining = false

		expect(await jobs.retry(childId)).toEqual({ outcome: "retried" })
		expect(await jobs.retry(parentEntry?.jobId ?? "")).toEqual({ outcome: "retried" })

		await waitFor(async () => (await jobs.get(childId))?.state === "completed")
		await waitFor(() => sent.length > 0)
		await waitFor(async () => (await jobs.get(parentEntry?.jobId ?? ""))?.state === "completed")

		const buried = await jobs.dead.list(flowQueue)
		const parentBurials = buried.filter((entry) => entry.envelope.name === sendInvoice.name)

		expect(sent).toEqual([{ paid: 900 }])
		expect(parentBurials.map((entry) => entry.reason)).toEqual(["child_dead"])
		expect(attemptsFailed).toEqual(reportedBefore)

		await runtime.close()
	}, 60_000)
})

describe("an idempotencyKey inside a flow over redis", () => {
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
			awaits: { settle: settleInvoice },
		})

		const live = await freshQueue(flowQueue)
		await inspectorConnection.del(leaseKey(settleInvoice.name, "inv-1"))

		const jobs = createJobs({ driver: redisDriver(workerConnection) })
		const ran: string[] = []
		const closed = Promise.withResolvers<{ total: number }>()

		const runtime = await jobs.start([
			defineHandler(settleInvoice, async (data) => {
				ran.push(data.invoiceId)

				return { total: 500 }
			}),
			defineHandler(closeTop, async (_data, context) => {
				closed.resolve(context.children.settle)
			}),
		])

		const enqueued = await jobs.enqueue(
			closeTop,
			{ id: "inv-1" },
			{ settle: { invoiceId: "inv-1" } },
		)

		await waitForFinished(live, storedId(enqueued))

		expect(await closed.promise).toEqual({ total: 500 })

		const repeated = await jobs.enqueue(settleInvoice, { invoiceId: "inv-1" })
		const replayed = await waitForFinished(live, storedId(repeated))

		expect(ran).toEqual(["inv-1"])
		expect(replayed.returnvalue).toEqual({ total: 500 })

		await runtime.close()
	}, 30_000)
})

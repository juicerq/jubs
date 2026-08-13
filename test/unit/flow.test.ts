import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { ChildDeadError, ChildrenPendingError, ChildrenShortError, childrenFor } from "@/Flow"
import {
	createJobs,
	DELIVERY_DEFAULTS,
	defineHandler,
	defineJob,
	type Envelope,
	type FlowChildNode,
	type JobDefinition,
} from "@/index"
import { composeChildId, readChildSlot } from "@/JobId"
import { ResultError } from "@/Result"
import { memoryDriver } from "@/testing/index"
import { liveId } from "../support/JobIds"
import { recordingDriver } from "./support/RecordingDriver"

const fetchXmls = defineJob({
	name: "nfe.fetch-xmls",
	queue: "nfe",
	payload: type({ page: "number" }),
	result: type({ xml: "string" }),
})

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
	result: type({ count: "number" }),
	awaits: { row: signRow },
})

const zipExport = defineJob({
	name: "nfe.zip",
	queue: "nfe",
	payload: type({ run_id: "string" }),
	result: type({ url: "string.url" }),
	awaits: { xmls: [fetchXmls], manifest: buildManifest },
})

const twinned = defineJob({
	name: "nfe.twinned",
	queue: "nfe",
	payload: type({ run_id: "string" }),
	awaits: { left: fetchXmls, right: fetchXmls },
})

const leafNode: FlowChildNode = {
	slot: "row",
	queue: "signing",
	envelope: { v: 1, name: "nfe.sign-row", data: { row: 7 }, origin: "flow" },
	delivery: DELIVERY_DEFAULTS,
	children: [],
}

describe("enqueuing a definition that waits on children", () => {
	test("adds the whole tree in one step, every node under the slot it fills", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const enqueued = await jobs.enqueue(
			zipExport,
			{ run_id: "r-1" },
			{
				xmls: [{ page: 1 }, { page: 2 }],
				manifest: { data: { run_id: "r-1" }, awaits: { row: { row: 7 } } },
			},
		)

		expect(liveId(enqueued)).toBe("nfe:1")
		expect(driver.flows).toEqual([
			{
				queue: "nfe",
				envelope: {
					v: 1,
					name: "nfe.zip",
					data: { run_id: "r-1" },
					origin: "flow",
					slots: { xmls: 2, manifest: 1 },
				},
				delivery: DELIVERY_DEFAULTS,
				children: [
					{
						slot: "xmls",
						queue: "nfe",
						envelope: { v: 1, name: "nfe.fetch-xmls", data: { page: 1 }, origin: "flow" },
						delivery: DELIVERY_DEFAULTS,
						children: [],
					},
					{
						slot: "xmls",
						queue: "nfe",
						envelope: { v: 1, name: "nfe.fetch-xmls", data: { page: 2 }, origin: "flow" },
						delivery: DELIVERY_DEFAULTS,
						children: [],
					},
					{
						slot: "manifest",
						queue: "documents",
						envelope: {
							v: 1,
							name: "nfe.manifest",
							data: { run_id: "r-1" },
							origin: "flow",
							slots: { row: 1 },
						},
						delivery: DELIVERY_DEFAULTS,
						children: [leafNode],
					},
				],
			},
		])
	})

	test("keeps two slots that hold the very same definition apart", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.enqueue(twinned, { run_id: "r-1" }, { left: { page: 1 }, right: { page: 2 } })

		expect(driver.flows[0]?.children.map((child) => [child.slot, child.envelope.data])).toEqual([
			["left", { page: 1 }],
			["right", { page: 2 }],
		])
	})

	test("writes a count of 0 for a many slot filled with an empty array, and enqueues no child for it", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.enqueue(
			zipExport,
			{ run_id: "r-1" },
			{ xmls: [], manifest: { data: { run_id: "r-1" }, awaits: { row: { row: 7 } } } },
		)

		expect(driver.flows[0]?.envelope.slots).toEqual({ xmls: 0, manifest: 1 })
		expect(driver.flows[0]?.children.map((child) => child.slot)).toEqual(["manifest"])
	})

	test("resolves the delivery of every node against its own definition", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const patient = defineJob({
			name: "nfe.patient",
			queue: "nfe",
			payload: type({ page: "number" }),
			delivery: { attempts: 2 },
		})

		const hurried = defineJob({
			name: "nfe.hurried",
			queue: "nfe",
			payload: type({ run_id: "string" }),
			delivery: { priority: 1 },
			awaits: { page: patient },
		})

		await jobs.enqueue(hurried, { run_id: "r-1" }, { page: { page: 1 } })

		expect(driver.flows[0]?.delivery.priority).toBe(1)
		expect(driver.flows[0]?.children[0]?.delivery.attempts).toBe(2)
	})

	test("refuses a child payload its schema rejects, and enqueues nothing at all", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(
				zipExport,
				{ run_id: "r-1" },
				{
					xmls: [{ page: "one" as unknown as number }],
					manifest: { data: { run_id: "r-1" }, awaits: { row: { row: 7 } } },
				},
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("nfe.fetch-xmls")
		expect(driver.flows).toEqual([])
	})

	test("refuses a grandchild payload its schema rejects, and enqueues nothing at all", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(
				zipExport,
				{ run_id: "r-1" },
				{
					xmls: [{ page: 1 }],
					manifest: {
						data: { run_id: "r-1" },
						awaits: { row: { row: "seven" as unknown as number } },
					},
				},
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("nfe.sign-row")
		expect(driver.flows).toEqual([])
	})

	test("enqueues a definition whose `awaits` declares no slot as an ordinary job", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const plain = defineJob({
			name: "nfe.plain",
			queue: "nfe",
			payload: type({ run_id: "string" }),
			awaits: {},
		})

		const enqueued = await jobs.enqueue(plain, { run_id: "r-1" })

		expect(liveId(enqueued)).toBe("nfe:1")
		expect(driver.flows).toEqual([])
		expect(driver.enqueued).toEqual([
			{
				queue: "nfe",
				envelope: { v: 1, name: "nfe.plain", data: { run_id: "r-1" }, origin: "direct" },
				delivery: DELIVERY_DEFAULTS,
			},
		])
	})

	test("keeps the `idempotencyKey` of a leaf, whose input really is its payload alone", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const settleOnce = defineJob({
			name: "nfe.settle",
			queue: "signing",
			payload: type({ row: "number" }),
			idempotencyKey: (data) => String(data.row),
		})

		const settling = defineJob({
			name: "nfe.settling",
			queue: "nfe",
			payload: type({ run_id: "string" }),
			awaits: { row: settleOnce },
		})

		await jobs.enqueue(settling, { run_id: "r-1" }, { row: { row: 7 } })

		expect(driver.flows[0]?.children[0]?.envelope.name).toBe("nfe.settle")
	})
})

describe("what defineJob refuses beside `awaits`", () => {
	test("refuses `unique`, because uniqueness does not apply inside a flow", () => {
		expect(() =>
			defineJob({
				name: "nfe.once",
				queue: "nfe",
				payload: type({ run_id: "string" }),
				delivery: { unique: { key: (data) => data.run_id, mode: "keepFirst" } },
				awaits: { xmls: [fetchXmls] },
			}),
		).toThrow("uniqueness does not apply inside a flow")
	})

	test("refuses `idempotencyKey`, because the payload alone does not name the run", () => {
		expect(() =>
			defineJob({
				name: "nfe.settled",
				queue: "nfe",
				payload: type({ run_id: "string" }),
				idempotencyKey: (data) => data.run_id,
				awaits: { xmls: [fetchXmls] },
			}),
		).toThrow("idempotencyKey")
	})

	test("refuses a slot name holding a colon, which a job id cannot carry", () => {
		expect(() =>
			defineJob({
				name: "nfe.colon",
				queue: "nfe",
				payload: type({ run_id: "string" }),
				awaits: { "nfe:xmls": fetchXmls },
			}),
		).toThrow('the slot "nfe:xmls", whose name holds ":"')
	})

	test("refuses a slot name holding the character that cuts a child id", () => {
		expect(() =>
			defineJob({
				name: "nfe.tilde",
				queue: "nfe",
				payload: type({ run_id: "string" }),
				awaits: { "xml~s": fetchXmls },
			}),
		).toThrow('the slot "xml~s", whose name holds "~"')
	})
})

describe("what an enqueue refuses that defineJob cannot see", () => {
	test("refuses a `unique` definition filling a slot anywhere in the tree", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const reconcileOnce = defineJob({
			name: "nfe.reconcile",
			queue: "signing",
			payload: type({ row: "number" }),
			delivery: { unique: { key: (data) => String(data.row), mode: "keepFirst" } },
		})

		const manifestOfOne = defineJob({
			name: "nfe.manifest-of-one",
			queue: "documents",
			payload: type({ run_id: "string" }),
			awaits: { row: reconcileOnce },
		})

		const exportOfOne = defineJob({
			name: "nfe.export-of-one",
			queue: "nfe",
			payload: type({ run_id: "string" }),
			awaits: { manifest: manifestOfOne },
		})

		const failure = await jobs
			.enqueue(
				exportOfOne,
				{ run_id: "r-1" },
				{ manifest: { data: { run_id: "r-1" }, awaits: { row: { row: 7 } } } },
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("nfe.reconcile")
		expect((failure as Error).message).toContain("uniqueness does not apply inside a flow")
		expect(driver.flows).toEqual([])
	})

	test("refuses a `unique` a delivery function returns, which defineJob never reads", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const uniqueLater = defineJob({
			name: "nfe.unique-later",
			queue: "nfe",
			payload: type({ run_id: "string" }),
			delivery: ({ data }) => ({ unique: { key: () => data.run_id, mode: "keepFirst" } }),
			awaits: { xmls: [fetchXmls] },
		})

		const failure = await jobs
			.enqueue(uniqueLater, { run_id: "r-1" }, { xmls: [{ page: 1 }] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("nfe.unique-later")
		expect((failure as Error).message).toContain("uniqueness does not apply inside a flow")
		expect(driver.flows).toEqual([])
	})
})

/**
 * A definition read through its own widest type declares slots the call site
 * cannot see, which is what makes the runtime assertion the only thing left
 * between a short input and a parent running over it.
 */
const widened: JobDefinition = zipExport

describe("the slots an enqueue has to fill", () => {
	test("refuses an enqueue that passes nothing for the slots at all", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs.enqueue(widened, { run_id: "r-1" }).catch((error: unknown) => error)

		expect((failure as Error).message).toContain('the slots "xmls", "manifest"')
		expect((failure as Error).message).toContain("passed nothing for them")
		expect(driver.flows).toEqual([])
	})

	test("refuses an enqueue that leaves one declared slot out", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(widened, { run_id: "r-1" }, { xmls: [{ page: 1 }] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain('passed nothing for "manifest"')
		expect(driver.flows).toEqual([])
	})

	test("refuses one value for a slot declared as an array", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(
				widened,
				{ run_id: "r-1" },
				{ xmls: { page: 1 }, manifest: { data: {}, awaits: {} } },
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain(
			'waits on many of "nfe.fetch-xmls" in the slot "xmls", and this enqueue passed one value',
		)
		expect(driver.flows).toEqual([])
	})

	test("refuses an array for a slot declared as exactly one", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(widened, { run_id: "r-1" }, { xmls: [{ page: 1 }], manifest: [] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain(
			'waits on exactly one "nfe.manifest" in the slot "manifest", and this enqueue passed an array',
		)
		expect(driver.flows).toEqual([])
	})
})

describe("the id one child of a flow is stored under", () => {
	test("carries the slot it fills, and reads back as that slot", () => {
		expect(readChildSlot(composeChildId("manifest"))).toBe("manifest")
	})

	test("keeps a slot name apart from the slot name it is a substring of", () => {
		expect(readChildSlot(composeChildId("xml"))).toBe("xml")
		expect(readChildSlot(composeChildId("xmls"))).toBe("xmls")
	})

	test("gives two children of one slot ids of their own", () => {
		expect(composeChildId("xmls")).not.toBe(composeChildId("xmls"))
	})

	test("reads no slot out of an id that carries none", () => {
		expect(readChildSlot("42")).toBe("")
	})
})

const zipEnvelope: Envelope = {
	v: 1,
	name: "nfe.zip",
	data: { run_id: "r-1" },
	origin: "flow",
}

const manifestResult = { slot: "manifest", value: { count: 2 } }

describe("childrenFor", () => {
	test("gives an empty object for a definition that waits on nothing, and reads no flow", async () => {
		const driver = recordingDriver()

		const children = await childrenFor({
			flow: driver.flow,
			definition: fetchXmls,
			envelope: { v: 1, name: "nfe.fetch-xmls", data: { page: 1 }, origin: "flow" },
			queue: "nfe",
			id: "1",
		})

		expect(children).toEqual({})
		expect(driver.flowReads).toEqual([])
	})

	test("gives one value for a slot that holds one, and an array for a slot that holds many", async () => {
		const driver = recordingDriver()

		driver.keepFlowState({
			results: [
				{ slot: "xmls", value: { xml: "<a/>" } },
				manifestResult,
				{ slot: "xmls", value: { xml: "<b/>" } },
			],
			failures: [],
			pending: 0,
		})

		const children = await childrenFor({
			flow: driver.flow,
			definition: zipExport,
			envelope: { ...zipEnvelope, slots: { xmls: 2, manifest: 1 } },
			queue: "nfe",
			id: "1",
		})

		expect(children).toEqual({
			xmls: [{ xml: "<a/>" }, { xml: "<b/>" }],
			manifest: { count: 2 },
		})
		expect(driver.flowReads).toEqual([{ queue: "nfe", id: "1" }])
	})

	test("refuses the read while a child has not finished", async () => {
		const driver = recordingDriver()

		driver.keepFlowState({ results: [manifestResult], failures: [], pending: 1 })

		const failure = await childrenFor({
			flow: driver.flow,
			definition: zipExport,
			envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			queue: "nfe",
			id: "1",
		}).catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(ChildrenPendingError)
		expect((failure as Error).message).toContain("1 of them has not finished yet")
	})

	test("refuses the read when a child failed every attempt, carrying what the others returned", async () => {
		const driver = recordingDriver()

		driver.keepFlowState({
			results: [manifestResult],
			failures: [{ id: "nfe:xmls~ab", slot: "xmls", reason: "the source timed out" }],
			pending: 0,
		})

		const failure = await childrenFor({
			flow: driver.flow,
			definition: zipExport,
			envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			queue: "nfe",
			id: "1",
		}).catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(ChildDeadError)
		expect((failure as Error).message).toContain('"xmls", which runs "nfe.fetch-xmls"')
		expect((failure as ChildDeadError).results).toEqual([manifestResult])
	})

	test("refuses the read when a slot holds fewer children than it was enqueued with", async () => {
		const driver = recordingDriver()

		driver.keepFlowState({ results: [manifestResult], failures: [], pending: 0 })

		const failure = await childrenFor({
			flow: driver.flow,
			definition: zipExport,
			envelope: { ...zipEnvelope, slots: { xmls: 2, manifest: 1 } },
			queue: "nfe",
			id: "1",
		}).catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(ChildrenShortError)
		expect((failure as Error).message).toContain('"xmls" was given 2 and 0 arrived')
	})

	test("refuses a child value the result schema of its definition rejects", async () => {
		const driver = recordingDriver()

		driver.keepFlowState({
			results: [{ slot: "xmls", value: { xml: 7 } }, manifestResult],
			failures: [],
			pending: 0,
		})

		const failure = await childrenFor({
			flow: driver.flow,
			definition: zipExport,
			envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			queue: "nfe",
			id: "1",
		}).catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(ResultError)
		expect((failure as Error).message).toContain("nfe.fetch-xmls")
	})
})

describe("context.children", () => {
	test("reads one value for a single slot and an array for a many slot", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let read: unknown

		driver.keepFlowState({
			results: [
				{ slot: "xmls", value: { xml: "<a/>" } },
				manifestResult,
				{ slot: "xmls", value: { xml: "<b/>" } },
			],
			failures: [],
			pending: 0,
		})

		await jobs.start([
			defineHandler(zipExport, async (_data, context) => {
				read = context.children

				return { url: `https://a.example/${context.children.manifest.count}` }
			}),
		])

		await driver.deliver("nfe", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: { ...zipEnvelope, slots: { xmls: 2, manifest: 1 } },
		})

		expect(driver.flowReads).toEqual([{ queue: "nfe", id: "1" }])
		expect(read).toEqual({
			xmls: [{ xml: "<a/>" }, { xml: "<b/>" }],
			manifest: { count: 2 },
		})
	})

	test("keeps two slots that hold the very same definition apart", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let read: unknown

		driver.keepFlowState({
			results: [
				{ slot: "right", value: { xml: "<b/>" } },
				{ slot: "left", value: { xml: "<a/>" } },
			],
			failures: [],
			pending: 0,
		})

		await jobs.start([
			defineHandler(twinned, async (_data, context) => {
				read = context.children
			}),
		])

		await driver.deliver("nfe", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: { ...zipEnvelope, name: "nfe.twinned", slots: { left: 1, right: 1 } },
		})

		expect(read).toEqual({ left: { xml: "<a/>" }, right: { xml: "<b/>" } })
	})

	test("reads an empty array for a many slot the flow was enqueued with no child for", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let read: unknown

		driver.keepFlowState({ results: [manifestResult], failures: [], pending: 0 })

		await jobs.start([
			defineHandler(zipExport, async (_data, context) => {
				read = context.children

				return { url: "https://a.example/1" }
			}),
		])

		await driver.deliver("nfe", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: { ...zipEnvelope, slots: { xmls: 0, manifest: 1 } },
		})

		expect(read).toEqual({ xmls: [], manifest: { count: 2 } })
	})

	test("reads an empty object for a definition that waits on nothing", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let read: unknown

		await jobs.start([
			defineHandler(fetchXmls, async (_data, context) => {
				read = context.children

				return { xml: "<a/>" }
			}),
		])

		await driver.deliver("nfe", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: { v: 1, name: "nfe.fetch-xmls", data: { page: 1 }, origin: "flow" },
		})

		expect(read).toEqual({})
	})

	test("fails the job for good without running the handler when a result schema refuses a child value", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let ran = false

		driver.keepFlowState({
			results: [{ slot: "xmls", value: { xml: 7 } }, manifestResult],
			failures: [],
			pending: 0,
		})

		await jobs.start([
			defineHandler(zipExport, async () => {
				ran = true

				return { url: "https://a.example/1" }
			}),
		])

		const failure = await driver
			.deliver("nfe", {
				id: "1",
				attempt: 1,
				maxAttempts: 5,
				envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			})
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect((failure as Error).message).toContain("nfe.fetch-xmls")
		expect(ran).toBe(false)
	})

	test("reads the flow state of a delivery from outside a flow, and fails it for good", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		let ran = false

		await jobs.start([
			defineHandler(zipExport, async () => {
				ran = true

				return { url: "https://a.example/1" }
			}),
		])

		const failure = await driver
			.deliver("nfe", {
				id: "1",
				attempt: 1,
				maxAttempts: 5,
				envelope: { ...zipEnvelope, origin: "direct" },
			})
			.catch((error: unknown) => error)

		expect(driver.flowReads).toEqual([{ queue: "nfe", id: "1" }])
		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect((failure as Error).message).toContain(
			"carries no record of how many children they were given",
		)
		expect(ran).toBe(false)
	})
})

describe("ChildDeadError", () => {
	test("names the slot that failed and the definition the parent declared for it", () => {
		const dead = new ChildDeadError(zipExport, {
			results: [],
			failures: [{ id: "nfe:xmls~ab", slot: "xmls", reason: "the source timed out" }],
			pending: 0,
		})

		expect(dead.message).toContain('"xmls", which runs "nfe.fetch-xmls"')
		expect(dead.message).toContain("nfe:xmls~ab")
		expect(dead.message).toContain("the source timed out")
	})

	test("names a slot alone when the parent declares no such slot", () => {
		const dead = new ChildDeadError(zipExport, {
			results: [],
			failures: [{ id: "nfe:pages~ab", slot: "pages", reason: "the source timed out" }],
			pending: 0,
		})

		expect(dead.message).toContain('"pages" (nfe:pages~ab)')
		expect(dead.message).not.toContain("which runs")
	})

	test("keeps what the children that did finish returned", () => {
		const dead = new ChildDeadError(zipExport, {
			results: [manifestResult],
			failures: [{ id: "nfe:xmls~ab", slot: "xmls", reason: "the source timed out" }],
			pending: 0,
		})

		expect(dead.results).toEqual([manifestResult])
	})
})

describe("a child still in flight when the parent is delivered", () => {
	test("ends the delivery before the handler, reporting no failed attempt and burying nothing", async () => {
		const driver = recordingDriver()
		const reported: string[] = []
		const buried: string[] = []

		const jobs = createJobs({
			driver,
			deadQueues: ["nfe"],
			hooks: {
				onAttemptFailed: (event) => {
					reported.push(event.name)
				},
				onDead: (event) => {
					buried.push(event.name)
				},
			},
		})

		let ran = 0

		driver.keepFlowState({ results: [manifestResult], failures: [], pending: 1 })

		await jobs.start([
			defineHandler(zipExport, async () => {
				ran += 1

				return { url: "https://a.example/1" }
			}),
		])

		const failure = await driver
			.deliver("nfe", {
				id: "1",
				attempt: 1,
				maxAttempts: 5,
				envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			})
			.catch((error: unknown) => error)

		expect(ran).toBe(0)
		expect(failure).toBeInstanceOf(ChildrenPendingError)
		expect((failure as Error).message).toContain("1 of them has not finished yet")
		expect((failure as Error).message).toContain("spending no attempt")
		expect(reported).toEqual([])
		expect(buried).toEqual([])
	})

	test("buries nothing on the last allowed attempt, where the parent goes back to waiting-children alive", async () => {
		const driver = recordingDriver()
		const reported: string[] = []
		const buried: string[] = []

		const jobs = createJobs({
			driver,
			deadQueues: ["nfe"],
			hooks: {
				onAttemptFailed: (event) => {
					reported.push(event.name)
				},
				onDead: (event) => {
					buried.push(event.name)
				},
			},
		})

		let ran = 0

		driver.keepFlowState({ results: [manifestResult], failures: [], pending: 1 })

		await jobs.start([
			defineHandler(zipExport, async () => {
				ran += 1

				return { url: "https://a.example/1" }
			}),
		])

		const failure = await driver
			.deliver("nfe", {
				id: "1",
				attempt: 5,
				maxAttempts: 5,
				envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			})
			.catch((error: unknown) => error)

		expect(ran).toBe(0)
		expect(failure).toBeInstanceOf(ChildrenPendingError)
		expect((failure as Error).message).toContain("spending no attempt")
		expect(buried).toEqual([])
		expect(reported).toEqual([])
	})
})

describe("a flow read that fails on the way to the handler", () => {
	test("leaves the job its remaining attempts, burying nothing", async () => {
		const driver = recordingDriver()
		const buried: string[] = []
		const refused = new Error("Connection is closed.")

		const jobs = createJobs({
			driver: {
				...driver,
				flow: {
					enqueue: (root) => driver.flow.enqueue(root),

					async read() {
						throw refused
					},
				},
			},
			deadQueues: ["nfe"],
			hooks: {
				onDead: (event) => {
					buried.push(event.name)
				},
			},
		})

		await jobs.start([defineHandler(zipExport, async () => ({ url: "https://a.example/1" }))])

		const failure = await driver
			.deliver("nfe", {
				id: "1",
				attempt: 1,
				maxAttempts: 5,
				envelope: { ...zipEnvelope, slots: { xmls: 1, manifest: 1 } },
			})
			.catch((error: unknown) => error)

		expect(failure).toBe(refused)
		expect(buried).toEqual([])
	})
})

describe("dead.replay of a flow job", () => {
	test("refuses it, and names the repair path in job ids", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver, deadQueues: ["nfe"], definitions: [zipExport] })

		const dead = new ChildDeadError(zipExport, {
			results: [],
			failures: [{ id: "nfe:xmls~ab", slot: "xmls", reason: "the source timed out" }],
			pending: 0,
		})

		await driver.dead.bury("nfe", {
			jobId: "nfe:1",
			envelope: zipEnvelope,
			error: { name: dead.name, message: dead.message },
			reason: "child_dead",
		})

		const buried = await jobs.dead.list("nfe")
		const failure = await jobs.dead.replay(buried[0]?.id ?? "").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("nfe:1")
		expect((failure as Error).message).toContain("jobs.retry")
		expect((failure as Error).message).toContain("jobs.dead.discard")
		expect(await jobs.dead.list("nfe")).toHaveLength(1)
	})
})

describe("memoryDriver", () => {
	test("refuses a flow instead of running one it cannot hold back", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.enqueue(
				zipExport,
				{ run_id: "r-1" },
				{
					xmls: [{ page: 1 }],
					manifest: { data: { run_id: "r-1" }, awaits: { row: { row: 7 } } },
				},
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("memoryDriver does not simulate")
		expect((failure as Error).message).toContain("waits on children")
	})

	test("refuses to say what the children of a job it delivers settled into", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		let ran = false

		await jobs.start([
			defineHandler(zipExport, async () => {
				ran = true

				return { url: "https://a.example/1" }
			}),
		])

		await driver.enqueue({ queue: "nfe", envelope: zipEnvelope, delivery: DELIVERY_DEFAULTS })

		const failure = await driver.drain().catch((error: unknown) => error)

		expect((failure as Error).message).toContain("memoryDriver does not simulate")
		expect((failure as Error).message).toContain("children of a job that waits on children")
		expect(ran).toBe(false)
	})
})

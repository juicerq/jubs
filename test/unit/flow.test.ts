import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import {
	child,
	createJobs,
	DELIVERY_DEFAULTS,
	defineHandler,
	defineJob,
	type Envelope,
	type FlowState,
} from "@/index"
import { type MemoryDriver, memoryDriver } from "@/testing/index"
import { recordingDriver } from "./support/RecordingDriver"

const sendInvoice = defineJob({
	name: "invoice.send",
	queue: "billing",
	payload: type({ id: "string" }),
})

const renderPdf = defineJob({
	name: "invoice.render",
	queue: "documents",
	payload: type({ id: "string" }),
	result: type({ url: "string.url" }),
})

const chargeCard = defineJob({
	name: "invoice.charge",
	queue: "billing",
	payload: type({ id: "string" }),
})

const reserveFunds = defineJob({
	name: "invoice.reserve",
	queue: "billing",
	payload: type({ id: "string" }),
})

const reconcileOnce = defineJob({
	name: "invoice.reconcile",
	queue: "billing",
	payload: type({ id: "string" }),
	delivery: { unique: { key: (data) => data.id, mode: "keepFirst" } },
})

describe("jobs.flow", () => {
	test("enqueues the parent with its children nested to any depth", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const enqueued = await jobs.flow(
			sendInvoice,
			{ id: "inv-1" },
			{
				children: [
					child(renderPdf, { id: "inv-1" }),
					child(chargeCard, { id: "inv-1" }, { children: [child(reserveFunds, { id: "inv-1" })] }),
				],
			},
		)

		expect(enqueued.id).toBe("billing:1")
		expect(driver.flows).toEqual([
			{
				queue: "billing",
				envelope: { v: 1, name: "invoice.send", data: { id: "inv-1" }, origin: "flow" },
				delivery: DELIVERY_DEFAULTS,
				children: [
					{
						queue: "documents",
						envelope: { v: 1, name: "invoice.render", data: { id: "inv-1" }, origin: "flow" },
						delivery: DELIVERY_DEFAULTS,
						children: [],
					},
					{
						queue: "billing",
						envelope: { v: 1, name: "invoice.charge", data: { id: "inv-1" }, origin: "flow" },
						delivery: DELIVERY_DEFAULTS,
						children: [
							{
								queue: "billing",
								envelope: { v: 1, name: "invoice.reserve", data: { id: "inv-1" }, origin: "flow" },
								delivery: DELIVERY_DEFAULTS,
								children: [],
							},
						],
					},
				],
			},
		])
	})

	test("enqueues a parent with no children at all", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.flow(sendInvoice, { id: "inv-1" }, {})

		expect(driver.flows[0]?.children).toEqual([])
	})

	test("refuses a parent whose definition declares unique", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(reconcileOnce, { id: "inv-1" }, { children: [child(renderPdf, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.reconcile")
		expect((failure as Error).message).toContain("uniqueness does not apply inside a flow")
		expect(driver.flows).toEqual([])
	})

	test("refuses a child whose definition declares unique", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [child(reconcileOnce, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.reconcile")
		expect(driver.flows).toEqual([])
	})

	test("rejects a child payload its schema refuses, before the driver is contacted", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(
				sendInvoice,
				{ id: "inv-1" },
				{ children: [child(renderPdf, { id: 1 as unknown as string })] },
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.render")
		expect(driver.flows).toEqual([])
	})

	test("rejects a grandchild payload its schema refuses", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(
				sendInvoice,
				{ id: "inv-1" },
				{
					children: [
						child(
							chargeCard,
							{ id: "inv-1" },
							{
								children: [child(reserveFunds, { id: 1 as unknown as string })],
							},
						),
					],
				},
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.reserve")
		expect(driver.flows).toEqual([])
	})
})

const flowEnvelope: Envelope = {
	v: 1,
	name: "invoice.send",
	data: { id: "inv-1" },
	origin: "flow",
}

describe("context.children", () => {
	test("gives back the results of that definition, validated by its result schema", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const read: unknown[] = []

		driver.keepFlowState({
			results: [
				{ queue: "documents", name: "invoice.render", value: { url: "https://a.example/1" } },
				{ queue: "billing", name: "invoice.charge", value: { paid: true } },
				{ queue: "documents", name: "invoice.render", value: { url: "https://a.example/2" } },
			],
			failures: [],
		})

		await jobs.start([
			defineHandler(sendInvoice, async (_data, context) => {
				read.push(...(await context.children(renderPdf)))
			}),
		])

		await driver.deliver("billing", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: flowEnvelope,
		})

		expect(driver.flowReads).toEqual([{ queue: "billing", id: "1" }])
		expect(read).toEqual([{ url: "https://a.example/1" }, { url: "https://a.example/2" }])
	})

	test("keeps two definitions sharing a queue apart", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const read: unknown[][] = []

		driver.keepFlowState({
			results: [
				{ queue: "billing", name: "invoice.charge", value: { paid: true } },
				{ queue: "billing", name: "invoice.reserve", value: { held: 1 } },
			],
			failures: [],
		})

		await jobs.start([
			defineHandler(sendInvoice, async (_data, context) => {
				read.push(
					[...(await context.children(chargeCard))],
					[...(await context.children(reserveFunds))],
				)
			}),
		])

		await driver.deliver("billing", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: flowEnvelope,
		})

		expect(read).toEqual([[{ paid: true }], [{ held: 1 }]])
	})

	test("fails the parent unrecoverably when a child value its schema refuses comes back", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		driver.keepFlowState({
			results: [{ queue: "documents", name: "invoice.render", value: { url: "not a url" } }],
			failures: [],
		})

		await jobs.start([
			defineHandler(sendInvoice, async (_data, context) => {
				await context.children(renderPdf)
			}),
		])

		const failure = await driver
			.deliver("billing", { id: "1", attempt: 1, maxAttempts: 5, envelope: flowEnvelope })
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect((failure as Error).message).toContain("invoice.render")
	})

	test("reads no flow state at all for a delivery that comes from outside a flow", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })
		const read: unknown[] = []

		await jobs.start([
			defineHandler(sendInvoice, async (_data, context) => {
				read.push(...(await context.children(renderPdf)))
			}),
		])

		await driver.deliver("billing", {
			id: "1",
			attempt: 1,
			maxAttempts: 5,
			envelope: { ...flowEnvelope, origin: "direct" },
		})

		expect(driver.flowReads).toEqual([])
		expect(read).toEqual([])
	})
})

/**
 * The memory driver runs handlers inline and never reaches `waiting-children`,
 * so it keeps no flow state of its own. Its dead queue is real, which is what
 * a burial is read through, so the state a parent finds is stated here.
 */
function driverOnFlowState(state: FlowState): MemoryDriver {
	const driver = memoryDriver()

	return { ...driver, flow: { ...driver.flow, read: async () => state } }
}

const renderedUrl = { url: "https://a.example/1" }

describe("a child that failed every attempt", () => {
	test("buries the parent with the reason child_dead and keeps what the others returned", async () => {
		const driver = driverOnFlowState({
			results: [{ queue: "documents", name: "invoice.render", value: renderedUrl }],
			failures: [{ id: "billing:2", name: "invoice.charge", reason: "the card was declined" }],
		})

		const jobs = createJobs({ driver, deadQueues: ["billing"] })
		let ran = 0

		await jobs.start([
			defineHandler(sendInvoice, async () => {
				ran += 1
			}),
		])

		await driver.enqueue({ queue: "billing", envelope: flowEnvelope, delivery: DELIVERY_DEFAULTS })

		const failure = await driver.runNext().catch((error: unknown) => error)
		const buried = await jobs.dead.list("billing")

		expect(ran).toBe(0)
		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect((failure as Error).message).toContain("invoice.charge")
		expect(buried).toHaveLength(1)
		expect(buried[0]?.reason).toBe("child_dead")
		expect(buried[0]?.children).toEqual([
			{ queue: "documents", name: "invoice.render", value: renderedUrl },
		])
	})
})

describe("memoryDriver", () => {
	test("refuses a flow instead of running one it cannot hold back", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [child(renderPdf, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("memoryDriver does not simulate")
		expect((failure as Error).message).toContain("flow")
	})

	test("reads no children for a job it delivers", async () => {
		const driver = memoryDriver()
		const jobs = createJobs({ driver })
		const read: unknown[] = []

		await jobs.start([
			defineHandler(sendInvoice, async (_data, context) => {
				read.push(...(await context.children(renderPdf)))
			}),
		])

		await driver.enqueue({ queue: "billing", envelope: flowEnvelope, delivery: DELIVERY_DEFAULTS })
		await driver.drain()

		expect(read).toEqual([])
	})
})

const settleOnce = defineJob({
	name: "invoice.settle",
	queue: "billing",
	payload: type({ id: "string" }),
	idempotencyKey: (data) => data.id,
})

describe("idempotencyKey inside a flow", () => {
	test("refuses a node that waits on children, whose input is more than its payload", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(settleOnce, { id: "inv-1" }, { children: [child(renderPdf, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.settle")
		expect((failure as Error).message).toContain("idempotencyKey")
		expect(driver.flows).toEqual([])
	})

	test("refuses a child that waits on children of its own", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const failure = await jobs
			.flow(
				sendInvoice,
				{ id: "inv-1" },
				{
					children: [
						child(settleOnce, { id: "inv-1" }, { children: [child(renderPdf, { id: "inv-1" })] }),
					],
				},
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.settle")
		expect(driver.flows).toEqual([])
	})

	test("keeps the key of a leaf, whose input really is its payload alone", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		await jobs.flow(
			sendInvoice,
			{ id: "inv-1" },
			{ children: [child(settleOnce, { id: "inv-1" })] },
		)

		expect(driver.flows[0]?.children[0]?.envelope.name).toBe("invoice.settle")
	})
})

describe("dead.replay of a flow job", () => {
	test("refuses it, and names the repair path in job ids", async () => {
		const driver = driverOnFlowState({
			results: [],
			failures: [
				{
					id: "billing:invoice.charge~ab",
					name: "invoice.charge",
					reason: "the card was declined",
				},
			],
		})

		const jobs = createJobs({ driver, deadQueues: ["billing"], definitions: [sendInvoice] })

		await jobs.start([defineHandler(sendInvoice, async () => {})])

		await driver.enqueue({ queue: "billing", envelope: flowEnvelope, delivery: DELIVERY_DEFAULTS })
		await driver.runNext().catch(() => {})

		const buried = await jobs.dead.list("billing")
		const failure = await jobs.dead.replay(buried[0]?.id ?? "").catch((error: unknown) => error)

		expect((failure as Error).message).toContain("billing:1")
		expect((failure as Error).message).toContain("jobs.retry")
		expect((failure as Error).message).toContain("jobs.dead.discard")
		expect(buried[0]?.error.message).toContain("billing:invoice.charge~ab")
		expect(await jobs.dead.list("billing")).toHaveLength(1)
	})
})

describe("a definition name a job id cannot carry", () => {
	test("refuses a child whose name holds a colon, which Redis itself would refuse", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const colon = defineJob({
			name: "billing:invoice.render",
			queue: "documents",
			payload: type({ id: "string" }),
		})

		const failure = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [child(colon, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("billing:invoice.render")
		expect((failure as Error).message).toContain('":"')
		expect(driver.flows).toEqual([])
	})

	test("refuses a child whose name holds the character that cuts the id", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const tilde = defineJob({
			name: "invoice~render",
			queue: "documents",
			payload: type({ id: "string" }),
		})

		const failure = await jobs
			.flow(sendInvoice, { id: "inv-1" }, { children: [child(tilde, { id: "inv-1" })] })
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice~render")
		expect(driver.flows).toEqual([])
	})

	test("leaves a root name alone, because a root carries no id of ours", async () => {
		const driver = recordingDriver()
		const jobs = createJobs({ driver })

		const colon = defineJob({
			name: "billing:invoice.send",
			queue: "billing",
			payload: type({ id: "string" }),
		})

		await jobs.flow(colon, { id: "inv-1" }, { children: [child(renderPdf, { id: "inv-1" })] })

		expect(driver.flows[0]?.envelope.name).toBe("billing:invoice.send")
	})
})

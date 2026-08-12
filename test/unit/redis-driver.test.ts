import { describe, expect, test } from "bun:test"
import type { ConnectionOptions } from "bullmq"
import { DELIVERY_DEFAULTS, type ResolvedUnique } from "@/Delivery"
import type { FlowNode } from "@/Driver"
import {
	assertBlockingConnection,
	cancelUnderLock,
	HELD_RETRY_MS,
	readFlowState,
	readLease,
	toFlowJob,
	toJobsOptions,
} from "@/RedisDriver"

function connection(shape: unknown): ConnectionOptions {
	return shape as ConnectionOptions
}

function deduplication(unique: ResolvedUnique) {
	return toJobsOptions({ ...DELIVERY_DEFAULTS, unique }).deduplication
}

describe("assertBlockingConnection", () => {
	test("refuses a client that retries requests, and says how to fix it", () => {
		expect(() =>
			assertBlockingConnection(connection({ options: { maxRetriesPerRequest: 20 } })),
		).toThrow("maxRetriesPerRequest: null")
	})

	test("reads a cluster client's redisOptions instead of its own options", () => {
		expect(() =>
			assertBlockingConnection(
				connection({ isCluster: true, options: { redisOptions: { maxRetriesPerRequest: 20 } } }),
			),
		).toThrow("maxRetriesPerRequest: null")
	})

	test("accepts a cluster client that turned retries off", () => {
		expect(() =>
			assertBlockingConnection(
				connection({ isCluster: true, options: { redisOptions: { maxRetriesPerRequest: null } } }),
			),
		).not.toThrow()
	})

	test("accepts a client that turned retries off", () => {
		expect(() =>
			assertBlockingConnection(connection({ options: { maxRetriesPerRequest: null } })),
		).not.toThrow()
	})

	test("skips a client that carries no ioredis options at all", () => {
		expect(() => assertBlockingConnection(connection({}))).not.toThrow()
	})
})

describe("toJobsOptions", () => {
	test("keepFirst holds the key for its window, so a later enqueue is dropped", () => {
		expect(deduplication({ mode: "keepFirst", key: "welcome:ada", ttlMs: 60_000 })).toEqual({
			id: "welcome:ada",
			ttl: 60_000,
		})
	})

	test("keepFirst with no window holds the key until the job finishes", () => {
		expect(deduplication({ mode: "keepFirst", key: "welcome:ada" })).toEqual({ id: "welcome:ada" })
	})

	test("keepLast extends the window and replaces the waiting job", () => {
		expect(deduplication({ mode: "keepLast", key: "index:42", ttlMs: 2_000 })).toEqual({
			id: "index:42",
			ttl: 2_000,
			extend: true,
			replace: true,
		})
	})

	test("noOverlap queues the latest payload behind the running job", () => {
		expect(deduplication({ mode: "noOverlap", key: "sync:42" })).toEqual({
			id: "sync:42",
			keepLastIfActive: true,
		})
	})

	test("a delivery with no unique sends no deduplication at all", () => {
		expect(toJobsOptions(DELIVERY_DEFAULTS).deduplication).toBeUndefined()
	})
})

describe("the idempotency lease Redis replies with", () => {
	test("an empty reply means the lease was taken, under the token that asked for it", () => {
		expect(readLease([], "token-1")).toEqual({ state: "acquired", token: "token-1" })
	})

	test("a running marker means another delivery holds the lease, and its ttl is the wait", () => {
		expect(readLease(["running:token-0", 5_000], "token-1")).toEqual({
			state: "held",
			retryInMs: 5_000,
		})
	})

	test("a stored kept result means the key is complete", () => {
		expect(readLease(['{"result":{"receipt":"r-1"}}', 5_000], "token-1")).toEqual({
			state: "complete",
			kept: { result: { receipt: "r-1" } },
		})
	})

	test("a completion marker alone means the key is complete and gives back nothing", () => {
		expect(readLease(["{}", 5_000], "token-1")).toEqual({ state: "complete", kept: {} })
	})

	test("a running marker without a positive ttl waits the default instead", () => {
		expect(readLease(["running:token-0", -2], "token-1")).toEqual({
			state: "held",
			retryInMs: HELD_RETRY_MS,
		})
	})
})

function node(queue: string, name: string, children: FlowNode[] = []): FlowNode {
	return {
		queue,
		envelope: { v: 1, name, data: { id: "inv-1" }, origin: "flow" },
		delivery: DELIVERY_DEFAULTS,
		children,
	}
}

describe("toFlowJob", () => {
	test("names each node after its definition and sends the envelope to its own queue", () => {
		const root = toFlowJob(node("billing", "invoice.send", [node("documents", "invoice.render")]))

		expect(root.name).toBe("invoice.send")
		expect(root.queueName).toBe("billing")
		expect(root.data).toEqual({ v: 1, name: "invoice.send", data: { id: "inv-1" }, origin: "flow" })
		expect(root.children?.[0]?.queueName).toBe("documents")
	})

	test("lets a child that failed for good drop out of its parent's dependencies", () => {
		const root = toFlowJob(
			node("billing", "invoice.send", [
				node("billing", "invoice.charge", [node("billing", "invoice.reserve")]),
			]),
		)

		const charge = root.children?.[0]

		expect(root.opts?.ignoreDependencyOnFailure).toBeUndefined()
		expect(charge?.opts?.ignoreDependencyOnFailure).toBe(true)
		expect(charge?.children?.[0]?.opts?.ignoreDependencyOnFailure).toBe(true)
	})

	test("carries the delivery every node resolved", () => {
		const root = toFlowJob(node("billing", "invoice.send"))

		expect(root.opts?.attempts).toBe(DELIVERY_DEFAULTS.attempts)
	})
})

describe("the flow state Redis replies with", () => {
	test("reads each child's queue out of its job key, and its value as JSON", () => {
		expect(
			readFlowState([
				["bull:documents:5", '{"url":"https://a.example/1"}'],
				[],
				["invoice.render"],
			]),
		).toEqual({
			results: [
				{ queue: "documents", name: "invoice.render", value: { url: "https://a.example/1" } },
			],
			failures: [],
		})
	})

	test("names the failed children after the processed ones, and keeps their reason as it is", () => {
		expect(
			readFlowState([
				["bull:documents:5", "null"],
				["bull:billing:7", "the card was declined"],
				["invoice.render", "invoice.charge"],
			]),
		).toEqual({
			results: [{ queue: "documents", name: "invoice.render", value: null }],
			failures: [{ id: "billing:7", name: "invoice.charge", reason: "the card was declined" }],
		})
	})

	test("keeps a child that returned nothing at all, as a result with no value", () => {
		expect(readFlowState([["bull:documents:5", ""], [], ["invoice.render"]]).results).toEqual([
			{ queue: "documents", name: "invoice.render", value: undefined },
		])
	})

	test("a parent that waits on nobody has no results and no failures", () => {
		expect(readFlowState([[], [], []])).toEqual({ results: [], failures: [] })
	})
})

describe("a cancellation Redis refused for a lock", () => {
	test("aborts the delivery of a job that turned active under the cancellation", () => {
		expect(cancelUnderLock("active")).toEqual({ outcome: "aborting" })
	})

	test("cancels nothing when the lock is held by a descendant of the flow it waits on", () => {
		expect(cancelUnderLock("waiting_children")).toEqual({ outcome: "children_running" })
	})
})

describe("the job id of a flow node", () => {
	test("names every child after its definition, so its name survives the child's own sweep", () => {
		const root = toFlowJob(node("billing", "invoice.send", [node("documents", "invoice.render")]))

		expect(root.opts?.jobId).toBeUndefined()
		expect(root.children?.[0]?.opts?.jobId).toMatch(/^invoice\.render~[0-9a-f-]{36}$/)
	})

	test("gives two children of one definition ids of their own", () => {
		const root = toFlowJob(
			node("billing", "invoice.send", [
				node("documents", "invoice.render"),
				node("documents", "invoice.render"),
			]),
		)

		expect(root.children?.[0]?.opts?.jobId).not.toBe(root.children?.[1]?.opts?.jobId)
	})
})

describe("a child whose hash Redis already swept", () => {
	test("reads its definition name back out of its job id", () => {
		expect(
			readFlowState([
				["bull:documents:invoice.render~ab-cd", '{"url":"https://a.example/1"}'],
				[],
				[""],
			]),
		).toEqual({
			results: [
				{ queue: "documents", name: "invoice.render", value: { url: "https://a.example/1" } },
			],
			failures: [],
		})
	})

	test("keeps the name the hash still holds over the one the id carries", () => {
		expect(
			readFlowState([
				["bull:documents:invoice.render~ab", '{"url":"https://a.example/1"}'],
				[],
				["invoice.render"],
			]).results[0]?.name,
		).toBe("invoice.render")
	})

	test("reads no name at all out of an id that carries none", () => {
		expect(readFlowState([["bull:documents:5", "null"], [], [""]]).results[0]?.name).toBe("")
	})
})

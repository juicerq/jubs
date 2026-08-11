import { describe, expect, test } from "bun:test"
import type { ConnectionOptions } from "bullmq"
import { DELIVERY_DEFAULTS, type ResolvedUnique } from "@/Delivery"
import { assertBlockingConnection, toJobsOptions } from "@/RedisDriver"

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

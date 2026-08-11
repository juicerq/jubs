import { describe, expect, test } from "bun:test"
import type { ConnectionOptions } from "bullmq"
import { assertBlockingConnection } from "@/RedisDriver"

function connection(shape: unknown): ConnectionOptions {
	return shape as ConnectionOptions
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

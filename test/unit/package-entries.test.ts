import { describe, expect, test } from "bun:test"
import { JuibsDashboard } from "@/dashboard/index"
import { createJobs, defineHandler, defineJob, redisDriver } from "@/index"
import { JuibsTesting } from "@/testing/index"

describe("package entries", () => {
	test("the root subpath exposes the job-core surface", () => {
		expect(typeof defineJob).toBe("function")
		expect(typeof defineHandler).toBe("function")
		expect(typeof createJobs).toBe("function")
		expect(typeof redisDriver).toBe("function")
	})

	test("every other subpath exposes its namespace", () => {
		expect(JuibsTesting).toEqual({})
		expect(JuibsDashboard).toEqual({})
	})
})

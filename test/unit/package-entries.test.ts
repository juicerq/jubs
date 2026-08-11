import { describe, expect, test } from "bun:test"
import { JuibsDashboard } from "@/dashboard/index"
import { Juibs } from "@/index"
import { JuibsTesting } from "@/testing/index"

describe("package entries", () => {
	test("every subpath exposes its namespace", () => {
		expect(Juibs).toEqual({})
		expect(JuibsTesting).toEqual({})
		expect(JuibsDashboard).toEqual({})
	})
})

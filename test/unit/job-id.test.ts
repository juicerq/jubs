import { describe, expect, test } from "bun:test"
import { composeJobId, readJobId } from "@/JobId"

describe("job ids", () => {
	test("reads back the queue and the stored id it composed", () => {
		expect(readJobId(composeJobId("emails", "42"))).toEqual({ queue: "emails", storedId: "42" })
	})

	test("keeps a stored id that holds colons, as a scheduled job's does", () => {
		const scheduled = "repeat:juibs.reports.digest:1712345678000"

		expect(readJobId(composeJobId("emails", scheduled))).toEqual({
			queue: "emails",
			storedId: scheduled,
		})
	})

	test("reads back a dead queue's name", () => {
		expect(readJobId(composeJobId("emails.dead", "7"))).toEqual({
			queue: "emails.dead",
			storedId: "7",
		})
	})

	test("refuses an id that names no queue, and one that names no job", () => {
		expect(() => readJobId("42")).toThrow("is not a job id")
		expect(() => readJobId(":42")).toThrow("is not a job id")
		expect(() => readJobId("emails:")).toThrow("is not a job id")
	})
})

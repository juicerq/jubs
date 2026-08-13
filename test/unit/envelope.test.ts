import { describe, expect, test } from "bun:test"
import { type Envelope, EnvelopeError, readEnvelope } from "@/Envelope"

const stored: Envelope = { v: 1, name: "nfe.zip", data: { run_id: "r-1" }, origin: "flow" }

describe("readEnvelope", () => {
	test("reads the child counts back as they were written", () => {
		expect(readEnvelope({ ...stored, slots: { xmls: 2, manifest: 1 } })).toEqual({
			...stored,
			slots: { xmls: 2, manifest: 1 },
		})
	})

	test("refuses child counts that are not an object", () => {
		expect(() => readEnvelope({ ...stored, slots: [2] })).toThrow(EnvelopeError)
		expect(() => readEnvelope({ ...stored, slots: [2] })).toThrow(
			"its child counts [2] are not an object",
		)
	})

	test("refuses a child count that is not a whole number of children", () => {
		expect(() => readEnvelope({ ...stored, slots: { xmls: 1.5 } })).toThrow(EnvelopeError)
		expect(() => readEnvelope({ ...stored, slots: { xmls: 1.5 } })).toThrow(
			'its child count for the slot "xmls" is 1.5, which is not a whole number of children',
		)
	})
})

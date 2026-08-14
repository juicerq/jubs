import { describe, expect, test } from "bun:test"
import { serializeError } from "@/Failure"

describe("serializeError", () => {
	test("keeps the error's own name, message and stack", () => {
		const failure = new TypeError("the card was declined")
		const serialized = serializeError(failure)

		expect(serialized.name).toBe("TypeError")
		expect(serialized.message).toBe("the card was declined")
		expect(serialized.stack).toBe(failure.stack)
	})

	test("serialises the cause one level deep and drops what is under it", () => {
		const root = new Error("connection reset")
		const middle = new Error("the gateway is down", { cause: root })
		const top = new Error("the charge failed", { cause: middle })

		const serialized = serializeError(top)

		expect(serialized.cause?.name).toBe("Error")
		expect(serialized.cause?.message).toBe("the gateway is down")
		expect(serialized.cause?.stack).toBe(middle.stack)
		expect(Object.keys(serialized.cause ?? {})).toEqual(["name", "message", "stack"])
	})

	test("reports a value that is not an error as an Error with no stack", () => {
		expect(serializeError("nope")).toEqual({ name: "Error", message: "nope" })
		expect(serializeError(undefined)).toEqual({ name: "Error", message: "undefined" }) // oxlint-disable-line unicorn/no-useless-undefined
	})

	test("omits the cause key when the error carries none", () => {
		expect(Object.keys(serializeError(new Error("alone")))).toEqual(["name", "message", "stack"])
	})
})

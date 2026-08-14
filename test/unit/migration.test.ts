import { describe, expect, test } from "bun:test"
import { type } from "arktype"
import { UnrecoverableError } from "bullmq"
import { defineHandler, defineJob, type Origin } from "@/Definition"
import type { Envelope } from "@/Envelope"
import { createJobs, DELIVERY_DEFAULTS, VersionAheadError } from "@/index"
import { migrateEnvelope } from "@/Migration"
import { type MemoryDriver, memoryDriver } from "@/testing/index"
import { errorOf } from "../support/Failures"
import { recordingDriver } from "./support/RecordingDriver"

const contactPayload = type({ email: "string" })
const storedContact = type({ mail: "string" })
const storedObject = type("object")

const oldContact = defineJob({
	name: "contact.sync",
	queue: "crm",
	payload: type({ mail: "string" }),
})

const currentContact = defineJob({
	name: "contact.sync",
	queue: "crm",
	payload: contactPayload,
	version: 2,
	migrations: { 1: (data) => ({ email: storedContact.assert(data).mail }) },
})

function envelopeAt(version: number, data: unknown): Envelope {
	return { v: version, name: "contact.sync", data, origin: "direct" }
}

describe("migrateEnvelope", () => {
	test("runs no migration when the stored version is the running one", async () => {
		const ran: number[] = []

		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 2,
			migrations: {
				1: (data) => {
					ran.push(1)

					return data
				},
			},
		})

		const migrated = await migrateEnvelope(syncContact, envelopeAt(2, { email: "ada@example.com" }))

		expect(migrated).toEqual({ email: "ada@example.com" })
		expect(ran).toEqual([])
	})

	test("runs the one step that separates the stored version from the running one", async () => {
		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 2,
			migrations: {
				1: (data) => ({ email: storedContact.assert(data).mail }),
			},
		})

		const migrated = await migrateEnvelope(syncContact, envelopeAt(1, { mail: "ada@example.com" }))

		expect(migrated).toEqual({ email: "ada@example.com" })
	})

	test("runs every step in order, each one on the output of the last", async () => {
		const ran: number[] = []

		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 4,
			migrations: {
				1: (data) => {
					ran.push(1)

					return { ...storedObject.assert(data), one: true }
				},
				2: async (data) => {
					ran.push(2)

					return { ...storedObject.assert(data), two: true }
				},
				3: (data) => {
					ran.push(3)

					return { ...storedObject.assert(data), three: true }
				},
			},
		})

		const migrated = await migrateEnvelope(syncContact, envelopeAt(1, { email: "ada@example.com" }))

		expect(ran).toEqual([1, 2, 3])
		expect(migrated).toEqual({ email: "ada@example.com", one: true, two: true, three: true })
	})

	test("starts at the stored version, so an older step never runs twice", async () => {
		const ran: number[] = []

		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 3,
			migrations: {
				1: (data) => {
					ran.push(1)

					return data
				},
				2: (data) => {
					ran.push(2)

					return data
				},
			},
		})

		await migrateEnvelope(syncContact, envelopeAt(2, { email: "ada@example.com" }))

		expect(ran).toEqual([2])
	})

	test("refuses a stored version the running definition declares no step from", async () => {
		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 3,
			migrations: { 2: (data) => data },
		})

		const failure = await migrateEnvelope(syncContact, envelopeAt(1, {})).catch(
			(error: unknown) => error,
		)

		expect(errorOf(failure).message).toContain("contact.sync")
		expect(errorOf(failure).message).toContain("1")
	})

	test("keeps the cause of a step that threw", async () => {
		const declined = new Error("the stored shape has no mail field")

		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 2,
			migrations: {
				1: () => {
					throw declined
				},
			},
		})

		const failure = await migrateEnvelope(syncContact, envelopeAt(1, {})).catch(
			(error: unknown) => error,
		)

		expect(errorOf(failure).message).toContain("the stored shape has no mail field")
		expect(errorOf(failure).cause).toBe(declined)
	})

	test("refuses an envelope written by a version ahead of the running one", async () => {
		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 2,
			migrations: { 1: (data) => data },
		})

		const failure = await migrateEnvelope(syncContact, envelopeAt(3, {})).catch(
			(error: unknown) => error,
		)

		expect(failure).toBeInstanceOf(VersionAheadError)
		expect(failure).toBeInstanceOf(UnrecoverableError)
		expect(errorOf(failure).message).toContain("contact.sync")
	})
})

describe("the enqueued envelope carries the definition's version", () => {
	test("writes version 1 for a definition that names none", async () => {
		const driver = recordingDriver()

		const syncContact = defineJob({ name: "contact.sync", queue: "crm", payload: contactPayload })

		await createJobs({ driver }).enqueue(syncContact, { email: "ada@example.com" })

		expect(driver.enqueued[0]?.envelope.v).toBe(1)
	})

	test("writes the version the definition names", async () => {
		const driver = recordingDriver()

		const syncContact = defineJob({
			name: "contact.sync",
			queue: "crm",
			payload: contactPayload,
			version: 2,
			migrations: { 1: (data) => data },
		})

		await createJobs({ driver }).enqueue(syncContact, { email: "ada@example.com" })

		expect(driver.enqueued[0]?.envelope.v).toBe(2)
	})
})

describe("a versioned job over the memory driver", () => {
	test("migrates the payload an older deploy wrote, then validates the result", async () => {
		const driver = memoryDriver()
		const seen: unknown[] = []
		const jobs = createJobs({ driver })

		await jobs.start([
			defineHandler(currentContact, async (data) => {
				seen.push(data)
			}),
		])

		await jobs.enqueue(oldContact, { mail: "ada@example.com" })

		expect(await driver.drain()).toBe(1)
		expect(seen).toEqual([{ email: "ada@example.com" }])
	})

	test("migrates an occurrence a schedule wrote before the deploy, and keeps its origin", async () => {
		const driver = memoryDriver()
		const seen: { data: unknown; origin: Origin }[] = []
		const jobs = createJobs({ driver })

		await jobs.start([
			defineHandler(currentContact, async (data, context) => {
				seen.push({ data, origin: context.origin })
			}),
		])

		await driver.enqueue({
			queue: "crm",
			envelope: {
				v: 1,
				name: "contact.sync",
				data: { mail: "ada@example.com" },
				origin: "schedule",
			},
			delivery: { ...DELIVERY_DEFAULTS, attempts: 1 },
		})

		expect(await driver.drain()).toBe(1)
		expect(seen).toEqual([{ data: { email: "ada@example.com" }, origin: "schedule" }])
	})

	test("holds a payload a newer deploy wrote, without ever starting it", async () => {
		const driver = memoryDriver()
		const events: string[] = []
		let ran = false

		const jobs = createJobs({
			driver,
			definitions: [oldContact],
			deadQueues: ["crm"],
			hooks: {
				onStart: () => {
					events.push("start")
				},
				onAttemptFailed: () => {
					events.push("attemptFailed")
				},
				onDead: () => {
					events.push("dead")
				},
			},
		})

		await jobs.start([
			defineHandler(oldContact, async () => {
				ran = true
			}),
		])

		await jobs.enqueue(currentContact, { email: "ada@example.com" })

		const failure = await driver.drain().catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(VersionAheadError)
		expect(ran).toBe(false)
		expect(events).toEqual(["attemptFailed", "dead"])

		const dead = await jobs.dead.list("crm")

		expect(dead).toHaveLength(1)
		expect(dead[0]?.reason).toBe("version_ahead")
		expect(dead[0]?.envelope.v).toBe(2)
	})
})

describe("replaying a dead job of another version", () => {
	function buryOne(driver: MemoryDriver, envelope: Envelope): Promise<void> {
		return driver.dead.bury("crm", {
			jobId: "crm:1",
			envelope,
			error: { name: "Error", message: "the contact was refused" },
			reason: "attempts_exhausted",
		})
	}

	test("migrates the stored payload before validating it, then enqueues it again", async () => {
		const driver = memoryDriver()

		const jobs = createJobs({ driver, definitions: [currentContact], deadQueues: ["crm"] })

		await buryOne(driver, {
			v: 1,
			name: "contact.sync",
			data: { mail: "ada@example.com" },
			origin: "direct",
		})

		const [dead] = await jobs.dead.list("crm")

		await jobs.dead.replay(dead?.id ?? "")

		expect(driver.enqueued(oldContact)).toEqual([{ mail: "ada@example.com" }])
		expect(await jobs.dead.list("crm")).toEqual([])
	})

	test("refuses a stored payload a newer deploy wrote, and keeps the entry", async () => {
		const driver = memoryDriver()

		const jobs = createJobs({ driver, definitions: [oldContact], deadQueues: ["crm"] })

		await buryOne(driver, {
			v: 2,
			name: "contact.sync",
			data: { email: "ada@example.com" },
			origin: "direct",
		})

		const [dead] = await jobs.dead.list("crm")

		const failure = await jobs.dead.replay(dead?.id ?? "").catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(VersionAheadError)
		expect(driver.enqueued(oldContact)).toEqual([])
		expect(await jobs.dead.list("crm")).toHaveLength(1)
	})
})

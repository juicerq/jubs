import { describe, expect, jest, test } from "bun:test"
import { type } from "arktype"
import { DEFAULT_PAYLOAD_VERSION } from "@/Definition"
import {
	createJobs,
	defineHandler,
	defineJob,
	type Envelope,
	type LeftBehindEvent,
	type Outbox,
	VersionAheadError,
} from "@/index"
import { memoryDriver } from "@/testing/index"
import { recordingDriver } from "./support/RecordingDriver"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ orderId: "string" }),
})

const chargeOnce = defineJob({
	name: "billing.charge-once",
	queue: "billing",
	payload: type({ orderId: "string" }),
	delivery: { unique: { key: (data) => data.orderId, mode: "keepFirst" } },
})

const renderInvoice = defineJob({
	name: "invoice.render",
	queue: "billing",
	payload: type({ order: "string" }),
	result: type({ url: "string.url" }),
})

const mailInvoice = defineJob({
	name: "invoice.mail",
	queue: "mail",
	payload: type({ order: "string" }),
	awaits: { render: renderInvoice },
})

const oldContact = defineJob({
	name: "contact.sync",
	queue: "crm",
	payload: type({ mail: "string" }),
})

const brittleDelivery = defineJob({
	name: "billing.brittle",
	queue: "billing",
	payload: type({ orderId: "string" }),
	delivery: () => {
		throw "the delivery policy blew up"
	},
})

const currentContact = defineJob({
	name: "contact.sync",
	queue: "crm",
	payload: type({ email: "string" }),
	version: 2,
	migrations: { 1: (data) => ({ email: (data as { mail: string }).mail }) },
})

interface OutboxRow {
	readonly id: string
	readonly envelope: Envelope
	delivered: boolean
}

interface FakeOutbox extends Outbox {
	readonly rows: OutboxRow[]
	readonly transactions: unknown[]
	readonly claims: number[]
	/** Writes one row the way another producer would have written it. */
	keep(envelope: Envelope): void
	dieBeforeMarking(): void
}

function fakeOutbox(): FakeOutbox {
	const rows: OutboxRow[] = []
	const transactions: unknown[] = []
	const claims: number[] = []
	let dying = false

	return {
		rows,
		transactions,
		claims,

		dieBeforeMarking() {
			dying = true
		},

		keep(envelope) {
			rows.push({ id: String(rows.length + 1), envelope, delivered: false })
		},

		async save(envelopes, tx) {
			transactions.push(tx)

			for (const envelope of envelopes) {
				rows.push({ id: String(rows.length + 1), envelope, delivered: false })
			}
		},

		async claim(limit) {
			claims.push(limit)

			return rows
				.filter((row) => !row.delivered)
				.slice(0, limit)
				.map((row) => ({ id: row.id, envelope: row.envelope }))
		},

		async markDelivered(ids) {
			if (dying) {
				return
			}

			for (const row of rows) {
				if (ids.includes(row.id)) {
					row.delivered = true
				}
			}
		},
	}
}

/** An outbox holding one row under the given id, whose marks a test can read. */
function oneRowNamed(id: string, marked: string[][]): Outbox {
	return {
		async save() {},

		async claim() {
			return [
				{
					id,
					envelope: {
						v: DEFAULT_PAYLOAD_VERSION,
						name: chargeCard.name,
						data: { orderId: "order-1" },
						origin: "relay",
					},
				},
			]
		},

		async markDelivered(ids) {
			marked.push([...ids])
		},
	}
}

describe("an atomic block given a transaction", () => {
	test("writes the envelopes to the outbox inside that transaction, and delivers nothing", async () => {
		const driver = recordingDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })
		const tx = { name: "the caller's transaction" }

		await jobs.atomic(
			async () => {
				await jobs.enqueue(chargeCard, { orderId: "order-1" })
				await jobs.enqueue(chargeCard, { orderId: "order-2" })

				expect(outbox.rows).toEqual([])
			},
			{ tx },
		)

		expect(outbox.transactions).toEqual([tx])
		expect(outbox.rows.map((row) => row.envelope.data)).toEqual([
			{ orderId: "order-1" },
			{ orderId: "order-2" },
		])
		expect(driver.enqueued).toEqual([])
	})

	test("writes the envelopes with the origin the relay gives them", async () => {
		const driver = recordingDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })

		await jobs.atomic(
			async () => {
				await jobs.enqueue(chargeCard, { orderId: "order-1" })
			},
			{ tx: {} },
		)

		expect(outbox.rows[0]?.envelope.origin).toBe("relay")
	})

	test("writes nothing at all when the block enqueued nothing", async () => {
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver: recordingDriver(), outbox, definitions: [chargeCard] })

		await jobs.atomic(async () => {}, { tx: {} })

		expect(outbox.transactions).toEqual([])
		expect(outbox.rows).toEqual([])
	})

	test("writes nothing when the block throws", async () => {
		const driver = recordingDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })

		const failure = await jobs
			.atomic(
				async () => {
					await jobs.enqueue(chargeCard, { orderId: "order-1" })

					throw new Error("the order was refused")
				},
				{ tx: {} },
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toBe("the order was refused")
		expect(outbox.rows).toEqual([])
		expect(outbox.transactions).toEqual([])
	})

	test("refuses a flow, because an outbox row holds one envelope and not a tree", async () => {
		const driver = recordingDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [mailInvoice] })

		const failure = await jobs
			.atomic(
				async () => {
					await jobs.enqueue(mailInvoice, { order: "order-1" }, { render: { order: "order-1" } })
				},
				{ tx: {} },
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain("invoice.mail")
		expect((failure as Error).message).toContain("outbox")
		expect(outbox.rows).toEqual([])
		expect(driver.flows).toEqual([])
	})

	test("refuses a job whose delivery declares unique, which the outbox cannot carry", async () => {
		const driver = recordingDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeOnce] })

		const failure = await jobs
			.atomic(
				async () => {
					await jobs.enqueue(chargeOnce, { orderId: "order-1" })
				},
				{ tx: {} },
			)
			.catch((error: unknown) => error)

		expect((failure as Error).message).toContain(chargeOnce.name)
		expect((failure as Error).message).toContain("unique")
		expect(outbox.rows).toEqual([])
		expect(driver.enqueued).toEqual([])
	})

	test("refuses a transaction inside a block that is already open, before the block runs", async () => {
		const driver = recordingDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })
		let ran = false

		await jobs.atomic(async () => {
			expect(() =>
				jobs.atomic(
					async () => {
						ran = true
					},
					{ tx: {} },
				),
			).toThrow("already open")
		})

		expect(ran).toBe(false)
	})
})

describe("the relay", () => {
	test("delivers every envelope it claims, and marks the rows delivered", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })

		await jobs.atomic(
			async () => {
				await jobs.enqueue(chargeCard, { orderId: "order-1" })
				await jobs.enqueue(chargeCard, { orderId: "order-2" })
			},
			{ tx: {} },
		)

		expect(driver.enqueued(chargeCard)).toEqual([])

		await jobs.startRelay().close()

		expect(driver.enqueued(chargeCard)).toEqual([{ orderId: "order-1" }, { orderId: "order-2" }])
		expect(outbox.rows.map((row) => row.delivered)).toEqual([true, true])
	})

	test("names the job it enqueues after the outbox row it came from", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })

		await jobs.atomic(
			async () => {
				await jobs.enqueue(chargeCard, { orderId: "order-1" })
			},
			{ tx: {} },
		)

		await jobs.startRelay().close()

		expect(await jobs.get("billing:outbox-1")).toMatchObject({ name: chargeCard.name })
	})

	test("delivers a row without the uniqueness its definition declares", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeOnce] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeOnce.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		await jobs.startRelay().close()

		expect(driver.enqueued(chargeOnce)).toEqual([{ orderId: "order-1" }])
	})

	test("enqueues no second job for a row a relay delivered and died before marking", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })
		let ran = 0

		await jobs.start([
			defineHandler(chargeCard, async () => {
				ran += 1
			}),
		])

		await jobs.atomic(
			async () => {
				await jobs.enqueue(chargeCard, { orderId: "order-1" })
			},
			{ tx: {} },
		)

		outbox.dieBeforeMarking()

		await jobs.startRelay().close()

		expect(outbox.rows[0]?.delivered).toBe(false)

		await jobs.startRelay().close()

		expect(driver.enqueued(chargeCard)).toEqual([{ orderId: "order-1" }])
		expect(await driver.drain()).toBe(1)
		expect(ran).toBe(1)
	})

	test("claims nothing more once the handle is closed", async () => {
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver: memoryDriver(), outbox, definitions: [chargeCard] })

		const relay = jobs.startRelay({ intervalMs: 1 })

		await relay.close()

		const claimed = outbox.claims.length

		await Bun.sleep(20)

		expect(outbox.claims).toHaveLength(claimed)
	})

	test("never opens a second cycle while the one before it is still going", async () => {
		const outbox = fakeOutbox()
		const holding = Promise.withResolvers<void>()
		const jobs = createJobs({
			driver: memoryDriver(),
			definitions: [chargeCard],
			outbox: {
				...outbox,

				async claim(limit) {
					const claimed = await outbox.claim(limit)

					await holding.promise

					return claimed
				},
			},
		})

		const relay = jobs.startRelay({ intervalMs: 1 })

		await Bun.sleep(20)

		expect(outbox.claims).toHaveLength(1)

		holding.resolve()

		await relay.close()
	})

	test("delivers nothing for a row whose id carries a colon, and leaves that row claimable", async () => {
		const driver = memoryDriver()
		const marked: string[][] = []
		const jobs = createJobs({
			driver,
			definitions: [chargeCard],
			outbox: oneRowNamed("42:7", marked),
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain('the row "42:7"')
		expect(marked).toEqual([])
		expect(driver.enqueued(chargeCard)).toEqual([])
	})

	test("tells the operator to take a row whose id carries a colon out of the claim, since no delivery was ever attempted for it", async () => {
		const marked: string[][] = []
		const jobs = createJobs({
			driver: memoryDriver(),
			definitions: [chargeCard],
			outbox: oneRowNamed("42:7", marked),
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain("mark it delivered by hand, or delete it")
		expect(failure).not.toContain("its job is not lost")
		expect(failure).not.toContain("failed inside the driver")
	})

	test("delivers nothing for a row with an empty id, which BullMQ would name itself", async () => {
		const driver = memoryDriver()
		const marked: string[][] = []
		const jobs = createJobs({ driver, definitions: [chargeCard], outbox: oneRowNamed("", marked) })

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain("empty id")
		expect(marked).toEqual([])
		expect(driver.enqueued(chargeCard)).toEqual([])
	})

	test("reports a row it cannot deliver by naming that row, and says it went on", async () => {
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver: memoryDriver(), outbox, definitions: [] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain('the outbox row "1"')
		expect(failure).toContain("left behind")
		expect(failure).toContain(chargeCard.name)
		expect(outbox.rows[0]?.delivered).toBe(false)
	})

	test("leaves behind only the row it cannot deliver, and delivers the rows around it", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})
		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: "billing.unknown",
			data: {},
			origin: "relay",
		})
		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-3" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failures = reported.mock.calls.map((call) => String(call[0]))

		reported.mockRestore()

		expect(driver.enqueued(chargeCard)).toEqual([{ orderId: "order-1" }, { orderId: "order-3" }])
		expect(outbox.rows.map((row) => row.delivered)).toEqual([true, false, true])
		expect(failures).toHaveLength(1)
		expect(failures[0]).toContain('the outbox row "2"')
	})

	test("migrates the payload of a row before validating it, and enqueues the envelope it claimed", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [currentContact] })

		outbox.keep({
			v: 1,
			name: currentContact.name,
			data: { mail: "ada@example.com" },
			origin: "relay",
		})

		await jobs.startRelay().close()

		expect(driver.enqueued(oldContact)).toEqual([{ mail: "ada@example.com" }])
		expect(outbox.rows[0]?.delivered).toBe(true)
	})

	test("leaves behind a row whose payload version a newer deploy wrote", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [oldContact] })

		outbox.keep({
			v: 2,
			name: oldContact.name,
			data: { email: "ada@example.com" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = reported.mock.calls[0]?.[0] as Error | undefined

		reported.mockRestore()

		expect(failure?.cause).toBeInstanceOf(VersionAheadError)
		expect(failure?.message).toContain('the outbox row "1"')
		expect(driver.enqueued(oldContact)).toEqual([])
		expect(outbox.rows[0]?.delivered).toBe(false)
	})

	test("tells the operator to wait out the deploy rather than take a version-ahead row out of the claim", async () => {
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver: memoryDriver(), outbox, definitions: [oldContact] })

		outbox.keep({
			v: 2,
			name: oldContact.name,
			data: { email: "ada@example.com" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain("that row is not broken")
		expect(failure).toContain("Wait for the deploy to finish")
		expect(failure).toContain("counts claims")
		expect(failure).not.toContain("mark it delivered by hand, or delete it")
	})

	test("keeps telling the operator to take a permanently broken row out of the claim", async () => {
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver: memoryDriver(), outbox, definitions: [mailInvoice] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: mailInvoice.name,
			data: { order: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain("mark it delivered by hand, or delete it")
		expect(failure).not.toContain("Wait for the deploy to finish")
	})

	test("tells the operator to leave in the claim a row whose delivery failed inside the driver", async () => {
		const outbox = fakeOutbox()
		const jobs = createJobs({
			driver: {
				...memoryDriver(),

				async enqueue() {
					throw new Error("connect ECONNREFUSED 127.0.0.1:6379")
				},
			},
			outbox,
			definitions: [chargeCard],
		})

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = reported.mock.calls[0]?.[0] as Error | undefined
		const cause = failure?.cause as Error | undefined

		reported.mockRestore()

		expect(failure?.message).toContain('the outbox row "1"')
		expect(failure?.message).toContain("its job is not lost")
		expect(failure?.message).toContain("leave the row in the claim")
		expect(failure?.message).toContain("ECONNREFUSED 127.0.0.1:6379")
		expect(failure?.message).not.toContain("mark it delivered by hand, or delete it")
		expect(cause?.message).toContain("ECONNREFUSED 127.0.0.1:6379")
		expect(outbox.rows[0]?.delivered).toBe(false)
	})

	test("leaves behind a row whose definition waits on slots, because a row holds no tree", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [mailInvoice] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: mailInvoice.name,
			data: { order: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs.startRelay().close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(failure).toContain('the outbox row "1"')
		expect(failure).toContain("waits on children")
		expect(driver.enqueued(mailInvoice)).toEqual([])
		expect(outbox.rows[0]?.delivered).toBe(false)
	})

	test("hands the hook the row whose delivery failed inside the driver, with the driver's own failure", async () => {
		const outbox = fakeOutbox()
		const refused = new Error("connect ECONNREFUSED 127.0.0.1:6379")
		const left: LeftBehindEvent[] = []
		const jobs = createJobs({
			driver: {
				...memoryDriver(),

				async enqueue() {
					throw refused
				},
			},
			outbox,
			definitions: [chargeCard],
		})

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs
			.startRelay({
				onLeftBehind(event) {
					left.push(event)
				},
			})
			.close()

		reported.mockRestore()

		expect(left).toEqual([
			{
				rowId: "1",
				name: chargeCard.name,
				reason: "driver_failed",
				error: refused,
			},
		])
		expect(left[0]?.error).toBe(refused)
		expect(left[0]?.error.message).toBe("connect ECONNREFUSED 127.0.0.1:6379")
		expect(left[0]?.error.message).not.toContain("failed inside the driver")
	})

	test("hands the hook the row whose payload version a newer deploy wrote", async () => {
		const outbox = fakeOutbox()
		const left: LeftBehindEvent[] = []
		const jobs = createJobs({ driver: memoryDriver(), outbox, definitions: [oldContact] })

		outbox.keep({
			v: 2,
			name: oldContact.name,
			data: { email: "ada@example.com" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs
			.startRelay({
				onLeftBehind(event) {
					left.push(event)
				},
			})
			.close()

		reported.mockRestore()

		expect(left).toEqual([
			{
				rowId: "1",
				name: oldContact.name,
				reason: "version_ahead",
				error: expect.any(VersionAheadError),
			},
		])
	})

	test("hands the hook the row no definition on the client answers to", async () => {
		const outbox = fakeOutbox()
		const left: LeftBehindEvent[] = []
		const jobs = createJobs({ driver: memoryDriver(), outbox, definitions: [] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs
			.startRelay({
				onLeftBehind(event) {
					left.push(event)
				},
			})
			.close()

		reported.mockRestore()

		expect(left).toEqual([
			{
				rowId: "1",
				name: chargeCard.name,
				reason: "unrecoverable",
				error: expect.any(Error),
			},
		])
		expect(left[0]?.error.message).toContain("which this client does not know")
	})

	test("wraps in an Error a driver that threw something that is not one, and still calls it a driver failure", async () => {
		const outbox = fakeOutbox()
		const left: LeftBehindEvent[] = []
		const jobs = createJobs({
			driver: {
				...memoryDriver(),

				async enqueue() {
					throw "redis went away"
				},
			},
			outbox,
			definitions: [chargeCard],
		})

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs
			.startRelay({
				onLeftBehind(event) {
					left.push(event)
				},
			})
			.close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(left[0]?.reason).toBe("driver_failed")
		expect(left[0]?.error).toBeInstanceOf(Error)
		expect(left[0]?.error.message).toBe("redis went away")
		expect(failure).toContain("its job is not lost")
		expect(failure).toContain("The failure it was left behind on: redis went away")
		expect(outbox.rows[0]?.delivered).toBe(false)
	})

	test("leaves behind as unrecoverable a row whose delivery policy threw something that is not an Error", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const left: LeftBehindEvent[] = []
		const jobs = createJobs({ driver, outbox, definitions: [brittleDelivery] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: brittleDelivery.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs
			.startRelay({
				onLeftBehind(event) {
					left.push(event)
				},
			})
			.close()

		const failure = String(reported.mock.calls[0]?.[0])

		reported.mockRestore()

		expect(left[0]?.reason).toBe("unrecoverable")
		expect(left[0]?.error).toBeInstanceOf(Error)
		expect(left[0]?.error.message).toBe("the delivery policy blew up")
		expect(failure).toContain("mark it delivered by hand, or delete it")
		expect(failure).toContain("The failure it was left behind on: the delivery policy blew up")
		expect(driver.enqueued(brittleDelivery)).toEqual([])
		expect(outbox.rows[0]?.delivered).toBe(false)
	})

	test("delivers and marks the rows around a row whose hook threw", async () => {
		const driver = memoryDriver()
		const outbox = fakeOutbox()
		const jobs = createJobs({ driver, outbox, definitions: [chargeCard] })

		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-1" },
			origin: "relay",
		})
		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: "billing.unknown",
			data: {},
			origin: "relay",
		})
		outbox.keep({
			v: DEFAULT_PAYLOAD_VERSION,
			name: chargeCard.name,
			data: { orderId: "order-3" },
			origin: "relay",
		})

		const reported = jest.spyOn(console, "error").mockImplementation(() => {})

		await jobs
			.startRelay({
				onLeftBehind() {
					throw new Error("the status column is gone")
				},
			})
			.close()

		const failures = reported.mock.calls.map((call) => String(call[0]))

		reported.mockRestore()

		expect(driver.enqueued(chargeCard)).toEqual([{ orderId: "order-1" }, { orderId: "order-3" }])
		expect(outbox.rows.map((row) => row.delivered)).toEqual([true, false, true])
		expect(failures.some((failure) => failure.includes("the onLeftBehind hook threw"))).toBe(true)
	})

	test("is refused on a client with no outbox, and names what to configure", () => {
		const jobs = createJobs({ driver: memoryDriver(), definitions: [chargeCard] })

		expect(() => jobs.startRelay()).toThrow("no outbox configured")
	})
})

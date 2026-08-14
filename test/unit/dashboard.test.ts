import { describe, expect, mock, test } from "bun:test"
import { type } from "arktype"
import { type BoardConfig, boardQueues, loaded } from "@/dashboard/Board"
import { expressDashboard, fastifyDashboard } from "@/dashboard/index"
import { defineJob } from "@/index"
import { errorOf } from "../support/Failures"

const chargeCard = defineJob({
	name: "billing.charge",
	queue: "billing",
	payload: type({ cents: "number" }),
})

const refundCard = defineJob({
	name: "billing.refund",
	queue: "billing",
	payload: type({ cents: "number" }),
})

const renderReport = defineJob({
	name: "reports.render",
	queue: "reports",
	payload: type({ reportId: "string" }),
})

const buryReport = defineJob({
	name: "reports.bury",
	queue: "reports.dead",
	payload: type({ reportId: "string" }),
})

function config(over: Partial<BoardConfig>): BoardConfig {
	return { definitions: [chargeCard], ...over }
}

/**
 * Enough of a dashboard to reach the peer import, over a connection nothing
 * ever opens: every test using it fails before a `Queue` is built.
 */
const options = {
	connection: { host: "127.0.0.1", port: 0 },
	definitions: [chargeCard],
	basePath: "/admin/queues",
}

/**
 * The very error the runtime throws for a package nobody installed, taken from
 * the runtime rather than written out here — the whole rewrite turns on the
 * `code` this carries, and a hand-made error would only prove itself.
 */
function missingModule(): Promise<unknown> {
	const absent: string = "@bull-board/express-nobody-installed"

	return import(absent)
}

function failureOf(loading: Promise<unknown>): Promise<unknown> {
	return loading.then(
		() => {},
		(error: unknown) => error,
	)
}

describe("the queues a board shows", () => {
	test("opens one queue per definition, however many definitions share it", () => {
		const queues = boardQueues(config({ definitions: [chargeCard, refundCard, renderReport] }))

		expect(queues.map((queue) => queue.name)).toEqual(["billing", "reports"])
	})

	test("holds a live queue read only until it is asked otherwise, and says what to call", () => {
		const [live] = boardQueues(config({ definitions: [chargeCard] }))

		expect(live?.readOnly).toBe(true)
		expect(live?.description).toContain("jobs.retry(id)")
		expect(live?.description).toContain("jobs.cancel(id)")
		expect(live?.description).toContain("jobs.pause(queue)")
		expect(live?.description).toContain("jobs.resume(queue)")
	})

	test("opens a live queue for writing on readOnly: false, and names what that lets through", () => {
		const [live] = boardQueues(config({ definitions: [chargeCard], readOnly: false }))

		expect(live?.readOnly).toBe(false)
		expect(live?.description).toContain("readOnly: false")
		expect(live?.description).toContain('"Add job"')
		expect(live?.description).toContain("jubs.* scheduler")
	})

	test("keeps a dead queue read only even when the live ones are opened for writing", () => {
		const queues = boardQueues(
			config({ definitions: [chargeCard], deadQueues: ["billing"], readOnly: false }),
		)

		expect(queues.map((queue) => queue.readOnly)).toEqual([false, true])
	})

	test("adds a read-only queue for every dead queue, named after the live one", () => {
		const queues = boardQueues(
			config({
				definitions: [chargeCard, renderReport],
				deadQueues: ["billing", "reports"],
			}),
		)
		const dead = queues.slice(2)

		expect(queues).toHaveLength(4)
		expect(dead.map((queue) => queue.name)).toEqual(["billing.dead", "reports.dead"])
		expect(dead.map((queue) => queue.readOnly)).toEqual([true, true])
		expect(dead[0]?.description).toContain('buried out of "billing"')
		expect(dead[1]?.description).toContain('buried out of "reports"')
		expect(dead[1]?.description).toContain("jobs.dead.replay(id)")
	})

	test("names a dead queue once, however many times it was given", () => {
		const queues = boardQueues(config({ deadQueues: ["billing", "billing"] }))

		expect(queues.map((queue) => queue.name)).toEqual(["billing", "billing.dead"])
	})

	test("names a live queue that is also a dead one once, and as the dead one", () => {
		const queues = boardQueues(
			config({
				definitions: [renderReport, buryReport],
				deadQueues: ["reports"],
				readOnly: false,
			}),
		)

		expect(queues.map((queue) => queue.name)).toEqual(["reports", "reports.dead"])
		expect(queues.at(-1)?.readOnly).toBe(true)
		expect(queues.at(-1)?.description).toContain('buried out of "reports"')
	})

	test("refuses a board that would open with no queue at all", () => {
		expect(() => boardQueues(config({ definitions: [] }))).toThrow(
			/the dashboard was given no definitions/,
		)
	})

	test("refuses a dead queue no definition uses, and names the queues that are used", () => {
		expect(() =>
			boardQueues(config({ definitions: [chargeCard, renderReport], deadQueues: ["biling"] })),
		).toThrow(
			'jubs: the dashboard was given the dead queue "biling", which no definition it was given uses — the queues its definitions use are "billing", "reports". Nothing was opened: the board opens a Queue for every name it is given, and opening one writes bull:biling.dead:meta to Redis for good.',
		)
	})
})

describe("a peer that is not installed", () => {
	test("names the package to install beside the api it renders through", async () => {
		const failure = await failureOf(loaded("@bull-board/express", missingModule()))

		expect(failure).toBeInstanceOf(Error)
		expect(errorOf(failure).message).toBe(
			'jubs: @juicerq/jubs/dashboard needs "@bull-board/express", which is an optional peer and is not installed — run `npm install @bull-board/api @bull-board/express`',
		)
	})

	test("names the api alone when the api is what is missing", async () => {
		const failure = await failureOf(loaded("@bull-board/api", missingModule()))

		expect(errorOf(failure).message).toBe(
			'jubs: @juicerq/jubs/dashboard needs "@bull-board/api", which is an optional peer and is not installed — run `npm install @bull-board/api`',
		)
	})

	test("reads the require flavour of a missing module too", async () => {
		const absent = Object.assign(new Error("Cannot find module"), { code: "MODULE_NOT_FOUND" })
		const failure = await failureOf(loaded("@bull-board/fastify", Promise.reject(absent)))

		expect(errorOf(failure).message).toContain('needs "@bull-board/fastify"')
	})

	test("hands back a failure that is not a missing module untouched", async () => {
		const thrown = new TypeError("the adapter blew up while it loaded")

		expect(await failureOf(loaded("@bull-board/express", Promise.reject(thrown)))).toBe(thrown)
		expect(await failureOf(loaded("@bull-board/express", Promise.reject("not an error")))).toBe(
			"not an error",
		)
	})
})

/**
 * These are the only tests in this run that touch a server package, and they
 * have to stay that way: `mock.module` replaces a module for the rest of the
 * process, `mock.restore()` does not put the real one back, and mocking one
 * that is already loaded throws the factory's error at the call itself. Which
 * package a mount really loads is asked in a runtime of its own, by the
 * integration test.
 */
describe("a mount whose peer is not installed", () => {
	for (const peer of ["@bull-board/express", "@bull-board/fastify"]) {
		test(`${peer} says what to install rather than what the runtime threw`, async () => {
			await mock.module(peer, () => {
				throw Object.assign(new Error("Cannot find package"), { code: "ERR_MODULE_NOT_FOUND" })
			})

			const mount = peer.endsWith("express") ? expressDashboard : fastifyDashboard
			const failure = await failureOf(mount(options))

			expect(errorOf(failure).message).toBe(
				`jubs: @juicerq/jubs/dashboard needs "${peer}", which is an optional peer and is not installed — run \`npm install @bull-board/api ${peer}\``,
			)
		})
	}
})

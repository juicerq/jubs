import { afterAll, describe, expect, test } from "bun:test"
import type { Server } from "node:net"
import { ExpressAdapter } from "@bull-board/express"
import { type } from "arktype"
import { Queue } from "bullmq"
import express from "express"
import Fastify from "fastify"
import IORedis from "ioredis"
import { expressDashboard, fastifyDashboard, mountDashboard } from "@/dashboard/index"
import { createJobs, defineJob, redisDriver } from "@/index"
import { scoped } from "./namespace"
import { REDIS_URL } from "./redis"

const connection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null })

const BASE_PATH = "/admin/queues"

const opened: Queue[] = []
const closing: (() => Promise<void>)[] = []

async function scrub(name: string): Promise<Queue> {
	const queue = new Queue(name, { connection })

	opened.push(queue)

	await queue.obliterate({ force: true })

	return queue
}

/**
 * A live queue holding one job nobody consumes, and the dead queue beside it,
 * both emptied first so the board reads only what this test put there.
 */
async function seeded(queue: string) {
	const chargeCard = defineJob({
		name: scoped("billing.charge"),
		queue,
		payload: type({ cents: "number" }),
	})

	await scrub(queue)

	const dead = `${queue}.dead`

	await scrub(dead)

	const jobs = createJobs({ driver: redisDriver(connection), deadQueues: [queue] })

	await jobs.enqueue(chargeCard, { cents: 500 })

	return { definition: chargeCard, dead }
}

interface BoardQueueRead {
	readonly name: string
	readonly readOnlyMode: boolean
	readonly description?: string
	readonly jobs: readonly { readonly data: { readonly origin?: string } }[]
}

async function readBoard(origin: string, activeQueue: string): Promise<BoardQueueRead[]> {
	const url = new URL(`${origin}${BASE_PATH}/api/queues`)

	url.searchParams.set("activeQueue", activeQueue)
	url.searchParams.set("status", "latest")

	const response = await fetch(url)

	expect(response.status).toBe(200)

	const body: unknown = await response.json()

	if (
		typeof body !== "object" ||
		body === null ||
		!("queues" in body) ||
		!Array.isArray(body.queues)
	) {
		throw new Error("the board answered no queue listing")
	}

	return body.queues
}

function portOf(server: Server): number {
	const address = server.address()

	if (typeof address !== "object" || address === null) {
		throw new Error("the board server reported no address")
	}

	return address.port
}

/**
 * Listens on a free port and gives back the origin the board answers on.
 */
function served(app: express.Express): string {
	const server = app.listen(0)

	closing.push(
		() =>
			new Promise<void>((settle) => {
				server.close(() => settle())
			}),
	)

	return `http://127.0.0.1:${portOf(server)}`
}

function found(queues: readonly BoardQueueRead[], name: string): BoardQueueRead {
	const queue = queues.find((candidate) => candidate.name === name)

	if (!queue) {
		throw new Error(`the board listed no queue "${name}" — it listed ${queues.length}`)
	}

	return queue
}

afterAll(async () => {
	await Promise.all(closing.map((close) => close()))
	await Promise.all(opened.map((queue) => queue.obliterate({ force: true })))
	await Promise.all(opened.map((queue) => queue.close()))
	await connection.quit()
})

describe("mounting the board on a real server", () => {
	test("express serves the live queue and the dead queue beside it", async () => {
		const { definition, dead } = await seeded(scoped("jubs.test.dashboard-express"))

		const router = await expressDashboard({
			connection,
			definitions: [definition],
			deadQueues: [definition.queue],
			basePath: BASE_PATH,
		})

		const app = express()

		app.use(BASE_PATH, router)

		const queues = await readBoard(served(app), definition.queue)
		const live = found(queues, definition.queue)

		expect(queues.map((queue) => queue.name)).toEqual([definition.queue, dead])
		expect(live.readOnlyMode).toBe(true)
		expect(live.description).toContain("jobs.retry(id)")
		expect(live.jobs[0]?.data.origin).toBe("direct")
		expect(found(queues, dead).readOnlyMode).toBe(true)
		expect(found(queues, dead).description).toContain("jobs.dead.replay(id)")
	})

	test("refuses to rewrite a scheduler of a live queue, and says the queue is read only", async () => {
		const { definition } = await seeded(scoped("jubs.test.dashboard-readonly"))

		const router = await expressDashboard({
			connection,
			definitions: [definition],
			basePath: BASE_PATH,
		})

		const app = express()

		app.use(BASE_PATH, router)

		const queue = encodeURIComponent(definition.queue)
		const response = await fetch(
			`${served(app)}${BASE_PATH}/api/queues/${queue}/job-schedulers/${definition.name}`,
			{
				method: "PATCH",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ every: 60_000 }),
			},
		)

		expect(response.status).toBe(405)
		expect(await response.json()).toEqual({ error: { key: "ERRORS.QUEUE_READ_ONLY" } })
	})

	test("mountDashboard serves the board on an adapter the caller built and mounts", async () => {
		const { definition, dead } = await seeded(scoped("jubs.test.dashboard-mount"))

		const serverAdapter = new ExpressAdapter()

		await mountDashboard({
			connection,
			definitions: [definition],
			deadQueues: [definition.queue],
			basePath: BASE_PATH,
			readOnly: false,
			serverAdapter,
		})

		const app = express()

		app.use(BASE_PATH, serverAdapter.getRouter())

		const queues = await readBoard(served(app), definition.queue)
		const live = found(queues, definition.queue)

		expect(queues.map((queue) => queue.name)).toEqual([definition.queue, dead])
		expect(live.readOnlyMode).toBe(false)
		expect(live.description).toContain("readOnly: false")
		expect(live.jobs[0]?.data.origin).toBe("direct")
		expect(found(queues, dead).readOnlyMode).toBe(true)
	})

	test("fastify serves the same board, and shows the origin the envelope carries", async () => {
		const { definition, dead } = await seeded(scoped("jubs.test.dashboard-fastify"))

		const plugin = await fastifyDashboard({
			connection,
			definitions: [definition],
			deadQueues: [definition.queue],
			basePath: BASE_PATH,
		})

		const app = Fastify()

		await app.register(plugin, { prefix: BASE_PATH })
		await app.listen({ port: 0, host: "127.0.0.1" })

		closing.push(() => app.close())

		const queues = await readBoard(`http://127.0.0.1:${portOf(app.server)}`, definition.queue)
		const live = found(queues, definition.queue)

		expect(queues.map((queue) => queue.name)).toEqual([definition.queue, dead])
		expect(live.readOnlyMode).toBe(true)
		expect(found(queues, dead).readOnlyMode).toBe(true)
		expect(live.jobs).toHaveLength(1)
		expect(live.jobs[0]?.data.origin).toBe("direct")
	})
})

const PUBLISHED = new URL("../../dist/dashboard.js", import.meta.url).pathname

/**
 * The built subpath, built now when nothing has built it yet. The probe below
 * reads the file the package publishes, because a bundler is free to turn a
 * dynamic import into a static one, and only the artifact settles that.
 */
async function published(): Promise<string> {
	if (await Bun.file(PUBLISHED).exists()) {
		return PUBLISHED
	}

	await Bun.spawn(["bun", "run", "build"], { cwd: new URL("../..", import.meta.url).pathname })
		.exited

	return PUBLISHED
}

/**
 * Which server packages one mount pulls in, read off the module cache of a Node
 * that loads nothing else.
 *
 * It cannot be asked in this process: proving that a mount leaves a package
 * alone means never having loaded that package, and this file mounts both. The
 * mount is given no definition, so it fails after the peer import and before
 * the first `Queue`, and needs no Redis at all.
 */
async function peersLoadedBy(mount: string): Promise<Record<string, boolean>> {
	const entry = JSON.stringify(await published())

	const probe = Bun.spawn(
		[
			"node",
			"--input-type=module",
			"-e",
			`import { createRequire } from "node:module"
			const loaded = createRequire(${entry}).cache
			const dashboard = await import(${entry})
			await dashboard.${mount}({ connection: {}, definitions: [], basePath: "/x" }).catch(() => {})
			const paths = Object.keys(loaded)
			console.log(JSON.stringify({
				express: paths.some((path) => path.includes("@bull-board/express")),
				fastify: paths.some((path) => path.includes("@bull-board/fastify")),
			}))`,
		],
		{ stdout: "pipe", stderr: "pipe" },
	)

	const [written, failure] = await Promise.all([
		new Response(probe.stdout).text(),
		new Response(probe.stderr).text(),
	])

	expect(failure).toBe("")

	return JSON.parse(written)
}

describe("what one mount loads", () => {
	test("the express mount loads the express adapter and never the fastify one", async () => {
		expect(await peersLoadedBy("expressDashboard")).toEqual({ express: true, fastify: false })
	})

	test("the fastify mount loads the fastify adapter and never the express one", async () => {
		expect(await peersLoadedBy("fastifyDashboard")).toEqual({ express: false, fastify: true })
	})
})

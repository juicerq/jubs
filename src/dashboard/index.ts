import type { IServerAdapter } from "@bull-board/api/typings/app"
import { type ConnectionOptions, Queue } from "bullmq"
import { type BoardConfig, boardQueues, loaded } from "@/dashboard/Board"

/**
 * What a mounted board is, said without naming the framework it mounts on.
 *
 * The two server packages are optional peers, so a published `.d.ts` that named
 * `express` would fail the typecheck of everyone who installed only Fastify,
 * and the other way around. These are the shapes `app.use(basePath, router)`
 * and `fastify.register(plugin, { prefix })` accept, written out here so the
 * types this subpath publishes carry no import at all.
 */
export type ExpressRouter = (
	request: unknown,
	response: unknown,
	next: (error?: unknown) => void,
) => void

export type FastifyPlugin = (
	instance: unknown,
	options: unknown,
	done: (error?: Error) => void,
) => void

export interface DashboardOptions extends BoardConfig {
	/**
	 * What the board reads its queues over. Pass the connection you already
	 * built and own — an `IORedis` instance — and not a bag of options.
	 *
	 * The board opens one `Queue` per queue it shows and closes none of them,
	 * exactly as `redisDriver` does, because the connection belongs to whoever
	 * made it. A shared instance is one client however many queues open over it.
	 * An options object is not shared: every `Queue` builds clients of its own,
	 * which nothing here ever closes, and a process that only mounts the board
	 * never exits.
	 */
	readonly connection: ConnectionOptions
	readonly basePath: string
}

/**
 * A server adapter as this module drives it. `setBasePath` is on every adapter
 * the Bull Board ships and on none of the `IServerAdapter` interface, so it is
 * named here rather than reached for through a cast.
 *
 * This is the one Bull Board package these published types may name. The Bull
 * Board ships an adapter per framework — eight of them — and jubs names two, so
 * everyone else builds the adapter and passes it here; and whoever builds one
 * already has `@bull-board/api` in the tree, since every adapter depends on it.
 */
export type BoardServer = IServerAdapter & {
	setBasePath(path: string): unknown
}

export interface MountOptions extends DashboardOptions {
	readonly serverAdapter: BoardServer
}

/**
 * Builds the Bull Board over the queues these definitions use, on the server
 * adapter you built — the way to mount it on any framework the Bull Board has
 * an adapter for, and the one jubs itself uses for Express and Fastify.
 *
 * It opens a `Queue` per board queue and gives nothing back: mount the board
 * through your own adapter afterwards, by whichever call that adapter offers,
 * and under the very `basePath` given here, since the board writes its own
 * links from it.
 *
 * The base path is set before `createBullBoard` because nothing says when an
 * adapter reads it. `IServerAdapter` does not declare `setBasePath` at all, so
 * it promises nothing about it; the two adapters jubs mounts happen to read
 * `basePath` inside the entry route's handler, at request time, and an adapter
 * that reads it while the board is being built is just as allowed. Setting it
 * first is the one order that serves both.
 */
export async function mountDashboard(options: MountOptions): Promise<void> {
	const queues = boardQueues(options)

	const [{ createBullBoard }, { BullMQAdapter }] = await Promise.all([
		loaded("@bull-board/api", import("@bull-board/api")),
		loaded("@bull-board/api", import("@bull-board/api/bullMQAdapter")),
	])

	const adapters = queues.map(
		(queue) =>
			new BullMQAdapter(new Queue(queue.name, { connection: options.connection }), {
				readOnlyMode: queue.readOnly,
				description: queue.description,
			}),
	)

	options.serverAdapter.setBasePath(options.basePath)

	createBullBoard({ queues: adapters, serverAdapter: options.serverAdapter })
}

/**
 * `mountDashboard` on an `ExpressAdapter` this builds for you, as a router to
 * mount with `app.use(basePath, router)`.
 */
export async function expressDashboard(options: DashboardOptions): Promise<ExpressRouter> {
	const { ExpressAdapter } = await loaded("@bull-board/express", import("@bull-board/express"))

	const serverAdapter = new ExpressAdapter()

	await mountDashboard({ ...options, serverAdapter })

	return serverAdapter.getRouter() as ExpressRouter
}

/**
 * `mountDashboard` on a `FastifyAdapter` this builds for you, as a plugin to
 * register with `fastify.register(plugin, { prefix: basePath })`.
 */
export async function fastifyDashboard(options: DashboardOptions): Promise<FastifyPlugin> {
	const { FastifyAdapter } = await loaded("@bull-board/fastify", import("@bull-board/fastify"))

	const serverAdapter = new FastifyAdapter()

	await mountDashboard({ ...options, serverAdapter })

	return serverAdapter.registerPlugin() as unknown as FastifyPlugin
}

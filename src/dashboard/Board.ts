import { deadQueueName } from "@/Dead"
import type { JobDefinition } from "@/Definition"

const BOARD_API = "@bull-board/api"

/**
 * What names the queues of a board.
 *
 * Every queue the board opens is the queue of one of these definitions, or the
 * dead queue of one of them, and no name is ever taken from anywhere else:
 * opening a `Queue` writes `bull:<name>:meta` to Redis and never takes it back,
 * so a name resolved from a URL or a search box would turn one operator's typo
 * into a key that outlives the process.
 */
export interface BoardConfig {
	readonly definitions: readonly JobDefinition[]
	readonly deadQueues?: readonly string[]
	/**
	 * Whether the live queues refuse the board's write actions. It defaults to
	 * `true`, and a dead queue ignores it altogether.
	 *
	 * Two of those actions cannot be made safe from here. "Add job" stores what
	 * the operator typed as the job's `data`, which a worker reads as an
	 * envelope, so the delivery fails before a handler is chosen and no hook
	 * reports it — there is no job name to report one under. And a `jubs.*`
	 * scheduler edited or removed on the screen is written back on the next boot
	 * of any process that starts that queue, so the change looks applied and is
	 * not.
	 *
	 * `readOnly: false` takes every button back, including those two.
	 */
	readonly readOnly?: boolean
}

/**
 * One queue of the board, and how it is registered.
 *
 * A dead queue is read only whatever the config says, because every action the
 * board offers over one is wrong: nothing consumes it, so a retry never runs,
 * and `clean` or `empty` would destroy the very record the queue exists to
 * keep.
 */
export interface BoardQueue {
	readonly name: string
	readonly readOnly: boolean
	readonly description: string
}

const LIVE_READ_ONLY =
	"Read only. The board's write buttons are gone on purpose: \"Add job\" stores what you type as the job's data, which a worker reads as an envelope, so the delivery fails before a handler is chosen and no hook reports it — there is no job name to report one under; and a jubs.* scheduler edited or removed here is written back on the next boot of any process that starts this queue. Retry a job with jobs.retry(id), stop one with jobs.cancel(id), and hold the whole queue with jobs.pause(queue) and jobs.resume(queue). Pass readOnly: false to the dashboard to take the buttons back."

const LIVE_WRITABLE =
	'Writable, because this dashboard was given readOnly: false. Two of these buttons act on a job jubs still owns. "Add job" stores what you type as the job\'s data, which a worker reads as an envelope, so the delivery fails before a handler is chosen and no hook reports it — there is no job name to report one under. And a jubs.* scheduler edited or removed here is written back on the next boot of any process that starts this queue, so the change looks applied and is not.'

/**
 * Reads back the queues a board shows, and refuses a board that would show
 * none.
 *
 * Every name comes out once, because registering the same name twice would open
 * two `Queue` handles over one set of Redis keys, and the board keeps its queues
 * in a `Map` — the second registration replaces the first, and the queue the
 * operator was meant to see is gone from the screen with nothing said.
 *
 * Many definitions share one queue, which is why the live names are
 * deduplicated. A live queue named like a dead one — a definition on `x.dead`
 * while `x` is a dead queue of this board — is the same Redis keys twice over,
 * and it comes out as the dead one: read only is the safe half of that pair,
 * since the board's actions over a burial destroy the record.
 */
export function boardQueues(config: BoardConfig): readonly BoardQueue[] {
	const live = [...new Set(config.definitions.map((definition) => definition.queue))]

	if (live.length === 0) {
		throw new Error(
			"jubs: the dashboard was given no definitions, so the board would open with no queue at all — it shows the queues of the definitions you registered, and it takes a queue name from nowhere else. Pass mountDashboard, expressDashboard or fastifyDashboard the same `definitions` list you gave createJobs.",
		)
	}

	const stray = config.deadQueues?.find((queue) => !live.includes(queue))

	if (stray) {
		const names = live.map((queue) => `"${queue}"`).join(", ")

		throw new Error(
			`jubs: the dashboard was given the dead queue "${stray}", which no definition it was given uses — the queues its definitions use are ${names}. Nothing was opened: the board opens a Queue for every name it is given, and opening one writes bull:${deadQueueName(stray)}:meta to Redis for good.`,
		)
	}

	const dead = [...new Set(config.deadQueues ?? [])].map((queue) => ({
		name: deadQueueName(queue),
		readOnly: true,
		description: `Jobs buried out of "${queue}". Read only, because nothing consumes this queue: replay an entry with jobs.dead.replay(id), and drop one with jobs.dead.discard(id).`,
	}))

	const readOnly = config.readOnly ?? true
	const buried = new Set(dead.map((queue) => queue.name))

	return [
		...live
			.filter((name) => !buried.has(name))
			.map((name) => ({
				name,
				readOnly,
				description: readOnly ? LIVE_READ_ONLY : LIVE_WRITABLE,
			})),
		...dead,
	]
}

function isModuleMissing(error: unknown): boolean {
	if (typeof error !== "object" || error === null) {
		return false
	}

	const { code } = error as { code?: unknown }

	return code === "ERR_MODULE_NOT_FOUND" || code === "MODULE_NOT_FOUND"
}

/**
 * The error a failed peer import throws, turned into an instruction when what
 * failed is the peer being absent.
 *
 * Only a missing module is rewritten. A peer that is installed and throws while
 * it loads has a real failure of its own, and telling that operator to install
 * a package they already have would send them away from it.
 *
 * The command always names `@bull-board/api` beside the server package — the
 * server adapter alone renders nothing, and the api alone mounts nowhere.
 */
function peerFailure(error: unknown, peer: string): unknown {
	if (!isModuleMissing(error)) {
		return error
	}

	const install = peer === BOARD_API ? BOARD_API : `${BOARD_API} ${peer}`

	return new Error(
		`jubs: @juicerq/jubs/dashboard needs "${peer}", which is an optional peer and is not installed — run \`npm install ${install}\``,
	)
}

/**
 * Awaits the import of an optional peer, and turns the peer being absent into
 * the command that installs it.
 *
 * The caller makes the import and this only awaits it, so this module names no
 * Bull Board package at all: an import written here would be a static one, and
 * the peer it names would stop being optional.
 */
export function loaded<Module>(peer: string, loading: Promise<Module>): Promise<Module> {
	return loading.catch((error: unknown) => {
		throw peerFailure(error, peer)
	})
}

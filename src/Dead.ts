import type { ChildResult } from "@/Driver"
import type { Envelope } from "@/Envelope"
import type { SerializedError } from "@/Failure"

export type DeadReason =
	| "attempts_exhausted"
	| "unrecoverable"
	| "version_ahead"
	| "cancelled"
	| "child_dead"

export interface DeadEntry {
	/**
	 * The id the job answered to while it was alive — the very string `enqueue`
	 * gave the producer, and the one `jobs.get` and `jobs.retry` take.
	 *
	 * It is **not** `DeadJob.id`, which names this record inside the dead queue
	 * and is what `jobs.dead.replay` and `jobs.dead.discard` take. A dead entry
	 * that cannot say which job it was leaves an operator with a record and no
	 * way back to the job, so every burial carries it.
	 */
	readonly jobId: string
	readonly envelope: Envelope
	readonly error: SerializedError
	readonly reason: DeadReason
	/**
	 * What the children of this job returned, kept only for a `child_dead`
	 * burial, so replaying it is an informed decision rather than a guess. The
	 * values are the raw JSON projection Redis holds, put through no `result`
	 * schema.
	 */
	readonly children?: readonly ChildResult[]
}

export interface DeadJob extends DeadEntry {
	readonly id: string
}

const DEAD_SUFFIX = ".dead"

export function deadQueueName(queue: string): string {
	return `${queue}${DEAD_SUFFIX}`
}

/**
 * Reads back the live queue a dead queue keeps the jobs of, and gives back
 * `undefined` when the name is not a dead queue's.
 */
export function liveQueueName(dead: string): string | undefined {
	if (!dead.endsWith(DEAD_SUFFIX) || dead.length === DEAD_SUFFIX.length) {
		return undefined
	}

	return dead.slice(0, -DEAD_SUFFIX.length)
}

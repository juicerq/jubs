import { UnrecoverableError } from "bullmq"
import { CancelledError } from "@/Cancellation"
import type { ChildResult, JobDelivery } from "@/Driver"
import type { Envelope } from "@/Envelope"
import type { SerializedError } from "@/Failure"
import { ChildDeadError, ChildrenShortError } from "@/Flow"
import { VersionAheadError } from "@/Migration"

/**
 * Why a job was buried.
 *
 * `child_dead` and `children_short` are the two ways a flow's parent dies over
 * its children, and they are apart because the fixes are apart. `child_dead` is
 * a child that ran and failed every attempt: the child is named, and retrying it
 * puts the flow back together. `children_short` is a slot holding fewer children
 * than it was enqueued with, and there is nothing to retry — the missing child
 * was cancelled, removed, or never enqueued at all, so the flow is enqueued
 * again from the top.
 */
export type DeadReason =
	| "attempts_exhausted"
	| "unrecoverable"
	| "version_ahead"
	| "cancelled"
	| "child_dead"
	| "children_short"

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

/**
 * The error a burial reads through, whether it is the one thrown or the cause
 * of the `UnrecoverableError` that carries it — the same shape `ResultError`
 * reaches a dead entry in.
 */
function raised(error: unknown): unknown {
	if (error instanceof UnrecoverableError && error.cause !== undefined) {
		return error.cause
	}

	return error
}

export function childDead(error: unknown): ChildDeadError | undefined {
	const cause = raised(error)

	if (cause instanceof ChildDeadError) {
		return cause
	}

	return undefined
}

export function deadReason(delivery: JobDelivery, error: unknown): DeadReason | undefined {
	if (error instanceof CancelledError) {
		return "cancelled"
	}

	if (error instanceof VersionAheadError) {
		return "version_ahead"
	}

	if (childDead(error)) {
		return "child_dead"
	}

	if (raised(error) instanceof ChildrenShortError) {
		return "children_short"
	}

	if (error instanceof UnrecoverableError) {
		return "unrecoverable"
	}

	if (delivery.attempt >= delivery.maxAttempts) {
		return "attempts_exhausted"
	}

	return undefined
}

import type { Envelope } from "@/Envelope"
import type { SerializedError } from "@/Failure"

export type DeadReason = "attempts_exhausted" | "unrecoverable" | "version_ahead" | "cancelled"

export interface DeadEntry {
	readonly envelope: Envelope
	readonly error: SerializedError
	readonly reason: DeadReason
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

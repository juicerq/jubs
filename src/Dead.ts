import type { Envelope } from "@/Envelope"
import type { SerializedError } from "@/Failure"

export type DeadReason = "attempts_exhausted" | "unrecoverable" | "version_ahead"

export interface DeadEntry {
	readonly envelope: Envelope
	readonly error: SerializedError
	readonly reason: DeadReason
}

export interface DeadJob extends DeadEntry {
	readonly id: string
}

export function deadQueueName(queue: string): string {
	return `${queue}.dead`
}

export function deadJobId(queue: string, storedId: string): string {
	return `${queue}:${storedId}`
}

export function readDeadJobId(id: string): { queue: string; storedId: string } {
	const cut = id.lastIndexOf(":")

	if (cut < 1 || cut === id.length - 1) {
		throw new Error(
			`juibs: "${id}" is not a dead job id — pass an id returned by jobs.dead.list(queue)`,
		)
	}

	return { queue: id.slice(0, cut), storedId: id.slice(cut + 1) }
}

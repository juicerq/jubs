import { randomUUID } from "node:crypto"

export function composeJobId(queue: string, storedId: string): string {
	return `${queue}:${storedId}`
}

/**
 * What parts a flow child's stored id into the slot it fills and the unique
 * half behind it. It is refused inside a slot name for that reason, and it is
 * neither a character BullMQ refuses in an id nor one that a Redis key or the
 * two-colon cut of a job key would read differently.
 */
export const FLOW_ID_MARKER = "~"

/**
 * The id one child of a flow is stored under.
 *
 * A completed child is swept by its `removeOnComplete`, and its hash goes with
 * it, while the value it returned stays in its parent's `:processed` hash under
 * the child's job key. Carrying the slot in the id is what makes that key still
 * say which slot the value fills — and the slot alone is enough, because the
 * parent knows slot to definition from its own `awaits`.
 */
export function composeChildId(slot: string): string {
	return `${slot}${FLOW_ID_MARKER}${randomUUID()}`
}

/** The slot a child fills, read back out of its stored id. */
export function readChildSlot(storedId: string): string {
	const cut = storedId.indexOf(FLOW_ID_MARKER)

	return cut < 1 ? "" : storedId.slice(0, cut)
}

/**
 * Splits a job id back into the queue that keeps the job and the id Redis
 * stores it under.
 *
 * The cut is at the **first** colon, because a queue name cannot hold one —
 * BullMQ refuses such a name — while a stored id can and does: a job a schedule
 * produced is stored under `repeat:<scheduler>:<millis>`. Cutting anywhere else
 * would read that job's id as a queue name nothing answers to.
 */
export function readJobId(id: string): { queue: string; storedId: string } {
	const cut = id.indexOf(":")

	if (cut < 1 || cut === id.length - 1) {
		throw new Error(
			`jubs: "${id}" is not a job id — pass an id returned by jobs.enqueue or by jobs.dead.list(queue)`,
		)
	}

	return { queue: id.slice(0, cut), storedId: id.slice(cut + 1) }
}

export function composeJobId(queue: string, storedId: string): string {
	return `${queue}:${storedId}`
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

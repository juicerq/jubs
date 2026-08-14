const SHUTDOWN_REDELIVERY_MS = 1_000

/**
 * The reason jubs aborts a handler's signal when `close({ timeoutMs })` runs
 * out of patience, and the error the delivery then fails with.
 *
 * It is not a job failure. A driver that can put a delivery back reschedules it
 * after `delayMs` and spends no attempt, so a deploy costs the job nothing. The
 * runtime therefore skips the failure hooks and the dead queue for it, the same
 * way it skips them for a held idempotency lease.
 *
 * `running` is that handler body: the promise that went on running after the
 * delivery already failed. It is present only when the abort replaced an error
 * carrying it, and it is what keeps the idempotency lease held against that
 * body instead of being freed under it — see `StillRunning`.
 *
 * `cause` keeps the error the abort replaced. It is absent when there was no
 * substitution — the case of a handler that obeys by rethrowing its own
 * `signal.reason`, where the error thrown is this one already.
 */
export class ShutdownAbortError extends Error {
	readonly delayMs: number

	readonly running: Promise<unknown> | undefined

	constructor(name: string, running?: Promise<unknown>) {
		super(
			`jubs: the job "${name}" was still running when close() ran out of time — this delivery waits ${SHUTDOWN_REDELIVERY_MS}ms, is delivered again and spends no attempt`,
		)
		this.name = "ShutdownAbortError"
		this.delayMs = SHUTDOWN_REDELIVERY_MS
		this.running = running
	}
}

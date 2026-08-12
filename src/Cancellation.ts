import { UnrecoverableError } from "bullmq"

/**
 * How long a cancellation mark waits for the runtime that runs the job.
 *
 * The mark only has to live from the moment a client writes it to the next
 * sweep of the worker holding that delivery, which is `CANCEL_SWEEP_MS` away.
 * The slack on top of that covers a delivery BullMQ hands out again after its
 * worker died, whose stalled check runs every 30s by default — the cancellation
 * then lands on the redelivery instead of being lost. Past that the mark
 * expires, so a mark that never found its job is neither eternal garbage nor a
 * trap that kills a `jobs.retry` made long after the cancellation.
 */
export const CANCEL_MARK_TTL_MS = 30_000

/**
 * How often a runtime asks which of its running jobs were cancelled.
 *
 * A cancellation is asked for by a person, so a quarter of a second of latency
 * is invisible, and it costs one round trip per queue with deliveries in
 * flight. A runtime holding nothing asks nothing.
 */
export const CANCEL_SWEEP_MS = 250

/**
 * One delivery of one job, as the cancellation channel names it.
 *
 * A cancellation belongs to the delivery it was asked against, not to the job:
 * a job id comes back on every retry, so a mark that named only the id would
 * kill a delivery nobody cancelled. `attemptsStarted` is what tells the two
 * apart — it counts the deliveries a job has been handed out for, it grows on
 * every one of them, and a `jobs.retry` does not reset it.
 */
export interface RunningDelivery {
	readonly id: string
	readonly attemptsStarted: number
}

/**
 * The reason juibs aborts a handler's signal when `jobs.cancel(id)` reaches the
 * job, and the error the delivery then fails with.
 *
 * It is a job failure and the opposite of a `ShutdownAbortError`: the job is not
 * wanted, so it spends no further attempt, runs the failure hooks and lands in
 * the dead queue under the reason `cancelled`. It extends `UnrecoverableError`
 * so the driver stops retrying it.
 *
 * `running` is the handler body that went on running after this delivery
 * already failed — a handler that ignored its signal and was cut short by its
 * `timeoutMs`. It is what keeps the idempotency lease held against that body
 * instead of being freed under it — see `StillRunning`. It is absent whenever
 * the delivery and the body ended together, which is the obedient case.
 *
 * `cause` keeps the error this one replaced, and is absent when there was no
 * substitution.
 */
export class CancelledError extends UnrecoverableError {
	readonly running: Promise<unknown> | undefined

	constructor(name: string, running?: Promise<unknown>) {
		super(
			`juibs: the job "${name}" was cancelled while it ran — its signal was aborted and the attempt failed without a retry`,
		)
		this.name = "CancelledError"
		this.running = running
	}
}

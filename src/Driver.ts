import type { RunningDelivery } from "@/Cancellation"
import type { DeadEntry } from "@/Dead"
import type { Delivery } from "@/Delivery"
import type { Envelope } from "@/Envelope"
import type { IdempotencyStore } from "@/Idempotency"
import type { Recurrence } from "@/Schedule"

export interface EnqueueRequest {
	readonly queue: string
	readonly envelope: Envelope
	readonly delivery: Delivery
}

export interface EnqueuedJob {
	readonly id: string
}

/**
 * One job of a flow, and the jobs it waits on. It carries the three fields an
 * `EnqueueRequest` carries, because every node of a flow is an ordinary job —
 * the children are what make it a flow.
 */
export interface FlowNode extends EnqueueRequest {
	readonly children: readonly FlowNode[]
}

/**
 * What one child of a flow gave back. `name` is the name of the definition that
 * ran, and `value` the JSON projection of what its handler returned — what
 * Redis stored, before any `result` schema has seen it.
 */
export interface ChildResult {
	readonly queue: string
	readonly name: string
	readonly value: unknown
}

/**
 * One child of a flow that failed every attempt. The parent runs all the same,
 * because a child's failure only drops it from the parent's dependencies, and
 * the reason is what Redis kept of the last attempt.
 *
 * `id` is the child's id in the one form the whole API speaks, so whoever reads
 * a buried parent can act on it: `jobs.retry(id)` puts the child back among the
 * parent's dependencies, and the parent runs again over a full set of results.
 */
export interface ChildFailure {
	readonly id: string
	readonly name: string
	readonly reason: string
}

/**
 * What the children of one flow job settled into, read in one step: the results
 * of those that finished, and the failures of those that did not.
 */
export interface FlowState {
	readonly results: readonly ChildResult[]
	readonly failures: readonly ChildFailure[]
}

/**
 * Enqueues whole flows, and reads back what their children settled into.
 *
 * `enqueue` takes the root and adds every node of the tree in one step, so a
 * flow either exists whole or not at all, and gives back the root — the job the
 * producer holds an id for. `read` answers for one parent, named by its queue
 * and stored id.
 */
export interface FlowStore {
	enqueue(root: FlowNode): Promise<EnqueuedJob>
	read(queue: string, id: string): Promise<FlowState>
}

export interface JobDelivery extends RunningDelivery {
	readonly attempt: number
	readonly maxAttempts: number
	readonly envelope: unknown
}

export type JobState =
	| "waiting"
	| "active"
	| "delayed"
	| "completed"
	| "failed"
	| "waiting_children"
	| "unknown"

export interface JobSnapshot {
	readonly id: string
	readonly queue: string
	readonly name: string
	readonly state: JobState
	/**
	 * The envelope the job carries, and `undefined` when what is stored is not a
	 * juibs envelope — a job another producer put on the same queue.
	 */
	readonly envelope: Envelope | undefined
	/**
	 * How many attempts this job has already ended, successfully or not. A job
	 * that has never run reads 0, and a job running its second attempt reads 1,
	 * because the attempt under way has not ended.
	 *
	 * It is a count, not a position: `HandlerContext.attempt` names the attempt a
	 * handler is running and is a different number.
	 */
	readonly attempts: number
	readonly maxAttempts: number
	readonly failure: string | undefined
}

export type RetryResult =
	| { readonly outcome: "retried" }
	| { readonly outcome: "unknown_job" }
	| { readonly outcome: "not_failed"; readonly state: JobState }

/**
 * What a cancellation reached. `removed` is a job that had not started and is
 * gone from the queue; `aborting` is a job already running, whose signal is
 * aborted by the runtime that holds it — here or in another process; `finished`
 * is a job the cancellation arrived too late for, and carries the state it
 * settled in.
 *
 * `children_running` is a job of a flow that waits on children of which at
 * least one is running right now. **Nothing was cancelled.** The job cannot be
 * removed while a descendant holds a lock, and it is not running itself, so
 * there is no delivery to abort either. Cancel the running child first, or ask
 * again once the children have settled.
 *
 * `removed` says the job is gone, not that it never ran. The state is read and
 * the job is deleted in two steps, so a job that was waiting when it was read
 * and completed before the deletion landed is deleted all the same, and the
 * work it did stands. Closing that window would cost a lock on every
 * cancellation to buy nothing: whoever cancels a job racing its own start
 * cannot know which of the two won anyway.
 */
export type CancelResult =
	| { readonly outcome: "removed" }
	| { readonly outcome: "aborting" }
	| { readonly outcome: "children_running" }
	| { readonly outcome: "unknown_job" }
	| { readonly outcome: "finished"; readonly state: JobState }

export interface QueueLimiter {
	readonly max: number
	readonly durationMs: number
}

export interface ConsumeRequest {
	readonly queue: string
	readonly concurrency: number
	readonly limiter?: QueueLimiter
	readonly run: (delivery: JobDelivery) => Promise<unknown>
}

export interface Consumer {
	/**
	 * Stops taking new deliveries, and resolves only once every delivery already
	 * in flight has settled.
	 *
	 * The runtime's `close({ timeoutMs })` is built on that promise: it waits for
	 * this one, and aborts the signals of the handlers still running only when it
	 * has waited too long. A `close` that resolved before its deliveries settled
	 * would report a drain that did not happen, and the runtime would abort
	 * nothing and let the process exit under a running handler.
	 */
	close(): Promise<void>
}

export interface DeadStore {
	bury(queue: string, entry: DeadEntry): Promise<void>
	list(queue: string): Promise<readonly { id: string; entry: DeadEntry }[]>
	read(queue: string, id: string): Promise<DeadEntry | undefined>
	remove(queue: string, id: string): Promise<boolean>
}

export interface ScheduleUpsert {
	readonly recurrence: Recurrence
	readonly timezone?: string
	readonly envelope: Envelope
	readonly delivery: Delivery
}

export interface ReconcileRequest {
	readonly queue: string
	readonly declared: readonly ScheduleUpsert[]
}

export interface JobDriver {
	enqueue(request: EnqueueRequest): Promise<EnqueuedJob>
	consume(request: ConsumeRequest): Promise<Consumer>
	get(queue: string, id: string): Promise<JobSnapshot | undefined>
	retry(queue: string, id: string): Promise<RetryResult>
	/**
	 * Stops the job an id names, and says what the cancellation reached.
	 *
	 * A job that has not started is deleted with the children it waits on, since
	 * a flow's children exist to feed the job being cancelled and would otherwise
	 * finish into a parent that is gone. The deletion is not atomic with the read
	 * of the state before it — see `CancelResult` for the window that opens.
	 *
	 * A job already running is marked instead, for the runtime holding it to
	 * abort. The mark names that one delivery, so a cancellation that arrives
	 * after its delivery ended is collected by nobody and expires.
	 */
	cancel(queue: string, id: string): Promise<CancelResult>
	/**
	 * Reads which of the deliveries a runtime is running right now were cancelled,
	 * and consumes those cancellations in the same step.
	 *
	 * It gives back the very objects it was given, so the caller aborts what it
	 * already holds. A cancellation is matched against the delivery it was asked
	 * against: one asked against a delivery that has already ended matches nothing
	 * and is never handed out, so it kills no later delivery of the same job.
	 */
	takeCancelled<Running extends RunningDelivery>(
		queue: string,
		running: readonly Running[],
	): Promise<readonly Running[]>
	pause(queue: string): Promise<void>
	resume(queue: string): Promise<void>
	reconcileSchedules(request: ReconcileRequest): Promise<void>
	readonly dead: DeadStore
	readonly idempotency: IdempotencyStore
	readonly flow: FlowStore
}

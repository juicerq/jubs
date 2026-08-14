import { UnrecoverableError } from "bullmq"
import { CancelledError } from "@/Cancellation"
import type { ChildResult, JobDelivery } from "@/Driver"
import { type Envelope, readEnvelope } from "@/Envelope"
import type { ErrorSnapshot, SerializedError } from "@/Failure"
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

const DEAD_REASONS: readonly DeadReason[] = [
	"attempts_exhausted",
	"unrecoverable",
	"version_ahead",
	"cancelled",
	"child_dead",
	"children_short",
]

/**
 * Why a job was buried, and what that reason carries with it.
 *
 * The children travel inside the `child_dead` arm rather than beside it, so a
 * burial for any other reason cannot be written holding them, and a
 * `child_dead` burial cannot be written without them.
 */
export type Burial =
	| {
			readonly reason: "child_dead"
			/**
			 * What the children of this job returned, so replaying it is an informed
			 * decision rather than a guess. The values are the raw JSON projection
			 * Redis holds, put through no `result` schema.
			 */
			readonly children: readonly ChildResult[]
	  }
	| { readonly reason: Exclude<DeadReason, "child_dead"> }

export type DeadEntry = {
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
} & Burial

export type DeadJob = DeadEntry & {
	readonly id: string
}

class DeadRecordError extends Error {
	constructor(reason: string) {
		super(
			`jubs: the stored dead record is not a jubs burial — ${reason}. Nothing can replay a record of this shape; drop it with jobs.dead.discard(id).`,
		)
		this.name = "DeadRecordError"
	}
}

function isDeadReason(value: unknown): value is DeadReason {
	return DEAD_REASONS.some((reason) => reason === value)
}

function readSnapshot(stored: unknown, field: string): ErrorSnapshot {
	if (typeof stored !== "object" || stored === null) {
		throw new DeadRecordError(
			`its ${field} is ${JSON.stringify(stored)}, which is not a serialized error`,
		)
	}

	const name: unknown = Reflect.get(stored, "name")
	const message: unknown = Reflect.get(stored, "message")
	const stack: unknown = Reflect.get(stored, "stack")

	if (typeof name !== "string") {
		throw new DeadRecordError(`its ${field} carries no error name`)
	}

	if (typeof message !== "string") {
		throw new DeadRecordError(`its ${field} carries no error message`)
	}

	if (stack !== undefined && typeof stack !== "string") {
		throw new DeadRecordError(`its ${field} carries a stack that is not text`)
	}

	const read: ErrorSnapshot = { name, message }

	if (stack === undefined) {
		return read
	}

	return { ...read, stack }
}

function readError(stored: unknown): SerializedError {
	const read = readSnapshot(stored, "error")

	if (typeof stored !== "object" || stored === null) {
		return read
	}

	const cause: unknown = Reflect.get(stored, "cause")

	if (cause === undefined) {
		return read
	}

	return { ...read, cause: readSnapshot(cause, "error cause") }
}

function readChildren(stored: unknown): readonly ChildResult[] {
	if (!Array.isArray(stored)) {
		throw new DeadRecordError(
			`it was buried for "child_dead" and its children are ${JSON.stringify(stored)}, which is not a list of what the children returned`,
		)
	}

	const read: ChildResult[] = []

	for (const child of stored) {
		if (
			typeof child !== "object" ||
			child === null ||
			!("slot" in child) ||
			typeof child.slot !== "string"
		) {
			throw new DeadRecordError(`its child ${JSON.stringify(child)} names no slot it filled`)
		}

		read.push({ ...child, slot: child.slot, value: Reflect.get(child, "value") })
	}

	return read
}

/**
 * Reads a dead record back out of the queue that keeps it, refusing anything
 * that is not a burial this library wrote.
 *
 * A record is read once, here, and past this point its shape is the truth
 * `jobs.dead.replay` and `jobs.dead.discard` act on. Refusing is the only sound
 * answer: a record the queue holds but nothing can read names no job to put
 * back, and replaying it would enqueue a payload no definition validated.
 */
export function readDeadEntry(stored: unknown): DeadEntry {
	if (typeof stored !== "object" || stored === null) {
		throw new DeadRecordError(`expected an object, got ${typeof stored}`)
	}

	const jobId: unknown = Reflect.get(stored, "jobId")
	const envelope: unknown = Reflect.get(stored, "envelope")
	const error: unknown = Reflect.get(stored, "error")
	const reason: unknown = Reflect.get(stored, "reason")
	const children: unknown = Reflect.get(stored, "children")

	if (typeof jobId !== "string" || jobId.length === 0) {
		throw new DeadRecordError("its job id is missing, so it names no job to put back")
	}

	if (!isDeadReason(reason)) {
		throw new DeadRecordError(`its reason ${JSON.stringify(reason)} is not one a job is buried for`)
	}

	const buried = {
		jobId,
		envelope: readEnvelope(envelope),
		error: readError(error),
	}

	if (reason === "child_dead") {
		return { ...buried, reason, children: readChildren(children) }
	}

	if (children !== undefined) {
		throw new DeadRecordError(
			`it was buried for "${reason}" and still carries children, which only a child_dead burial holds`,
		)
	}

	return { ...buried, reason }
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

function childDead(error: unknown): ChildDeadError | undefined {
	const cause = raised(error)

	if (cause instanceof ChildDeadError) {
		return cause
	}

	return undefined
}

/**
 * Whether this failure buries the job, and for what.
 *
 * It gives back the whole burial rather than the reason alone, so the children
 * a `child_dead` burial keeps are read in the same step that decides it, and no
 * caller classifies the error a second time to graft them on.
 */
export function burialFor(delivery: JobDelivery, error: unknown): Burial | undefined {
	if (error instanceof CancelledError) {
		return { reason: "cancelled" }
	}

	if (error instanceof VersionAheadError) {
		return { reason: "version_ahead" }
	}

	const dead = childDead(error)

	if (dead) {
		return { reason: "child_dead", children: dead.results }
	}

	if (raised(error) instanceof ChildrenShortError) {
		return { reason: "children_short" }
	}

	if (error instanceof UnrecoverableError) {
		return { reason: "unrecoverable" }
	}

	if (delivery.attempt >= delivery.maxAttempts) {
		return { reason: "attempts_exhausted" }
	}

	return undefined
}

import { AsyncLocalStorage } from "node:async_hooks"
import type { Envelope } from "@/Envelope"

/**
 * One enqueue an atomic block holds: the call that would deliver it, and the
 * envelope an outbox row keeps in place of that call. A block writing to an
 * outbox never makes the call, so the two travel together — and `forOutbox` is
 * also where an enqueue no row can hold refuses itself, since it is the only
 * part of the pair a writing block asks for.
 */
export interface HeldEnqueue<Delivered> {
	readonly deliver: () => Promise<Delivered>
	readonly forOutbox: () => Envelope
}

type WriteEnvelopes = (envelopes: readonly Envelope[]) => Promise<void>

interface AtomicScope {
	readonly held: HeldEnqueue<unknown>[]
	readonly written: Envelope[]
	readonly write: WriteEnvelopes | undefined
	closed: boolean
}

const openScopes = new AsyncLocalStorage<AtomicScope>()

export function inAtomicBlock(): boolean {
	const scope = openScopes.getStore()

	return !!scope && !scope.closed
}

// oxlint-disable-next-line juicerq/no-trivial-call-wrapper
export function outsideAtomicBlock<Result>(run: () => Result): Result {
	return openScopes.exit(run)
}

function endedMessage(scope: AtomicScope): string {
	const done = scope.write ? "wrote what it held to the outbox" : "delivered what it held"

	return `jubs: this enqueue reached an atomic block that has already ended, so nothing holds it — it was made from a callback the block scheduled and never waited for, which keeps the block's context and outlives it. The block ${done} and cannot take this one, and it is refused instead of being dropped in silence. Await that work inside the block, or enqueue outside the block.`
}

export async function holdOrDeliver<Delivered>(
	held: HeldEnqueue<Delivered>,
): Promise<Delivered | null> {
	const scope = openScopes.getStore()

	if (!scope) {
		return held.deliver()
	}

	if (scope.closed) {
		throw new Error(endedMessage(scope))
	}

	if (scope.write) {
		scope.written.push(held.forOutbox())

		return null
	}

	scope.held.push(held)

	return null
}

async function flush(held: readonly HeldEnqueue<unknown>[]): Promise<void> {
	for (const [index, enqueue] of held.entries()) {
		try {
			await enqueue.deliver()
		} catch (failure) {
			throw new Error(
				`jubs: an atomic block delivered ${index} of the ${held.length} jobs it held and then failed — the job it failed on is not enqueued, and the ${held.length - index - 1} after it were abandoned without being tried. What is enqueued stands: a job a worker may have taken already cannot be unenqueued, so running the block again delivers those ${index} a second time.`,
				{ cause: failure },
			)
		}
	}
}

/**
 * Runs a block, and settles what it held once it resolves: a block with no
 * `write` delivers its jobs one by one, and a block with one hands every
 * envelope to it in a single call and delivers nothing itself.
 *
 * A block that enqueued nothing calls `write` not at all, rather than with an
 * empty list. An insert of no rows is not a statement every database library
 * builds — several compose invalid SQL out of it — and that statement would run
 * on the caller's own transaction, where it takes the whole transaction down.
 */
export async function runAtomicBlock<Result>(
	run: () => Promise<Result>,
	write?: WriteEnvelopes,
): Promise<Result> {
	if (inAtomicBlock()) {
		return run()
	}

	const scope: AtomicScope = { held: [], written: [], write, closed: false }
	let result: Result

	try {
		result = await openScopes.run(scope, run)
	} finally {
		scope.closed = true
	}

	if (scope.write) {
		if (scope.written.length > 0) {
			await scope.write(scope.written)
		}

		return result
	}

	await flush(scope.held)

	return result
}

import { AsyncLocalStorage } from "node:async_hooks"

type HeldDelivery = () => Promise<unknown>

interface AtomicScope {
	readonly held: HeldDelivery[]
	closed: boolean
}

const openScopes = new AsyncLocalStorage<AtomicScope>()

export function inAtomicBlock(): boolean {
	const scope = openScopes.getStore()

	return !!scope && !scope.closed
}

export function outsideAtomicBlock<Result>(run: () => Result): Result {
	return openScopes.exit(run)
}

export async function holdOrDeliver<Delivered>(
	deliver: () => Promise<Delivered>,
): Promise<Delivered | null> {
	const scope = openScopes.getStore()

	if (!scope) {
		return deliver()
	}

	if (scope.closed) {
		throw new Error(
			"jubs: this enqueue reached an atomic block that has already ended, so nothing holds it — it was made from a callback the block scheduled and never waited for, which keeps the block's context and outlives it. The block delivered what it held and cannot deliver this one, and it is refused instead of being dropped in silence. Await that work inside the block, or enqueue outside the block.",
		)
	}

	scope.held.push(deliver)

	return null
}

async function flush(held: readonly HeldDelivery[]): Promise<void> {
	for (const [index, deliver] of held.entries()) {
		try {
			await deliver()
		} catch (failure) {
			throw new Error(
				`jubs: an atomic block delivered ${index} of the ${held.length} jobs it held and then failed — the job it failed on is not enqueued, and the ${held.length - index - 1} after it were abandoned without being tried. What is enqueued stands: a job a worker may have taken already cannot be unenqueued, so running the block again delivers those ${index} a second time.`,
				{ cause: failure },
			)
		}
	}
}

export async function runAtomicBlock<Result>(run: () => Promise<Result>): Promise<Result> {
	if (inAtomicBlock()) {
		return run()
	}

	const scope: AtomicScope = { held: [], closed: false }
	let result: Result

	try {
		result = await openScopes.run(scope, run)
	} finally {
		scope.closed = true
	}

	await flush(scope.held)

	return result
}

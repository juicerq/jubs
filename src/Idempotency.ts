import type { JobDefinition } from "@/Definition"

export const IDEMPOTENCY_LEASE_MS = 30_000

export const IDEMPOTENCY_RENEW_MS = 10_000

export const IDEMPOTENCY_RESULT_RETENTION_MS: number = 24 * 60 * 60 * 1_000

export const IDEMPOTENCY_MAX_RESULT_BYTES: number = 64 * 1_024

export interface KeptResult {
	readonly result?: unknown
}

export type IdempotencyLease =
	| { readonly state: "acquired"; readonly token: string }
	| { readonly state: "held"; readonly retryInMs: number }
	| { readonly state: "complete"; readonly kept: KeptResult }

export interface AcquireRequest {
	readonly key: string
	readonly leaseMs: number
}

export interface RenewRequest extends AcquireRequest {
	readonly token: string
}

export interface ReleaseRequest {
	readonly key: string
	readonly token: string
}

export interface CompleteRequest extends ReleaseRequest {
	readonly kept: KeptResult
	readonly retainForMs: number
}

/**
 * Keeps the three states an idempotency key can be in: absent, held by a
 * running delivery under a lease that expires, and complete with a kept result.
 *
 * `acquire` mints a token that names this one possession of the key. `renew`,
 * `complete` and `release` carry it back, and a store acts only while the key
 * still holds that token. A worker whose lease expired under a running handler
 * therefore cannot renew, finish or free the lease a second worker took after
 * it — without the token it would break the very guarantee the lease exists for.
 */
export interface IdempotencyStore {
	acquire(request: AcquireRequest): Promise<IdempotencyLease>
	renew(request: RenewRequest): Promise<void>
	complete(request: CompleteRequest): Promise<void>
	release(request: ReleaseRequest): Promise<void>
}

/**
 * What an error says when the delivery it failed left the handler's body running.
 *
 * A `timeoutMs` fails the attempt without waiting, because nothing can kill a
 * running function — so the body stays alive with the key it was given. The
 * lease has to outlive the delivery and follow the body, or the next delivery
 * finds the key free and runs a second body beside the first.
 */
export interface StillRunning {
	readonly running: Promise<unknown>
}

export function stillRunning(error: unknown): Promise<unknown> | undefined {
	if (!(error instanceof Error) || !("running" in error)) {
		return undefined
	}

	return error.running instanceof Promise ? error.running : undefined
}

export class LeaseHeldError extends Error {
	readonly delayMs: number

	constructor(key: string, delayMs: number) {
		super(
			`jubs: the idempotency key "${key}" is held by a running delivery — this delivery waits ${delayMs}ms and is delivered again`,
		)
		this.name = "LeaseHeldError"
		this.delayMs = delayMs
	}
}

export function idempotencyKeyFor(definition: JobDefinition, data: unknown): string | undefined {
	if (!definition.idempotencyKey) {
		return undefined
	}

	return `${definition.name}:${definition.idempotencyKey(data)}`
}

function serialize(result: unknown): string | undefined {
	try {
		return JSON.stringify(result)
	} catch {
		return undefined
	}
}

function keptResultOf(result: unknown): KeptResult {
	const serialized = serialize(result)

	if (serialized === undefined) {
		return {}
	}

	if (new TextEncoder().encode(serialized).length > IDEMPOTENCY_MAX_RESULT_BYTES) {
		return {}
	}

	return { result }
}

function renewWhileRunning(store: IdempotencyStore, request: RenewRequest): () => void {
	const renewing = setInterval(() => {
		store.renew(request).catch((error: unknown) => {
			console.error(
				`jubs: the idempotency lease of "${request.key}" could not be renewed; the job may run again`,
				error,
			)
		})
	}, IDEMPOTENCY_RENEW_MS)

	renewing.unref?.()

	return () => clearInterval(renewing)
}

function settleWhenBodyEnds(
	store: IdempotencyStore,
	held: ReleaseRequest,
	running: Promise<unknown>,
	stop: () => void,
): void {
	running
		.then(
			(result) =>
				store.complete({
					...held,
					kept: keptResultOf(result),
					retainForMs: IDEMPOTENCY_RESULT_RETENTION_MS,
				}),
			() => store.release(held),
		)
		.catch((error: unknown) => {
			console.error(
				`jubs: the idempotency lease of "${held.key}" outlived its delivery and could not be settled; the key stays held until it expires`,
				error,
			)
		})
		.finally(stop)
}

export async function runUnderKey(
	store: IdempotencyStore,
	key: string,
	run: () => Promise<unknown>,
): Promise<unknown> {
	const lease = await store.acquire({ key, leaseMs: IDEMPOTENCY_LEASE_MS })

	if (lease.state === "held") {
		throw new LeaseHeldError(key, lease.retryInMs)
	}

	if (lease.state === "complete") {
		return lease.kept.result
	}

	const { token } = lease
	const stop = renewWhileRunning(store, { key, token, leaseMs: IDEMPOTENCY_LEASE_MS })

	const outcome = await run().then(
		(result) => ({ result }),
		(error: unknown) => ({ error }),
	)

	if ("error" in outcome) {
		const running = stillRunning(outcome.error)

		if (running) {
			settleWhenBodyEnds(store, { key, token }, running, stop)

			throw outcome.error
		}

		stop()

		await store.release({ key, token })

		throw outcome.error
	}

	stop()

	await store.complete({
		key,
		token,
		kept: keptResultOf(outcome.result),
		retainForMs: IDEMPOTENCY_RESULT_RETENTION_MS,
	})

	return outcome.result
}

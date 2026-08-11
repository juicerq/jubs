import type { JobDefinition } from "@/Definition"

const KEEP_LAST_WINDOW_FIX =
	'juibs: unique mode "keepLast" needs a window — give `ttlMs` to the definition\'s `unique`, so a later enqueue has a period in which it can replace the job'

type UniqueMode = "keepFirst" | "keepLast" | "noOverlap"

export interface Unique<Data = unknown> {
	readonly key: (data: Data) => string
	readonly mode: UniqueMode
	readonly ttlMs?: number
}

export type ResolvedUnique =
	| { readonly mode: "keepFirst"; readonly key: string; readonly ttlMs?: number }
	| { readonly mode: "keepLast"; readonly key: string; readonly ttlMs: number }
	| { readonly mode: "noOverlap"; readonly key: string }

export interface Delivery {
	readonly attempts: number
	readonly backoff: { readonly type: "exponential"; readonly delayMs: number }
	readonly priority: number
	readonly keepCompletedForMs: number
	readonly keepFailedCount: number
	readonly delayMs?: number
	readonly unique?: ResolvedUnique
}

export type DeliveryOverrides<Data = unknown> = {
	[Key in keyof Omit<Delivery, "unique">]?: Delivery[Key] | undefined
} & { unique?: Unique<Data> | undefined }

export type DeliveryPolicy<Data = unknown> =
	| DeliveryOverrides<Data>
	| ((input: { data: Data; options: Delivery }) => DeliveryOverrides<Data>)

export const DELIVERY_DEFAULTS: Delivery = {
	attempts: 5,
	backoff: { type: "exponential", delayMs: 2_000 },
	priority: 20,
	keepCompletedForMs: 60 * 60 * 1_000,
	keepFailedCount: 200,
}

function resolveUnique(unique: Unique, data: unknown): ResolvedUnique {
	const key = unique.key(data)

	if (unique.mode === "noOverlap") {
		return { mode: "noOverlap", key }
	}

	if (unique.mode === "keepLast") {
		if (unique.ttlMs === undefined) {
			throw new Error(KEEP_LAST_WINDOW_FIX)
		}

		return { mode: "keepLast", key, ttlMs: unique.ttlMs }
	}

	if (unique.ttlMs === undefined) {
		return { mode: "keepFirst", key }
	}

	return { mode: "keepFirst", key, ttlMs: unique.ttlMs }
}

function resolveDelayMs(delayMs: number | undefined, unique?: ResolvedUnique): number | undefined {
	if (unique?.mode !== "keepLast") {
		return delayMs
	}

	return Math.max(delayMs ?? 0, unique.ttlMs)
}

function resolve(definition: JobDefinition, data: unknown, keepUnique: boolean): Delivery {
	const policy = definition.delivery

	if (!policy) {
		return DELIVERY_DEFAULTS
	}

	const overrides =
		typeof policy === "function" ? policy({ data, options: DELIVERY_DEFAULTS }) : policy

	const resolved: Delivery = {
		attempts: overrides.attempts ?? DELIVERY_DEFAULTS.attempts,
		backoff: overrides.backoff ?? DELIVERY_DEFAULTS.backoff,
		priority: overrides.priority ?? DELIVERY_DEFAULTS.priority,
		keepCompletedForMs: overrides.keepCompletedForMs ?? DELIVERY_DEFAULTS.keepCompletedForMs,
		keepFailedCount: overrides.keepFailedCount ?? DELIVERY_DEFAULTS.keepFailedCount,
	}

	const unique = keepUnique && overrides.unique ? resolveUnique(overrides.unique, data) : undefined
	const delivery = unique ? { ...resolved, unique } : resolved
	const delayMs = resolveDelayMs(overrides.delayMs, unique)

	if (delayMs === undefined) {
		return delivery
	}

	return { ...delivery, delayMs }
}

export function resolveDelivery(definition: JobDefinition, data: unknown): Delivery {
	return resolve(definition, data, true)
}

export function resolveReplayDelivery(definition: JobDefinition, data: unknown): Delivery {
	return resolve(definition, data, false)
}

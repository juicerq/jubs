import type { JobDefinition } from "@/Definition"

export interface Delivery {
	readonly attempts: number
	readonly backoff: { readonly type: "exponential"; readonly delayMs: number }
	readonly priority: number
	readonly keepCompletedForMs: number
	readonly keepFailedCount: number
	readonly delayMs?: number
}

export type DeliveryOverrides = { [Key in keyof Delivery]?: Delivery[Key] | undefined }

export type DeliveryPolicy<Data = unknown> =
	| DeliveryOverrides
	| ((input: { data: Data; options: Delivery }) => DeliveryOverrides)

export const DELIVERY_DEFAULTS: Delivery = {
	attempts: 5,
	backoff: { type: "exponential", delayMs: 2_000 },
	priority: 20,
	keepCompletedForMs: 60 * 60 * 1_000,
	keepFailedCount: 200,
}

export function resolveDelivery(definition: JobDefinition, data: unknown): Delivery {
	const policy = definition.delivery

	if (!policy) {
		return DELIVERY_DEFAULTS
	}

	const overrides =
		typeof policy === "function" ? policy({ data, options: DELIVERY_DEFAULTS }) : policy

	const delivery: Delivery = {
		attempts: overrides.attempts ?? DELIVERY_DEFAULTS.attempts,
		backoff: overrides.backoff ?? DELIVERY_DEFAULTS.backoff,
		priority: overrides.priority ?? DELIVERY_DEFAULTS.priority,
		keepCompletedForMs: overrides.keepCompletedForMs ?? DELIVERY_DEFAULTS.keepCompletedForMs,
		keepFailedCount: overrides.keepFailedCount ?? DELIVERY_DEFAULTS.keepFailedCount,
	}

	if (overrides.delayMs === undefined) {
		return delivery
	}

	return { ...delivery, delayMs: overrides.delayMs }
}

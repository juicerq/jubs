export interface Delivery {
	readonly attempts: number
	readonly backoff: { readonly type: "exponential"; readonly delayMs: number }
	readonly priority: number
	readonly keepCompletedForMs: number
	readonly keepFailedCount: number
}

export const DELIVERY_DEFAULTS: Delivery = {
	attempts: 5,
	backoff: { type: "exponential", delayMs: 2_000 },
	priority: 20,
	keepCompletedForMs: 60 * 60 * 1_000,
	keepFailedCount: 200,
}

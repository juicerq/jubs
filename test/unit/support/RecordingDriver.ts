import { randomUUID } from "node:crypto"
import type { KeptResult } from "@/Idempotency"
import type {
	ConsumeRequest,
	Delivery,
	Envelope,
	JobDelivery,
	JobDriver,
	ReconcileRequest,
} from "@/index"

export interface RecordedEnqueue {
	readonly queue: string
	readonly envelope: Envelope
	readonly delivery: Delivery
}

export interface RecordedCompletion {
	readonly key: string
	readonly kept: KeptResult
}

export interface RecordingDriver extends JobDriver {
	readonly enqueued: RecordedEnqueue[]
	readonly consumed: ConsumeRequest[]
	readonly reconciled: ReconcileRequest[]
	readonly consuming: string[]
	readonly acquired: string[]
	readonly renewed: string[]
	readonly released: string[]
	readonly completed: RecordedCompletion[]
	deliver(queue: string, delivery: JobDelivery): Promise<unknown>
	refuseClose(): void
	refuseSchedules(): void
	keepResult(key: string, kept: KeptResult): void
	holdLease(key: string, retryInMs: number): void
}

const REFUSED_RECONCILE = "recordingDriver was told to refuse the schedules of this start"

const REFUSED_CLOSE = "recordingDriver was told to refuse the close of this consumer"

export function recordingDriver(): RecordingDriver {
	const enqueued: RecordedEnqueue[] = []
	const consumed: ConsumeRequest[] = []
	const reconciled: ReconcileRequest[] = []
	let refusing = false
	let refusingClose = false
	const consumers = new Map<string, ConsumeRequest["run"]>()
	const acquired: string[] = []
	const renewed: string[] = []
	const released: string[] = []
	const completed: RecordedCompletion[] = []
	const kept = new Map<string, KeptResult>()
	const holds = new Map<string, number>()
	const tokens = new Map<string, string>()
	const inFlight = new Set<Promise<unknown>>()
	let delivered = 0

	function unsupported(): Promise<never> {
		return Promise.reject(
			new Error("recordingDriver keeps no dead queue — test the dead queue against memoryDriver"),
		)
	}

	return {
		enqueued,
		consumed,
		reconciled,
		acquired,
		renewed,
		released,
		completed,

		refuseSchedules() {
			refusing = true
		},

		refuseClose() {
			refusingClose = true
		},

		keepResult(key, result) {
			kept.set(key, result)
		},

		holdLease(key, retryInMs) {
			holds.set(key, retryInMs)
		},

		idempotency: {
			async acquire({ key }) {
				acquired.push(key)

				const complete = kept.get(key)

				if (complete) {
					return { state: "complete", kept: complete }
				}

				const retryInMs = holds.get(key)

				if (retryInMs !== undefined) {
					return { state: "held", retryInMs }
				}

				const token = randomUUID()

				tokens.set(key, token)

				return { state: "acquired", token }
			},

			async renew({ key, token }) {
				if (tokens.get(key) !== token) {
					return
				}

				renewed.push(key)
			},

			async complete({ key, token, kept: result }) {
				if (tokens.get(key) !== token) {
					return
				}

				completed.push({ key, kept: result })
				kept.set(key, result)
			},

			async release({ key, token }) {
				if (tokens.get(key) !== token) {
					return
				}

				released.push(key)
				tokens.delete(key)
			},
		},

		async reconcileSchedules(request) {
			if (refusing) {
				throw new Error(REFUSED_RECONCILE)
			}

			reconciled.push(request)
		},

		dead: {
			bury: unsupported,
			list: unsupported,
			read: unsupported,
			remove: unsupported,
		},

		get consuming() {
			return [...consumers.keys()]
		},

		async enqueue(request) {
			delivered += 1
			enqueued.push({
				queue: request.queue,
				envelope: request.envelope,
				delivery: request.delivery,
			})

			return { id: String(delivered) }
		},

		async consume(request) {
			consumed.push(request)
			consumers.set(request.queue, request.run)

			return {
				async close() {
					if (refusingClose) {
						throw new Error(REFUSED_CLOSE)
					}

					consumers.delete(request.queue)

					await Promise.allSettled([...inFlight])
				},
			}
		},

		async deliver(queue, delivery) {
			const run = consumers.get(queue)

			if (!run) {
				throw new Error(`no consumer is open on queue "${queue}"`)
			}

			const running = run(delivery)

			inFlight.add(running)

			return running.finally(() => inFlight.delete(running))
		},
	}
}

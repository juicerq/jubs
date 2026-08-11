import type { ConsumeRequest, Delivery, Envelope, JobDelivery, JobDriver } from "@/index"

export interface RecordedEnqueue {
	readonly queue: string
	readonly envelope: Envelope
	readonly delivery: Delivery
}

export interface RecordingDriver extends JobDriver {
	readonly enqueued: RecordedEnqueue[]
	readonly consumed: ConsumeRequest[]
	readonly consuming: string[]
	deliver(queue: string, delivery: JobDelivery): Promise<unknown>
}

export function recordingDriver(): RecordingDriver {
	const enqueued: RecordedEnqueue[] = []
	const consumed: ConsumeRequest[] = []
	const consumers = new Map<string, ConsumeRequest["run"]>()
	let delivered = 0

	function unsupported(): Promise<never> {
		return Promise.reject(
			new Error("recordingDriver keeps no dead queue — test the dead queue against memoryDriver"),
		)
	}

	return {
		enqueued,
		consumed,

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
					consumers.delete(request.queue)
				},
			}
		},

		async deliver(queue, delivery) {
			const run = consumers.get(queue)

			if (!run) {
				throw new Error(`no consumer is open on queue "${queue}"`)
			}

			return run(delivery)
		},
	}
}

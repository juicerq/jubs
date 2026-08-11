import type { Delivery } from "@/Delivery"
import type { Envelope } from "@/Envelope"

export interface EnqueueRequest {
	readonly queue: string
	readonly envelope: Envelope
	readonly delivery: Delivery
}

export interface EnqueuedJob {
	readonly id: string
}

export interface JobDelivery {
	readonly id: string
	readonly attempt: number
	readonly maxAttempts: number
	readonly envelope: unknown
}

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
	close(): Promise<void>
}

export interface JobDriver {
	enqueue(request: EnqueueRequest): Promise<EnqueuedJob>
	consume(request: ConsumeRequest): Promise<Consumer>
}

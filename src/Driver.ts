import type { DeadEntry } from "@/Dead"
import type { Delivery } from "@/Delivery"
import type { Envelope } from "@/Envelope"
import type { Recurrence } from "@/Schedule"

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

export interface DeadStore {
	bury(queue: string, entry: DeadEntry): Promise<void>
	list(queue: string): Promise<readonly { id: string; entry: DeadEntry }[]>
	read(queue: string, id: string): Promise<DeadEntry | undefined>
	remove(queue: string, id: string): Promise<boolean>
}

export interface ScheduleUpsert {
	readonly recurrence: Recurrence
	readonly timezone?: string
	readonly envelope: Envelope
	readonly delivery: Delivery
}

export interface ReconcileRequest {
	readonly queue: string
	readonly declared: readonly ScheduleUpsert[]
}

export interface JobDriver {
	enqueue(request: EnqueueRequest): Promise<EnqueuedJob>
	consume(request: ConsumeRequest): Promise<Consumer>
	reconcileSchedules(request: ReconcileRequest): Promise<void>
	readonly dead: DeadStore
}

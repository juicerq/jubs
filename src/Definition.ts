import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { DeliveryPolicy } from "@/Delivery"
import type { Schedule } from "@/Schedule"

export type Origin = "direct" | "schedule" | "flow" | "relay"

export interface HandlerContext {
	readonly id: string
	readonly attempt: number
	readonly maxAttempts: number
	readonly origin: Origin
}

export interface JobDefinition<Payload extends StandardSchemaV1 = StandardSchemaV1> {
	readonly name: string
	readonly queue: string
	readonly payload: Payload
	readonly delivery?: DeliveryPolicy
	readonly schedule?: Schedule
}

export interface JobDefinitionInput<Payload extends StandardSchemaV1>
	extends Omit<JobDefinition<Payload>, "delivery" | "schedule"> {
	readonly delivery?: DeliveryPolicy<StandardSchemaV1.InferOutput<Payload>>
	readonly schedule?: Schedule<StandardSchemaV1.InferInput<Payload>>
}

export interface JobHandler {
	readonly definition: JobDefinition
	readonly run: (data: unknown, context: HandlerContext) => Promise<unknown>
}

export type HandlerRun<Payload extends StandardSchemaV1> = (
	data: StandardSchemaV1.InferOutput<Payload>,
	context: HandlerContext,
) => Promise<unknown>

export function defineJob<Payload extends StandardSchemaV1>(
	input: JobDefinitionInput<Payload>,
): JobDefinition<Payload> {
	const named = { name: input.name, queue: input.queue, payload: input.payload }
	const definition = input.schedule ? { ...named, schedule: input.schedule } : named

	if (!input.delivery) {
		return definition
	}

	return { ...definition, delivery: input.delivery as DeliveryPolicy }
}

export function defineHandler<Payload extends StandardSchemaV1>(
	definition: JobDefinition<Payload>,
	run: HandlerRun<Payload>,
): JobHandler {
	return { definition, run: run as JobHandler["run"] }
}

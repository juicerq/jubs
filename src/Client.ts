import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { JobDefinition, JobHandler } from "@/Definition"
import { resolveDelivery } from "@/Delivery"
import type { EnqueuedJob, JobDriver } from "@/Driver"
import { PAYLOAD_VERSION } from "@/Envelope"
import type { JobHooks } from "@/Hooks"
import { validatePayload } from "@/Payload"
import { type JobsRuntime, type StartOptions, startRuntime } from "@/Runtime"

export interface JobsConfig {
	readonly driver: JobDriver
	readonly definitions?: readonly JobDefinition[]
	readonly hooks?: JobHooks
}

export interface JobsClient {
	enqueue<Payload extends StandardSchemaV1>(
		definition: JobDefinition<Payload>,
		data: StandardSchemaV1.InferInput<Payload>,
	): Promise<EnqueuedJob>
	start(handlers: JobHandler[], options?: StartOptions): Promise<JobsRuntime>
}

export function createJobs(config: JobsConfig): JobsClient {
	return {
		async enqueue(definition, data) {
			const validated = await validatePayload(definition, data)

			return config.driver.enqueue({
				queue: definition.queue,
				envelope: { v: PAYLOAD_VERSION, name: definition.name, data, origin: "direct" },
				delivery: resolveDelivery(definition, validated),
			})
		},

		start(handlers, options) {
			return startRuntime(config, handlers, options)
		},
	}
}

import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { JobDefinition, JobHandler } from "@/Definition"
import { DELIVERY_DEFAULTS } from "@/Delivery"
import type { EnqueuedJob, JobDriver } from "@/Driver"
import { PAYLOAD_VERSION } from "@/Envelope"
import { validatePayload } from "@/Payload"
import { type JobsRuntime, startRuntime } from "@/Runtime"

export interface JobsConfig {
	readonly driver: JobDriver
}

export interface JobsClient {
	enqueue<Payload extends StandardSchemaV1>(
		definition: JobDefinition<Payload>,
		data: StandardSchemaV1.InferInput<Payload>,
	): Promise<EnqueuedJob>
	start(handlers: JobHandler[]): Promise<JobsRuntime>
}

export function createJobs(config: JobsConfig): JobsClient {
	return {
		async enqueue(definition, data) {
			await validatePayload(definition, data)

			return config.driver.enqueue({
				queue: definition.queue,
				envelope: { v: PAYLOAD_VERSION, name: definition.name, data, origin: "direct" },
				delivery: DELIVERY_DEFAULTS,
			})
		},

		start(handlers) {
			return startRuntime(config.driver, handlers)
		},
	}
}

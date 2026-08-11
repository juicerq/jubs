import { UnrecoverableError } from "bullmq"
import type { JobHandler } from "@/Definition"
import type { JobDelivery, JobDriver } from "@/Driver"
import { readEnvelope } from "@/Envelope"
import { validatePayload } from "@/Payload"

export interface JobsRuntime {
	close(): Promise<void>
}

function unrecoverable(error: unknown): UnrecoverableError {
	const failure = new UnrecoverableError(error instanceof Error ? error.message : String(error))

	failure.cause = error

	return failure
}

export async function startRuntime(
	driver: JobDriver,
	handlers: JobHandler[],
): Promise<JobsRuntime> {
	const byName = new Map(handlers.map((handler) => [handler.definition.name, handler]))

	async function resolve(delivery: JobDelivery) {
		const envelope = readEnvelope(delivery.envelope)
		const handler = byName.get(envelope.name)

		if (!handler) {
			throw new Error(`juibs: no handler is registered for job "${envelope.name}"`)
		}

		return {
			handler,
			data: await validatePayload(handler.definition, envelope.data),
			context: {
				id: delivery.id,
				attempt: delivery.attempt,
				maxAttempts: delivery.maxAttempts,
				origin: envelope.origin,
			},
		}
	}

	async function run(delivery: JobDelivery): Promise<unknown> {
		const dispatch = await resolve(delivery).catch((error: unknown) => {
			throw unrecoverable(error)
		})

		return dispatch.handler.run(dispatch.data, dispatch.context)
	}

	const queues = [...new Set(handlers.map((handler) => handler.definition.queue))]
	const consumers = await Promise.all(queues.map((queue) => driver.consume({ queue, run })))

	return {
		async close() {
			await Promise.all(consumers.map((consumer) => consumer.close()))
		},
	}
}

import type { StandardSchemaV1 } from "@standard-schema/spec"
import { type DeadJob, deadJobId, readDeadJobId } from "@/Dead"
import { type JobDefinition, type JobHandler, payloadVersion } from "@/Definition"
import { resolveDelivery, resolveDeliveryWithoutUniqueness } from "@/Delivery"
import type { EnqueuedJob, JobDriver } from "@/Driver"
import type { JobHooks } from "@/Hooks"
import { migrateEnvelope } from "@/Migration"
import { validatePayload } from "@/Payload"
import { type JobsRuntime, type StartOptions, startRuntime } from "@/Runtime"
import { assertTimezone } from "@/Schedule"

export interface JobsConfig {
	readonly driver: JobDriver
	readonly definitions?: readonly JobDefinition[]
	readonly deadQueues?: readonly string[]
	readonly timezone?: string
	readonly hooks?: JobHooks
}

export interface JobsClient {
	enqueue<Payload extends StandardSchemaV1>(
		definition: JobDefinition<Payload>,
		data: StandardSchemaV1.InferInput<Payload>,
	): Promise<EnqueuedJob>
	start<Queue extends string>(
		handlers: JobHandler<Queue>[],
		options?: StartOptions<Queue>,
	): Promise<JobsRuntime>
	readonly dead: {
		list(queue: string): Promise<DeadJob[]>
		replay(id: string): Promise<EnqueuedJob>
		discard(id: string): Promise<void>
	}
}

function assertEveryDeadQueueIsUsed(config: JobsConfig): void {
	if (!config.definitions) {
		return
	}

	const used = new Set(config.definitions.map((definition) => definition.queue))
	const stray = config.deadQueues?.find((queue) => !used.has(queue))

	if (!stray) {
		return
	}

	const names = [...used].map((queue) => `"${queue}"`).join(", ")

	throw new Error(
		`juibs: createJobs was given the dead queue "${stray}", which no registered definition uses — the queues its definitions use are ${names}`,
	)
}

export function createJobs(config: JobsConfig): JobsClient {
	assertEveryDeadQueueIsUsed(config)

	if (config.timezone) {
		assertTimezone(config.timezone)
	}

	return {
		dead: {
			async list(queue) {
				const buried = await config.driver.dead.list(queue)

				return buried.map((job) => ({ ...job.entry, id: deadJobId(queue, job.id) }))
			},

			async replay(id) {
				const { queue, storedId } = readDeadJobId(id)
				const entry = await config.driver.dead.read(queue, storedId)

				if (!entry) {
					throw new Error(
						`juibs: no dead job "${id}" is kept — it was replayed or discarded already`,
					)
				}

				const definition = config.definitions?.find(
					(candidate) => candidate.name === entry.envelope.name,
				)

				if (!definition) {
					throw new Error(
						`juibs: the dead job "${id}" runs "${entry.envelope.name}", which this client does not know — register its definition in createJobs({ definitions }) to replay it`,
					)
				}

				const migrated = await migrateEnvelope(definition, entry.envelope)
				const validated = await validatePayload(definition, migrated)

				const enqueued = await config.driver.enqueue({
					queue: definition.queue,
					envelope: entry.envelope,
					delivery: resolveDeliveryWithoutUniqueness(definition, validated),
				})

				await config.driver.dead.remove(queue, storedId)

				return enqueued
			},

			async discard(id) {
				const { queue, storedId } = readDeadJobId(id)
				const dropped = await config.driver.dead.remove(queue, storedId)

				if (dropped) {
					return
				}

				throw new Error(`juibs: no dead job "${id}" is kept — it was replayed or discarded already`)
			},
		},

		async enqueue(definition, data) {
			const validated = await validatePayload(definition, data)

			return config.driver.enqueue({
				queue: definition.queue,
				envelope: { v: payloadVersion(definition), name: definition.name, data, origin: "direct" },
				delivery: resolveDelivery(definition, validated),
			})
		},

		start(handlers, options) {
			return startRuntime(config, handlers, options)
		},
	}
}

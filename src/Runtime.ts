import { UnrecoverableError } from "bullmq"
import type { JobsConfig } from "@/Client"
import { type DeadReason, deadQueueName } from "@/Dead"
import type { JobDefinition, JobHandler } from "@/Definition"
import { resolveDeliveryWithoutUniqueness } from "@/Delivery"
import type {
	ConsumeRequest,
	Consumer,
	JobDelivery,
	JobDriver,
	QueueLimiter,
	ScheduleUpsert,
} from "@/Driver"
import { type Envelope, PAYLOAD_VERSION, readEnvelope } from "@/Envelope"
import { serializeError } from "@/Failure"
import { type JobEvent, type JobFailureEvent, notify } from "@/Hooks"
import { validatePayload } from "@/Payload"
import type { Schedule } from "@/Schedule"

export const DEFAULT_CONCURRENCY = 10

const DEFAULT_TIMEZONE = "UTC"

export interface QueueTuning {
	readonly concurrency?: number
	readonly limiter?: QueueLimiter
}

export interface StartOptions {
	readonly queues?: Record<string, QueueTuning>
}

export interface JobsRuntime {
	close(): Promise<void>
}

function unrecoverable(error: unknown): UnrecoverableError {
	const failure = new UnrecoverableError(error instanceof Error ? error.message : String(error))

	failure.cause = error

	return failure
}

function envelopeOf(delivery: JobDelivery): Envelope {
	try {
		return readEnvelope(delivery.envelope)
	} catch (error) {
		throw unrecoverable(error)
	}
}

function deadReason(delivery: JobDelivery, error: unknown): DeadReason | undefined {
	if (error instanceof UnrecoverableError) {
		return "unrecoverable"
	}

	if (delivery.attempt >= delivery.maxAttempts) {
		return "attempts_exhausted"
	}

	return undefined
}

function handlersByName(handlers: JobHandler[]): Map<string, JobHandler> {
	const byName = new Map<string, JobHandler>()

	for (const handler of handlers) {
		const { name } = handler.definition

		if (byName.has(name)) {
			throw new Error(
				`juibs: two handlers are registered for the job "${name}" — a job name takes exactly one handler`,
			)
		}

		byName.set(name, handler)
	}

	return byName
}

function assertEveryDefinitionRuns(
	definitions: readonly JobDefinition[],
	byName: Map<string, JobHandler>,
	queues: string[],
): void {
	const started = new Set(queues)

	const orphan = definitions.find(
		(definition) => started.has(definition.queue) && !byName.has(definition.name),
	)

	if (!orphan) {
		return
	}

	throw new Error(
		`juibs: the job "${orphan.name}" is registered on the started queue "${orphan.queue}" but no handler runs it — pass its handler to start(), or drop it from createJobs({ definitions })`,
	)
}

function assertEveryTunedQueueStarted(tuned: string[], queues: string[]): void {
	const started = new Set(queues)
	const stray = tuned.find((queue) => !started.has(queue))

	if (!stray) {
		return
	}

	const names = queues.map((queue) => `"${queue}"`).join(", ")

	throw new Error(
		`juibs: start() was given tuning for the queue "${stray}", which no handler starts — the started queues are ${names}`,
	)
}

function assertNoQueueConsumesADeadQueue(deadQueues: Set<string>, queues: string[]): void {
	const started = new Set(queues)
	const guarded = [...deadQueues].find((queue) => started.has(deadQueueName(queue)))

	if (!guarded) {
		return
	}

	throw new Error(
		`juibs: start() would open a worker on "${deadQueueName(guarded)}", which is the dead queue of "${guarded}" — a dead queue is consumed by nobody, so rename that queue or drop "${guarded}" from createJobs({ deadQueues })`,
	)
}

async function scheduleUpsert(
	config: JobsConfig,
	definition: JobDefinition,
	schedule: Schedule,
): Promise<ScheduleUpsert> {
	const data = await validatePayload(definition, schedule.data).catch((error: unknown) => {
		const reason = error instanceof Error ? error.message : String(error)

		throw new Error(
			`juibs: the schedule of the job "${definition.name}" carries no payload its schema accepts — a scheduled job has no producer to pass one, so give the schedule its own \`data\`, as in every("5 minutes", { data: { ... } }) — ${reason}`,
			{ cause: error },
		)
	})

	const delivery = resolveDeliveryWithoutUniqueness(definition, data)

	if (delivery.delayMs !== undefined) {
		throw new Error(
			`juibs: the job "${definition.name}" has a schedule and a delivery \`delayMs\` — a delay postpones one enqueue, which a recurrence has no place for, so drop \`delayMs\` from its delivery`,
		)
	}

	const upsert: ScheduleUpsert = {
		recurrence: schedule.recurrence,
		envelope: {
			v: PAYLOAD_VERSION,
			name: definition.name,
			data: schedule.data,
			origin: "schedule",
		},
		delivery,
	}

	if ("everyMs" in schedule.recurrence) {
		return upsert
	}

	return { ...upsert, timezone: schedule.timezone ?? config.timezone ?? DEFAULT_TIMEZONE }
}

function declaredSchedules(
	config: JobsConfig,
	handlers: JobHandler[],
	queue: string,
): Promise<ScheduleUpsert[]> {
	return Promise.all(
		handlers.flatMap(({ definition }) =>
			definition.queue === queue && definition.schedule
				? [scheduleUpsert(config, definition, definition.schedule)]
				: [],
		),
	)
}

async function reconcileEveryQueue(
	config: JobsConfig,
	handlers: JobHandler[],
	queues: string[],
): Promise<void> {
	const requests = await Promise.all(
		queues.map(async (queue) => ({
			queue,
			declared: await declaredSchedules(config, handlers, queue),
		})),
	)

	for (const request of requests) {
		await config.driver.reconcileSchedules(request)
	}
}

async function openConsumers(driver: JobDriver, requests: ConsumeRequest[]): Promise<Consumer[]> {
	const settled = await Promise.allSettled(requests.map((request) => driver.consume(request)))

	const opened = settled.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []))
	const refused = settled.flatMap((result) => (result.status === "rejected" ? [result.reason] : []))

	if (refused.length === 0) {
		return opened
	}

	await Promise.all(
		opened.map((consumer) =>
			consumer.close().catch((error: unknown) => {
				console.error("juibs: a consumer opened before start failed could not be closed", error)
			}),
		),
	)

	throw refused[0]
}

function consumeRequest(
	queue: string,
	tuning: QueueTuning | undefined,
	run: ConsumeRequest["run"],
): ConsumeRequest {
	const request = { queue, concurrency: tuning?.concurrency ?? DEFAULT_CONCURRENCY, run }

	if (!tuning?.limiter) {
		return request
	}

	return { ...request, limiter: tuning.limiter }
}

export async function startRuntime(
	config: JobsConfig,
	handlers: JobHandler[],
	options?: StartOptions,
): Promise<JobsRuntime> {
	const byName = handlersByName(handlers)
	const queues = [...new Set(handlers.map((handler) => handler.definition.queue))]
	const deadQueues = new Set(config.deadQueues ?? [])

	assertEveryDefinitionRuns(config.definitions ?? [], byName, queues)
	assertEveryTunedQueueStarted(Object.keys(options?.queues ?? {}), queues)
	assertNoQueueConsumesADeadQueue(deadQueues, queues)

	await reconcileEveryQueue(config, handlers, queues)

	async function dispatch(
		envelope: Envelope,
		event: JobEvent,
		delivery: JobDelivery,
	): Promise<unknown> {
		const handler = byName.get(envelope.name)

		if (!handler) {
			throw unrecoverable(new Error(`juibs: no handler is registered for job "${envelope.name}"`))
		}

		await notify(config.hooks?.onStart, "onStart", event)

		const data = await validatePayload(handler.definition, envelope.data).catch(
			(error: unknown) => {
				throw unrecoverable(error)
			},
		)

		const result = await handler.run(data, {
			id: delivery.id,
			attempt: delivery.attempt,
			maxAttempts: delivery.maxAttempts,
			origin: envelope.origin,
		})

		await notify(config.hooks?.onSuccess, "onSuccess", event)

		return result
	}

	async function report(
		envelope: Envelope,
		event: JobEvent,
		delivery: JobDelivery,
		error: unknown,
	): Promise<void> {
		const failure: JobFailureEvent = { ...event, error: serializeError(error) }

		await notify(config.hooks?.onAttemptFailed, "onAttemptFailed", failure)

		const reason = deadReason(delivery, error)

		if (!reason) {
			return
		}

		if (deadQueues.has(event.queue)) {
			await config.driver.dead
				.bury(event.queue, { envelope, error: failure.error, reason })
				.catch((refused: unknown) => {
					console.error(
						`juibs: the dead queue did not keep job "${event.name}"; the job outcome is unchanged`,
						refused,
					)
				})
		}

		await notify(config.hooks?.onDead, "onDead", failure)
	}

	function runOn(queue: string): ConsumeRequest["run"] {
		return async (delivery) => {
			const envelope = envelopeOf(delivery)

			const event: JobEvent = {
				name: envelope.name,
				queue,
				id: delivery.id,
				attempt: delivery.attempt,
				origin: envelope.origin,
			}

			return dispatch(envelope, event, delivery).catch(async (error: unknown) => {
				await report(envelope, event, delivery, error)

				throw error
			})
		}
	}

	const consumers = await openConsumers(
		config.driver,
		queues.map((queue) => consumeRequest(queue, options?.queues?.[queue], runOn(queue))),
	)

	return {
		async close() {
			await Promise.all(consumers.map((consumer) => consumer.close()))
		},
	}
}

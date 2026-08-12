import { UnrecoverableError } from "bullmq"
import { CANCEL_SWEEP_MS, CancelledError, type RunningDelivery } from "@/Cancellation"
import type { JobsConfig } from "@/Client"
import { type DeadReason, deadQueueName } from "@/Dead"
import { type JobDefinition, type JobHandler, payloadVersion } from "@/Definition"
import { resolveDeliveryWithoutUniqueness } from "@/Delivery"
import type {
	ConsumeRequest,
	Consumer,
	JobDelivery,
	JobDriver,
	QueueLimiter,
	ScheduleUpsert,
} from "@/Driver"
import { type Envelope, readEnvelope } from "@/Envelope"
import { serializeError } from "@/Failure"
import { type JobEvent, type JobFailureEvent, notify } from "@/Hooks"
import {
	idempotencyKeyFor,
	LeaseHeldError,
	runUnderKey,
	type StillRunning,
	stillRunning,
} from "@/Idempotency"
import { composeJobId } from "@/JobId"
import { assertVersionIsKnown, migrateEnvelope, VersionAheadError } from "@/Migration"
import { validatePayload } from "@/Payload"
import { validateResult } from "@/Result"
import type { Schedule } from "@/Schedule"
import { ShutdownAbortError } from "@/Shutdown"

export const DEFAULT_CONCURRENCY = 10

const DEFAULT_TIMEZONE = "UTC"

export interface QueueTuning {
	readonly concurrency?: number
	readonly limiter?: QueueLimiter
}

export interface StartOptions<Queue extends string = string> {
	readonly queues?: Partial<Record<Queue, QueueTuning>>
}

export interface CloseOptions {
	readonly timeoutMs?: number
}

export interface JobsRuntime {
	close(options?: CloseOptions): Promise<void>
}

class HandlerTimeoutError extends Error implements StillRunning {
	readonly running: Promise<unknown>

	constructor(name: string, timeoutMs: number, running: Promise<unknown>) {
		super(
			`juibs: the job "${name}" ran past its \`timeoutMs\` of ${timeoutMs}ms — its signal was aborted and the attempt failed, while the handler body kept running`,
		)
		this.name = "HandlerTimeoutError"
		this.running = running
	}
}

function runUnderTimeout(
	definition: Pick<JobDefinition, "name" | "timeoutMs">,
	controller: AbortController,
	run: () => Promise<unknown>,
): Promise<unknown> {
	const { timeoutMs } = definition

	if (timeoutMs === undefined) {
		return run()
	}

	const running = run()
	const expiry = Promise.withResolvers<never>()

	const timer = setTimeout(() => {
		const expired = new HandlerTimeoutError(definition.name, timeoutMs, running)

		controller.abort(expired)
		expiry.reject(expired)
	}, timeoutMs)

	return Promise.race([running, expiry.promise]).finally(() => clearTimeout(timer))
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
	if (error instanceof CancelledError) {
		return "cancelled"
	}

	if (error instanceof VersionAheadError) {
		return "version_ahead"
	}

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
			v: payloadVersion(definition),
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

/**
 * A delivery this runtime is running right now.
 *
 * Each one is its own entry, held by identity rather than under a key, because
 * two deliveries of the same job can overlap — a redelivery of a job whose
 * handler ignored a shutdown abort runs beside the body still going. Keyed by
 * id, the second would overwrite the first and the first to end would delete
 * the entry of the other.
 *
 * The queue and the stored id sit here because the cancellation sweep asks one
 * queue at a time, in stored ids.
 */
interface LiveDelivery extends RunningDelivery {
	readonly queue: string
	readonly name: string
	readonly controller: AbortController
}

function liveByQueue(live: Set<LiveDelivery>): Map<string, LiveDelivery[]> {
	const byQueue = new Map<string, LiveDelivery[]>()

	for (const delivery of live) {
		const running = byQueue.get(delivery.queue) ?? []

		running.push(delivery)
		byQueue.set(delivery.queue, running)
	}

	return byQueue
}

export async function startRuntime(
	config: JobsConfig,
	handlers: JobHandler[],
	options?: StartOptions,
): Promise<JobsRuntime> {
	const byName = handlersByName(handlers)
	const queues = [...new Set(handlers.map((handler) => handler.definition.queue))]
	const deadQueues = new Set(config.deadQueues ?? [])
	const live = new Set<LiveDelivery>()

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

		assertVersionIsKnown(handler.definition, envelope)

		await notify(config.hooks?.onStart, "onStart", event)

		const migrated = await migrateEnvelope(handler.definition, envelope).catch((error: unknown) => {
			throw unrecoverable(error)
		})

		const data = await validatePayload(handler.definition, migrated).catch((error: unknown) => {
			throw unrecoverable(error)
		})

		const controller = new AbortController()

		const run = () => {
			const entry: LiveDelivery = {
				queue: event.queue,
				id: delivery.id,
				attemptsStarted: delivery.attemptsStarted,
				name: envelope.name,
				controller,
			}

			live.add(entry)

			return runUnderTimeout(handler.definition, controller, async () => {
				const value = await handler.run(data, {
					id: event.id,
					attempt: delivery.attempt,
					maxAttempts: delivery.maxAttempts,
					origin: envelope.origin,
					signal: controller.signal,
				})

				return validateResult(handler.definition, value).catch((error: unknown) => {
					throw unrecoverable(error)
				})
			})
				.catch((error: unknown) => {
					const { reason } = controller.signal

					if (reason instanceof CancelledError) {
						const body = stillRunning(error)
						const cancelled = body ? new CancelledError(envelope.name, body) : reason

						if (cancelled !== error) {
							cancelled.cause = error
						}

						throw cancelled
					}

					if (!(reason instanceof ShutdownAbortError) || error instanceof UnrecoverableError) {
						throw error
					}

					const running = stillRunning(error)
					const aborted = running ? new ShutdownAbortError(envelope.name, running) : reason

					if (aborted !== error) {
						aborted.cause = error
					}

					throw aborted
				})
				.finally(() => live.delete(entry))
		}

		const key = idempotencyKeyFor(handler.definition, data)
		const result = key ? await runUnderKey(config.driver.idempotency, key, run) : await run()

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
				id: composeJobId(queue, delivery.id),
				attempt: delivery.attempt,
				origin: envelope.origin,
			}

			return dispatch(envelope, event, delivery).catch(async (error: unknown) => {
				if (error instanceof LeaseHeldError || error instanceof ShutdownAbortError) {
					throw error
				}

				await report(envelope, event, delivery, error)

				throw error
			})
		}
	}

	async function sweepCancellations(): Promise<void> {
		for (const [queue, running] of liveByQueue(live)) {
			const cancelled = await config.driver.takeCancelled(queue, running)

			for (const delivery of cancelled) {
				delivery.controller.abort(new CancelledError(delivery.name))
			}
		}
	}

	const consumers = await openConsumers(
		config.driver,
		queues.map((queue) => consumeRequest(queue, options?.queues?.[queue], runOn(queue))),
	)

	const sweeping = setInterval(() => {
		if (live.size === 0) {
			return
		}

		sweepCancellations().catch((error: unknown) => {
			console.error("juibs: the cancellations of the running jobs could not be read", error)
		})
	}, CANCEL_SWEEP_MS)

	sweeping.unref?.()

	async function drainConsumers(options?: CloseOptions): Promise<void> {
		const drained = Promise.all(consumers.map((consumer) => consumer.close()))
		const timeoutMs = options?.timeoutMs

		if (timeoutMs === undefined) {
			await drained
			return
		}

		const expiry = Promise.withResolvers<"expired">()
		const timer = setTimeout(() => expiry.resolve("expired"), timeoutMs)

		try {
			const outcome = await Promise.race([drained.then(() => "drained" as const), expiry.promise])

			if (outcome === "drained") {
				return
			}
		} finally {
			clearTimeout(timer)
		}

		for (const { name, controller } of live.values()) {
			controller.abort(new ShutdownAbortError(name))
		}

		drained.catch((error: unknown) => {
			console.error("juibs: a consumer left running past close() could not be closed", error)
		})
	}

	return {
		async close(options) {
			try {
				await drainConsumers(options)
			} finally {
				clearInterval(sweeping)
			}
		},
	}
}

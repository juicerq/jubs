import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { DeliveryPolicy } from "@/Delivery"
import type { PayloadMigration } from "@/Migration"
import type { Schedule } from "@/Schedule"

export const DEFAULT_PAYLOAD_VERSION = 1

export type Origin = "direct" | "schedule" | "flow" | "relay"

export interface HandlerContext {
	readonly id: string
	readonly attempt: number
	readonly maxAttempts: number
	readonly origin: Origin
	/**
	 * Aborts when the job's `timeoutMs` expires, and when `close({ timeoutMs })`
	 * runs out of patience during a shutdown. `signal.reason` tells the two
	 * apart: a shutdown aborts with a `ShutdownAbortError`.
	 *
	 * Pass it to whatever waits — `fetch`, a database client, another
	 * `AbortSignal`-aware library — and **throw** once it aborts. A handler that
	 * returns normally after an abort is recorded as a success, and juibs has no
	 * way to know the work stopped half done. A handler that finished just before
	 * the abort should still return: a return is always a success.
	 */
	readonly signal: AbortSignal
}

export interface JobDefinition<
	Payload extends StandardSchemaV1 = StandardSchemaV1,
	Queue extends string = string,
> {
	readonly name: string
	readonly queue: Queue
	readonly payload: Payload
	readonly version?: number
	readonly migrations?: Readonly<Record<number, PayloadMigration>>
	readonly delivery?: DeliveryPolicy
	readonly schedule?: Schedule
	readonly idempotencyKey?: (data: unknown) => string
	readonly timeoutMs?: number
}

export interface JobDefinitionInput<Payload extends StandardSchemaV1, Queue extends string = string>
	extends Omit<JobDefinition<Payload, Queue>, "delivery" | "schedule" | "idempotencyKey"> {
	readonly delivery?: DeliveryPolicy<StandardSchemaV1.InferOutput<Payload>>
	readonly schedule?: Schedule<StandardSchemaV1.InferInput<Payload>>
	readonly idempotencyKey?: (data: StandardSchemaV1.InferOutput<Payload>) => string
}

export function payloadVersion(definition: JobDefinition): number {
	return definition.version ?? DEFAULT_PAYLOAD_VERSION
}

export interface JobHandler<Queue extends string = string> {
	readonly definition: JobDefinition<StandardSchemaV1, Queue>
	readonly run: (data: unknown, context: HandlerContext) => Promise<unknown>
}

export type HandlerRun<Payload extends StandardSchemaV1> = (
	data: StandardSchemaV1.InferOutput<Payload>,
	context: HandlerContext,
) => Promise<unknown>

function strayMigration(name: string, from: string, version: number): Error {
	if (version <= DEFAULT_PAYLOAD_VERSION) {
		return new Error(
			`juibs: the job "${name}" declares a migration from version ${from}, which never runs — the job runs payload version ${version}, so it takes no migration at all; raise its \`version\` first`,
		)
	}

	return new Error(
		`juibs: the job "${name}" declares a migration from version ${from}, which never runs — the job runs payload version ${version}, so a migration key is a whole number between 1 and ${version - 1}`,
	)
}

function assertVersioning(
	definition: Pick<JobDefinition, "name" | "version" | "migrations">,
): void {
	const version = definition.version ?? DEFAULT_PAYLOAD_VERSION

	if (!Number.isInteger(version) || version < 1) {
		throw new Error(
			`juibs: the job "${definition.name}" declares the payload version ${version} — a payload version is a whole number of 1 or more`,
		)
	}

	const stray = Object.keys(definition.migrations ?? {}).find((key) => {
		const from = Number(key)

		return !Number.isInteger(from) || from < 1 || from >= version
	})

	if (!stray) {
		return
	}

	throw strayMigration(definition.name, stray, version)
}

function assertTimeout(definition: Pick<JobDefinition, "name" | "timeoutMs">): void {
	const { timeoutMs } = definition

	if (timeoutMs === undefined) {
		return
	}

	if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
		return
	}

	throw new Error(
		`juibs: the job "${definition.name}" declares the \`timeoutMs\` ${timeoutMs} — a timeout is a number of milliseconds above 0, so give it one or drop \`timeoutMs\` to let the handler run for as long as it takes`,
	)
}

export function defineJob<Payload extends StandardSchemaV1, const Queue extends string = string>(
	input: JobDefinitionInput<Payload, Queue>,
): JobDefinition<Payload, Queue> {
	assertVersioning(input)
	assertTimeout(input)

	const named = { name: input.name, queue: input.queue, payload: input.payload }
	const versioned = input.version ? { ...named, version: input.version } : named
	const migrating = input.migrations ? { ...versioned, migrations: input.migrations } : versioned
	const scheduled = input.schedule ? { ...migrating, schedule: input.schedule } : migrating
	const timed = input.timeoutMs ? { ...scheduled, timeoutMs: input.timeoutMs } : scheduled

	const definition = input.idempotencyKey
		? { ...timed, idempotencyKey: input.idempotencyKey as (data: unknown) => string }
		: timed

	if (!input.delivery) {
		return definition
	}

	return { ...definition, delivery: input.delivery as DeliveryPolicy }
}

export function defineHandler<Payload extends StandardSchemaV1, Queue extends string = string>(
	definition: JobDefinition<Payload, Queue>,
	run: HandlerRun<Payload>,
): JobHandler<Queue> {
	return { definition, run: run as JobHandler["run"] }
}

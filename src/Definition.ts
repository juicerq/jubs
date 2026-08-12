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
}

export interface JobDefinition<Payload extends StandardSchemaV1 = StandardSchemaV1> {
	readonly name: string
	readonly queue: string
	readonly payload: Payload
	readonly version?: number
	readonly migrations?: Readonly<Record<number, PayloadMigration>>
	readonly delivery?: DeliveryPolicy
	readonly schedule?: Schedule
}

export interface JobDefinitionInput<Payload extends StandardSchemaV1>
	extends Omit<JobDefinition<Payload>, "delivery" | "schedule"> {
	readonly delivery?: DeliveryPolicy<StandardSchemaV1.InferOutput<Payload>>
	readonly schedule?: Schedule<StandardSchemaV1.InferInput<Payload>>
}

export function payloadVersion(definition: JobDefinition): number {
	return definition.version ?? DEFAULT_PAYLOAD_VERSION
}

export interface JobHandler {
	readonly definition: JobDefinition
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

export function defineJob<Payload extends StandardSchemaV1>(
	input: JobDefinitionInput<Payload>,
): JobDefinition<Payload> {
	assertVersioning(input)

	const named = { name: input.name, queue: input.queue, payload: input.payload }
	const versioned = input.version ? { ...named, version: input.version } : named
	const migrating = input.migrations ? { ...versioned, migrations: input.migrations } : versioned
	const definition = input.schedule ? { ...migrating, schedule: input.schedule } : migrating

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

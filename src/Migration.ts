import { UnrecoverableError } from "bullmq"
import { type JobDefinition, payloadVersion } from "@/Definition"
import type { Envelope } from "@/Envelope"
import { serializeError } from "@/Failure"

export type PayloadMigration = (data: unknown) => unknown

export class VersionAheadError extends UnrecoverableError {
	constructor(jobName: string, stored: number, running: number) {
		super(
			`jubs: the job "${jobName}" stored a payload of version ${stored}, and this process runs version ${running} — a newer deploy wrote it, so this process cannot read it`,
		)
		this.name = "VersionAheadError"
	}
}

export function assertVersionIsKnown(definition: JobDefinition, envelope: Envelope): void {
	const running = payloadVersion(definition)

	if (envelope.v <= running) {
		return
	}

	throw new VersionAheadError(definition.name, envelope.v, running)
}

function stepOf(definition: JobDefinition, from: number): PayloadMigration {
	const step = definition.migrations?.[from]

	if (step) {
		return step
	}

	throw new Error(
		`jubs: the job "${definition.name}" stored a payload of version ${from} and declares no migration from it — add \`migrations: { ${from}: (data) => ... }\` to its definition`,
	)
}

export async function migrateEnvelope(
	definition: JobDefinition,
	envelope: Envelope,
): Promise<unknown> {
	assertVersionIsKnown(definition, envelope)

	const running = payloadVersion(definition)
	let data = envelope.data

	for (let from = envelope.v; from < running; from += 1) {
		const step = stepOf(definition, from)

		try {
			data = await step(data)
		} catch (error) {
			throw new Error(
				`jubs: the migration of job "${definition.name}" from version ${from} to ${from + 1} threw — ${serializeError(error).message}`,
				{ cause: error },
			)
		}
	}

	return data
}

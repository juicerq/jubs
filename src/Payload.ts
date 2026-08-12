import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { JobDefinition } from "@/Definition"

function readPath(issue: StandardSchemaV1.Issue): string {
	if (!issue.path || issue.path.length === 0) {
		return "payload"
	}

	return issue.path
		.map((segment) => String(typeof segment === "object" ? segment.key : segment))
		.join(".")
}

export class PayloadError extends Error {
	constructor(jobName: string, issues: readonly StandardSchemaV1.Issue[]) {
		const reasons = issues.map((issue) => `${readPath(issue)}: ${issue.message}`).join("; ")

		super(`jubs: the payload for job "${jobName}" is invalid — ${reasons}`)
		this.name = "PayloadError"
	}
}

export async function validatePayload<Payload extends StandardSchemaV1>(
	definition: JobDefinition<Payload>,
	data: unknown,
): Promise<StandardSchemaV1.InferOutput<Payload>> {
	const result = await definition.payload["~standard"].validate(data)

	if (result.issues) {
		throw new PayloadError(definition.name, result.issues)
	}

	return result.value
}

import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { JobDefinition } from "@/Definition"

function readPath(issue: StandardSchemaV1.Issue): string {
	if (!issue.path || issue.path.length === 0) {
		return "result"
	}

	return issue.path
		.map((segment) => String(typeof segment === "object" ? segment.key : segment))
		.join(".")
}

export class ResultError extends Error {
	constructor(jobName: string, issues: readonly StandardSchemaV1.Issue[]) {
		const reasons = issues.map((issue) => `${readPath(issue)}: ${issue.message}`).join("; ")

		super(`jubs: the result of job "${jobName}" is invalid — ${reasons}`)
		this.name = "ResultError"
	}
}

/**
 * Validates what a handler resolved against the `result` schema of its
 * definition, and gives back the validated value — the one that is stored and
 * the one an idempotency key replays. A definition that declares no `result`
 * runs no validation at all and keeps the value as it is.
 */
export async function validateResult(definition: JobDefinition, value: unknown): Promise<unknown> {
	if (!definition.result) {
		return value
	}

	const outcome = await definition.result["~standard"].validate(value)

	if (outcome.issues) {
		throw new ResultError(definition.name, outcome.issues)
	}

	return outcome.value
}

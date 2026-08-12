import type { EnqueuedJob } from "@/index"
import { readJobId } from "@/JobId"

const RUN = Bun.randomUUIDv7().slice(-12)

export function scoped(name: string): string {
	return `${name}.${RUN}`
}

export function storedId(job: EnqueuedJob): string {
	return readJobId(job.id).storedId
}

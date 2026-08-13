import type { EnqueuedJob } from "@/index"
import { readJobId } from "@/JobId"
import { liveId } from "../support/JobIds"

const RUN = Bun.randomUUIDv7().slice(-12)

export function scoped(name: string): string {
	return `${name}.${RUN}`
}

export function storedId(job: EnqueuedJob | null): string {
	return readJobId(liveId(job)).storedId
}

import type { EnqueuedJob } from "@/index"

export function liveId(job: EnqueuedJob | null): string {
	if (!job) {
		throw new Error(
			"this enqueue was held by an atomic block, so no job answers to it yet — enqueue outside the block to read an id",
		)
	}

	return job.id
}

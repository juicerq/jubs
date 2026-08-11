export interface ErrorSnapshot {
	readonly name: string
	readonly message: string
	readonly stack?: string
}

export interface SerializedError extends ErrorSnapshot {
	readonly cause?: ErrorSnapshot
}

function snapshot(error: unknown): ErrorSnapshot {
	if (!(error instanceof Error)) {
		return { name: "Error", message: String(error) }
	}

	if (error.stack === undefined) {
		return { name: error.name, message: error.message }
	}

	return { name: error.name, message: error.message, stack: error.stack }
}

export function serializeError(error: unknown): SerializedError {
	const serialized = snapshot(error)

	if (!(error instanceof Error) || error.cause === undefined) {
		return serialized
	}

	return { ...serialized, cause: snapshot(error.cause) }
}

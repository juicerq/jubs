export function errorOf(failure: unknown): Error {
	if (failure instanceof Error) {
		return failure
	}

	throw new Error(`expected an Error, got ${String(failure)}`)
}

const RUN = Bun.randomUUIDv7().slice(-12)

export function scoped(name: string): string {
	return `${name}.${RUN}`
}

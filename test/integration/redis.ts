function requireRedisUrl() {
	const url = process.env.REDIS_URL

	if (url) {
		return url
	}

	throw new Error(
		"jubs: the integration suite needs `REDIS_URL` and it is not set — copy `.env.test.example` to `.env.test` and point it at a Redis this suite owns alone, because the suite obliterates its queues and deletes keys by default",
	)
}

export const REDIS_URL: string = requireRedisUrl()

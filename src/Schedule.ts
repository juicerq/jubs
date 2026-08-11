export type Recurrence = { readonly everyMs: number } | { readonly pattern: string }

export interface Schedule<Data = unknown> {
	readonly recurrence: Recurrence
	readonly timezone?: string
	readonly data?: Data
}

export interface ScheduleOptions<Data = unknown> {
	readonly timezone?: string
	readonly data?: Data
}

export type IntervalOptions<Data = unknown> = Omit<ScheduleOptions<Data>, "timezone">

export type Weekday =
	| "monday"
	| "tuesday"
	| "wednesday"
	| "thursday"
	| "friday"
	| "saturday"
	| "sunday"

const WEEKDAYS: Record<Weekday, number> = {
	sunday: 0,
	monday: 1,
	tuesday: 2,
	wednesday: 3,
	thursday: 4,
	friday: 5,
	saturday: 6,
}

const INTERVAL_MS: Record<string, number> = {
	second: 1_000,
	seconds: 1_000,
	minute: 60_000,
	minutes: 60_000,
	hour: 3_600_000,
	hours: 3_600_000,
	day: 86_400_000,
	days: 86_400_000,
}

const INTERVAL_FORM = /^(\d+) ([a-z]+)$/

function intervalMs(interval: string): number {
	const parsed = INTERVAL_FORM.exec(interval)
	const unitMs = parsed?.[2] === undefined ? undefined : INTERVAL_MS[parsed[2]]
	const count = Number(parsed?.[1])

	if (unitMs === undefined || count <= 0) {
		throw new Error(
			`juibs: every("${interval}") is not an interval — give a whole count above zero and a unit of second, minute, hour or day, as in every("5 minutes")`,
		)
	}

	return count * unitMs
}

const TIME_FORM = /^([01]\d|2[0-3]):([0-5]\d)$/

function timeFields(time: string): { hour: number; minute: number } {
	const parsed = TIME_FORM.exec(time)

	if (!parsed) {
		throw new Error(
			`juibs: "${time}" is not a time — give "HH:MM" in 24 hours, as in dailyAt("09:30")`,
		)
	}

	return { hour: Number(parsed[1]), minute: Number(parsed[2]) }
}

export function assertTimezone(timezone: string): void {
	try {
		new Intl.DateTimeFormat(undefined, { timeZone: timezone })
	} catch {
		throw new Error(
			`juibs: "${timezone}" is not a time zone — give an IANA name, as in "America/Sao_Paulo"`,
		)
	}
}

const EVERY_TIMEZONE_FIX =
	"juibs: every() takes no timezone — an interval recurrence has no time of day to shift, so write dailyAt, weeklyOn, monthlyOn or cron when the time zone matters"

function scheduleOf<Data>(recurrence: Recurrence, options?: ScheduleOptions<Data>): Schedule<Data> {
	if (options?.timezone) {
		assertTimezone(options.timezone)
	}

	return { recurrence, ...options }
}

export function every<Data = never>(
	interval: string,
	options?: IntervalOptions<Data>,
): Schedule<Data> {
	if (options && "timezone" in options) {
		throw new Error(EVERY_TIMEZONE_FIX)
	}

	return scheduleOf({ everyMs: intervalMs(interval) }, options)
}

export function dailyAt<Data = never>(
	time: string,
	options?: ScheduleOptions<Data>,
): Schedule<Data> {
	const { hour, minute } = timeFields(time)

	return scheduleOf({ pattern: `${minute} ${hour} * * *` }, options)
}

export function weeklyOn<Data = never>(
	day: Weekday,
	time: string,
	options?: ScheduleOptions<Data>,
): Schedule<Data> {
	const { hour, minute } = timeFields(time)

	return scheduleOf({ pattern: `${minute} ${hour} * * ${WEEKDAYS[day]}` }, options)
}

export function monthlyOn<Data = never>(
	dayOfMonth: number,
	time: string,
	options?: ScheduleOptions<Data>,
): Schedule<Data> {
	if (!Number.isInteger(dayOfMonth) || dayOfMonth < 1 || dayOfMonth > 31) {
		throw new Error(
			`juibs: monthlyOn(${dayOfMonth}) is not a day of the month — give a whole day from 1 to 31, as in monthlyOn(1, "09:00")`,
		)
	}

	const { hour, minute } = timeFields(time)

	return scheduleOf({ pattern: `${minute} ${hour} ${dayOfMonth} * *` }, options)
}

export function cron<Data = never>(
	pattern: string,
	options?: ScheduleOptions<Data>,
): Schedule<Data> {
	return scheduleOf({ pattern }, options)
}

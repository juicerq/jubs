import { describe, expect, test } from "bun:test"
import { assertTimezone, cron, dailyAt, every, monthlyOn, weeklyOn } from "@/index"

describe("every", () => {
	test("reads a count and a unit in readable terms", () => {
		expect(every("30 seconds").recurrence).toEqual({ everyMs: 30_000 })
		expect(every("5 minutes").recurrence).toEqual({ everyMs: 300_000 })
		expect(every("2 hours").recurrence).toEqual({ everyMs: 7_200_000 })
		expect(every("3 days").recurrence).toEqual({ everyMs: 259_200_000 })
	})

	test("takes a singular unit as well", () => {
		expect(every("1 second").recurrence).toEqual({ everyMs: 1_000 })
		expect(every("1 minute").recurrence).toEqual({ everyMs: 60_000 })
		expect(every("1 hour").recurrence).toEqual({ everyMs: 3_600_000 })
		expect(every("1 day").recurrence).toEqual({ everyMs: 86_400_000 })
	})

	test("refuses a unit it does not know", () => {
		expect(() => every("2 fortnights")).toThrow('every("5 minutes")')
	})

	test("refuses a count that is not whole", () => {
		expect(() => every("1.5 hours")).toThrow('every("5 minutes")')
	})

	test("refuses zero and a negative count", () => {
		expect(() => every("0 minutes")).toThrow('every("5 minutes")')
		expect(() => every("-1 minutes")).toThrow('every("5 minutes")')
	})

	test("refuses a shape that is not a count and a unit", () => {
		expect(() => every("minutes")).toThrow('every("5 minutes")')
		expect(() => every("5 minutes and a bit")).toThrow('every("5 minutes")')
	})

	test("refuses a timezone, which an interval recurrence never applies", () => {
		// @ts-expect-error an interval recurrence takes no timezone
		expect(() => every("5 minutes", { timezone: "UTC" })).toThrow("dailyAt")
	})
})

describe("dailyAt", () => {
	test("runs at one time of every day", () => {
		expect(dailyAt("09:30").recurrence).toEqual({ pattern: "30 9 * * *" })
		expect(dailyAt("00:00").recurrence).toEqual({ pattern: "0 0 * * *" })
		expect(dailyAt("23:59").recurrence).toEqual({ pattern: "59 23 * * *" })
	})

	test("refuses an hour above 23 and a minute above 59", () => {
		expect(() => dailyAt("24:00")).toThrow('"HH:MM"')
		expect(() => dailyAt("09:60")).toThrow('"HH:MM"')
	})

	test("refuses a time that is not two digits, a colon and two digits", () => {
		expect(() => dailyAt("9:30")).toThrow('"HH:MM"')
		expect(() => dailyAt("0930")).toThrow('"HH:MM"')
		expect(() => dailyAt("09:30:00")).toThrow('"HH:MM"')
		expect(() => dailyAt("noon")).toThrow('"HH:MM"')
	})
})

describe("weeklyOn", () => {
	test("counts sunday as day zero of the week", () => {
		expect(weeklyOn("sunday", "06:00").recurrence).toEqual({ pattern: "0 6 * * 0" })
		expect(weeklyOn("monday", "06:00").recurrence).toEqual({ pattern: "0 6 * * 1" })
		expect(weeklyOn("saturday", "06:00").recurrence).toEqual({ pattern: "0 6 * * 6" })
	})

	test("refuses a time it cannot read", () => {
		expect(() => weeklyOn("monday", "6:00")).toThrow('"HH:MM"')
	})
})

describe("monthlyOn", () => {
	test("runs on one day of every month", () => {
		expect(monthlyOn(1, "00:15").recurrence).toEqual({ pattern: "15 0 1 * *" })
		expect(monthlyOn(31, "12:00").recurrence).toEqual({ pattern: "0 12 31 * *" })
	})

	test("refuses a day outside the month", () => {
		expect(() => monthlyOn(0, "12:00")).toThrow("1 to 31")
		expect(() => monthlyOn(32, "12:00")).toThrow("1 to 31")
		expect(() => monthlyOn(1.5, "12:00")).toThrow("1 to 31")
	})
})

describe("cron", () => {
	test("passes a pattern through without reading it", () => {
		expect(cron("*/10 8-18 * * 1-5").recurrence).toEqual({ pattern: "*/10 8-18 * * 1-5" })
	})
})

describe("schedule options", () => {
	test("carries the timezone the caller overrides", () => {
		expect(dailyAt("09:30", { timezone: "America/Sao_Paulo" }).timezone).toBe("America/Sao_Paulo")
		expect(cron("0 9 * * *", { timezone: "Europe/Lisbon" }).timezone).toBe("Europe/Lisbon")
	})

	test("refuses a name that is no time zone, before a pattern reaches the parser", () => {
		expect(() => dailyAt("07:00", { timezone: "Mars/Olympus" })).toThrow("is not a time zone")
		expect(() => weeklyOn("monday", "07:00", { timezone: "Mars/Olympus" })).toThrow(
			"is not a time zone",
		)
		expect(() => monthlyOn(1, "07:00", { timezone: "Mars/Olympus" })).toThrow("is not a time zone")
		expect(() => cron("* * * * *", { timezone: "Mars/Olympus" })).toThrow("is not a time zone")
	})

	test("carries the data every occurrence runs with", () => {
		const schedule = weeklyOn("monday", "07:00", { data: { report: "weekly" } })

		expect(schedule.data).toEqual({ report: "weekly" })
	})

	test("leaves timezone and data off when the caller names none", () => {
		expect(Object.keys(cron("* * * * *"))).toEqual(["recurrence"])
	})
})

describe("assertTimezone", () => {
	test("takes an IANA name", () => {
		expect(() => assertTimezone("UTC")).not.toThrow()
		expect(() => assertTimezone("America/Sao_Paulo")).not.toThrow()
	})

	test("refuses a name no time zone carries", () => {
		expect(() => assertTimezone("Mars/Olympus")).toThrow("is not a time zone")
		expect(() => assertTimezone("GMT-3")).toThrow("is not a time zone")
	})
})

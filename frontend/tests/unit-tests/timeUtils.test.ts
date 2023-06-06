import { hasTime, hasTimezone, isoStringToTimestampUtcMs, timestampUtcMsToIsoString } from "@shared-utils/timeUtils";


describe("timeUtils tests", () => {
    test("Check if ISO 8601 string contains time", () => {
        expect(hasTime("2018-01-01")).toBe(false);
        expect(hasTime("2018-01-01T")).toBe(false);

        expect(hasTime("2018-01-01T00")).toBe(true);
        expect(hasTime("2018-01-01T00:00:00")).toBe(true);
        expect(hasTime("2018-01-01T00:00:00Z")).toBe(true);
        expect(hasTime("2018-01-01T00:00:00+01:00")).toBe(true);
    });

    test("Check if ISO 8601 string has timezone information", () => {
        expect(hasTimezone("2018-01-01T00:00:00")).toBe(false);
        expect(hasTimezone("2018-01-01T00:00:00Z")).toBe(true);
        expect(hasTimezone("2018-01-01T00:00:00+01:00")).toBe(true);
        expect(hasTimezone("2018-01-01T00:00:00-02:00")).toBe(true);
    });

    test("Convert ISO string to timestamp", () => {
        // Test data generated with: https://www.timestamp-converter.com/
        expect(isoStringToTimestampUtcMs("2018-01-01T00:00:00Z")).toBe(1514764800000);
        expect(isoStringToTimestampUtcMs("2018-01-01T00:00:00")).toBe(1514764800000);

        expect(isoStringToTimestampUtcMs("2018-01-01")).toBe(1514764800000);

        expect(isoStringToTimestampUtcMs("2018-01-01T00:00:00.001")) .toBe(1514764800001);
        expect(isoStringToTimestampUtcMs("2018-01-01T00:00:00.001Z")).toBe(1514764800001);
        expect(isoStringToTimestampUtcMs("2017-12-31T23:59:59.999")) .toBe(1514764799999);
        expect(isoStringToTimestampUtcMs("2017-12-31T23:59:59.999Z")).toBe(1514764799999);
    });

    test("Convert timestamp to ISO string", () => {
        // Test data generated with: https://www.timestamp-converter.com/
        expect(timestampUtcMsToIsoString(1514764800000)).toBe("2018-01-01T00:00:00.000Z");
        expect(timestampUtcMsToIsoString(1514764800001)).toBe("2018-01-01T00:00:00.001Z");
        expect(timestampUtcMsToIsoString(1514764799999)).toBe("2017-12-31T23:59:59.999Z");
    });
});

package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ValidatorTest {
    static final LocalDate DAY = LocalDate.of(2026, 10, 28);
    static final long DAY_START = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    static final long NOON = DAY_START + 12 * 3_600_000L;
    final Validator validator = new Validator(DAY);

    /** A valid ais.raw v1 message, as the replayer writes it. */
    static ObjectNode valid() {
        ObjectNode n = Validator.MAPPER.createObjectNode();
        n.put("schema_version", 1);
        n.put("event_ts", NOON);
        n.put("mmsi", 219026147L);
        n.put("lat", 55.123456);
        n.put("lon", 11.5);
        n.put("sog", 12.3);
        n.put("cog", 90.0);
        n.putNull("heading");
        n.put("raw", "28/10/2026 12:00:00,Class A,219026147,55.123456,11.5,...");
        n.put("run_id", "20261103T000000Z-abcdef");
        return n;
    }

    RawRecord check(ObjectNode n) {
        return validator.validate(n.toString().getBytes(StandardCharsets.UTF_8));
    }

    RawRecord check(String s) {
        return validator.validate(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void validRecordKeepsFieldsAndGetsStationClass() {
        RawRecord r = check(valid());
        assertTrue(r.isValid(), r.detail);
        assertEquals(219026147L, r.mmsi);
        assertEquals(NOON, r.eventTs);
        assertEquals(55.123456, r.lat);
        assertEquals(11.5, r.lon);
        assertEquals("20261103T000000Z-abcdef", r.runId);
        assertEquals("ship", r.stationClass);
    }

    @Test
    void nonShipStationsAreValid() {
        ObjectNode n = valid();
        n.put("mmsi", 2190047L);
        RawRecord r = check(n);
        assertTrue(r.isValid());
        assertEquals("coast", r.stationClass);
    }

    @Test
    void farOutsideTheReceiverAreaIsValid() {
        ObjectNode n = valid();
        n.put("lat", -60.0);
        assertTrue(check(n).isValid(), "TELEPORT catches impossible jumps, validation does not");
    }

    @Test
    void decodeErrors() {
        assertEquals("decode_error", validator.validate(null).reason);
        assertEquals("decode_error", check("").reason);
        assertEquals("decode_error", check("{not json").reason);
        assertEquals("decode_error", check("[1,2]").reason);
        assertEquals("decode_error", validator.validate(new byte[] {(byte) 0xff, (byte) 0xfe}).reason);

        ObjectNode v2 = valid();
        v2.put("schema_version", 2);
        assertEquals("decode_error", check(v2).reason);

        for (String field : Validator.REQUIRED) {
            ObjectNode n = valid();
            n.remove(field);
            RawRecord r = check(n);
            assertEquals("decode_error", r.reason, field);
            assertEquals("missing " + field, r.detail);
        }
    }

    @Test
    void runIdIsKeptOnRejectsWhenParsable() {
        ObjectNode n = valid();
        n.putNull("mmsi");
        RawRecord r = check(n);
        assertEquals("bad_mmsi", r.reason);
        assertEquals("20261103T000000Z-abcdef", r.runId);
        assertNull(check("{not json").runId);
    }

    @ParameterizedTest
    @CsvSource({
        "1000000000, bad_mmsi",
        "-5, bad_mmsi",
        "0, valid",
        "999999999, valid",
    })
    void mmsiRange(long mmsi, String expected) {
        ObjectNode n = valid();
        n.put("mmsi", mmsi);
        RawRecord r = check(n);
        assertEquals(expected, r.isValid() ? "valid" : r.reason);
    }

    @Test
    void mmsiNullOrTextIsBad() {
        ObjectNode n = valid();
        n.putNull("mmsi");
        assertEquals("bad_mmsi", check(n).reason);
        n.put("mmsi", "219026147");
        assertEquals("bad_mmsi", check(n).reason);
        n.put("mmsi", 219026147.5);
        assertEquals("bad_mmsi", check(n).reason);
    }

    @ParameterizedTest
    @CsvSource({
        "null, 11.5, no_position",
        "55.0, null, no_position",
        "91, 11.5, position_unavailable",
        "55.0, 181, position_unavailable",
        "91, 181, position_unavailable",
        "90.5, 11.5, position_out_of_range",
        "-90.5, 11.5, position_out_of_range",
        "55.0, 180.5, position_out_of_range",
        "55.0, -181, position_out_of_range",
        "0, 0, null_island",
        "0, 11.5, valid",
        "90, 180, valid",
        "-90, -180, valid",
    })
    void positions(String lat, String lon, String expected) {
        ObjectNode n = valid();
        if (lat.equals("null")) n.putNull("lat"); else n.put("lat", Double.parseDouble(lat));
        if (lon.equals("null")) n.putNull("lon"); else n.put("lon", Double.parseDouble(lon));
        RawRecord r = check(n);
        assertEquals(expected, r.isValid() ? "valid" : r.reason);
    }

    @Test
    void timestamps() {
        long day = Validator.DAY_MS;
        long[][] cases = {
            {DAY_START - day, 1},          // exactly one day before the day: still valid
            {DAY_START - day - 1, 0},
            {DAY_START + 2 * day - 1, 1},  // last millisecond of the day after
            {DAY_START + 2 * day, 0},
        };
        for (long[] c : cases) {
            ObjectNode n = valid();
            n.put("event_ts", c[0]);
            assertEquals(c[1] == 1, check(n).isValid(), Long.toString(c[0]));
        }
        ObjectNode n = valid();
        n.putNull("event_ts");
        assertEquals("bad_timestamp", check(n).reason);
        n.put("event_ts", "yesterday");
        assertEquals("bad_timestamp", check(n).reason);
    }

    @ParameterizedTest
    @CsvSource({
        "102.2, valid",
        "102.3, valid",
        "102.25, sog_invalid",
        "150, sog_invalid",
        "0, valid",
    })
    void sog(double sog, String expected) {
        ObjectNode n = valid();
        n.put("sog", sog);
        RawRecord r = check(n);
        assertEquals(expected, r.isValid() ? "valid" : r.reason);
    }

    @Test
    void sogNullOrAbsentIsFineButTextIsNot() {
        ObjectNode n = valid();
        n.putNull("sog");
        assertTrue(check(n).isValid());
        n.remove("sog");
        assertTrue(check(n).isValid());
        n.put("sog", "fast");
        assertEquals("sog_invalid", check(n).reason);
    }

    @Test
    void firstFailureWins() {
        ObjectNode n = valid();
        n.putNull("mmsi");
        n.putNull("lat");
        n.putNull("event_ts");
        n.put("sog", 500);
        assertEquals("bad_mmsi", check(n).reason);
        n.put("mmsi", 219026147L);
        assertEquals("no_position", check(n).reason);
        n.put("lat", 0.0);
        n.put("lon", 0.0);
        assertEquals("null_island", check(n).reason);
        n.put("lat", 55.0);
        assertEquals("bad_timestamp", check(n).reason);
        n.put("event_ts", NOON);
        assertEquals("sog_invalid", check(n).reason);
    }
}

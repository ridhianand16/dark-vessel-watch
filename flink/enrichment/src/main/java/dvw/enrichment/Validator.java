package dvw.enrichment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Parses one ais.raw value (schema_version 1, see ingest/replayer/record.py) and applies the
 * DLQ rules in {@link Reason} order. Pure and never throws: any input yields a RawRecord,
 * either valid or carrying one reason code.
 *
 * <p>"Required key absent" means the key is missing from the object. The replayer always
 * writes every key and uses null for a value it could not parse, so a null is judged by that
 * field's own rule (null mmsi is bad_mmsi, null lat is no_position), not as decode_error.
 */
public final class Validator {
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final int SCHEMA_VERSION = 1;
    static final String[] REQUIRED = {"run_id", "mmsi", "event_ts", "lat", "lon"};
    static final long DAY_MS = 86_400_000L;
    static final double SOG_MAX_KN = 102.2;
    static final double SOG_NOT_AVAILABLE = 102.3;

    /** Valid event times: [day start - 1 day, day end + 1 day), in epoch milliseconds UTC. */
    private final long minEventTs;
    private final long maxEventTsExclusive;

    public Validator(LocalDate aisDay) {
        long dayStart = aisDay.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        this.minEventTs = dayStart - DAY_MS;
        this.maxEventTsExclusive = dayStart + 2 * DAY_MS;
    }

    public RawRecord validate(byte[] value) {
        RawRecord r = new RawRecord();
        r.value = value;
        check(value, r);
        return r;
    }

    private void check(byte[] value, RawRecord r) {
        if (value == null || value.length == 0) {
            reject(r, Reason.DECODE_ERROR, "empty value");
            return;
        }
        JsonNode n;
        try {
            n = MAPPER.readTree(value);
        } catch (IOException e) {
            reject(r, Reason.DECODE_ERROR, "invalid JSON");
            return;
        }
        if (n == null || !n.isObject()) {
            reject(r, Reason.DECODE_ERROR, "not a JSON object");
            return;
        }
        JsonNode runId = n.get("run_id");
        if (runId != null && runId.isTextual() && !runId.asText().isEmpty()) {
            r.runId = runId.asText();
        }
        JsonNode version = n.get("schema_version");
        if (version == null || !version.isIntegralNumber() || version.asInt() != SCHEMA_VERSION) {
            reject(r, Reason.DECODE_ERROR, "schema_version is not " + SCHEMA_VERSION);
            return;
        }
        for (String field : REQUIRED) {
            if (!n.has(field)) {
                reject(r, Reason.DECODE_ERROR, "missing " + field);
                return;
            }
        }
        if (r.runId == null) {
            reject(r, Reason.DECODE_ERROR, "run_id is not a non-empty string");
            return;
        }

        JsonNode mmsi = n.get("mmsi");
        if (!mmsi.isIntegralNumber() || !mmsi.canConvertToLong()
                || mmsi.asLong() < 0 || mmsi.asLong() > StationClass.MMSI_MAX) {
            reject(r, Reason.BAD_MMSI, "mmsi " + mmsi);
            return;
        }
        r.mmsi = mmsi.asLong();

        JsonNode lat = n.get("lat");
        JsonNode lon = n.get("lon");
        if (!lat.isNumber() || !lon.isNumber()) {
            reject(r, Reason.NO_POSITION, "lat " + lat + ", lon " + lon);
            return;
        }
        r.lat = lat.asDouble();
        r.lon = lon.asDouble();
        if (r.lat == 91.0 || r.lon == 181.0) {
            reject(r, Reason.POSITION_UNAVAILABLE, "lat " + r.lat + ", lon " + r.lon);
            return;
        }
        if (Math.abs(r.lat) > 90.0 || Math.abs(r.lon) > 180.0) {
            reject(r, Reason.POSITION_OUT_OF_RANGE, "lat " + r.lat + ", lon " + r.lon);
            return;
        }
        if (r.lat == 0.0 && r.lon == 0.0) {
            reject(r, Reason.NULL_ISLAND, "lat 0, lon 0");
            return;
        }

        JsonNode ts = n.get("event_ts");
        if (!ts.isIntegralNumber() || !ts.canConvertToLong()
                || ts.asLong() < minEventTs || ts.asLong() >= maxEventTsExclusive) {
            reject(r, Reason.BAD_TIMESTAMP, "event_ts " + ts);
            return;
        }
        r.eventTs = ts.asLong();

        JsonNode sog = n.get("sog");
        if (sog != null && !sog.isNull()) {
            if (!sog.isNumber()) {
                reject(r, Reason.SOG_INVALID, "sog " + sog);
                return;
            }
            double kn = sog.asDouble();
            if (kn > SOG_MAX_KN && kn != SOG_NOT_AVAILABLE) {
                reject(r, Reason.SOG_INVALID, "sog " + kn);
                return;
            }
        }

        r.stationClass = StationClass.of(r.mmsi).wire;
    }

    private static void reject(RawRecord r, Reason reason, String detail) {
        r.reason = reason.code;
        r.detail = detail;
    }
}

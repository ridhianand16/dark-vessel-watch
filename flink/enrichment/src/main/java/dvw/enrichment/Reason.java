package dvw.enrichment;

/**
 * Why a record went to ais.dlq. Checked in this order; the first failure wins, so each DLQ
 * record carries exactly one code.
 */
public enum Reason {
    /** Not valid JSON, not an object, wrong schema_version, or a required key absent. */
    DECODE_ERROR("decode_error"),
    /** MMSI null, not an integer, or not 9 digits (0 to 999999999 with leading zeros). */
    BAD_MMSI("bad_mmsi"),
    /** lat or lon null or not a number. */
    NO_POSITION("no_position"),
    /** The AIS "not available" values: lat 91 or lon 181. */
    POSITION_UNAVAILABLE("position_unavailable"),
    /** lat outside +-90 or lon outside +-180. */
    POSITION_OUT_OF_RANGE("position_out_of_range"),
    /** Exactly 0,0: a receiver or transponder with no fix. */
    NULL_ISLAND("null_island"),
    /** event_ts null, not an integer, or more than one day off the replayed AIS day. */
    BAD_TIMESTAMP("bad_timestamp"),
    /** SOG negative, above 102.2 kn and not the 102.3 "not available" code, or not a number
     * (the sog rule in contracts/ais_enriched.json). */
    SOG_INVALID("sog_invalid");

    public final String code;

    Reason(String code) {
        this.code = code;
    }
}

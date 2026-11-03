package dvw.enrichment;

/**
 * One ais.raw message after parsing and validation, valid or not.
 *
 * <p>Flink serializes records between operators (and into checkpoints) with its own type
 * system. A class with a public no-argument constructor and public fields is a Flink POJO
 * and gets the fast PojoSerializer; anything else falls back to Kryo, which the job turns
 * off (pipeline.generic-types: false) so a slow or unstable type fails at submit time.
 */
public class RawRecord {
    /** Where the message came from in ais.raw. Every output carries these. */
    public int srcPartition;
    public long srcOffset;

    /** The original Kafka key and value, untouched. Either may be null. */
    public byte[] key;
    public byte[] value;

    /** DLQ reason code ({@link Reason#code}); null when the record is valid. */
    public String reason;
    /** Human-readable detail for a DLQ record, e.g. which field was missing. */
    public String detail;

    /** Set whenever the payload had a string run_id, valid or not. */
    public String runId;

    /** Only meaningful when reason is null. */
    public long mmsi;
    public long eventTs;
    public double lat;
    public double lon;
    public String stationClass;

    /** Zone tags (ZoneTagging, docs/adr/0018). zonesVersion is set on every enriched record;
     * the others are null or empty when the point is outside every zone or cannot be
     * projected. */
    public String zonesVersion;
    public String eezId;
    public String[] mpaIds;
    public boolean inPort;
    public Double distEezBoundaryM;
    public Double distMpaM;

    public RawRecord() {}

    public boolean isValid() {
        return reason == null;
    }
}

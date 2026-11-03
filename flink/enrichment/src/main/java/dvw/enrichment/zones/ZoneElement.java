package dvw.enrichment.zones;

/**
 * One element of the zone stream: a zone or boundary piece from PostGIS, or the "zones
 * complete" marker that ends a package. A Flink POJO (public fields, no-argument
 * constructor), like RawRecord.
 *
 * <p>The zone source emits every piece of the active package, numbered 0..n-1 in
 * {@link #seq}, then one marker whose {@link #seq} is n. A task that receives the marker has
 * received every piece before it, because one source subtask sends them all down the same
 * channel in order.
 */
public class ZoneElement {
    public static final String EEZ = "eez";
    public static final String MPA = "mpa";
    public static final String PORT = "port";
    public static final String BOUNDARY = "boundary";
    public static final String COMPLETE = "complete";

    /** One of the constants above. */
    public String kind;
    /** zones.zone_id (eez:5674, wdpa:..., wpi:...) or the boundary's line id; null for the marker. */
    public String id;
    /** refdata_packages.package_version of the active package. */
    public String version;
    /** Position in the package's stream; on the marker, the number of pieces sent. */
    public int seq;
    /** The piece in EPSG:25832 as WKB; null for the marker. */
    public byte[] wkb;

    public ZoneElement() {}

    static ZoneElement piece(String kind, String id, String version, int seq, byte[] wkb) {
        ZoneElement e = new ZoneElement();
        e.kind = kind;
        e.id = id;
        e.version = version;
        e.seq = seq;
        e.wkb = wkb;
        return e;
    }

    static ZoneElement complete(String version, int pieces) {
        ZoneElement e = new ZoneElement();
        e.kind = COMPLETE;
        e.version = version;
        e.seq = pieces;
        return e;
    }

    public boolean isComplete() {
        return COMPLETE.equals(kind);
    }
}

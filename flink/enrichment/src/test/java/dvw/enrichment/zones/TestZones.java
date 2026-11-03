package dvw.enrichment.zones;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.io.WKBWriter;

/** Builds zone packages for tests from lon/lat outlines, projected like the loader does. */
public final class TestZones {
    public static final String VERSION = "zones-test00000001";
    private static final GeometryFactory GF = new GeometryFactory();

    private final String version;
    private final List<ZoneElement> pieces = new ArrayList<>();
    private final Utm32 utm = new Utm32();

    public TestZones(String version) {
        this.version = version;
    }

    public TestZones() {
        this(VERSION);
    }

    /** A rectangle from (lon0, lat0) to (lon1, lat1), edges straight in UTM. */
    public TestZones box(String kind, String id, double lon0, double lat0, double lon1, double lat1) {
        return polygon(kind, id, new double[][] {{lon0, lat0}, {lon1, lat0}, {lon1, lat1}, {lon0, lat1}, {lon0, lat0}});
    }

    /** A polygon from closed lon/lat rings: the first is the shell, the rest are holes. */
    public TestZones polygon(String kind, String id, double[][]... rings) {
        LinearRing shell = GF.createLinearRing(project(rings[0]));
        LinearRing[] holes = new LinearRing[rings.length - 1];
        for (int i = 1; i < rings.length; i++) {
            holes[i - 1] = GF.createLinearRing(project(rings[i]));
        }
        return add(kind, id, new WKBWriter().write(GF.createPolygon(shell, holes)));
    }

    public TestZones line(String id, double[][] lonLat) {
        return add(ZoneElement.BOUNDARY, id, new WKBWriter().write(GF.createLineString(project(lonLat))));
    }

    /** A piece given as hex WKB in EPSG:25832, e.g. from ST_AsBinary in PostGIS. */
    public TestZones wkbHex(String kind, String id, String hex) {
        return add(kind, id, HexFormat.of().parseHex(hex));
    }

    private TestZones add(String kind, String id, byte[] wkb) {
        pieces.add(ZoneElement.piece(kind, id, version, pieces.size(), wkb));
        return this;
    }

    private Coordinate[] project(double[][] lonLat) {
        Coordinate[] out = new Coordinate[lonLat.length];
        double[] xy = new double[2];
        for (int i = 0; i < lonLat.length; i++) {
            utm.forward(lonLat[i][0], lonLat[i][1], xy);
            out[i] = new Coordinate(xy[0], xy[1]);
        }
        return out;
    }

    public List<ZoneElement> pieces() {
        return List.copyOf(pieces);
    }

    public ZoneElement marker() {
        return ZoneElement.complete(version, pieces.size());
    }

    /** The pieces followed by the marker: what the zone source sends. */
    public List<ZoneElement> stream() {
        List<ZoneElement> out = new ArrayList<>(pieces);
        out.add(marker());
        return out;
    }

    public ZoneIndex index() {
        return ZoneIndex.build(version, pieces);
    }

    public static void resetShared() {
        SharedZones.reset();
    }
}

package dvw.enrichment.zones;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.sf.geographiclib.Geodesic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ZoneIndexTest {
    /**
     * The boundary line (14.6 54.9, 15.2 55.5, 16.0 55.8) prepared as the loader prepares
     * boundaries: ST_Transform(ST_Segmentize(geom::geography, 5000)::geometry, 25832).
     */
    static final String BORNHOLM_BOUNDARY = "0102000000210000007acdc12650362a41e63daf481c43574104f8b3f22b462a41"
            + "309245cc5f475741aa67bf7e07562a418a57c95ba34b574176b47ecae2652a41bd9648f7e64f5741d46d8cd5bd752a41"
            + "9657d19e2a5457419f1a839f98852a41eda071526e585741fc38fd2773952a41a8783712b25c57416a3e956e4da52a41"
            + "b3e330def56057419c97e57227b52a41ffe56bb63965574189a8883401c52a418682f69a7d69574155cc18b3dad42a41"
            + "4dbbde8bc16d5741535530eeb3e42a415091328905725741eb8c69e58cf42a419f040093497657419db35e9865042b41"
            + "3d1455a98d7a5741fe00aa063e142b4134be3fccd17e57419ca3e52f16242b4192ffcdfb15835741fbc0ab13ee332b41"
            + "5fd40d385a8757419ca3fe8ce54a2b411916eb4cad8957410441cf17dd612b410bff336b008c5741b92c37b4d4782b41"
            + "f80ff992538e574198f54f62cc8f2b4148ca4ac4a6905741d8253322c4a62b4103b039fff9925741f242faf3bbbd2b41"
            + "dd43d6434d955741bacdbed7b3d42b412c093192a09757412c429acdabeb2b41ee835aeaf39957418d17a6d5a3022c41"
            + "ce38634c479c574144c0fbef9b192c411cad5bb89a9e5741e8a9b41c94302c41da66542eeea057412a3dea5b8c472c41"
            + "b4ec5dae41a35741dcddb5ad845e2c4100c6883895a55741dcea30127d752c41ca7ae5cce8a7574112be7489758c2c41"
            + "cd93846b3caa574169ac9a136ea32c41719a761490ac5741";

    /** The same distances in PostGIS: ST_Distance(point::geography, line::geography). */
    @ParameterizedTest
    @CsvSource({
        "15.5, 55.3, 27616.878",  // near Bornholm, where UTM 32N alone is 0.16% long
        "14.0, 55.0, 40023.403",
        "16.5, 56.2, 54375.551",
    })
    void boundaryDistanceMatchesPostgisGeography(double lon, double lat, double expected) {
        ZoneIndex index = new TestZones()
                .wkbHex(ZoneElement.BOUNDARY, "325", BORNHOLM_BOUNDARY)
                .box(ZoneElement.EEZ, "eez:5674", 10, 54, 13, 57)
                .index();
        ZoneIndex.Tags t = index.lookup().tag(lon, lat);
        assertEquals(expected, t.distEezBoundaryM, 0.5);
    }

    static final TestZones AREA = new TestZones()
            .box(ZoneElement.EEZ, "eez:5674", 10, 55, 13, 58)
            .box(ZoneElement.EEZ, "eez:5694", 13, 55, 16, 58)
            .box(ZoneElement.MPA, "wdpa:2", 11.1, 56.1, 11.3, 56.3)
            .box(ZoneElement.MPA, "wdpa:1", 11.0, 56.0, 11.2, 56.2)
            .polygon(ZoneElement.MPA, "wdpa:ring",
                    new double[][] {{12.0, 57.0}, {12.4, 57.0}, {12.4, 57.4}, {12.0, 57.4}, {12.0, 57.0}},
                    new double[][] {{12.1, 57.1}, {12.3, 57.1}, {12.3, 57.3}, {12.1, 57.3}, {12.1, 57.1}})
            .box(ZoneElement.PORT, "wpi:29230", 12.5, 55.6, 12.7, 55.7)
            .line("1", new double[][] {{13.0, 55.0}, {13.0, 58.0}});

    @Test
    void containmentGivesEezMpasAndPort() {
        ZoneIndex.Lookup lookup = AREA.index().lookup();

        ZoneIndex.Tags both = lookup.tag(11.15, 56.15);
        assertEquals("eez:5674", both.eezId);
        assertArrayEquals(new String[] {"wdpa:1", "wdpa:2"}, both.mpaIds); // sorted
        assertEquals(0.0, both.distMpaM);
        assertFalse(both.inPort);

        ZoneIndex.Tags port = lookup.tag(12.6, 55.65);
        assertTrue(port.inPort);
        assertEquals(0, port.mpaIds.length);

        ZoneIndex.Tags sea = lookup.tag(14.0, 56.0);
        assertEquals("eez:5694", sea.eezId);
        assertNull(lookup.tag(9.5, 56.0).eezId);
    }

    @Test
    void holeIsOutsideTheProtectedArea() {
        ZoneIndex.Lookup lookup = AREA.index().lookup();
        assertArrayEquals(new String[] {"wdpa:ring"}, lookup.tag(12.05, 57.2).mpaIds);
        ZoneIndex.Tags inHole = lookup.tag(12.2, 57.2);
        assertEquals(0, inHole.mpaIds.length);
        // ~0.1 degree of longitude at 57.2 N to the hole's edge: about 6 km.
        assertEquals(6_000, inHole.distMpaM, 100);
    }

    @Test
    void pointOnASharedCornerTakesTheSmallestEezId() {
        // (13 E, 55 N) is a vertex of both EEZ boxes, so it is on the boundary of both.
        assertEquals("eez:5674", AREA.index().lookup().tag(13.0, 55.0).eezId);
    }

    /**
     * Outside, the distance is to the nearest point found in UTM, measured on the
     * ellipsoid. Compare with brute force: the true geodesic minimum over 20,000 points
     * along the piece's edges.
     */
    @Test
    void distanceToProtectedAreaIsTheGeodesicMinimum() {
        ZoneIndex.Tags t = AREA.index().lookup().tag(11.6, 56.25);
        double brute = bruteForce(11.6, 56.25, 11.3, 56.1, 11.3, 56.3);
        assertEquals(brute, t.distMpaM, 0.5);
        assertTrue(t.distMpaM > 15_000 && t.distMpaM < 20_000, "about 0.3 degrees of longitude");
    }

    /** Geodesic minimum from (lon, lat) to the UTM-straight segment between two lon/lat points. */
    static double bruteForce(double lon, double lat, double lon0, double lat0, double lon1, double lat1) {
        Utm32 utm = new Utm32();
        double[] a = new double[2];
        double[] b = new double[2];
        double[] q = new double[2];
        utm.forward(lon0, lat0, a);
        utm.forward(lon1, lat1, b);
        double best = Double.MAX_VALUE;
        for (int i = 0; i <= 20_000; i++) {
            double f = i / 20_000.0;
            utm.inverse(a[0] + f * (b[0] - a[0]), a[1] + f * (b[1] - a[1]), q);
            best = Math.min(best, Geodesic.WGS84.Inverse(lat, lon, q[1], q[0]).s12);
        }
        return best;
    }

    @Test
    void boundaryDistanceToALineAlongTheMeridian() {
        // 13 E is a straight line in UTM only approximately; the geodesic to it from 14 E at
        // 56 N is about 62.4 km (one degree of longitude at 56 N, less the curvature).
        ZoneIndex.Tags t = AREA.index().lookup().tag(14.0, 56.0);
        assertEquals(bruteForce(14.0, 56.0, 13.0, 55.0, 13.0, 58.0), t.distEezBoundaryM, 0.5);
    }

    /**
     * Stray fixes far outside the area pass validation (TELEPORT needs them). They get no
     * zones and distances of thousands of kilometres: to the nearest point found in UTM,
     * which out there may not be the true nearest, but no distance rule cares.
     */
    @ParameterizedTest
    @CsvSource({"10.0, -60.0", "179.0, 10.0", "-170.0, 89.0"})
    void strayFixesFarAwayGetNoZonesAndLargeDistances(double lon, double lat) {
        ZoneIndex.Tags t = AREA.index().lookup().tag(lon, lat);
        assertNull(t.eezId);
        assertEquals(0, t.mpaIds.length);
        assertFalse(t.inPort);
        assertTrue(t.distEezBoundaryM > 3_000_000, "boundary " + t.distEezBoundaryM);
        assertTrue(t.distMpaM > 3_000_000, "mpa " + t.distMpaM);
    }

    @Test
    void emptyPackageTagsNothing() {
        ZoneIndex.Tags t = new TestZones().index().lookup().tag(11.0, 56.0);
        assertNull(t.eezId);
        assertNull(t.distEezBoundaryM);
        assertNull(t.distMpaM);
    }
}

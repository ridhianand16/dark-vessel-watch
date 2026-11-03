package dvw.enrichment.zones;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * proj4j against PostGIS (PROJ 7.2.1): {@code ST_Transform(ST_SetSRID(ST_Point(lon, lat),
 * 4326), 25832)}. The zones in PostGIS were projected by PROJ; the job projects AIS points
 * with proj4j, so the two must put the same point in the same place.
 */
class Utm32Test {
    @ParameterizedTest
    @CsvSource({
        "11.6, 57.0, 657922.4470, 6320391.8242",   // Kattegat
        "15.5, 55.3, 912374.8310, 6147441.0494",   // Bornholm
        "17.5, 55.5, 1036239.2367, 6183313.1946",  // eastern edge of the area, 8.5 deg from 9 E
        "2.7, 53.6, 83338.4563, 5957488.6136",     // western edge
        "9.0, 56.0, 500000.0000, 6206079.5871",    // central meridian
        "12.6, 55.7, 726205.9457, 6178565.0293",   // Copenhagen
    })
    void matchesPostgis(double lon, double lat, double x, double y) {
        Utm32 utm = new Utm32();
        double[] xy = new double[2];
        utm.forward(lon, lat, xy);
        assertEquals(x, xy[0], 0.01, "easting");
        assertEquals(y, xy[1], 0.01, "northing");

        double[] back = new double[2];
        utm.inverse(xy[0], xy[1], back);
        assertEquals(lon, back[0], 1e-9);
        assertEquals(lat, back[1], 1e-9);
    }
}

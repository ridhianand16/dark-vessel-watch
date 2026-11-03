package dvw.enrichment.zones;

import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransform;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.ProjCoordinate;

/**
 * WGS84 lon/lat to EPSG:25832 (ETRS89 / UTM zone 32N, metres) and back, with proj4j.
 *
 * <p>ETRS89 and WGS84 differ by well under a metre in Denmark and PostGIS treats them as
 * the same datum for this transform, so both ends use the GRS80 ellipsoid with no datum
 * shift. The definition is spelled out rather than looked up by EPSG code, so the job needs
 * no EPSG database at run time.
 *
 * <p>Not thread-safe: proj4j transforms keep scratch coordinates. Each task has its own.
 */
public final class Utm32 {
    static final String WGS84 = "+proj=longlat +datum=WGS84 +no_defs";
    static final String EPSG_25832 = "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs";

    private final CoordinateTransform forward;
    private final CoordinateTransform inverse;
    private final ProjCoordinate in = new ProjCoordinate();
    private final ProjCoordinate out = new ProjCoordinate();

    public Utm32() {
        CRSFactory crs = new CRSFactory();
        CoordinateReferenceSystem geo = crs.createFromParameters("WGS84", WGS84);
        CoordinateReferenceSystem utm = crs.createFromParameters("EPSG:25832", EPSG_25832);
        CoordinateTransformFactory f = new CoordinateTransformFactory();
        forward = f.createTransform(geo, utm);
        inverse = f.createTransform(utm, geo);
    }

    /** Projects (lon, lat) into {@code xy}: easting, northing in metres. */
    public void forward(double lon, double lat, double[] xy) {
        in.x = lon;
        in.y = lat;
        forward.transform(in, out);
        xy[0] = out.x;
        xy[1] = out.y;
    }

    /** Back from (easting, northing) into {@code lonLat}. */
    public void inverse(double x, double y, double[] lonLat) {
        in.x = x;
        in.y = y;
        inverse.transform(in, out);
        lonLat[0] = out.x;
        lonLat[1] = out.y;
    }
}

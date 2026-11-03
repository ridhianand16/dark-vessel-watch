package dvw.enrichment.zones;

import java.util.List;
import java.util.TreeSet;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.locationtech.jts.algorithm.RayCrossingCounter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * One zone package in memory, built from the pieces in zone_pieces_25832 and
 * eez_boundary_pieces_25832 (docs/adr/0017), and immutable once built: every task in the
 * TaskManager shares one (docs/adr/0018). Per-task scratch lives in {@link Lookup}.
 *
 * <p>Three STRtrees over piece bounding boxes. An STRtree (sort-tile-recursive R-tree) is a
 * static spatial index: built once from all items, it answers "which boxes contain this
 * point" and "which item is nearest" without looking at most of them.
 * <ul>
 *   <li>areas: every EEZ, protected area and port piece, for containment;
 *   <li>mpas: protected area pieces only, for the nearest protected area;
 *   <li>boundaries: EEZ boundary line pieces, for the nearest boundary.
 * </ul>
 *
 * <p>Coordinates are stored packed (two doubles per vertex, no Coordinate objects): 3.1
 * million vertices take 50 MB instead of about 140 MB. The code below reads them through
 * CoordinateSequence.getX/getY and never asks a geometry for a Coordinate[] copy.
 */
public final class ZoneIndex {
    private static final GeometryFactory GEOMETRY =
            new GeometryFactory(new PrecisionModel(), 25832, PackedCoordinateSequenceFactory.DOUBLE_FACTORY);

    public final String version;
    public final int pieces;
    private final STRtree areas = new STRtree();
    private final STRtree mpas = new STRtree();
    private final STRtree boundaries = new STRtree();

    /** One piece of a zone or boundary. */
    private static final class Piece {
        final String kind;
        final String id;
        final Geometry geom;

        Piece(String kind, String id, Geometry geom) {
            this.kind = kind;
            this.id = id;
            this.geom = geom;
        }
    }

    private ZoneIndex(String version, List<ZoneElement> elements) {
        this.version = version;
        this.pieces = elements.size();
        WKBReader wkb = new WKBReader(GEOMETRY);
        for (ZoneElement e : elements) {
            Geometry g;
            try {
                g = wkb.read(e.wkb);
            } catch (ParseException ex) {
                throw new IllegalArgumentException("zone piece " + e.kind + " " + e.id + " is not valid WKB", ex);
            }
            Piece p = new Piece(e.kind, e.id, g);
            if (ZoneElement.BOUNDARY.equals(e.kind)) {
                if (!(g instanceof LineString)) {
                    throw new IllegalArgumentException("boundary piece " + e.id + " is a " + g.getGeometryType());
                }
                boundaries.insert(g.getEnvelopeInternal(), p);
            } else {
                if (!(g instanceof Polygon)) {
                    throw new IllegalArgumentException("zone piece " + e.id + " is a " + g.getGeometryType());
                }
                areas.insert(g.getEnvelopeInternal(), p);
                if (ZoneElement.MPA.equals(e.kind)) {
                    mpas.insert(g.getEnvelopeInternal(), p);
                }
            }
        }
        // Build now, on this thread. A tree builds itself lazily on its first query otherwise,
        // and four tasks would race to do it.
        areas.build();
        mpas.build();
        boundaries.build();
    }

    /** Builds the index from every piece of one package (markers excluded). */
    public static ZoneIndex build(String version, List<ZoneElement> pieces) {
        return new ZoneIndex(version, pieces);
    }

    /** A new lookup for one task; not thread-safe. */
    public Lookup lookup() {
        return new Lookup(this);
    }

    /** What one position is tagged with. */
    public static final class Tags {
        /** The EEZ containing the point, or null. EEZs only overlap in slivers of a few
         * square metres along shared boundaries; there the smallest zone_id wins. */
        public String eezId;
        /** Every protected area containing the point, sorted; empty if none. */
        public String[] mpaIds;
        public boolean inPort;
        /** Geodesic metres to the nearest EEZ boundary line; null only if the point cannot
         * be projected, which proj4j has not done for any valid latitude and longitude. */
        public Double distEezBoundaryM;
        /** Geodesic metres to the nearest protected area, 0 inside one; null if the point
         * cannot be projected or the package has no protected areas. */
        public Double distMpaM;
    }

    /**
     * Tags positions against the shared index, with this task's own projection and scratch
     * arrays.
     */
    public static final class Lookup {
        private static final String[] NONE = new String[0];
        private static final double ROUND_TRIP_DEG = 1e-7; // about 1 cm
        private final ZoneIndex index;
        private final Utm32 utm = new Utm32();
        private final double[] xy = new double[2];
        private final double[] lonLat = new double[2];
        private final Coordinate point = new Coordinate();
        private final Nearest nearest = new Nearest();

        Lookup(ZoneIndex index) {
            this.index = index;
        }

        public String version() {
            return index.version;
        }

        public Tags tag(double lon, double lat) {
            Tags t = new Tags();
            t.mpaIds = NONE;
            if (!project(lon, lat)) {
                return t;
            }
            point.x = xy[0];
            point.y = xy[1];

            TreeSet<String> eez = null;
            TreeSet<String> mpa = null;
            for (Object o : index.areas.query(new Envelope(point))) {
                Piece p = (Piece) o;
                if (!covers((Polygon) p.geom, point)) {
                    continue;
                }
                switch (p.kind) {
                    case ZoneElement.EEZ -> (eez == null ? eez = new TreeSet<>() : eez).add(p.id);
                    case ZoneElement.MPA -> (mpa == null ? mpa = new TreeSet<>() : mpa).add(p.id);
                    case ZoneElement.PORT -> t.inPort = true;
                    default -> throw new IllegalStateException("unknown zone kind " + p.kind);
                }
            }
            t.eezId = eez == null ? null : eez.first();
            if (mpa != null) {
                t.mpaIds = mpa.toArray(NONE);
                t.distMpaM = 0.0;
            } else {
                t.distMpaM = geodesicToNearest(index.mpas, lon, lat);
            }
            t.distEezBoundaryM = geodesicToNearest(index.boundaries, lon, lat);
            return t;
        }

        /**
         * Projects into {@code xy} and checks the round trip. A guard: proj4j's transverse
         * Mercator projects and inverts even points on the far side of the globe from 9 E,
         * but if it ever throws or returns numbers that do not invert, the point gets no
         * zones and null distances rather than wrong ones.
         */
        private boolean project(double lon, double lat) {
            try {
                utm.forward(lon, lat, xy);
                if (!Double.isFinite(xy[0]) || !Double.isFinite(xy[1])) {
                    return false;
                }
                utm.inverse(xy[0], xy[1], lonLat);
            } catch (RuntimeException e) {
                return false;
            }
            return Math.abs(lonLat[0] - lon) < ROUND_TRIP_DEG && Math.abs(lonLat[1] - lat) < ROUND_TRIP_DEG;
        }

        /**
         * Finds the nearest piece in UTM, the nearest point on it, and returns the geodesic
         * distance from (lon, lat) to that point. UTM 32N stretches distances by up to 0.3%
         * at the eastern edge of the area; the nearest point it finds is only slightly off
         * the true nearest point, and the distance along a line is flat at its minimum, so
         * the geodesic distance to it matches PostGIS geography to centimetres (ADR 0013).
         *
         * <p>For a protected area the point is outside every piece (inside, the distance is
         * 0 and this is not called), so the nearest point is on the area's real edge, never
         * on a cut line between two of its pieces (ADR 0017).
         */
        private Double geodesicToNearest(STRtree tree, double lon, double lat) {
            if (tree.isEmpty()) {
                return null;
            }
            Piece p = (Piece) tree.nearestNeighbour(new Envelope(point), point, nearest);
            nearest.closest(p.geom, point.x, point.y);
            utm.inverse(nearest.x, nearest.y, lonLat);
            return Geodesic.WGS84.Inverse(lat, lon, lonLat[1], lonLat[0], GeodesicMask.DISTANCE).s12;
        }
    }

    /** Point in polygon, counting the boundary as inside (ST_Covers / ST_Intersects). */
    static boolean covers(Polygon poly, Coordinate p) {
        if (!poly.getEnvelopeInternal().covers(p)) {
            return false;
        }
        int shell = RayCrossingCounter.locatePointInRing(p, poly.getExteriorRing().getCoordinateSequence());
        if (shell != Location.INTERIOR) {
            return shell == Location.BOUNDARY;
        }
        for (int i = 0; i < poly.getNumInteriorRing(); i++) {
            int hole = RayCrossingCounter.locatePointInRing(p, poly.getInteriorRingN(i).getCoordinateSequence());
            if (hole == Location.INTERIOR) {
                return false;
            }
        }
        return true;
    }

    /**
     * Nearest point on a piece's lines (a boundary piece, or a polygon piece's rings) to a
     * point, by projecting onto each segment. Also the STRtree's exact item distance for its
     * branch-and-bound nearest search: a subtree is skipped when its box is already further
     * than the best piece so far.
     */
    static final class Nearest implements ItemDistance {
        double x;
        double y;
        private double best2;

        void closest(Geometry g, double px, double py) {
            best2 = Double.POSITIVE_INFINITY;
            if (g instanceof Polygon poly) {
                scan(poly.getExteriorRing().getCoordinateSequence(), px, py);
                for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                    scan(poly.getInteriorRingN(i).getCoordinateSequence(), px, py);
                }
            } else {
                scan(((LineString) g).getCoordinateSequence(), px, py);
            }
        }

        private void scan(CoordinateSequence s, double px, double py) {
            int n = s.size();
            if (n == 1) {
                consider(s.getX(0), s.getY(0), px, py);
            }
            for (int i = 0; i + 1 < n; i++) {
                double ax = s.getX(i);
                double ay = s.getY(i);
                double dx = s.getX(i + 1) - ax;
                double dy = s.getY(i + 1) - ay;
                double len2 = dx * dx + dy * dy;
                double t = len2 == 0 ? 0 : ((px - ax) * dx + (py - ay) * dy) / len2;
                t = t < 0 ? 0 : (t > 1 ? 1 : t);
                consider(ax + t * dx, ay + t * dy, px, py);
            }
        }

        private void consider(double qx, double qy, double px, double py) {
            double d2 = (qx - px) * (qx - px) + (qy - py) * (qy - py);
            if (d2 < best2) {
                best2 = d2;
                x = qx;
                y = qy;
            }
        }

        @Override
        public double distance(ItemBoundable a, ItemBoundable b) {
            Coordinate p = (Coordinate) (a.getItem() instanceof Coordinate ? a.getItem() : b.getItem());
            Piece piece = (Piece) (a.getItem() instanceof Piece ? a.getItem() : b.getItem());
            closest(piece.geom, p.x, p.y);
            return Math.sqrt(best2);
        }
    }
}

package dvw.enrichment.zones;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One zone index per TaskManager JVM, shared by every zone-tagging task in it
 * (docs/adr/0018).
 *
 * <p>Every tagging task receives the whole broadcast zone stream. If each built its own
 * index, the TaskManager's 4 slots would hold 4 copies of about 60 MB in a task heap of
 * about 410 MB. Instead each task offers the pieces it receives to the {@link Loading}
 * for that package version. The first copy of each piece is kept and later copies are
 * dropped. When a task reaches the "zones complete" marker it asks for the index: the
 * first task to ask builds it, and the others wait for that build and then get the same
 * object. The index is immutable, so sharing it across task threads is safe.
 *
 * <p>Static state lives as long as the job's classloader in this TaskManager. A task
 * that restarts after a failure finds the index already built for its version and skips
 * the build. A new job gets a new classloader and starts empty.
 */
public final class SharedZones {
    private static Loading current; // guarded by SharedZones.class

    private SharedZones() {}

    /** The loading slot for a package version. Replaces the slot for any other version. */
    public static synchronized Loading forVersion(String version) {
        if (current == null || !current.version.equals(version)) {
            current = new Loading(version);
        }
        return current;
    }

    /** For tests: forget every package. */
    static synchronized void reset() {
        current = null;
    }

    /** The pieces of one package, collected until the marker, then its built index. */
    public static final class Loading {
        public final String version;
        private final ConcurrentHashMap<Integer, ZoneElement> pieces = new ConcurrentHashMap<>();
        private volatile ZoneIndex index;

        Loading(String version) {
            this.version = version;
        }

        /** Keeps the first copy of a piece. A no-op once the index exists. */
        public void offer(ZoneElement piece) {
            if (index == null) {
                pieces.putIfAbsent(piece.seq, piece);
            }
        }

        /**
         * The index, built on the first call. {@code expected} is the piece count on the
         * marker. The caller has offered every piece before its marker, so on the first
         * call all of them must be here.
         */
        public synchronized ZoneIndex complete(int expected) {
            if (index != null) {
                return index;
            }
            if (pieces.size() != expected) {
                throw new IllegalStateException("zones " + version + ": marker says " + expected
                        + " pieces, " + pieces.size() + " arrived");
            }
            List<ZoneElement> sorted = new ArrayList<>(pieces.values());
            sorted.sort(Comparator.comparingInt(e -> e.seq));
            index = ZoneIndex.build(version, sorted);
            pieces.clear(); // the WKB is no longer needed
            return index;
        }
    }
}

package dvw.enrichment.zones;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

/**
 * The startup gate: the job is only submitted once PostGIS has an active zone package.
 *
 * <p>Runs in the job's main method, which executes in the submitting process (the
 * flink-submit container), before the job graph is built. Without it, a job started before
 * the first `make refdata-load` would have no zones to tag with, and the zone source would
 * fail and restart until one appeared. Prints to stderr, which the submitting container
 * shows; the Flink client's own logger writes to a file.
 */
public final class ZonesGate {
    private ZonesGate() {}

    /** Polls until a package is active and returns its version. Waits forever. */
    public static String awaitActivePackage(PostgisConfig db, Duration every, PrintStream log)
            throws InterruptedException {
        String last = null;
        while (true) {
            String status;
            try (Connection c = db.connect()) {
                String version = PostgisZoneRows.activeVersion(c);
                if (version != null) {
                    log.println("zones gate: active package " + version + "; submitting");
                    return version;
                }
                status = "no active zone package in PostGIS yet; load one with: make refdata-load";
            } catch (SQLException e) {
                status = "PostGIS at " + db.url() + " not ready: " + e.getMessage();
            }
            if (!status.equals(last)) {
                log.println("zones gate: " + status + " (checking every " + every.toSeconds() + " s)");
                last = status;
            }
            Thread.sleep(every.toMillis());
        }
    }
}

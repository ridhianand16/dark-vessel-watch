package dvw.enrichment.zones;

import java.io.Serializable;

/** The pieces of the active package, one at a time. */
public interface ZoneRows extends AutoCloseable {
    /** refdata_packages.package_version of the package being read. */
    String version();

    /** The next piece with its seq set, or null after the last. */
    ZoneElement next() throws Exception;

    /** Opens a read of whatever package is active now. Serialized with the zone source. */
    interface Opener extends Serializable {
        ZoneRows open() throws Exception;
    }
}

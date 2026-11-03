package dvw.enrichment.zones;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Reads every piece of the active package from PostGIS in one consistent snapshot.
 *
 * <p>The loader replaces the zone tables by dropping them and renaming new ones in, in one
 * transaction (ADR 0010). This read takes ACCESS SHARE locks on the tables before its
 * snapshot starts, inside a REPEATABLE READ transaction. A swap that commits before the
 * locks are taken is fully visible; one that starts after waits for the read to finish.
 * So the active version and the pieces always come from the same load, and every row's
 * package_version is checked against it anyway.
 *
 * <p>Rows are streamed with a server-side cursor (fetch size with autocommit off), so the
 * 48 MB of WKB never sits in one JDBC result.
 */
public final class PostgisZoneRows implements ZoneRows {
    static final String ACTIVE = "SELECT package_version FROM refdata_packages WHERE active";
    static final String PIECES =
            "SELECT kind, zone_id, package_version, ST_AsBinary(geom) FROM zone_pieces_25832"
            + " UNION ALL"
            + " SELECT 'boundary', id::text, package_version, ST_AsBinary(geom) FROM eez_boundary_pieces_25832";

    private final Connection conn;
    private final PreparedStatement query;
    private final ResultSet rows;
    private final String version;
    private int seq;

    private PostgisZoneRows(PostgisConfig db) throws SQLException {
        conn = db.connect();
        try {
            conn.setAutoCommit(false);
            conn.setReadOnly(true);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try (Statement s = conn.createStatement()) {
                // Before any SELECT: the snapshot starts at the first query, after the locks.
                s.execute("LOCK TABLE refdata_packages, zone_pieces_25832, eez_boundary_pieces_25832"
                        + " IN ACCESS SHARE MODE");
            }
            version = activeVersion(conn);
            if (version == null) {
                throw new IllegalStateException("no active zone package in PostGIS; load one with make refdata-load");
            }
            query = conn.prepareStatement(PIECES);
            query.setFetchSize(1000);
            rows = query.executeQuery();
        } catch (SQLException | RuntimeException e) {
            conn.close();
            throw e;
        }
    }

    public static Opener opener(PostgisConfig db) {
        return () -> new PostgisZoneRows(db);
    }

    /** The active package version, or null if there is none. */
    static String activeVersion(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet r = s.executeQuery(ACTIVE)) {
            return r.next() ? r.getString(1) : null;
        }
    }

    @Override
    public String version() {
        return version;
    }

    @Override
    public ZoneElement next() throws SQLException {
        if (!rows.next()) {
            return null;
        }
        String rowVersion = rows.getString(3);
        if (!version.equals(rowVersion)) {
            throw new IllegalStateException("zone piece " + rows.getString(2) + " is from package " + rowVersion
                    + ", the active package is " + version);
        }
        return ZoneElement.piece(rows.getString(1), rows.getString(2), version, seq++, rows.getBytes(4));
    }

    @Override
    public void close() throws SQLException {
        try {
            conn.rollback(); // read-only; ends the transaction and releases the locks
        } finally {
            conn.close();
        }
    }
}

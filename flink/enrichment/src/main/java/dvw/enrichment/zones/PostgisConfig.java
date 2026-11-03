package dvw.enrichment.zones;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;

/**
 * Where the zone package lives. Read from the POSTGRES_* environment of the process that
 * submits the job, like the Python services (common/postgis.py), and serialized into the
 * job graph with the zone source.
 */
public final class PostgisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String host;
    final int port;
    final String database;
    final String user;
    final String password;

    public PostgisConfig(String host, int port, String database, String user, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public static PostgisConfig fromEnv(Function<String, String> env) {
        return new PostgisConfig(
                or(env.apply("POSTGRES_HOST"), "localhost"),
                Integer.parseInt(or(env.apply("POSTGRES_PORT"), "5432")),
                or(env.apply("POSTGRES_DB"), "dvw"),
                or(env.apply("POSTGRES_USER"), "dvw"),
                or(env.apply("POSTGRES_PASSWORD"), ""));
    }

    private static String or(String v, String fallback) {
        return v == null || v.isBlank() ? fallback : v;
    }

    /**
     * A new connection. Calls the driver directly instead of going through DriverManager:
     * DriverManager only hands out drivers visible to its own classloader, and Flink loads
     * the job's jar, with the driver in it, in a separate one.
     */
    Connection connect() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        p.setProperty("ApplicationName", "dvw-flink-zones");
        p.setProperty("connectTimeout", "10");
        Connection c = new org.postgresql.Driver().connect(url(), p);
        if (c == null) {
            throw new SQLException("PostgreSQL driver refused " + url());
        }
        return c;
    }

    String url() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }
}

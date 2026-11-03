package dvw.enrichment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;

/**
 * contracts/ais_enriched.json: the rules this job and the tracker must agree on. Maven passes
 * its path as the dvw.contract system property. A missing file fails the test, never skips it.
 */
final class Contract {
    private Contract() {}

    static JsonNode load() {
        String path = System.getProperty("dvw.contract");
        if (path == null) {
            throw new IllegalStateException("system property dvw.contract is not set (see pom.xml)");
        }
        try {
            return new ObjectMapper().readTree(Path.of(path).toFile());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the contract at " + path, e);
        }
    }
}

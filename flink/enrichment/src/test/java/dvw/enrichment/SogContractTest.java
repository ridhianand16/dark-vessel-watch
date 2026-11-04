package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

/**
 * The Validator's SOG bounds are the sog rule in contracts/ais_enriched.json. The tracker's
 * test_contract.py checks its own idea of a usable SOG against the same rule.
 */
class SogContractTest {
    private static final JsonNode RULE = Contract.load().get("sog");

    @Test
    void boundsAreTheContract() {
        assertEquals(RULE.get("min_valid_kn").asDouble(), Validator.SOG_MIN_KN);
        assertEquals(RULE.get("max_valid_kn").asDouble(), Validator.SOG_MAX_KN);
        assertEquals(RULE.get("not_available_kn").asDouble(), Validator.SOG_NOT_AVAILABLE);
    }
}

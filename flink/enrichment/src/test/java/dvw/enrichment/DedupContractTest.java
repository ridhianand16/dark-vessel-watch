package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Dedup's coordinate rounding agrees with the duplicate_key rule in contracts/ais_enriched.json.
 * The tracker's test_contract.py checks its own duplicate check against the same examples.
 */
class DedupContractTest {
    private static final JsonNode RULE = Contract.load().get("duplicate_key");

    @Test
    void decimalsAreTheContract() {
        assertEquals(RULE.get("coordinate_decimals").asInt(), Dedup.COORD_DECIMALS);
    }

    static Stream<Arguments> examples() {
        List<Arguments> out = new ArrayList<>();
        RULE.get("coordinate_examples").forEach(e ->
                out.add(Arguments.of(e.get("coordinate").asDouble(), e.get("rounded").asLong())));
        return out.stream();
    }

    @ParameterizedTest
    @MethodSource("examples")
    void coordinateRounding(double coordinate, long rounded) {
        assertEquals(rounded, Dedup.coordKey(coordinate));
    }
}

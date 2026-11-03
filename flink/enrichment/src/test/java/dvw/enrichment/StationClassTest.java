package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * StationClass agrees with contracts/ais_enriched.json. The tracker's test_contract.py
 * checks stream/faust_app/mmsi.py against the same file, so the two cannot drift apart.
 */
class StationClassTest {
    private static final JsonNode CLASSES = Contract.load().get("station_classes");

    private static List<JsonNode> classes() {
        List<JsonNode> out = new ArrayList<>();
        CLASSES.forEach(out::add);
        return out;
    }

    @Test
    void wireValuesAreExactlyTheContract() {
        List<String> contract = classes().stream().map(c -> c.get("wire").asText()).toList();
        List<String> ours = Arrays.stream(StationClass.values()).map(c -> c.wire).toList();
        assertEquals(contract, ours);
    }

    @Test
    void shipClassesAreExactlyTheContract() {
        Set<String> contract = classes().stream()
                .filter(c -> c.get("is_ship").asBoolean())
                .map(c -> c.get("wire").asText())
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> ours = Arrays.stream(StationClass.values())
                .filter(c -> c.isShip)
                .map(c -> c.wire)
                .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(contract, ours);
    }

    static Stream<Arguments> exampleMmsis() {
        return classes().stream().flatMap(c -> {
            List<Arguments> args = new ArrayList<>();
            c.get("example_mmsis").forEach(m -> args.add(
                    Arguments.of(m.asLong(), c.get("wire").asText(), c.get("is_ship").asBoolean())));
            return args.stream();
        });
    }

    @ParameterizedTest
    @MethodSource("exampleMmsis")
    void exampleMmsi(long mmsi, String wire, boolean isShip) {
        assertEquals(wire, StationClass.of(mmsi).wire);
        assertEquals(isShip, StationClass.of(mmsi).isShip);
    }
}

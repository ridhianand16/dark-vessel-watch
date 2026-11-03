package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Same cases as stream/faust_app/tests/test_mmsi.py, so the two implementations agree. */
class StationClassTest {
    @ParameterizedTest
    @CsvSource({
        "219026147, ship",
        "257000000, ship",
        "200000000, ship",
        "799999999, ship",
        "300000000, ship",
        "412345678, ship",
        "538001234, ship",
        "636012345, ship",
        "982190001, craft_with_parent",
        "2190047, coast",
        "2190000, coast",
        "21900000, group",
        "99999999, group",
        "111219500, sar_aircraft",
        "100000000, other",
        "110999999, other",
        "112000000, other",
        "812345678, handheld",
        "992191234, aid_to_navigation",
        "999000001, aid_to_navigation",
        "970123456, sart",
        "972123456, mob",
        "974123456, epirb",
        "971000000, other",
        "973000000, other",
        "975000000, other",
        "900000000, other",
        "1000000000, other",
        "-1, other",
    })
    void stationClass(long mmsi, String expected) {
        assertEquals(expected, StationClass.of(mmsi).wire);
    }
}

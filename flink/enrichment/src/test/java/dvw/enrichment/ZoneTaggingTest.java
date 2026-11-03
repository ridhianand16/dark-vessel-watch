package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dvw.enrichment.zones.TestZones;
import dvw.enrichment.zones.ZoneElement;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ZoneTaggingTest {
    static final TestZones ZONES = new TestZones()
            .box(ZoneElement.EEZ, "eez:5674", 10, 55, 13, 58)
            .box(ZoneElement.MPA, "wdpa:1", 11.0, 56.0, 11.2, 56.2)
            .line("1", new double[][] {{13.0, 55.0}, {13.0, 58.0}});

    @AfterEach
    void reset() {
        TestZones.resetShared();
    }

    static KeyedBroadcastOperatorTestHarness<Long, RawRecord, ZoneElement, RawRecord> harness() throws Exception {
        return new KeyedBroadcastOperatorTestHarness<>(
                new CoBroadcastWithKeyedOperator<>(new ZoneTagging(), List.of(ZoneTagging.ZONES)),
                r -> r.mmsi, Types.LONG, 128, 1, 0);
    }

    static RawRecord record(long mmsi, long offset, double lon, double lat) {
        RawRecord r = new RawRecord();
        r.mmsi = mmsi;
        r.srcOffset = offset;
        r.lon = lon;
        r.lat = lat;
        return r;
    }

    static void sendZones(KeyedBroadcastOperatorTestHarness<Long, RawRecord, ZoneElement, RawRecord> h, TestZones z)
            throws Exception {
        for (ZoneElement e : z.stream()) {
            h.processBroadcastElement(e, Long.MIN_VALUE);
        }
    }

    static List<Long> offsets(List<RawRecord> out, long mmsi) {
        return out.stream().filter(r -> r.mmsi == mmsi).map(r -> r.srcOffset).collect(Collectors.toList());
    }

    @Test
    void recordsWaitForTheMarkerThenLeaveInOrder() throws Exception {
        try (var h = harness()) {
            h.open();
            for (long i = 0; i < 6; i++) {
                h.processElement(record(219000000L + i % 2, i, 11.1, 56.1), 1000 + i);
            }
            // Pieces alone release nothing: only the marker says the package is complete.
            for (ZoneElement e : ZONES.pieces()) {
                h.processBroadcastElement(e, Long.MIN_VALUE);
            }
            assertEquals(0, h.extractOutputValues().size());

            h.processBroadcastElement(ZONES.marker(), Long.MIN_VALUE);
            h.processElement(record(219000000L, 6, 12.0, 56.5), 1006);

            List<RawRecord> out = h.extractOutputValues();
            assertEquals(7, out.size());
            assertEquals(List.of(0L, 2L, 4L, 6L), offsets(out, 219000000L));
            assertEquals(List.of(1L, 3L, 5L), offsets(out, 219000001L));
            for (RawRecord r : out) {
                assertEquals(TestZones.VERSION, r.zonesVersion);
                assertEquals("eez:5674", r.eezId);
                assertNotNull(r.distEezBoundaryM);
            }
            assertArrayEquals(new String[] {"wdpa:1"}, out.get(0).mpaIds);
            assertEquals(0, out.get(6).mpaIds.length);
            assertTrue(out.get(6).distMpaM > 0);
        }
    }

    /**
     * A checkpoint taken while records wait keeps them. After a restore the index is gone
     * (it is not Flink state), the zone source sends the package again, and the restored
     * records come out once, tagged.
     */
    @Test
    void bufferedRecordsSurviveARestore() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            h.processElement(record(219000000L, 0, 11.1, 56.1), 1000);
            h.processElement(record(219000001L, 1, 12.0, 56.5), 1001);
            snapshot = h.snapshot(1L, 1L);
            assertEquals(0, h.extractOutputValues().size());
        }
        TestZones.resetShared(); // a new TaskManager: nothing in memory

        try (var h = harness()) {
            h.initializeState(snapshot);
            h.open();
            h.processElement(record(219000000L, 2, 11.1, 56.1), 1002); // still no zones: buffered
            assertEquals(0, h.extractOutputValues().size());
            sendZones(h, ZONES);
            List<RawRecord> out = h.extractOutputValues();
            assertEquals(List.of(0L, 2L), offsets(out, 219000000L));
            assertEquals(List.of(1L), offsets(out, 219000001L));
            assertTrue(out.stream().allMatch(r -> TestZones.VERSION.equals(r.zonesVersion)));
        }
    }

    /** Stop with a savepoint, load a new package, start from the savepoint. */
    @Test
    void restartPicksUpTheNewPackage() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = harness()) {
            h.open();
            sendZones(h, ZONES);
            h.processElement(record(219000000L, 0, 11.1, 56.1), 1000);
            assertEquals(TestZones.VERSION, h.extractOutputValues().get(0).zonesVersion);
            snapshot = h.snapshot(1L, 1L);
        }

        TestZones next = new TestZones("zones-test00000002")
                .box(ZoneElement.EEZ, "eez:5674", 10, 55, 13, 58)
                .line("1", new double[][] {{13.0, 55.0}, {13.0, 58.0}});
        try (var h = harness()) {
            h.initializeState(snapshot);
            h.open();
            h.processElement(record(219000000L, 1, 11.1, 56.1), 1001);
            sendZones(h, next);
            RawRecord r = h.extractOutputValues().get(0);
            assertEquals("zones-test00000002", r.zonesVersion);
            assertEquals(0, r.mpaIds.length); // the new package has no protected area there
        }
    }
}

package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the dedup operator alone. The harness stands in for the runtime: it feeds elements
 * with timestamps, advances the watermark (which fires event-time timers) and exposes the
 * main output, side outputs and the number of keyed state entries.
 */
class DedupTest {
    static final long T0 = 1_761_609_600_000L; // 2026-10-28T00:00:00Z
    static final String RUN = "run-a";
    KeyedOneInputStreamOperatorTestHarness<Long, RawRecord, RawRecord> h;

    @BeforeEach
    void open() throws Exception {
        h = harness();
        h.open();
    }

    @AfterEach
    void close() throws Exception {
        h.close();
    }

    static KeyedOneInputStreamOperatorTestHarness<Long, RawRecord, RawRecord> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new Dedup()), r -> r.mmsi, Types.LONG);
    }

    static RawRecord rec(String run, long mmsi, long ts, double lat, double lon, long offset) {
        RawRecord r = new RawRecord();
        r.runId = run;
        r.mmsi = mmsi;
        r.eventTs = ts;
        r.lat = lat;
        r.lon = lon;
        r.srcPartition = 3;
        r.srcOffset = offset;
        r.stationClass = "ship";
        return r;
    }

    void send(RawRecord r) throws Exception {
        h.processElement(r, r.eventTs);
    }

    List<Long> forwardedOffsets() {
        return h.extractOutputValues().stream().map(r -> r.srcOffset).collect(Collectors.toList());
    }

    int duplicates() {
        var q = h.getSideOutput(Dedup.DUPLICATES);
        return q == null ? 0 : q.size();
    }

    @Test
    void sameReportFromAnotherStationIsDroppedAndCounted() throws Exception {
        send(rec(RUN, 219026147, T0, 55.123456, 11.5, 1));
        send(rec(RUN, 219026147, T0, 55.123456, 11.5, 2));
        send(rec(RUN, 219026147, T0, 55.123459, 11.500001, 3)); // same at 5 dp (rounds equal)
        assertEquals(List.of(1L), forwardedOffsets());
        assertEquals(2, duplicates());
        StreamRecord<Duplicate> d = h.getSideOutput(Dedup.DUPLICATES).peek();
        assertEquals(RUN, d.getValue().runId);
        assertEquals(3, d.getValue().srcPartition);
        assertEquals(T0, d.getTimestamp(), "the duplicate keeps its event time for the count window");
    }

    @Test
    void secondShipOnTheSameMmsiSurvives() throws Exception {
        send(rec(RUN, 219026147, T0, 55.12345, 11.5, 1));
        send(rec(RUN, 219026147, T0, 55.12355, 11.5, 2)); // 11 m away, same second
        send(rec(RUN, 219026147, T0 + 1000, 55.12345, 11.5, 3)); // same place, next second
        assertEquals(List.of(1L, 2L, 3L), forwardedOffsets());
        assertEquals(0, duplicates());
    }

    @Test
    void otherVesselsAndOtherRunsAreIndependent() throws Exception {
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 1));
        send(rec(RUN, 257000000, T0, 55.0, 11.0, 2));
        send(rec("run-b", 219026147, T0, 55.0, 11.0, 3)); // the same day replayed again
        assertEquals(List.of(1L, 2L, 3L), forwardedOffsets());
    }

    @Test
    void keyIsRememberedForTwoMinutesOfTheVesselsOwnEventTime() throws Exception {
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 1));
        send(rec(RUN, 219026147, T0 + 119_999, 55.1, 11.0, 2));
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 3)); // still within 2 min: duplicate
        send(rec(RUN, 219026147, T0 + 120_000, 55.2, 11.0, 4)); // vessel clock reaches T0 + 2 min
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 5)); // forgotten: forwarded
        assertEquals(List.of(1L, 2L, 4L, 5L), forwardedOffsets());
        assertEquals(1, duplicates());
    }

    @Test
    void timerClearsAQuietVesselWhenTheWatermarkPasses() throws Exception {
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 1));
        send(rec(RUN, 219026147, T0 + 10_000, 55.1, 11.0, 2));
        // Heap MapState counts as one entry per key and state name: seen and latest.
        assertEquals(2, h.numKeyedStateEntries());

        h.processWatermark(new Watermark(T0 + 10_000 + Dedup.REMEMBER_MS - 1));
        assertEquals(2, h.numKeyedStateEntries(), "not yet");
        h.processWatermark(new Watermark(T0 + 10_000 + Dedup.REMEMBER_MS));
        assertEquals(0, h.numKeyedStateEntries());

        send(rec(RUN, 219026147, T0 + 10_000, 55.1, 11.0, 3));
        assertEquals(List.of(1L, 2L, 3L), forwardedOffsets());
    }

    @Test
    void timerClearsOnlyRunsThatAreQuiet() throws Exception {
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 1));
        send(rec("run-b", 219026147, T0 + 60_000, 55.0, 11.0, 2));
        h.processWatermark(new Watermark(T0 + Dedup.REMEMBER_MS)); // run-a's timer fires
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 3)); // run-a forgotten: forwarded
        send(rec("run-b", 219026147, T0 + 60_000, 55.0, 11.0, 4)); // run-b still known
        assertEquals(List.of(1L, 2L, 3L), forwardedOffsets());
        assertEquals(1, duplicates());
    }

    @Test
    void vesselClockForgetsWithoutAnyWatermark() throws Exception {
        // A vessel reporting every 2 s for an hour, and never a watermark.
        for (int i = 0; i < 1800; i++) {
            send(rec(RUN, 219026147, T0 + i * 2_000L, 55.0 + i * 1e-4, 11.0, i));
        }
        // Newest is i = 1799. Report 1740 is 118 s older: remembered. 1739 is 120 s older: forgotten.
        send(rec(RUN, 219026147, T0 + 1740 * 2_000L, 55.0 + 1740 * 1e-4, 11.0, 10_000));
        send(rec(RUN, 219026147, T0 + 1739 * 2_000L, 55.0 + 1739 * 1e-4, 11.0, 10_001));
        assertEquals(1, duplicates());
        assertEquals(10_001L, forwardedOffsets().get(forwardedOffsets().size() - 1));
    }

    @Test
    void stateSurvivesSnapshotAndRestore() throws Exception {
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 1));
        OperatorSubtaskState snapshot = h.snapshot(1L, 1L);
        h.close();

        h = harness();
        h.setup();
        h.initializeState(snapshot);
        h.open();
        send(rec(RUN, 219026147, T0, 55.0, 11.0, 2));
        assertEquals(List.of(), forwardedOffsets());
        assertEquals(1, duplicates());
    }
}

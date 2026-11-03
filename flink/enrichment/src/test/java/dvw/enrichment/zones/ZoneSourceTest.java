package dvw.enrichment.zones;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.Test;

class ZoneSourceTest {
    static final AtomicInteger OPENED = new AtomicInteger();

    /** Rows from memory instead of PostGIS. */
    static ZoneRows.Opener rows(TestZones zones) {
        List<ZoneElement> pieces = zones.pieces();
        return () -> {
            OPENED.incrementAndGet();
            Iterator<ZoneElement> it = pieces.iterator();
            return new ZoneRows() {
                @Override
                public String version() {
                    return TestZones.VERSION;
                }

                @Override
                public ZoneElement next() {
                    return it.hasNext() ? it.next() : null;
                }

                @Override
                public void close() {}
            };
        };
    }

    static final class Collected implements ReaderOutput<ZoneElement> {
        final List<ZoneElement> records = new ArrayList<>();
        final List<Long> watermarks = new ArrayList<>();

        @Override
        public void collect(ZoneElement record) {
            records.add(record);
        }

        @Override
        public void collect(ZoneElement record, long timestamp) {
            records.add(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarks.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<ZoneElement> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }

    static Collected drain(ZoneSource.Reader reader) throws Exception {
        Collected out = new Collected();
        for (int i = 0; i < 1000 && reader.pollNext(out) == InputStatus.MORE_AVAILABLE; i++) {
            // one piece per call
        }
        return out;
    }

    @Test
    void sendsEveryPieceThenTheMarkerThenAFinalWatermark() throws Exception {
        TestZones zones = ZoneIndexTest.AREA;
        ZoneSource.Reader reader = new ZoneSource.Reader(rows(zones));
        assertFalse(reader.isAvailable().isDone()); // nothing to do before the split
        reader.addSplits(List.of(new ZoneSource.Split()));
        assertTrue(reader.isAvailable().isDone());

        Collected out = drain(reader);
        int n = zones.pieces().size();
        assertEquals(n + 1, out.records.size());
        ZoneElement marker = out.records.get(n);
        assertTrue(marker.isComplete());
        assertEquals(n, marker.seq);
        assertEquals(List.of(Long.MAX_VALUE), out.watermarks);

        // Then idle for good, but the split stays in every checkpoint.
        assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(out));
        assertFalse(reader.isAvailable().isDone());
        assertEquals(1, reader.snapshotState(7L).size());
    }

    @Test
    void restoredReaderReadsThePackageAgain() throws Exception {
        TestZones zones = ZoneIndexTest.AREA;
        ZoneSource.Reader first = new ZoneSource.Reader(rows(zones));
        first.addSplits(List.of(new ZoneSource.Split()));
        drain(first);
        List<ZoneSource.Split> state = first.snapshotState(1L);

        int before = OPENED.get();
        ZoneSource.Reader restored = new ZoneSource.Reader(rows(zones));
        restored.addSplits(state); // what Flink does with the reader's checkpointed splits
        Collected out = drain(restored);
        assertEquals(before + 1, OPENED.get());
        assertEquals(zones.pieces().size() + 1, out.records.size());
    }

    @Test
    void enumeratorAssignsTheSplitOnceAndAgainIfItComesBack() throws Exception {
        try (MockSplitEnumeratorContext<ZoneSource.Split> ctx = new MockSplitEnumeratorContext<>(1)) {
            ZoneSource.Enumerator e = new ZoneSource.Enumerator(ctx, false);
            ctx.registerReader(new ReaderInfo(0, "localhost"));
            e.addReader(0);
            e.addReader(0);
            assertEquals(1, ctx.getSplitsAssignmentSequence().size());
            assertTrue(e.snapshotState(1L));

            e.addSplitsBack(List.of(new ZoneSource.Split()), 0); // reader failed before a checkpoint
            assertEquals(2, ctx.getSplitsAssignmentSequence().size());

            ZoneSource.Enumerator restored = new ZoneSource.Enumerator(ctx, true);
            restored.addReader(0); // the restored reader has the split in its own state
            assertEquals(2, ctx.getSplitsAssignmentSequence().size());
        }
    }
}

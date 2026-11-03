package dvw.enrichment.zones;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits every piece of the active zone package, then a "zones complete" marker, then
 * nothing (docs/adr/0018). Runs with parallelism 1.
 *
 * <p>A Flink source has two halves: a split enumerator on the JobManager that hands out
 * units of work (splits), and readers in the task slots that turn a split into records.
 * Here there is one split, "the active package", and one reader.
 *
 * <p>The read happens once per job start and again after every restore. Nothing is ever
 * done with the split, so the reader keeps it in every checkpoint, and a restored reader
 * gets it back and reads the package again. This is deliberate. The zone-tagging operator
 * keeps its index outside Flink state (a checkpoint would otherwise copy 50 MB of zones
 * per task every 3 s), so after a restore it has no zones until this source sends them
 * again. It also means a restart reads whatever package is active at that moment, which
 * is how a new package goes live: stop with a savepoint, load, start from the savepoint.
 *
 * <p>The source is unbounded even though it has a fixed amount to say. When a bounded
 * source finishes, Flink records it as finished in checkpoints and does not run it again
 * on restore, which would leave the restored tagging tasks without zones. After the
 * marker the reader idles and emits a final watermark, so this input never holds back
 * event time downstream.
 */
public final class ZoneSource implements Source<ZoneElement, ZoneSource.Split, Boolean> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ZoneSource.class);

    private final ZoneRows.Opener opener;

    public ZoneSource(ZoneRows.Opener opener) {
        this.opener = opener;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<ZoneElement, Split> createReader(SourceReaderContext ctx) {
        return new Reader(opener);
    }

    @Override
    public SplitEnumerator<Split, Boolean> createEnumerator(SplitEnumeratorContext<Split> ctx) {
        return new Enumerator(ctx, false);
    }

    @Override
    public SplitEnumerator<Split, Boolean> restoreEnumerator(SplitEnumeratorContext<Split> ctx, Boolean assigned) {
        return new Enumerator(ctx, assigned);
    }

    @Override
    public SimpleVersionedSerializer<Split> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
        return new AssignedSerializer();
    }

    /** The only split: read the package that is active when the reader gets it. */
    public static final class Split implements SourceSplit {
        static final String ID = "active-package";

        @Override
        public String splitId() {
            return ID;
        }
    }

    /**
     * Gives the split to the reader once. Its checkpoint is whether it has: after that the
     * reader's own checkpoint carries the split. If the reader fails before a checkpoint
     * recorded it, Flink hands the split back here and it is assigned again.
     */
    static final class Enumerator implements SplitEnumerator<Split, Boolean> {
        private final SplitEnumeratorContext<Split> ctx;
        private boolean assigned;

        Enumerator(SplitEnumeratorContext<Split> ctx, boolean assigned) {
            this.ctx = ctx;
            this.assigned = assigned;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {}

        @Override
        public void addSplitsBack(List<Split> splits, int subtaskId) {
            assigned = false;
            assignIfReady();
        }

        @Override
        public void addReader(int subtaskId) {
            assignIfReady();
        }

        private void assignIfReady() {
            Map<Integer, ?> readers = ctx.registeredReaders();
            if (!assigned && !readers.isEmpty()) {
                int subtask = readers.keySet().iterator().next();
                ctx.assignSplit(new Split(), subtask);
                assigned = true;
            }
        }

        @Override
        public Boolean snapshotState(long checkpointId) {
            return assigned;
        }

        @Override
        public void close() {}
    }

    /**
     * Reads the package one piece per pollNext call, so checkpoints and cancellation are
     * not held up for the whole read.
     */
    static final class Reader implements SourceReader<ZoneElement, Split> {
        private static final CompletableFuture<Void> AVAILABLE = CompletableFuture.completedFuture(null);
        private final ZoneRows.Opener opener;
        private Split split;
        private ZoneRows rows;
        private boolean done;
        private int sent;
        /** Completed when a split arrives; after the marker, a future that never completes. */
        private CompletableFuture<Void> waiting = new CompletableFuture<>();
        private long startedAt;

        Reader(ZoneRows.Opener opener) {
            this.opener = opener;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<ZoneElement> output) throws Exception {
            if (split == null || done) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            if (rows == null) {
                startedAt = System.nanoTime();
                rows = opener.open();
                sent = 0;
                LOG.info("zone source: reading package {}", rows.version());
            }
            ZoneElement piece = rows.next();
            if (piece != null) {
                output.collect(piece);
                sent++;
                return InputStatus.MORE_AVAILABLE;
            }
            String version = rows.version();
            output.collect(ZoneElement.complete(version, sent));
            rows.close();
            rows = null;
            done = true;
            waiting = new CompletableFuture<>();
            // No event time on this input from here on: min(AIS, MAX) is the AIS watermark.
            output.emitWatermark(new Watermark(Long.MAX_VALUE));
            LOG.info("zone source: sent {} pieces of {} and the marker in {} ms",
                    sent, version, (System.nanoTime() - startedAt) / 1_000_000);
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<Split> snapshotState(long checkpointId) {
            return split == null ? List.of() : List.of(split);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return split != null && !done ? AVAILABLE : waiting;
        }

        @Override
        public void addSplits(List<Split> splits) {
            if (!splits.isEmpty()) {
                split = splits.get(0);
                done = false;
                waiting.complete(null);
            }
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() throws Exception {
            if (rows != null) {
                rows.close();
            }
        }
    }

    static final class SplitSerializer implements SimpleVersionedSerializer<Split> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Split split) {
            return new byte[0];
        }

        @Override
        public Split deserialize(int version, byte[] serialized) {
            return new Split();
        }
    }

    static final class AssignedSerializer implements SimpleVersionedSerializer<Boolean> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Boolean assigned) {
            return new byte[] {(byte) (assigned ? 1 : 0)};
        }

        @Override
        public Boolean deserialize(int version, byte[] serialized) {
            return serialized.length == 1 && serialized[0] == 1;
        }
    }
}

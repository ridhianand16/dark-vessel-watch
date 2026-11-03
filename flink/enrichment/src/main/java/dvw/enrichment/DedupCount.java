package dvw.enrichment;

/**
 * How many duplicates of one run and ais.raw partition were dropped in one 10 s event-time
 * window. Several records can exist for the same window (an early flush, then more
 * duplicates); readers sum them (docs/adr/0015).
 */
public class DedupCount {
    public String runId;
    public int srcPartition;
    public long windowStart;
    public long windowEnd;
    public long count;

    public DedupCount() {}

    /** A count of one for a duplicate that arrived after its window was discarded. */
    public static DedupCount single(Duplicate d, long eventTs, long windowMs) {
        DedupCount c = new DedupCount();
        c.runId = d.runId;
        c.srcPartition = d.srcPartition;
        c.windowStart = eventTs - Math.floorMod(eventTs, windowMs);
        c.windowEnd = c.windowStart + windowMs;
        c.count = 1;
        return c;
    }
}

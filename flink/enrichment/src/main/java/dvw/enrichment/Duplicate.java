package dvw.enrichment;

/** A record dropped by dedup: only what the count needs. Its event time travels as the
 * element timestamp, which the count window reads. */
public class Duplicate {
    public String runId;
    public int srcPartition;

    public Duplicate() {}

    public Duplicate(String runId, int srcPartition) {
        this.runId = runId;
        this.srcPartition = srcPartition;
    }

    /** The count's key: one count stream per run and ais.raw partition. */
    public String countKey() {
        return runId + "/" + srcPartition;
    }
}

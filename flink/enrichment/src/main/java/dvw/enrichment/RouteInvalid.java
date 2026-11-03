package dvw.enrichment;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Valid records continue on the main output; invalid ones leave through the DLQ side output.
 *
 * <p>A side output is a second, typed output stream of an operator, addressed by an
 * OutputTag. Unlike filtering into two streams, each record is looked at once, and both
 * outputs are part of the same checkpoint, so a record lands in exactly one of them.
 */
public final class RouteInvalid extends ProcessFunction<RawRecord, RawRecord> {
    public static final OutputTag<RawRecord> DLQ = new OutputTag<>("dlq") {};
    /** For dashboards only (see Dedup). */
    private transient Counter rejected;

    @Override
    public void open(OpenContext openContext) {
        rejected = getRuntimeContext().getMetricGroup().counter("dlq");
    }

    @Override
    public void processElement(RawRecord r, Context ctx, Collector<RawRecord> out) {
        if (r.isValid()) {
            out.collect(r);
        } else {
            ctx.output(DLQ, r);
            rejected.inc();
        }
    }
}

package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dvw.enrichment.zones.TestZones;
import dvw.enrichment.zones.ZoneElement;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * The whole topology between source and sinks on a local mini cluster: for every partition,
 * raw = enriched + dlq + sum(dedup_counts), no raw offset is forwarded twice, and every
 * enriched record is zone-tagged. The zones arrive whenever the mini cluster schedules
 * them, before, among or after the AIS records, so the tagging buffer is exercised too.
 */
class TopologyTest {
    @Test
    void everyRawRecordIsAccountedForExactlyOnce() throws Exception {
        Validator v = new Validator(ValidatorTest.DAY);
        List<RawRecord> raw = new ArrayList<>();
        Map<Integer, Integer> perPartition = new HashMap<>();
        long offset = 0;
        // 3 partitions, 40 vessels, 200 seconds; every third report heard twice; some broken rows.
        for (int s = 0; s < 200; s++) {
            for (int vessel = 0; vessel < 40; vessel++) {
                ObjectNode n = ValidatorTest.valid();
                n.put("mmsi", 219000000L + vessel);
                n.put("event_ts", ValidatorTest.NOON + s * 1000L);
                n.put("lat", 55.0 + vessel * 0.01 + s * 1e-4);
                int copies = (s + vessel) % 3 == 0 ? 2 : 1;
                for (int c = 0; c < copies; c++) {
                    byte[] value = (s % 50 == 0 && vessel == 7 && c == 0)
                            ? "{\"truncated".getBytes(StandardCharsets.UTF_8)
                            : n.toString().getBytes(StandardCharsets.UTF_8);
                    RawRecord r = v.validate(value);
                    r.srcPartition = vessel % 3;
                    r.srcOffset = offset++;
                    raw.add(r);
                    perPartition.merge(r.srcPartition, 1, Integer::sum);
                }
            }
        }

        Configuration conf = new Configuration();
        conf.set(PipelineOptions.GENERIC_TYPES, false); // as in the job: fails if a type needs Kryo
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        TestZones zones = new TestZones()
                .box(ZoneElement.EEZ, "eez:5674", 10, 54, 13, 58)
                .box(ZoneElement.MPA, "wdpa:1", 11.4, 55.0, 11.6, 55.2)
                .line("1", new double[][] {{13.0, 54.0}, {13.0, 58.0}});
        EnrichmentJob.Outputs out = EnrichmentJob.topology(
                env.fromData(raw, TypeInformation.of(RawRecord.class))
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<RawRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((r, ts) -> r.isValid() ? r.eventTs : Long.MIN_VALUE)),
                env.fromData(zones.stream(), TypeInformation.of(ZoneElement.class)).setParallelism(1));
        CloseableIterator<RawRecord> enriched = out.enriched.collectAsync();
        CloseableIterator<RawRecord> dlq = out.dlq.collectAsync();
        CloseableIterator<DedupCount> counts = out.dedupCounts.collectAsync();
        env.execute("topology-test");

        Map<Integer, Integer> accounted = new HashMap<>();
        Set<Long> seenOffsets = new HashSet<>();
        int enrichedTotal = 0;
        int dupTotal = 0;
        while (enriched.hasNext()) {
            RawRecord r = enriched.next();
            assertEquals(true, seenOffsets.add(r.srcOffset), "offset forwarded twice: " + r.srcOffset);
            assertEquals(TestZones.VERSION, r.zonesVersion, "untagged record at offset " + r.srcOffset);
            assertEquals("eez:5674", r.eezId);
            accounted.merge(r.srcPartition, 1, Integer::sum);
            enrichedTotal++;
        }
        while (dlq.hasNext()) {
            RawRecord r = dlq.next();
            assertEquals(true, seenOffsets.add(r.srcOffset), "offset in enriched and dlq: " + r.srcOffset);
            accounted.merge(r.srcPartition, 1, Integer::sum);
        }
        while (counts.hasNext()) {
            DedupCount c = counts.next();
            accounted.merge(c.srcPartition, (int) c.count, Integer::sum);
            dupTotal += (int) c.count;
        }

        // Vessel 7's first copy is broken at s = 0, 50, 100, 150; only s = 50 has a valid twin.
        int dlqTotal = 4;
        assertEquals(perPartition, accounted);
        assertEquals(200 * 40 - 3, enrichedTotal);
        assertEquals(raw.size() - enrichedTotal - dlqTotal, dupTotal);
    }
}

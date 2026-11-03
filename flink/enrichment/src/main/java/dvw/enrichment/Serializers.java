package dvw.enrichment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka records for the three outputs.
 *
 * <p>Every record leaves the partition null. The Kafka producer then picks it with its
 * default partitioner, murmur2 of the key modulo the partition count: the same function the
 * replayer uses (ADR 0001). So an MMSI lands in the same partition number in ais.enriched as
 * in ais.raw. The timestamp is also left null, so the producer stamps wall-clock time; the
 * reconciliation job seeks by it.
 */
public final class Serializers {
    static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String RAW_TOPIC = "ais.raw";
    public static final int SCHEMA_VERSION = 1;

    private Serializers() {}

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * ais.enriched: the ais.raw object unchanged, plus src_partition, src_offset,
     * station_class and the zone tags (docs/adr/0018): zones_version, eez_id (null outside
     * every EEZ), mpa_ids (array, empty outside protected areas), in_port, and
     * dist_eez_boundary_m and dist_mpa_m (geodesic metres to 0.1 m; dist_mpa_m is 0 inside
     * a protected area; both null for a point that cannot be projected). Keyed by MMSI, as
     * ais.raw.
     */
    public static final class Enriched implements KafkaRecordSerializationSchema<RawRecord> {
        private final String topic;

        public Enriched(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(RawRecord r, KafkaSinkContext ctx, Long timestamp) {
            return new ProducerRecord<>(topic, null, null, utf8(Long.toString(r.mmsi)), enrich(r));
        }

        static byte[] enrich(RawRecord r) {
            try {
                ObjectNode n = (ObjectNode) MAPPER.readTree(r.value);
                n.put("src_partition", r.srcPartition);
                n.put("src_offset", r.srcOffset);
                n.put("station_class", r.stationClass);
                n.put("zones_version", r.zonesVersion);
                n.put("eez_id", r.eezId);
                ArrayNode mpas = n.putArray("mpa_ids");
                if (r.mpaIds != null) {
                    for (String id : r.mpaIds) {
                        mpas.add(id);
                    }
                }
                n.put("in_port", r.inPort);
                n.put("dist_eez_boundary_m", decimetres(r.distEezBoundaryM));
                n.put("dist_mpa_m", decimetres(r.distMpaM));
                return MAPPER.writeValueAsBytes(n);
            } catch (IOException e) {
                // Unreachable: the validator parsed this value as a JSON object.
                throw new UncheckedIOException("re-parsing a validated ais.raw value failed", e);
            }
        }
    }

    /**
     * ais.dlq: reason code, detail, where it came from, and the original payload as text
     * (bytes that are not UTF-8 are replaced). Keyed by the original key, so a valid MMSI key
     * still decides the partition; a null key leaves it to the producer.
     */
    public static final class Dlq implements KafkaRecordSerializationSchema<RawRecord> {
        private final String topic;

        public Dlq(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(RawRecord r, KafkaSinkContext ctx, Long timestamp) {
            return new ProducerRecord<>(topic, null, null, r.key, toJson(r));
        }

        static byte[] toJson(RawRecord r) {
            ObjectNode n = MAPPER.createObjectNode();
            n.put("schema_version", SCHEMA_VERSION);
            n.put("reason", r.reason);
            n.put("detail", r.detail);
            n.put("run_id", r.runId);
            n.put("src_topic", RAW_TOPIC);
            n.put("src_partition", r.srcPartition);
            n.put("src_offset", r.srcOffset);
            n.put("payload", r.value == null ? null : new String(r.value, StandardCharsets.UTF_8));
            return write(n);
        }
    }

    /** ais.dedup_counts: one count record per window firing. Keyed by run_id. */
    public static final class Counts implements KafkaRecordSerializationSchema<DedupCount> {
        private final String topic;

        public Counts(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(DedupCount c, KafkaSinkContext ctx, Long timestamp) {
            return new ProducerRecord<>(topic, null, null, utf8(c.runId), toJson(c));
        }

        static byte[] toJson(DedupCount c) {
            ObjectNode n = MAPPER.createObjectNode();
            n.put("schema_version", SCHEMA_VERSION);
            n.put("run_id", c.runId);
            n.put("src_partition", c.srcPartition);
            n.put("window_start", c.windowStart);
            n.put("window_end", c.windowEnd);
            n.put("count", c.count);
            return write(n);
        }
    }

    private static Double decimetres(Double metres) {
        return metres == null ? null : Math.round(metres * 10) / 10.0;
    }

    private static byte[] write(JsonNode n) {
        try {
            return MAPPER.writeValueAsBytes(n);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

package dvw.enrichment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class SerializersTest {
    static JsonNode json(byte[] b) throws Exception {
        return Serializers.MAPPER.readTree(b);
    }

    @Test
    void enrichedKeepsEveryRawFieldAndAddsSourceAndClass() throws Exception {
        ObjectNode raw = ValidatorTest.valid();
        raw.put("name", "ÆRØ FÆRGEN"); // non-ASCII survives as-is
        RawRecord r = new Validator(ValidatorTest.DAY).validate(raw.toString().getBytes(StandardCharsets.UTF_8));
        r.srcPartition = 4;
        r.srcOffset = 562950;

        ProducerRecord<byte[], byte[]> pr = new Serializers.Enriched("ais.enriched").serialize(r, null, 123L);
        assertEquals("ais.enriched", pr.topic());
        assertNull(pr.partition(), "the producer's murmur2 partitioner picks the partition (ADR 0001)");
        assertNull(pr.timestamp(), "the producer stamps wall-clock time");
        assertArrayEquals("219026147".getBytes(StandardCharsets.UTF_8), pr.key());

        JsonNode out = json(pr.value());
        JsonNode in = json(r.value);
        for (Iterator<String> it = in.fieldNames(); it.hasNext(); ) {
            String f = it.next();
            assertEquals(in.get(f), out.get(f), f);
        }
        assertEquals(4, out.get("src_partition").asInt());
        assertEquals(562950L, out.get("src_offset").asLong());
        assertEquals("ship", out.get("station_class").asText());
        assertEquals(raw.size() + 3, out.size());
    }

    @Test
    void dlqCarriesReasonSourceAndPayload() throws Exception {
        RawRecord r = new Validator(ValidatorTest.DAY).validate("{\"broken\"".getBytes(StandardCharsets.UTF_8));
        r.srcPartition = 2;
        r.srcOffset = 77;
        r.key = "invalid".getBytes(StandardCharsets.UTF_8);

        ProducerRecord<byte[], byte[]> pr = new Serializers.Dlq("ais.dlq").serialize(r, null, null);
        assertArrayEquals(r.key, pr.key());
        assertNull(pr.partition());
        JsonNode out = json(pr.value());
        assertEquals("decode_error", out.get("reason").asText());
        assertEquals("ais.raw", out.get("src_topic").asText());
        assertEquals(2, out.get("src_partition").asInt());
        assertEquals(77, out.get("src_offset").asLong());
        assertEquals("{\"broken\"", out.get("payload").asText());
        assertEquals(true, out.get("run_id").isNull());
    }

    @Test
    void countRecord() throws Exception {
        DedupCount c = DedupCount.single(new Duplicate("run-a", 5), 1_761_609_612_345L, 10_000);
        ProducerRecord<byte[], byte[]> pr = new Serializers.Counts("ais.dedup_counts").serialize(c, null, null);
        assertArrayEquals("run-a".getBytes(StandardCharsets.UTF_8), pr.key());
        JsonNode out = json(pr.value());
        assertEquals(5, out.get("src_partition").asInt());
        assertEquals(1_761_609_610_000L, out.get("window_start").asLong());
        assertEquals(1_761_609_620_000L, out.get("window_end").asLong());
        assertEquals(1, out.get("count").asLong());
    }
}

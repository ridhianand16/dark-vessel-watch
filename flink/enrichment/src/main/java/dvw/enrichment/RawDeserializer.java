package dvw.enrichment;

import java.time.LocalDate;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Turns every ais.raw record into a RawRecord, valid or not, and never throws.
 *
 * <p>Validation happens here, inside the source, rather than in a later operator because the
 * watermark is computed inside the source too (per Kafka partition, see EnrichmentJob). Its
 * timestamp assigner needs to know which event times are trustworthy: an invalid record must
 * not move event time forward.
 */
public final class RawDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {
    private final LocalDate aisDay;
    private transient Validator validator;

    public RawDeserializer(LocalDate aisDay) {
        this.aisDay = aisDay;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) {
        if (validator == null) {
            validator = new Validator(aisDay);
        }
        RawRecord r = validator.validate(record.value());
        r.srcPartition = record.partition();
        r.srcOffset = record.offset();
        r.key = record.key();
        out.collect(r);
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() {
        return TypeInformation.of(RawRecord.class);
    }
}

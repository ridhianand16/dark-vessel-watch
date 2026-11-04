# 0001. Partition by murmur2(MMSI) on every producer

Status: accepted (2026-11-02)

## Context

Every topic is keyed by MMSI and has 6 partitions. Per-vessel state in
Faust and Flink relies on all messages for one MMSI landing in the same partition,
and later on the same MMSI landing in the *same partition number* across topics, so
a worker that owns partition 3 of `ais.enriched` also owns partition 3 of `ais.tracks`.

The default partitioner depends on the client library:

| Producer | Library | Default |
|---|---|---|
| replayer (Python) | confluent-kafka / librdkafka | `consistent_random` (CRC32) |
| Flink job (Java) | Kafka Java client | murmur2 |
| Faust (Python) | aiokafka | murmur2 (Java-compatible) |

With the defaults, MMSI 219024675 could go to partition 2 in `ais.raw` and to
partition 5 in `ais.enriched`. Nothing fails, but co-partitioning silently breaks.

## Decision

Every producer uses Java-compatible murmur2 on the UTF-8 key:
`partition = (murmur2(key) & 0x7fffffff) % num_partitions`.
In librdkafka that is `partitioner=murmur2_random`. The `_random` suffix only
affects null keys, and we never send one: unparseable MMSIs use the key `"invalid"`.

Verified on the first smoke run: 3,000 messages in `ais.raw` checked against a
reference murmur2 implementation, 0 mismatches.

## Alternatives

- **librdkafka default (CRC32).** Fine while the replayer is the only producer, but
  wrong as soon as Flink writes `ais.enriched`.
- **Explicit partition numbers chosen by the application.** Gives the most control,
  but every producer then has to reimplement the same function anyway.

## Consequences

- Any new producer (live AISStream source, synthetic anomaly injector) must set the same partitioner.
- Changing the partition count remaps keys. That needs a new ADR and a replay.
- All rows with an unparseable MMSI share one partition. That is acceptable at DMA's
  error rate (0 of the first 10,000 rows) and keeps them out of real vessels' partitions.

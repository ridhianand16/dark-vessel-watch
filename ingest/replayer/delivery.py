"""Track what the broker acknowledged, per partition.

This is the "produced" side of the M5 reconciliation: only messages whose delivery
callback reported success are counted. Callbacks run on the thread that calls
poll()/flush() (here, the main thread), so no locking is needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaError, Message


@dataclass
class PartitionStats:
    first_offset: int
    last_offset: int
    count: int


@dataclass
class DeliveryTracker:
    partitions: dict[int, PartitionStats] = field(default_factory=dict)
    delivered: int = 0
    errors: int = 0
    error_samples: list[str] = field(default_factory=list)

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.errors += 1
            if len(self.error_samples) < 10:
                self.error_samples.append(str(err))
            return
        p, off = msg.partition(), msg.offset()
        if p is None or off is None:  # not possible for a successful delivery
            self.errors += 1
            return
        self.record(p, off)

    def record(self, partition: int, offset: int) -> None:
        s = self.partitions.get(partition)
        if s is None:
            self.partitions[partition] = PartitionStats(offset, offset, 1)
        else:
            s.first_offset = min(s.first_offset, offset)
            s.last_offset = max(s.last_offset, offset)
            s.count += 1
        self.delivered += 1

    def summary(self) -> dict[str, Any]:
        return {
            "total": self.delivered,
            "delivery_errors": self.errors,
            "error_samples": self.error_samples,
            "partitions": {
                str(p): {"first_offset": s.first_offset, "last_offset": s.last_offset, "count": s.count}
                for p, s in sorted(self.partitions.items())
            },
        }

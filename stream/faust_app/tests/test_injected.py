"""The shipped anomalies.yaml through the real tracker logic, without Kafka.

Each labelled event must raise exactly one alert, with the alert_id the scorer expects
(reconcile/score_alerts.py), and the decoys none. Real traffic is left out: the
synthetic vessels report until the end of the run, so they move the watermark
themselves.

The records stand in for ais.enriched. Each vessel gets fixed zone tags: the ones the
Flink job gives it where it goes dark, measured in PostGIS for anomalies.yaml. The
end-to-end run (make score) checks the same with the real tags.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ingest.replayer import inject
from ingest.replayer.record import build_message
from ingest.replayer.source import read_layout
from ingest.replayer.tests.test_record import ROW, write_csv
from stream.faust_app.detect import AlertType, alert_id
from stream.faust_app.tests.test_detect import Pipeline
from stream.faust_app.tracker import RawRecord

SHIPPED = Path(__file__).resolve().parents[3] / "ingest" / "replayer" / "anomalies.yaml"
T0 = 1_761_609_601_000
RUN = "20261103T120000Z-abc123"
NM = 1852.0
FAR = {"in_port": False, "mpa_ids": [], "dist_eez_boundary_m": 38 * NM, "dist_mpa_m": 30 * NM}
TAGS: dict[int, dict[str, Any]] = {
    200000001: {**FAR, "dist_mpa_m": 4.0 * NM},  # DARK 1, near Lysegrund
    200000002: {**FAR, "dist_eez_boundary_m": 6.1 * NM},  # DARK 2, near a boundary
    200000003: {**FAR, "dist_mpa_m": 3.4 * NM},  # DARK 3, near Adler Grund
    200000021: {**FAR, "in_port": True, "dist_mpa_m": 0.0},  # MOORED, Copenhagen
    200000023: {**FAR, "in_port": True, "dist_mpa_m": 0.0},  # DARK PORT, Copenhagen
}


def enriched(value: bytes, partition: int, offset: int) -> bytes:
    msg = json.loads(value)
    tags = TAGS.get(msg["mmsi"], FAR)  # DARK FAR, and the jumps (zones play no part)
    msg.update(station_class="ship", zones_version="test", eez_id="eez:5674", src_partition=partition,
               src_offset=offset, **tags)
    return json.dumps(msg).encode()


def test_each_injected_event_raises_exactly_its_alert(tmp_path: Path) -> None:
    lay = read_layout(write_csv(tmp_path, ROW))
    spec = inject.load(SHIPPED)
    p = inject.plan(spec, T0, T0 + 58 * 60_000, len(lay.columns), lay.field_index, lay.timestamp_format)

    pipe = Pipeline()
    alerts: list[dict[str, object]] = []
    batch: list[RawRecord] = []
    for offset, (ts, line) in enumerate(p.rows):
        key, value = build_message(line, ts, lay.field_index, RUN, now_ms=lambda: 0)
        partition = int(key) % 6
        batch.append(RawRecord(key, enriched(value, partition, offset), partition, offset))
        if len(batch) == 50:
            alerts += pipe.feed(*batch)
            batch = []
    alerts += pipe.feed(*batch)

    expected = {
        alert_id(AlertType(lb["expect"]), lb["mmsi"], RUN, lb["trigger_ts"])
        for lb in p.labels
        if lb["expect"] is not None
    }
    got = [str(a["alert_id"]) for a in alerts]
    assert len(expected) == 6
    assert sorted(got) == sorted(expected), json.dumps(alerts, indent=1)

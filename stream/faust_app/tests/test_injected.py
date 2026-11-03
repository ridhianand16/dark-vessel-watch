"""The shipped anomalies.yaml through the real tracker logic, without Kafka.

Each labelled event must raise exactly one alert, with the alert_id the scorer expects
(reconcile/score_alerts.py), and the decoys none. Real traffic is left out: the
synthetic vessels report until the end of the run, so they move the watermark
themselves.
"""

from __future__ import annotations

import json
from pathlib import Path

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


def test_each_injected_event_raises_exactly_its_alert(tmp_path: Path) -> None:
    lay = read_layout(write_csv(tmp_path, ROW))
    spec = inject.load(SHIPPED)
    p = inject.plan(spec, T0, T0 + 58 * 60_000, len(lay.columns), lay.field_index, lay.timestamp_format)

    pipe = Pipeline()
    alerts: list[dict[str, object]] = []
    batch: list[RawRecord] = []
    for offset, (ts, line) in enumerate(p.rows):
        key, value = build_message(line, ts, lay.field_index, RUN, now_ms=lambda: 0)
        batch.append(RawRecord(key, value, int(key) % 6, offset))
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

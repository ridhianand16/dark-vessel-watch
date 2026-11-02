from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ingest.replayer.delivery import DeliveryTracker
from ingest.replayer.record import build_message
from ingest.replayer.source import SourceError, read_layout, sorted_lines

DMA_HEADER = (
    "# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,ROT,SOG,COG,"
    "Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,Type of position fixing device,"
    "Draught,Destination,ETA,Data source type,A,B,C,D"
)
ROW = (
    "28/10/2026 00:00:01,Class A,219026147,56.610395,10.305530,Under way using engine,0.0,0.0,"
    "315.6,314,Unknown,OXYZ2,NORDIC STAR,Cargo,,12,89,GPS,,Unknown,,AIS,,,,"
)


def write_csv(tmp_path: Path, *rows: str, header: str = DMA_HEADER) -> Path:
    p = tmp_path / "ais.csv"
    p.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return p


def test_layout_reads_real_dma_header(tmp_path: Path) -> None:
    layout = read_layout(write_csv(tmp_path, ROW))
    assert layout.columns[0] == "# Timestamp"
    assert layout.field_index["event_ts"] == 0
    assert layout.field_index["mmsi"] == 2
    assert layout.field_index["length"] == 16
    assert layout.timestamp_format == "%d/%m/%Y %H:%M:%S"


def test_layout_rejects_missing_mmsi_column(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="mmsi"):
        read_layout(write_csv(tmp_path, "28/10/2026 00:00:01,1", header="# Timestamp,Other"))


def test_sorted_lines_orders_by_event_time_and_keeps_bad_rows(tmp_path: Path) -> None:
    rows = [
        "28/10/2026 00:00:05,Class A,3",
        "garbage line",
        "28/10/2026 00:00:01,Class A,1",
        "28/10/2026 00:00:05,Class A,4",  # same second as row 0: original order kept
    ]
    path = write_csv(tmp_path, *rows)
    out = [
        (line, ts)
        for lines, tss in sorted_lines(path, read_layout(path), None)
        for line, ts in zip(lines, tss, strict=True)
    ]
    assert [line for line, _ in out] == [rows[2], rows[0], rows[3], rows[1]]
    assert out[0][1] == 1_793_145_601_000  # 2026-10-28T00:00:01Z
    assert out[-1][1] is None


def test_message_fields_and_key(tmp_path: Path) -> None:
    layout = read_layout(write_csv(tmp_path, ROW))
    key, value = build_message(
        ROW, 1_793_145_601_000, layout.field_index, "20261102T132701Z-6cdc0c", now_ms=lambda: 42
    )
    msg = json.loads(value)
    assert key == b"219026147"
    assert msg["schema_version"] == 1
    assert msg["event_ts"] == 1_793_145_601_000
    assert msg["mmsi"] == 219026147
    assert (msg["lat"], msg["lon"], msg["sog"], msg["cog"]) == (56.610395, 10.30553, 0.0, 315.6)
    assert (msg["heading"], msg["imo"], msg["width"], msg["length"]) == (314, None, 12, 89)
    assert msg["callsign"] == "OXYZ2" and msg["name"] == "NORDIC STAR"
    assert msg["raw"] == ROW
    assert msg["replay_send_ts"] == 42
    assert len(msg["trace_id"]) == 36
    assert msg["run_id"] == "20261102T132701Z-6cdc0c"


def test_unparseable_values_become_null_and_raw_is_kept(tmp_path: Path) -> None:
    layout = read_layout(write_csv(tmp_path, ROW))
    line = '28/10/2026 00:00:01,Class A,21902X147,nan,abc,"unterminated'
    key, value = build_message(line, None, layout.field_index, "run-a")
    msg = json.loads(value)
    assert msg["run_id"] == "run-a"
    assert key == b"invalid"
    assert msg["mmsi"] is None and msg["lat"] is None and msg["event_ts"] is None
    assert msg["raw"] == line


def test_delivery_tracker_min_max_count() -> None:
    t = DeliveryTracker()
    for p, off in [(0, 7), (0, 5), (0, 6), (3, 100)]:
        t.record(p, off)
    s = t.summary()
    assert s["total"] == 4
    assert s["partitions"] == {
        "0": {"first_offset": 5, "last_offset": 7, "count": 3},
        "3": {"first_offset": 100, "last_offset": 100, "count": 1},
    }


def test_cli_stamps_messages_with_the_runs_dir_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run_id in every message must equal the data/runs/<run_id>/ directory name."""
    import ingest.replayer.cli as cli

    path = write_csv(tmp_path, ROW, ROW.replace("219026147", "211000001"))
    sent: list[bytes] = []

    class FakeProducer:
        def __init__(self) -> None:
            self.pending: list[tuple[int, Any]] = []

        def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None:
            sent.append(value)
            self.pending.append((len(sent) - 1, on_delivery))

        def poll(self, timeout: float = 0) -> int:
            return 0

        def flush(self, timeout: float = 0) -> int:
            for offset, cb in self.pending:
                cb(None, FakeMessage(offset))
            self.pending.clear()
            return 0

    class FakeMessage:
        def __init__(self, offset: int) -> None:
            self._offset = offset

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return self._offset

    runs = tmp_path / "runs"
    monkeypatch.setattr(cli, "make_producer", lambda brokers, run_id: FakeProducer())
    rc = cli.main(["--file", str(path), "--speed", "100", "--runs-dir", str(runs)])
    assert rc == 0
    (run_dir,) = runs.iterdir()
    produced = json.loads((run_dir / "produced.json").read_text())
    assert produced["run_id"] == run_dir.name
    assert produced["partitions"] == {"0": {"first_offset": 0, "last_offset": 1, "count": 2}}
    assert [json.loads(v)["run_id"] for v in sent] == [run_dir.name, run_dir.name]
